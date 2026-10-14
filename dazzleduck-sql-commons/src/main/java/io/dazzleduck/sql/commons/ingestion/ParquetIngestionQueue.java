package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.SqlVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ParquetIngestionQueue extends BulkIngestQueue<String, IngestionResult> {

    private static final Logger logger = LoggerFactory.getLogger(ParquetIngestionQueue.class);

    /**
     * Virtual thread executor for async file cleanup.
     * Virtual threads are ideal for I/O-bound tasks like file deletion.
     * This is a shared executor - virtual threads are lightweight so no pooling needed.
     */
    private static final ExecutorService CLEANUP_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
    /** Trailing semicolons/whitespace, stripped before a transformation is nested as a subquery. */
    private static final Pattern TRAILING_SEMICOLONS = Pattern.compile("[;\\s]+$");

    /** Transformations re-rendered by {@link #embeddable}, by their original text. */
    private static final java.util.Map<String, String> EMBEDDABLE = new java.util.concurrent.ConcurrentHashMap<>();
    private static final int EMBEDDABLE_CACHE_LIMIT = 256;

    private final String outputPath;
    private final String queueId;
    private final IngestionHandler postIngestionHandler;
    private final String applicationId;
    private final String inputFormat;
    private final String parquetCompression;

    /**
     * Per-phase commit timings. The write is two phases with different parallelism potential:
     * the data phase (DuckDB COPY to Parquet — internally multi-threaded, and overlappable
     * across commit lanes) and the post-ingestion phase (e.g. the DuckLake catalog commit —
     * single-writer, serialized no matter how many lanes exist). Their ratio bounds what
     * parallelizing the commit path can gain (Amdahl: max speedup = (data + post) / post),
     * so both are tracked separately and exposed as metrics.
     */
    private final java.util.concurrent.atomic.LongAccumulator dataPhaseNanos =
            new java.util.concurrent.atomic.LongAccumulator(Long::sum, 0L);
    private final java.util.concurrent.atomic.LongAccumulator postIngestPhaseNanos =
            new java.util.concurrent.atomic.LongAccumulator(Long::sum, 0L);

    /** Cumulative rows written to output (from each {@link IngestionResult#rowCount()}). */
    private final java.util.concurrent.atomic.LongAccumulator rowsWritten =
            new java.util.concurrent.atomic.LongAccumulator(Long::sum, 0L);

    /**
     * @param applicationId    producer identifier
     * @param inputFormat      source file format (e.g. {@code "parquet"}, {@code "arrow"})
     * @param outputPath       destination path for written Parquet files
     * @param ingestionQueue   queue identifier — passed to {@link IngestionHandler#getTransformation}
     *                         on every write so the transformation is always current
     * @param minBucketSize    flush when accumulated size reaches this threshold (bytes)
     * @param maxBucketSize    hard upper limit before forced flush (bytes)
     * @param maxBatches       max number of batches before forced flush
     * @param maxPendingWrite  backpressure limit (bytes)
     * @param maxDelay         time-based flush interval
     * @param postIngestionHandler handler that provides transformation SQL and post-write tasks
     * @param executorService  scheduler for time-based flush
     * @param clock            clock for scheduling
     */
    public ParquetIngestionQueue(String applicationId,
                                 String inputFormat,
                                 String outputPath,
                                 String ingestionQueue,
                                 long minBucketSize,
                                 long maxBucketSize,
                                 int maxBatches,
                                 long maxPendingWrite,
                                 Duration maxDelay,
                                 IngestionHandler postIngestionHandler,
                                 ScheduledExecutorService executorService,
                                 Clock clock) {
        this(applicationId, inputFormat, outputPath, ingestionQueue, minBucketSize, maxBucketSize,
                maxBatches, maxPendingWrite, maxDelay, null, postIngestionHandler, executorService, clock);
    }

    /**
     * @param parquetCompression codec for written Parquet files, or {@code null} for DuckDB's
     *                           default; see {@link IngestionConfig#parquetCompression()}
     */
    public ParquetIngestionQueue(String applicationId,
                                 String inputFormat,
                                 String outputPath,
                                 String ingestionQueue,
                                 long minBucketSize,
                                 long maxBucketSize,
                                 int maxBatches,
                                 long maxPendingWrite,
                                 Duration maxDelay,
                                 String parquetCompression,
                                 IngestionHandler postIngestionHandler,
                                 ScheduledExecutorService executorService,
                                 Clock clock) {
        super(ingestionQueue, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, executorService, clock);
        this.outputPath = outputPath;
        this.queueId = ingestionQueue;
        this.postIngestionHandler = postIngestionHandler;
        this.applicationId = applicationId;
        this.inputFormat = inputFormat;
        this.parquetCompression = parquetCompression;
        // The output path (local or object store) is expected to already exist — provisioning it is
        // the operator's responsibility, outside the scope of this project. We never create it here.
    }

    @Override
    public void write(WriteTask<String, IngestionResult> writeTask) {
        logger.debug("Ingestion queue '{}' received batch with {} files, outputPath={}",
                queueId, writeTask.bucket().batches().size(), outputPath);
        try {
            long start = System.nanoTime();
            IngestionResult ingestionResult = tryWrite(writeTask);
            long copyDone = System.nanoTime();
            var postIngestionTask = postIngestionHandler.createPostIngestionTask(ingestionResult);
            postIngestionTask.execute();
            long postIngestDone = System.nanoTime();
            dataPhaseNanos.accumulate(copyDone - start);
            postIngestPhaseNanos.accumulate(postIngestDone - copyDone);
            rowsWritten.accumulate(ingestionResult.rowCount());
            recordRowsWritten(ingestionResult.rowCount());
            logger.debug("Queue '{}' commit phases: data(COPY)={}ms, postIngest(catalog)={}ms",
                    queueId, (copyDone - start) / 1_000_000, (postIngestDone - copyDone) / 1_000_000);
            writeTask.bucket().futures().forEach(action -> action.complete(ingestionResult));
        } catch (Exception e) {
            var sql = constructWriteQuery(writeTask);
            logger.atError().setCause(e).log("Failed to write to queue {} sql {}", queueId, sql);
            // Propagate instead of completing the futures here: BulkIngestQueue.processWriteQueue
            // must account this bucket as failed (pendingWrite stays truthful, failed-write
            // metrics accumulate) and roll back producer sequences BEFORE the futures complete,
            // so a client observing the failure can immediately retry the same batch. Swallowing
            // the exception would make the failed bytes count as written and leave retries
            // rejected as OutOfSequenceBatch.
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(e);
        } finally {
            cleanupInputFiles(writeTask);
        }
    }

    /** Cumulative nanoseconds spent in the data phase (DuckDB COPY to Parquet). */
    public long getDataPhaseNanos() {
        return dataPhaseNanos.get();
    }

    /** Cumulative nanoseconds spent in the post-ingestion phase (e.g. DuckLake catalog commit). */
    public long getPostIngestPhaseNanos() {
        return postIngestPhaseNanos.get();
    }

    /** Cumulative rows written to output. */
    public long getRowsWritten() {
        return rowsWritten.get();
    }

    @Override
    protected long rowsWritten() {
        return rowsWritten.get();
    }

    @Override
    protected long dataPhaseMillis() {
        return getDataPhaseNanos() / 1_000_000;
    }

    @Override
    protected long postIngestMillis() {
        return getPostIngestPhaseNanos() / 1_000_000;
    }

    /**
     * Asynchronously cleans up input files using virtual threads.
     * This is fire-and-forget - we don't wait for deletion to complete
     * since it doesn't affect the write result.
     */
    private void cleanupInputFiles(WriteTask<String, IngestionResult> writeTask) {
        writeTask.bucket().batches().forEach(this::onBatchAbandoned);
    }

    @Override
    protected void onBatchAbandoned(Batch<String> batch) {
        final String filePath = batch.record();
        CLEANUP_EXECUTOR.execute(() -> {
            try {
                Files.deleteIfExists(Path.of(filePath));
            } catch (Exception e) {
                logger.warn("Failed to delete temporary input file: {}", filePath, e);
            }
        });
    }

    private String getClause(String[] values, String clause){
        if(values == null || values.length == 0){
            return "";
        } else {
            var nested = Arrays.stream(values).filter(Objects::nonNull).map(String::trim).collect(Collectors.joining(","));
            return clause.formatted(nested);
        }
    }

    /**
     * Exposes {@code inputSql} as {@code __this} and applies {@code transformation} over it.
     * The transformation is nested as a subquery rather than appended after the {@code __this}
     * CTE, so a transformation that declares its own {@code WITH} clause stays valid SQL; its
     * CTEs can still reference the outer {@code __this}. Returns {@code inputSql} unchanged when
     * there is no transformation.
     *
     * <p>The closing parenthesis goes on its own line so a trailing {@code --} comment cannot
     * swallow it; see {@link #embeddable} for semicolons and comments.
     */
    static String applyTransformation(String inputSql, String transformation) {
        if (transformation == null || transformation.isBlank()) {
            return inputSql;
        }
        return "WITH __this AS (%s) SELECT * FROM (%s\n)".formatted(inputSql, embeddable(transformation));
    }

    /**
     * {@code transformation} as text that can be nested inside parentheses. Most transformations
     * are used verbatim. A {@code ;} is valid at the end of a statement but not inside a subquery,
     * and with a comment after it ({@code ...; -- note}) it cannot be trimmed off the end; neither
     * can be removed reliably as text, since either may sit inside a string literal. So a
     * transformation containing {@code ;}, {@code --} or {@code /*} is re-rendered by DuckDB's own
     * parser ({@code json_serialize_sql} / {@code json_deserialize_sql}, as view transformations
     * already are), which drops comments and statement terminators. Memoized, since it runs for
     * every batch. If DuckDB cannot parse it as one statement, trailing semicolons are trimmed and
     * the COPY reports DuckDB's own error.
     */
    static String embeddable(String transformation) {
        if (transformation.indexOf(';') < 0 && !transformation.contains("--") && !transformation.contains("/*")) {
            return transformation;
        }
        String cached = EMBEDDABLE.get(transformation);
        if (cached != null) {
            return cached;
        }
        String result;
        try (var conn = ConnectionPool.getConnection()) {
            var tree = Transformations.parseToTree(conn, transformation);
            boolean single = !tree.path("error").asBoolean(false) && tree.path("statements").size() == 1;
            result = single ? Transformations.parseToSql(conn, tree)
                    : TRAILING_SEMICOLONS.matcher(transformation).replaceAll("");
        } catch (Exception e) {
            result = TRAILING_SEMICOLONS.matcher(transformation).replaceAll("");
        }
        if (EMBEDDABLE.size() >= EMBEDDABLE_CACHE_LIMIT) {
            EMBEDDABLE.clear();
        }
        EMBEDDABLE.put(transformation, result);
        return result;
    }

    /**
     * The relation the output files are written from: the temp input files with the
     * transformation and any partition projections applied. This is also the relation the
     * watermark rows are computed over — same schema, same rows as the written output, read
     * while the input files are still local.
     */
    private String constructSourceRelation(WriteTask<String, IngestionResult> writeTask) {
        var batches = writeTask.bucket().batches();
        // All Arrow files
        var arrowFiles = batches.stream().map(Batch::record).map("'%s'"::formatted).collect(Collectors.joining(","));
        String[] batchPartitionBy = batches.get(0).partitionBy();
        boolean hasBatchPartitionBy = batchPartitionBy != null && batchPartitionBy.length > 0;
        String sortOrderClause = getClause(batches.get(0).sortOrder(), "ORDER BY %s ");

        // Inner SQL reads from the temp Arrow files
        var innerSql = "SELECT * FROM read_%s([%s]) %s".formatted(this.inputFormat, arrowFiles, sortOrderClause);

        // Fetch transformation fresh from the handler on every write so view-based
        // and handler-refreshed transformations are always current without caching.
        String transformation = postIngestionHandler.getTransformation(queueId);
        var querySql = applyTransformation(innerSql, transformation);

        // Add handler-supplied derived-column projections (e.g. day(timestamp) AS day) so the
        // PARTITION_BY tokens resolve. Not needed for header-supplied partitionBy, which names
        // columns that already exist in the relation.
        if (!hasBatchPartitionBy) {
            String[] partitionProjections = postIngestionHandler.getPartitionProjections(queueId);
            if (partitionProjections.length > 0) {
                querySql = "SELECT *, %s FROM (%s)".formatted(
                        String.join(", ", partitionProjections), querySql);
            }
        }
        return querySql;
    }

    private String constructWriteQuery(WriteTask<String, IngestionResult> writeTask) {
        var batches = writeTask.bucket().batches();
        String[] batchPartitionBy = batches.get(0).partitionBy();
        boolean hasBatchPartitionBy = batchPartitionBy != null && batchPartitionBy.length > 0;
        String[] effectivePartitionBy = hasBatchPartitionBy
                ? batchPartitionBy
                : postIngestionHandler.getPartitionBy(queueId);
        String partitionByClause = getClause(effectivePartitionBy, ", PARTITION_BY(%s)");
        // Last format
        var outputFormat = batches.isEmpty() ? "" : batches.get(batches.size() - 1).format();
        String fullFilePath;
        if (partitionByClause.isEmpty()) {
            String uniqueFileName = "dd_" + UUID.randomUUID() + "." + outputFormat;
            fullFilePath = this.outputPath + "/" + uniqueFileName;
        } else {
            fullFilePath = this.outputPath;
        }

        var querySql = constructSourceRelation(writeTask);

        String compressionClause = parquetCompression != null && "parquet".equalsIgnoreCase(outputFormat)
                ? ", COMPRESSION %s".formatted(parquetCompression) : "";

        // Build SQL
        // https://duckdb.org/docs/stable/sql/statements/copy
        var sql = """
                COPY
                    (%s)
                    TO '%s'
                    (FORMAT %s%s %s, RETURN_FILES, APPEND);
                """.formatted(querySql, fullFilePath, outputFormat, compressionClause, partitionByClause);
        return sql;
    }

    /**
     * Sets the queue's configured session variables on this write's connection, so a transformation
     * can read a deployment- or operator-supplied value with {@code getvariable('name')} instead of
     * carrying it as a literal. Read from the handler on every write, like the transformation
     * itself, so a reloaded value takes effect on the next batch.
     */
    protected void applyVariables(java.sql.Statement stmt) throws java.sql.SQLException {
        var variables = postIngestionHandler.getVariables(queueId);
        if (variables.isEmpty()) {
            return;
        }
        for (String setSql : SqlVariables.toSetStatements(variables, IngestionVariables.NOUN)) {
            stmt.execute(setSql);
        }
        logger.debug("Queue '{}': applied {} session variable(s): {}",
                queueId, variables.size(), variables.keySet());
    }

    private IngestionResult tryWrite(WriteTask<String, IngestionResult> writeTask) throws Exception {
        var sql = constructWriteQuery(writeTask);
        logger.debug("Executing COPY SQL: {}", sql);
        List<String> files = new ArrayList<>();
        long count = 0;
        // Watermark rows are computed BEFORE the COPY, over the same source relation the output
        // is written from: the data is still local, the transformation is already applied (so
        // partition columns are real typed columns, not hive path fragments), and a misconfigured
        // spec fails fast without leaving an unregistered output file behind.
        WatermarkSpec watermarkSpec = postIngestionHandler.getWatermarkSpec(queueId);
        List<List<String>> watermarkRows = null;
        try (var conn = ConnectionPool.getConnection();
             var stmt = conn.createStatement()) {

            // The queue's session variables, before anything reads the relation: the transformation
            // inside it (and so the watermark rows computed over it, and the COPY) may reference
            // them with getvariable(). ConnectionPool hands out a duplicate per use, so these are
            // scoped to this write and cannot leak into another queue's connection.
            applyVariables(stmt);

            if (watermarkSpec != null) {
                watermarkRows = watermarkSpec.computeRows(conn, constructSourceRelation(writeTask));
            }

            // Set up cancellation hook
            var cancelHookSet = writeTask.setCancelHook(() -> {
                try {
                    stmt.cancel();
                } catch (Exception e) {
                    // Ignore cancellation errors
                }
            });

            // If cancel was already called, don't execute the query
            if (!cancelHookSet) {
                throw new IllegalStateException("Write task was cancelled");
            }

            // Execute the query using our statement so the cancel hook works
            stmt.execute(sql);
            try (var rs = stmt.getResultSet()) {
                while (rs.next()) {
                    var rowCount = rs.getLong("count");
                    var rowFilesArray = rs.getArray("files");
                    count += rowCount;
                    if (rowFilesArray != null) {
                        var rowFiles = (Object[]) rowFilesArray.getArray();
                        files.addAll(Arrays.stream(rowFiles).map(Object::toString).toList());
                    }
                }
            }
        }
        logger.debug("COPY completed for queue '{}': {} rows written, {} files: {}",
                queueId, count, files.size(), files);
        return new IngestionResult(this.queueId, writeTask.taskId(), this.applicationId,
                writeTask.bucket().getProducerMaxBatchId(),
                count,
                files, sql, watermarkRows);
    }
}
