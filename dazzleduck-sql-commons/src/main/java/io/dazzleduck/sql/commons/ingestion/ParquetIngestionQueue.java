package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
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
import java.util.stream.Collectors;

public class ParquetIngestionQueue extends BulkIngestQueue<String, IngestionResult> {

    private static final Logger logger = LoggerFactory.getLogger(ParquetIngestionQueue.class);

    /**
     * Virtual thread executor for async file cleanup.
     * Virtual threads are ideal for I/O-bound tasks like file deletion.
     * This is a shared executor - virtual threads are lightweight so no pooling needed.
     */
    private static final ExecutorService CLEANUP_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

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

    /** Catalog-change retries and their failures; output files of failed attempts deleted / left behind. */
    private final java.util.concurrent.atomic.LongAdder catalogRetries = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder catalogRetryFailures = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder orphanFilesDeleted = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder orphanFilesLeft = new java.util.concurrent.atomic.LongAdder();

    /**
     * What a write is built from that can change under a running queue: the transformation and the
     * partitioning (from the batch header, or the handler's partition columns and projections).
     * Captured once per attempt, so the attempt is built from one consistent view and a retry can
     * tell whether the catalog changed since.
     */
    private record WriteInputs(String transformation, List<String> partitionBy, List<String> partitionProjections) {}

    /** A write whose files were written but whose post-ingestion step (registration) failed. */
    private static final class RegistrationFailedException extends RuntimeException {
        private final List<String> files;

        RegistrationFailedException(List<String> files, RuntimeException cause) {
            super(cause.getMessage(), cause);
            this.files = files;
        }
    }

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
        // The COPY of the most recent attempt, for the failure log: rebuilding it there would read the
        // handler's state again, which after a catalog refresh is not what the failed attempt ran.
        var lastAttemptSql = new java.util.concurrent.atomic.AtomicReference<String>();
        try {
            IngestionResult ingestionResult = writeWithCatalogRetry(writeTask, lastAttemptSql);
            writeTask.bucket().futures().forEach(action -> action.complete(ingestionResult));
        } catch (Exception e) {
            logger.atError().setCause(e).log("Failed to write to queue {} sql {}", queueId,
                    lastAttemptSql.get() == null ? "(not built)" : lastAttemptSql.get());
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

    /**
     * Writes and registers the batch, retrying once if the failure was caused by a catalog change.
     *
     * <p>The handler caches catalog-derived state (partitioning, transformation) and normally
     * re-reads it only every refresh interval. A batch written under a stale partition spec is
     * rejected at registration, and without this the client's resend would be written with the same
     * stale spec and rejected again until the cache expired. So after any failure the state is
     * re-read immediately ({@link IngestionHandler#refreshNow}); if the inputs this batch was built
     * from changed, the batch is written again from its input files (still present: they are only
     * deleted after this method returns) and registered. The client sees no failure. If nothing
     * changed, or the retry fails too, the original failure propagates as before.
     *
     * <p>Output files of a failed attempt are deleted when the post-ingestion step reports that it
     * rejected them before committing ({@link FilesNotRegisteredException}); otherwise they are
     * counted as orphans, for {@code ducklake_delete_orphaned_files} to reclaim.
     */
    private IngestionResult writeWithCatalogRetry(WriteTask<String, IngestionResult> writeTask,
                                                  java.util.concurrent.atomic.AtomicReference<String> lastAttemptSql)
            throws Exception {
        WriteInputs used = currentWriteInputs(writeTask);
        try {
            return writeAndRegister(writeTask, used, lastAttemptSql);
        } catch (Exception first) {
            discardOutput(first);
            if (writeTask.isCancel()) {
                throw unwrap(first);
            }
            WriteInputs current;
            try {
                postIngestionHandler.refreshNow(queueId);
                current = currentWriteInputs(writeTask);
            } catch (Exception refreshFailure) {
                Exception original = unwrap(first);
                original.addSuppressed(refreshFailure);
                throw original;
            }
            if (current.equals(used)) {
                throw unwrap(first);
            }
            if (!nothingRegistered(first)) {
                // The failed attempt may be in the table (e.g. its commit failed after landing), so
                // writing the batch again could register every row twice. Fail it as before; the
                // refresh above still applies to the next batch.
                logger.atWarn().setCause(unwrap(first)).log(
                        "Queue '{}': write failed and the catalog has changed, but the failed attempt's "
                                + "registration outcome is unknown; not retrying, to avoid registering the batch twice",
                        queueId);
                throw unwrap(first);
            }
            catalogRetries.increment();
            logger.atInfo().setCause(unwrap(first)).log(
                    "Queue '{}': write failed and the catalog has changed since it was built "
                            + "(partitioning {} -> {}, transformation changed: {}); retrying once",
                    queueId, used.partitionBy(), current.partitionBy(),
                    !Objects.equals(used.transformation(), current.transformation()));
            try {
                return writeAndRegister(writeTask, current, lastAttemptSql);
            } catch (Exception second) {
                discardOutput(second);
                catalogRetryFailures.increment();
                Exception failure = unwrap(second);
                failure.addSuppressed(unwrap(first));
                throw failure;
            }
        }
    }

    /** One attempt: COPY the batch to output files, then run the post-ingestion step (registration). */
    private IngestionResult writeAndRegister(WriteTask<String, IngestionResult> writeTask, WriteInputs inputs,
                                             java.util.concurrent.atomic.AtomicReference<String> lastAttemptSql)
            throws Exception {
        long start = System.nanoTime();
        IngestionResult ingestionResult = tryWrite(writeTask, inputs, lastAttemptSql);
        long copyDone = System.nanoTime();
        try {
            postIngestionHandler.createPostIngestionTask(ingestionResult).execute();
        } catch (RuntimeException e) {
            throw new RegistrationFailedException(ingestionResult.filesCreated(), e);
        }
        // The batch is registered. Nothing below may throw: a failure here would not be a
        // RegistrationFailedException, so nothingRegistered() would read it as "failed before
        // registration" and the catalog-change retry could register the batch a second time.
        long postIngestDone = System.nanoTime();
        dataPhaseNanos.accumulate(copyDone - start);
        postIngestPhaseNanos.accumulate(postIngestDone - copyDone);
        rowsWritten.accumulate(ingestionResult.rowCount());
        recordRowsWritten(ingestionResult.rowCount());
        logger.debug("Queue '{}' commit phases: data(COPY)={}ms, postIngest(catalog)={}ms",
                queueId, (copyDone - start) / 1_000_000, (postIngestDone - copyDone) / 1_000_000);
        return ingestionResult;
    }

    /**
     * Deals with the output files of a failed attempt: deletes them when the post-ingestion step
     * says it rejected them before committing, otherwise counts them as orphans. A failure before
     * any file was reported (e.g. the COPY itself) has nothing to clean up here.
     */
    private void discardOutput(Exception failure) {
        if (!(failure instanceof RegistrationFailedException registration)) {
            return;
        }
        List<String> files = registration.files == null ? List.of() : registration.files;
        if (files.isEmpty()) {
            return;
        }
        if (!(registration.getCause() instanceof FilesNotRegisteredException rejected)) {
            // The registration outcome is unknown (e.g. the commit failed): the files may be live.
            orphanFilesLeft.add(files.size());
            logger.warn("Queue '{}': left {} output file(s) of a failed write in place because their "
                    + "registration outcome is unknown: {}", queueId, files.size(), files);
            return;
        }
        // The rejecting task's own list is authoritative, not just this write's output: a post
        // ingestion task may have written files the write path never saw — a sidecar's rows, for
        // instance — and they are equally unreferenced once the registration is rejected. Unioned
        // rather than replaced so a task that reports nothing still gets its output cleaned up.
        if (!rejected.files().isEmpty()) {
            List<String> union = new ArrayList<>(files);
            rejected.files().stream().filter(f -> !union.contains(f)).forEach(union::add);
            files = union;
        }
        List<String> left = new ArrayList<>();
        for (String file : files) {
            if (deleteLocalFile(file)) {
                orphanFilesDeleted.increment();
            } else {
                left.add(file);
            }
        }
        if (!left.isEmpty()) {
            orphanFilesLeft.add(left.size());
            logger.warn("Queue '{}': could not delete {} rejected output file(s); reclaim them with "
                    + "ducklake_delete_orphaned_files: {}", queueId, left.size(), left);
        }
    }

    /**
     * Deletes {@code file} if it is on the local filesystem (a plain path or a {@code file:} URI).
     * Returns {@code false} for object-store paths ({@code s3://} and the like), which this class
     * has no client for, and for local files that could not be deleted.
     */
    static boolean deleteLocalFile(String file) {
        String path = file;
        if (path.startsWith("file://")) {
            path = path.substring("file://".length());
        } else if (path.startsWith("file:")) {
            path = path.substring("file:".length());
        } else if (path.contains("://")) {
            return false;
        }
        try {
            Files.deleteIfExists(Path.of(path));
            return true;
        } catch (Exception e) {
            logger.warn("Failed to delete output file of a failed write: {}", file, e);
            return false;
        }
    }

    /**
     * Whether a failed attempt is certainly not in the table, so the batch may be written again: it
     * failed before registration (e.g. the COPY), or registration rejected its files before
     * committing ({@link FilesNotRegisteredException}). Any other registration failure — a failed
     * commit above all — may have landed.
     *
     * <p>"Not a {@link RegistrationFailedException}" is read as "failed before registration", which
     * holds only because nothing in {@link #writeAndRegister} after the post-ingestion step can
     * throw.
     */
    static boolean nothingRegistered(Exception failure) {
        return !(failure instanceof RegistrationFailedException)
                || failure.getCause() instanceof FilesNotRegisteredException;
    }

    /** The underlying failure of an attempt, without this class's registration wrapper. */
    private static Exception unwrap(Exception e) {
        return e instanceof RegistrationFailedException && e.getCause() instanceof Exception cause ? cause : e;
    }

    /** Writes retried once after a failure revealed a catalog change. */
    public long getCatalogRetries() {
        return catalogRetries.sum();
    }

    /** Catalog-change retries that failed again. */
    public long getCatalogRetryFailures() {
        return catalogRetryFailures.sum();
    }

    /** Output files of failed attempts that were deleted. */
    public long getOrphanFilesDeleted() {
        return orphanFilesDeleted.sum();
    }

    /** Output files of failed attempts left in place (object store, or registration outcome unknown). */
    public long getOrphanFilesLeft() {
        return orphanFilesLeft.sum();
    }

    @Override
    protected long catalogRetries() {
        return getCatalogRetries();
    }

    @Override
    protected long catalogRetryFailures() {
        return getCatalogRetryFailures();
    }

    @Override
    protected long orphanFilesDeleted() {
        return getOrphanFilesDeleted();
    }

    @Override
    protected long orphanFilesLeft() {
        return getOrphanFilesLeft();
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
     * {@code transformation} as text that can be nested inside parentheses: the user's own SQL with
     * only its trailing statement terminators and comments removed (see
     * {@link TransformationText#stripTrailing}). A {@code ;} is valid at the end of a statement but
     * not inside a subquery, and a trailing comment would otherwise run into the closing
     * parenthesis.
     */
    static String embeddable(String transformation) {
        return TransformationText.stripTrailing(transformation);
    }

    /**
     * The relation the output files are written from: the temp input files with the
     * transformation and any partition projections applied. This is also the relation the
     * watermark rows are computed over — same schema, same rows as the written output, read
     * while the input files are still local.
     */
    private String constructSourceRelation(WriteTask<String, IngestionResult> writeTask, WriteInputs inputs) {
        var batches = writeTask.bucket().batches();
        // All Arrow files
        var arrowFiles = batches.stream().map(Batch::record).map("'%s'"::formatted).collect(Collectors.joining(","));
        String sortOrderClause = getClause(batches.get(0).sortOrder(), "ORDER BY %s ");

        // Inner SQL reads from the temp Arrow files
        var innerSql = "SELECT * FROM read_%s([%s]) %s".formatted(this.inputFormat, arrowFiles, sortOrderClause);

        // The transformation is fetched fresh from the handler on every write (currentWriteInputs)
        // so view-based and handler-refreshed transformations are always current without caching.
        var querySql = applyTransformation(innerSql, inputs.transformation());

        // Add handler-supplied derived-column projections (e.g. day(timestamp) AS day) so the
        // PARTITION_BY tokens resolve. Not needed for header-supplied partitionBy, which names
        // columns that already exist in the relation.
        // (Empty when the batch header supplies partitionBy — see currentWriteInputs.)
        if (!inputs.partitionProjections().isEmpty()) {
            querySql = "SELECT *, %s FROM (%s)".formatted(
                    String.join(", ", inputs.partitionProjections()), querySql);
        }
        return querySql;
    }

    /** Reads the current {@link WriteInputs} for this batch from the batch header and the handler. */
    private WriteInputs currentWriteInputs(WriteTask<String, IngestionResult> writeTask) {
        String[] batchPartitionBy = writeTask.bucket().batches().get(0).partitionBy();
        boolean hasBatchPartitionBy = batchPartitionBy != null && batchPartitionBy.length > 0;
        String transformation = postIngestionHandler.getTransformation(queueId);
        if (hasBatchPartitionBy) {
            return new WriteInputs(transformation, List.of(batchPartitionBy), List.of());
        }
        return new WriteInputs(transformation,
                nonNullList(postIngestionHandler.getPartitionBy(queueId)),
                nonNullList(postIngestionHandler.getPartitionProjections(queueId)));
    }

    private static List<String> nonNullList(String[] values) {
        return values == null ? List.of() : Arrays.asList(values);
    }

    private String constructWriteQuery(WriteTask<String, IngestionResult> writeTask, WriteInputs inputs) {
        var batches = writeTask.bucket().batches();
        String partitionByClause = getClause(inputs.partitionBy().toArray(String[]::new), ", PARTITION_BY(%s)");
        // Last format
        var outputFormat = batches.isEmpty() ? "" : batches.get(batches.size() - 1).format();
        String fullFilePath;
        if (partitionByClause.isEmpty()) {
            String uniqueFileName = "dd_" + UUID.randomUUID() + "." + outputFormat;
            fullFilePath = this.outputPath + "/" + uniqueFileName;
        } else {
            fullFilePath = this.outputPath;
        }

        var querySql = constructSourceRelation(writeTask, inputs);

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

    private IngestionResult tryWrite(WriteTask<String, IngestionResult> writeTask, WriteInputs inputs,
                                     java.util.concurrent.atomic.AtomicReference<String> lastAttemptSql) throws Exception {
        var sql = constructWriteQuery(writeTask, inputs);
        lastAttemptSql.set(sql);
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
                watermarkRows = watermarkSpec.computeRows(conn, constructSourceRelation(writeTask, inputs));
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
                files, sql, watermarkRows,
                // The bucket's batches' sidecar rows, unioned: one COPY registers one set of files,
                // so they commit together with it or not at all.
                SidecarRows.union(writeTask.bucket().batches().stream().map(Batch::sidecarRows).toList()));
    }
}
