package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
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
        super(ingestionQueue, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, executorService, clock);
        this.outputPath = outputPath;
        this.queueId = ingestionQueue;
        this.postIngestionHandler = postIngestionHandler;
        this.applicationId = applicationId;
        this.inputFormat = inputFormat;
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
            logger.debug("Queue '{}' commit phases: data(COPY)={}ms, postIngest(catalog)={}ms",
                    queueId, (copyDone - start) / 1_000_000, (postIngestDone - copyDone) / 1_000_000);
            writeTask.bucket().futures().forEach(action -> action.complete(ingestionResult));
        } catch (Exception e) {
            var sql = constructWriteQuery(writeTask);
            logger.atError().setCause(e).log("Failed to write to queue {} sql {}", queueId, sql);
            writeTask.bucket().futures().forEach(action -> action.completeExceptionally(e));
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

    private String constructWriteQuery(WriteTask<String, IngestionResult> writeTask) {
        var batches = writeTask.bucket().batches();
        // All Arrow files
        var arrowFiles = batches.stream().map(Batch::record).map("'%s'"::formatted).collect(Collectors.joining(","));
        String[] batchPartitionBy = batches.get(0).partitionBy();
        boolean hasBatchPartitionBy = batchPartitionBy != null && batchPartitionBy.length > 0;
        String[] effectivePartitionBy = hasBatchPartitionBy
                ? batchPartitionBy
                : postIngestionHandler.getPartitionBy(queueId);
        String partitionByClause = getClause(effectivePartitionBy, ", PARTITION_BY(%s)");
        String sortOrderClause = getClause(batches.get(0).sortOrder(), "ORDER BY %s ");
        // Last format
        var outputFormat = batches.isEmpty() ? "" : batches.get(batches.size() - 1).format();
        String fullFilePath;
        if (partitionByClause.isEmpty()) {
            String uniqueFileName = "dd_" + UUID.randomUUID() + "." + outputFormat;
            fullFilePath = this.outputPath + "/" + uniqueFileName;
        } else {
            fullFilePath = this.outputPath;
        }

        // Inner SQL reads from the temp Arrow files
        var innerSql = "SELECT * FROM read_%s([%s]) %s".formatted(this.inputFormat, arrowFiles, sortOrderClause);

        // Fetch transformation fresh from the handler on every write so view-based
        // and handler-refreshed transformations are always current without caching.
        String transformation = postIngestionHandler.getTransformation(queueId);
        var querySql = (transformation != null && !transformation.isBlank())
                ? "WITH __this AS (%s) %s".formatted(innerSql, transformation)
                : innerSql;

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

        // Build SQL
        // https://duckdb.org/docs/stable/sql/statements/copy
        var sql = """
                COPY
                    (%s)
                    TO '%s'
                    (FORMAT %s %s, RETURN_FILES, APPEND);
                """.formatted(querySql, fullFilePath, outputFormat, partitionByClause);
        return sql;
    }

    private IngestionResult tryWrite(WriteTask<String, IngestionResult> writeTask) throws Exception {
        var sql = constructWriteQuery(writeTask);
        logger.debug("Executing COPY SQL: {}", sql);
        List<String> files = new ArrayList<>();
        long count = 0;
        try (var conn = ConnectionPool.getConnection();
             var stmt = conn.createStatement()) {

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
                files, sql);
    }
}
