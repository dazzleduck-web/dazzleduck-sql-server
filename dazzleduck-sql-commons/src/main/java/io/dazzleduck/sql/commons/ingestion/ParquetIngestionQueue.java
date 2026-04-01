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
    private volatile String transformation;



    /**
     * Thw write will be performed as soon as bucket is full or after the maxDelay is exported since the first batch is inserted
     * @param applicationId
     * @param inputFormat
     * @param outputPath
     * @param ingestionQueue  the ingestion queue identifier used for mapping to target tables
     * @param minBucketSize   size of the bucket. Write will be performed as soon as bucket is reached to this size or more
     * @param maxBucketSize   maximum size of the bucket. Bucket will be flushed before exceeding this size
     * @param maxBatches      maximum number of batches in a bucket. Write will be performed when this limit is reached
     * @param maxPendingWrite maximum pending bytes allowed before rejecting new batches
     * @param maxDelay        write will be performed just after this delay.
     * @param postIngestionHandler
     * @param executorService Executor service.
     * @param clock
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
                maxBatches, maxPendingWrite, maxDelay, postIngestionHandler, executorService, clock, null);
    }

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
                                 Clock clock,
                                 String transformation) {
        super(ingestionQueue, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, executorService, clock);
        this.outputPath = outputPath;
        this.queueId = ingestionQueue;
        this.postIngestionHandler = postIngestionHandler;
        this.applicationId = applicationId;
        this.inputFormat = inputFormat;
        this.transformation = transformation;
    }

    @Override
    public void write(WriteTask<String, IngestionResult> writeTask) {
        logger.info("Ingestion queue '{}' received batch with {} files, outputPath={}",
                queueId, writeTask.bucket().batches().size(), outputPath);
        try {
            IngestionResult ingestionResult = tryWrite(writeTask);
            var postIngestionTask = postIngestionHandler.createPostIngestionTask(ingestionResult);
            postIngestionTask.execute();
            writeTask.bucket().futures().forEach(action -> action.complete(ingestionResult));
        } catch (Exception e) {
            writeTask.bucket().futures().forEach(action -> action.completeExceptionally(e));
        } finally {
            cleanupInputFiles(writeTask);
        }
    }

    /**
     * Asynchronously cleans up input files using virtual threads.
     * This is fire-and-forget - we don't wait for deletion to complete
     * since it doesn't affect the write result.
     */
    private void cleanupInputFiles(WriteTask<String, IngestionResult> writeTask) {
        for (var batch : writeTask.bucket().batches()) {
            final String filePath = batch.record();
            CLEANUP_EXECUTOR.execute(() -> {
                try {
                    Files.deleteIfExists(Path.of(filePath));
                } catch (Exception e) {
                    logger.warn("Failed to delete temporary input file: {}", filePath, e);
                }
            });
        }
    }

    public void setTransformation(String transformation) {
        validateTransformation(transformation);
        this.transformation = transformation;
    }

    protected void validateTransformation(String transformation) {
        if (transformation != null && !transformation.isBlank()) {
            DuckLakeIngestionTaskFactoryProvider.validateTransformation(queueId, transformation);
        }
    }

    private String getClause(String[] values, String clause){
        if(values == null || values.length == 0){
            return "";
        } else {
            var nested = Arrays.stream(values).filter(Objects::nonNull).map(String::trim).collect(Collectors.joining(","));
            return clause.formatted(nested);
        }
    }

    private IngestionResult tryWrite(WriteTask<String, IngestionResult> writeTask) throws Exception {
        var batches = writeTask.bucket().batches();
        // All Arrow files
        var arrowFiles = batches.stream().map(Batch::record).map("'%s'"::formatted).collect(Collectors.joining(","));
        String partitionByClause = getClause(batches.get(0).partitionBy(), ", PARTITION_BY(%s)");
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

        // If a transformation is configured, wrap the inner SQL as a CTE named __this
        // and use the transformation query (e.g. "SELECT a, b, c FROM __this") as the outer query.
        var querySql = (transformation != null && !transformation.isBlank())
                ? "WITH __this AS (%s) %s".formatted(innerSql, transformation)
                : innerSql;

        // Build SQL
        // https://duckdb.org/docs/stable/sql/statements/copy
        var sql = """
                COPY
                    (%s)
                    TO '%s'
                    (FORMAT %s %s, RETURN_FILES, APPEND);
                """.formatted(querySql, fullFilePath, outputFormat, partitionByClause);
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
                files);
    }
}
