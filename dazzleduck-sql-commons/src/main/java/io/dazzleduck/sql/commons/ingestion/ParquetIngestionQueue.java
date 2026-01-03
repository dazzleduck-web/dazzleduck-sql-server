package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class ParquetIngestionQueue extends BulkIngestQueue<String, IngestionResult> {

    private final String path;
    private final PostIngestionTaskFactory postIngestionTaskFactory;
    private final String applicationId;
    private final String inputFormat;



    /**
     * Thw write will be performed as soon as bucket is full or after the maxDelay is exported since the first batch is inserted
     * @param applicationId
     * @param inputFormat
     * @param path
     * @param identifier      identify the queue. Generally this will the path of the bucket
     * @param minBucketSize   size of the bucket. Write will be performed as soon as bucket is reached to this size  or more
     * @param maxDelay        write will be performed just after this delay.
     * @param postIngestionTaskFactory
     * @param executorService Executor service.
     * @param clock
     */
    public ParquetIngestionQueue(String applicationId,
                                 String inputFormat,
                                 String path,
                                 String identifier,
                                 long minBucketSize,
                                 Duration maxDelay,
                                 PostIngestionTaskFactory postIngestionTaskFactory,
                                 ScheduledExecutorService executorService,
                                 Clock clock) {
        super(identifier, minBucketSize, maxDelay, executorService, clock);
        this.path = path;
        this.postIngestionTaskFactory = postIngestionTaskFactory;
        this.applicationId = applicationId;
        this.inputFormat = inputFormat;
    }

    @Override
    public void write(WriteTask<String, IngestionResult> writeTask) {
        try {
            IngestionResult ingestionResult = tryWrite(writeTask);
            var postIngestionTask = postIngestionTaskFactory.create(ingestionResult);
            postIngestionTask.execute();
            writeTask.bucket().futures().forEach(action -> action.complete(ingestionResult));
        }  catch (Exception e) {
            writeTask.bucket().futures().forEach(action -> action.completeExceptionally(e));
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
        String firstTransformationString = getClause(batches.get(0).transformations(), "%s");
        String firstPartitionString = getClause(batches.get(0).partitions(), ", PARTITION_BY(%s)");
        var firstSortOrder = getClause(batches.get(0).sortOrder(), "ORDER BY %s ");

        // Select clause
        var selectClause = firstTransformationString.isEmpty() ? "*" : "*, " + firstTransformationString;
        // Last format
        var outputFormat = batches.isEmpty() ? "" : batches.get(batches.size() - 1).format();
        String fullFilePath;
        if (firstPartitionString.isEmpty()) {
            String uniqueFileName = "dd_" + UUID.randomUUID() + "." + outputFormat;
            fullFilePath = this.path + "/" + uniqueFileName;
        } else {
            fullFilePath = this.path;
        }

        // Build SQL
        // https://duckdb.org/docs/stable/sql/statements/copy
        // Search for return File and stats during the copy statement.
        // Need to create correct postIngestionTaskFactory so that you create correct IngestionTask which will be executed
        // The ingestion task will essentially insert the data into the database which will complete our implementation
        var sql = """
                COPY
                    (SELECT %s FROM read_%s([%s]) %s)
                    TO '%s'
                    (FORMAT %s %s, RETURN_FILES);
                """.formatted(selectClause, this.inputFormat, arrowFiles, firstSortOrder, fullFilePath, outputFormat, firstPartitionString);
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
        return new IngestionResult(this.path, writeTask.taskId(), this.applicationId,
                writeTask.bucket().getProducerMaxBatchId(),
                count,
                files);
    }
}
