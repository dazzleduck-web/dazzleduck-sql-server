package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class BulkIngestQueue<T, R> implements BulkIngestQueueInterface<T, R> {
    private final ScheduledExecutorService executorService;
    private final Duration maxDelay;
    private final String identifier;
    private final long maxBucketSize;
    private final Map<String, Long> inProgressBatchIds = new HashMap<>();
    private final Set<ScheduledCheckTask> scheduledCheckTasks = new HashSet<>();
    private final Clock clock;
    private long awaitingWriteMemorySize = 0;
    private long writeInProgressSize = 0;
    private Instant expectedNextTrigger = Instant.ofEpochMilli(Long.MAX_VALUE);
    private Bucket<T,R> currentBucket;
    private long totalWrite;
    private long totalWriteBatches;
    private long totalTimeSpentWriting;
    private long writeTaskId;
    private long scheduledWriteCount;

    private boolean terminating;

    /**
     * Thw write will be performed as soon as bucket is full or after the maxDelay is exported since the first batch is inserted
     *
     * @param identifier      identify the queue. Generally this will the path of the bucket
     * @param minBucketSize   size of the bucket. Write will be performed as soon as bucket is full or overflowing
     * @param maxDelay        write will be performed just after this delay.
     * @param executorService Executor service.
     * @param clock
     */
    public BulkIngestQueue(String identifier,
                           long minBucketSize,
                           Duration maxDelay,
                           ScheduledExecutorService executorService,
                           Clock clock) {
        this.maxBucketSize = minBucketSize;
        this.identifier = identifier;
        this.executorService = executorService;
        this.maxDelay = maxDelay;
        this.clock = clock;
        this.currentBucket = new Bucket<>(minBucketSize, maxDelay);
    }

    public String identifier() {
        return identifier;
    }

    public synchronized Future<R> addToQueue(Batch<T> batch) {
        if (terminating) {
            throw new IllegalStateException("Queue is terminating");
        }
        if (batch.producerId() != null) {
            var progressBatch = inProgressBatchIds.get(batch.producerId());
            if (progressBatch != null && progressBatch >= batch.producerBatchId()) {
                return CompletableFuture.failedFuture(
                        new OutOfSequenceBatch(progressBatch, batch.producerBatchId()));
            }
        }
        var result = new CompletableFuture<R>();
        currentBucket.add(batch, result);
        updateStatsWithNewBatch(batch);
        triggerWriteIfRequired(null);
        return result;
    }

    private synchronized void triggerWriteIfRequired(ScheduledCheckTask task) {
        // When write is performed all task which were scheduled need tp be skipped
        // since write has already emptied the bucket
        if (task != null && task.isCanceled()) {
            return;
        }
        var currentTime = clock.instant();


        // scheduleNow if because bucket is full
        if (currentBucket.readyForWrite(currentTime)){
            scheduledCheckTasks.forEach(ScheduledCheckTask::cancel);
            scheduledCheckTasks.clear();
            scheduleNow(currentBucket.timeExpired(currentTime));
            return;
        }
        // If it's not schedules already then schedule it.
        if (scheduledCheckTasks.isEmpty()) {
            var delay = expectedNextTrigger.toEpochMilli() - currentTime.toEpochMilli();
            var scheduledTask = new ScheduledCheckTask();
            scheduledCheckTasks.add(scheduledTask);
            executorService.schedule(() -> triggerWriteIfRequired(scheduledTask), delay, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Empty the bucket. Update stats. Cancel all previously scheduled task
     *
     * @param isScheduledWrite is this a schedule write or because bucket is full
     */
    private synchronized void scheduleNow(boolean isScheduledWrite) {
        if (isScheduledWrite) {
            scheduledWriteCount++;
        }
        Bucket<T, R> toWrite = currentBucket;
        currentBucket = new Bucket<>(maxBucketSize, maxDelay);
        writeInProgressSize = awaitingWriteMemorySize;
        awaitingWriteMemorySize = 0;
        expectedNextTrigger = Instant.ofEpochMilli(Long.MAX_VALUE);
        var writeTask = new WriteTask<T, R>(writeTaskId++, clock.instant(), toWrite);
        writeInProgressSize += writeTask.size();
        executorService.submit(() -> {
            var start = clock.instant();
            writeInProgressSize = writeTask.size();
            write(writeTask);
            var end = clock.instant();
            updatePostWriteStats(writeTask, end.toEpochMilli() - start.toEpochMilli());
        });
    }


    private synchronized void updatePostWriteStats(WriteTask<T, R> writeTask,
                                                   long timeSpentWriting) {
        totalWrite += writeTask.size();
        writeInProgressSize = 0;
        totalWriteBatches++;
        totalTimeSpentWriting += timeSpentWriting;
    }

    private void updateStatsWithNewBatch(Batch<T> batch) {
        inProgressBatchIds.put(batch.producerId(), batch.producerBatchId());
        awaitingWriteMemorySize += batch.totalSize();
        var n = batch.receivedTime().plus(maxDelay);
        if (expectedNextTrigger.isAfter(n)) {
            expectedNextTrigger = n;
        }
    }

    public synchronized Stats getStats() {
        return null;
        //return new Stats(identifier, totalWrite, totalWriteBatches, totalTimeSpentWriting, scheduledWriteCount);
    }

    private static class ScheduledCheckTask {
        private boolean canceled = false;

        private boolean isCanceled() {
            return canceled;
        }

        private void cancel() {
            this.canceled = true;
        }
    }

    public static Path writeAndValidateTempArrowFile(Path tempDir, InputStream inputStream) throws IOException {
        String uniqueFileName = "ingestion_" + UUID.randomUUID() + ".arrow";
        Path tempFilePath = tempDir.resolve(uniqueFileName);
        try (OutputStream out = Files.newOutputStream(tempFilePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            inputStream.transferTo(out);
        }
        return tempFilePath;
    }

    public static Path writeAndValidateTempArrowFile(Path tempDir, ArrowReader reader) throws IOException {
        String uniqueFileName = "ingestion_" + UUID.randomUUID() + ".arrow";
        Path tempFilePath = tempDir.resolve(uniqueFileName);
        try (FileOutputStream fos = new FileOutputStream(String.valueOf(tempFilePath));
             ArrowStreamWriter writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, Channels.newChannel(fos))) {
            while (reader.loadNextBatch()){
                writer.writeBatch();
            }
            writer.end();
        }
        return tempFilePath;
    }

    public static Path writeAndValidateTempParquetFile(Path tempDir, ArrowReader reader, BufferAllocator allocator) throws  SQLException {

        String uniqueFileName = "ingestion_" + UUID.randomUUID() + ".parquet";
        Path tempFilePath = tempDir.resolve(uniqueFileName);
        ConnectionPool.bulkIngestToFile(reader, allocator, tempFilePath.toString(), List.of(),"parquet");
        return tempFilePath;
    }

    @Override
    public void close() {
        terminating = true;
    }
}