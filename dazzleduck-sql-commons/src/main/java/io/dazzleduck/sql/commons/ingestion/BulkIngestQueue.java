package io.dazzleduck.sql.commons.ingestion;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public abstract class BulkIngestQueue<T, R> implements BulkIngestQueueInterface<T, R> {

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
    private static final int MAX_PRODUCER_IDS = 10000;
    private final Map<String, Long> inProgressBatchIds = new LinkedHashMap<String, Long>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > MAX_PRODUCER_IDS;
        }
    };
    private final long minBucketSize;
    private final String identifier;
    private final ScheduledExecutorService executorService;
    private final Duration maxDelay;
    private final Clock clock;
    private Instant lastWrite = Instant.EPOCH;
    private Bucket<T, R> currentBucket;
    private volatile boolean terminating;

    private volatile WriteTask<T, R> runningWrite;
    private long writeTaskId;

    private final BlockingQueue<WriteTask<T, R>> writeQueue = new LinkedBlockingQueue<>();
    private final Thread writeThread;
    private final AtomicLong totalWriteBatches = new AtomicLong();
    private final AtomicLong totalWriteBuckets = new AtomicLong();
    private final AtomicLong acceptedBatches = new AtomicLong();
    private final AtomicLong bucketsCreated =  new AtomicLong();

    public BulkIngestQueue(String identifier,
                           long minBucketSize,
                           Duration maxDelay,
                           ScheduledExecutorService executorService,
                           Clock clock) {
        this.minBucketSize = minBucketSize;
        this.identifier = identifier;
        this.executorService = executorService;
        this.maxDelay = maxDelay;
        this.clock = clock;
        createNewBucket();
        this.writeThread = new Thread(this::processWriteQueue, "BulkIngestQueue-" + identifier + "-writer");
        this.writeThread.setDaemon(true);
        this.writeThread.start();
        executorService.schedule(this::triggerWriteIfRequired, maxDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void createNewBucket(){
        this.currentBucket = new Bucket<>(minBucketSize, maxDelay);
        bucketsCreated.incrementAndGet();
    }


    private void processWriteQueue() {
        while (!terminating) {
            try {
                var task = writeQueue.take();
                runningWrite = task;
                try {
                    write(task);
                    totalWriteBatches.addAndGet(task.bucket().batches().size());
                    totalWriteBuckets.incrementAndGet();
                } catch (Exception e) {
                    // Complete futures with exception but continue processing remaining tasks
                    for (var future : task.bucket().futures()) {
                        if (!future.isDone()) {
                            future.completeExceptionally(e);
                        }
                    }
                    // Don't break - continue processing the next task in the queue
                } finally {
                    runningWrite = null;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public Stats getStats(){
        var totalWrite = totalWriteBatches.get();
        var accepted = acceptedBatches.get();
        return new Stats(identifier, 0, totalWriteBatches.get(), totalWriteBuckets.get(), 0,accepted - totalWrite, writeQueue.size());
    }

    @Override
    public String identifier() {
        return identifier;
    }

    public synchronized CompletableFuture<R> add(Batch<T> batch) {
        if (terminating) {
            throw new IllegalStateException("The queue is closed");
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
        acceptedBatches.incrementAndGet();
        if (batch.producerId() != null) {
            inProgressBatchIds.put(batch.producerId(), batch.producerBatchId());
        }
        if (currentBucket.isFull()) {
           submitWriteTask();
        }
        return result;
    }

    private synchronized void triggerWriteIfRequired() {
        if (terminating) {
            return;
        }
        var now = clock.instant();
        if (currentBucket.isEmpty()) {
                scheduleNextTrigger(now);
                return;
        }

        var nextWrite = lastWrite.plus(maxDelay);
        if (currentBucket.isFull() || !nextWrite.isAfter(now)) {
            submitWriteTask();
        } else {
            scheduleNextTrigger(now);
        }
    }

    private void scheduleNextTrigger(Instant now) {
        var nextTrigger = lastWrite.plus(maxDelay);
        var timeRemaining = Duration.between(now, nextTrigger);
        executorService.schedule(this::triggerWriteIfRequired, Math.max(0, timeRemaining.toMillis()), TimeUnit.MILLISECONDS);
    }

    private synchronized void submitWriteTask() {
        if (currentBucket.isEmpty()) {
            return;
        }
        var toWrite = currentBucket;
        toWrite.markFinalized();
        createNewBucket();
        var writeTask = new WriteTask<T, R>(writeTaskId++, clock.instant(), toWrite);
        lastWrite = clock.instant();
        writeQueue.offer(writeTask);
        if (!terminating) {
            scheduleNextTrigger(clock.instant());
        }
    }
    @Override
    public synchronized void close() throws Exception {
        terminating = true;
        var c = runningWrite;
        if (c != null) {
            c.cancel();
        }

        // Interrupt and wait for write thread to finish processing
        // The write thread will exit the loop when it sees terminating=true,
        // or when interrupted if it's blocked waiting for a task
        writeThread.interrupt();
        writeThread.join();

        // Fail any futures in the current bucket
        var exception = new IllegalStateException("Server shutting down before batch could be written");
        if (!currentBucket.isEmpty()) {
            for (var future : currentBucket.futures()) {
                future.completeExceptionally(exception);
            }
        }


        // Fail any remaining tasks that weren't processed
        WriteTask<T, R> remaining;
        while ((remaining = writeQueue.poll()) != null) {
            for (var future : remaining.bucket().futures()) {
                future.completeExceptionally(exception);
            }
        }
    }
}
