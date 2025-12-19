package io.dazzleduck.sql.commons.ingestion;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public abstract class BulkIngestQueueV2<T, R> implements BulkIngestQueueInterface<T, R> {

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
    private boolean terminating;
    private long writeTaskId;

    private final BlockingQueue<WriteTask<T, R>> writeQueue = new LinkedBlockingQueue<>();
    private final Thread writeThread;
    private final AtomicLong totalWriteBatches = new AtomicLong();
    private final AtomicLong totalWriteBuckets = new AtomicLong();
    private final AtomicLong acceptedBatches = new AtomicLong();
    private final AtomicLong bucketsCreated;

    public BulkIngestQueueV2(String identifier,
                             long minBucketSize,
                             Duration maxDelay,
                             ScheduledExecutorService executorService,
                             Clock clock) {
        this.minBucketSize = minBucketSize;
        this.identifier = identifier;
        this.executorService = executorService;
        this.maxDelay = maxDelay;
        this.clock = clock;
        this.currentBucket = new Bucket<>(minBucketSize, maxDelay);
        this.bucketsCreated =  new AtomicLong(1);
        this.writeThread = new Thread(this::processWriteQueue, "BulkIngestQueue-" + identifier + "-writer");
        this.writeThread.start();
        executorService.schedule(this::triggerWriteIfRequired, maxDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void processWriteQueue() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                var task = writeQueue.take();
                try {
                    write(task);
                    totalWriteBatches.addAndGet(task.bucket().batches().size());
                    totalWriteBuckets.incrementAndGet();
                } catch (Exception e) {
                    for (var future : task.bucket().futures()) {
                        future.completeExceptionally(e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public Stats getStats(){
        var totalWrite = totalWriteBatches.get();
        var accepted = acceptedBatches.get();
        return new Stats(identifier, 0, totalWriteBatches.get(), totalWriteBuckets.get(), 0,accepted - totalWrite, writeQueue.size());
    }
    public String identifier() {
        return identifier;
    }

    public synchronized Future<R> addToQueue(Batch<T> batch) {
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
        var now = clock.instant();
        if (currentBucket.isEmpty() && !terminating) {
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
        var toWrite = currentBucket;
        toWrite.markFinalized();
        currentBucket = new Bucket<>(minBucketSize, maxDelay);
        bucketsCreated.incrementAndGet();
        var writeTask = new WriteTask<T, R>(writeTaskId++, clock.instant(), toWrite);
        lastWrite = clock.instant();
        writeQueue.offer(writeTask);
        scheduleNextTrigger(clock.instant());
    }
    @Override
    public synchronized void close() throws Exception {
        terminating = true;
        writeThread.interrupt();
        writeThread.join(5000);

        // Fail any remaining tasks in the queue
        var exception = new IllegalStateException("Queue closed before batch could be written");
        WriteTask<T, R> remaining;
        while ((remaining = writeQueue.poll()) != null) {
            for (var future : remaining.bucket().futures()) {
                future.completeExceptionally(exception);
            }
        }

        // Fail any futures in the current bucket
        if (!currentBucket.isEmpty()) {
            for (var future : currentBucket.futures()) {
                future.completeExceptionally(exception);
            }
        }
    }
}
