package io.dazzleduck.sql.commons.ingestion;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAccumulator;

public abstract class BulkIngestQueue<T, R> implements BulkIngestQueueInterface<T, R> {

    private static final Logger logger = LoggerFactory.getLogger(BulkIngestQueue.class);

    /**
     * Upper bound for {@link #close()} to wait on the write thread after cancelling/interrupting it.
     * cancel() + interrupt should unblock a responsive write almost immediately; this bound keeps a
     * forceful close from hanging on a write that ignores cancellation. The write thread is a daemon,
     * so a writer still stuck past this bound cannot block JVM exit.
     */
    private static final Duration CLOSE_JOIN_TIMEOUT = Duration.ofSeconds(5);

    /** Bound used by {@link #close()} when waiting on the write thread. Overridable for tests. */
    protected Duration closeJoinTimeout() {
        return CLOSE_JOIN_TIMEOUT;
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
        } catch (Exception e) {
            Files.deleteIfExists(tempFilePath);
            if (e instanceof IOException ioe) throw ioe;
            throw new IOException(e);
        }
        return tempFilePath;
    }

    /**
     * Called for each batch that is abandoned at shutdown time (i.e. the batch was accepted but
     * never reached the write phase). Subclasses override this to release batch-level resources
     * such as temporary files.
     */
    protected void onBatchAbandoned(Batch<T> batch) {}

    /**
     * Creates a new combined bucket from multiple buckets.
     * The combined bucket contains all batches and futures from the source buckets.
     *
     * @param buckets the list of buckets to combine
     * @param minBucketSize minimum bucket size for the new bucket
     * @param maxBatches maximum batches for the new bucket
     * @param maxDelay maximum delay for the new bucket
     * @return a new bucket containing all content from the source buckets
     */
    static <T, R> Bucket<T, R> combineBuckets(List<Bucket<T, R>> buckets, long minBucketSize, int maxBatches, Duration maxDelay) {
        var combined = new Bucket<T, R>(minBucketSize, maxBatches, maxDelay);
        for (var bucket : buckets) {
            for (int i = 0; i < bucket.batches().size(); i++) {
                combined.add(bucket.batches().get(i), bucket.futures().get(i));
            }
        }
        combined.markFinalized();
        return combined;
    }

    /**
     * Checks if a bucket can be added to a combined bucket without exceeding limits.
     */
    private static <T, R> boolean canCombine(long currentSize, int currentBatchCount,
                                              Bucket<T, R> candidate, long maxSize, int maxBatchCount) {
        return (currentSize + candidate.size() <= maxSize) &&
               (currentBatchCount + candidate.batchCount() <= maxBatchCount);
    }
    /**
     * Upper bound on tracked producer ids. The tracking map is an access-ordered LRU: when a new
     * producer pushes the size past this limit, the least-recently-used producer's entry is
     * evicted and duplicate/ordering protection for that producer silently disappears — its next
     * batch is accepted with any sequence number. Evictions are logged and counted (see
     * {@link #getProducerIdEvictions()}) so this is observable; size the limit above the number
     * of concurrently active producers.
     */
    private static final int MAX_PRODUCER_IDS = 10000;
    private final Map<String, Long> inProgressBatchIds = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            if (size() > MAX_PRODUCER_IDS) {
                producerIdEvictions.accumulate(1);
                logger.warn("Producer-id cache for queue '{}' exceeded {} entries; evicting " +
                                "least-recently-used producer '{}' (last batch id {}) — duplicate and " +
                                "ordering protection no longer applies to it",
                        identifier, MAX_PRODUCER_IDS, eldest.getKey(), eldest.getValue());
                return true;
            }
            return false;
        }
    };
    private final LongAccumulator producerIdEvictions = new LongAccumulator(Long::sum, 0L);
    private final long minBucketSize;
    private final long maxBucketSize;
    private final int maxBatches;
    private final long maxPendingWrite;
    private final String identifier;
    private final ScheduledExecutorService executorService;
    private final Duration maxDelay;
    private final Clock clock;
    private Instant lastWrite = Instant.EPOCH;
    private Bucket<T, R> currentBucket;
    private volatile boolean terminating;
    private volatile boolean draining;
    /**
     * Single-flight guard for the {@link #triggerWriteIfRequired} chain: true while a trigger is
     * scheduled and not yet run. Exactly one chain must exist — the constructor starts it and each
     * run schedules its successor. Without this guard every {@link #submitWriteTask} (one per
     * flushed bucket) would spawn an additional never-terminating chain, and under sustained
     * ingestion thousands of chains accumulate, all contending for the queue lock. Guarded by
     * {@code this}.
     */
    private boolean triggerScheduled;

    private volatile WriteTask<T, R> runningWrite;
    private long writeTaskId;

    private final BlockingQueue<WriteTask<T, R>> writeQueue = new LinkedBlockingQueue<>();
    /**
     * Sentinel offered to {@link #writeQueue} by {@link #drain()}. Because the queue is FIFO,
     * the write thread only reaches it after every previously enqueued task has been written,
     * at which point it stops. Its bucket is {@code null} and must never be dereferenced.
     */
    private final WriteTask<T, R> poisonPill = new WriteTask<>(-1L, Instant.EPOCH, null);
    private final Thread writeThread;
    private final LongAccumulator totalWriteBatches = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator totalWriteBuckets = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator acceptedBatches = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator acceptedBytes = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator bucketsCreated = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator totalWrite = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator failedWriteBytes = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator failedWriteBatches = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator failedWriteBuckets = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator timeSpentWriting = new LongAccumulator(Long::sum, 0L);

    public BulkIngestQueue(String identifier,
                           long minBucketSize,
                           long maxBucketSize,
                           int maxBatches,
                           long maxPendingWrite,
                           Duration maxDelay,
                           ScheduledExecutorService executorService,
                           Clock clock) {
        this.minBucketSize = minBucketSize;
        this.maxBucketSize = maxBucketSize;
        this.maxBatches = maxBatches;
        this.maxPendingWrite = maxPendingWrite;
        this.identifier = identifier;
        this.executorService = executorService;
        this.maxDelay = maxDelay;
        this.clock = clock;
        createNewBucket();
        this.writeThread = new Thread(this::processWriteQueue, "BulkIngestQueue-" + identifier + "-writer");
        this.writeThread.setDaemon(true);
        this.writeThread.start();
        scheduleTrigger(maxDelay.toMillis());
    }

    private void createNewBucket(){
        this.currentBucket = new Bucket<>(minBucketSize, maxBatches, maxDelay);
        bucketsCreated.accumulate(1);
    }


    private void processWriteQueue() {
        while (!terminating) {
            try {
                var task = writeQueue.take();

                // drain() enqueues the poison pill after the final real task; reaching it means
                // every accepted batch has been written, so the drain is complete and we stop.
                if (task == poisonPill) {
                    break;
                }

                // Try to combine with additional buckets from the queue
                var bucketsToCombine = new ArrayList<Bucket<T, R>>();
                bucketsToCombine.add(task.bucket());
                long combinedSize = task.bucket().size();
                int combinedBatchCount = task.bucket().batchCount();

                // Poll additional tasks while they can be combined. Combine on the task actually
                // returned by poll() — not the peeked one — so this stays correct even if close()
                // concurrently drains the queue after a timed-out join: each task is then consumed
                // by exactly one of {writer, close}, never written and abandoned at once.
                WriteTask<T, R> nextTask;
                while ((nextTask = writeQueue.peek()) != null && nextTask != poisonPill) {
                    if (!canCombine(combinedSize, combinedBatchCount, nextTask.bucket(), maxBucketSize, maxBatches)) {
                        break; // Can't combine more
                    }
                    var polled = writeQueue.poll();
                    if (polled == null || polled == poisonPill) {
                        break; // lost the race to a concurrent consumer, or reached the pill
                    }
                    var polledBucket = polled.bucket();
                    bucketsToCombine.add(polledBucket);
                    combinedSize += polledBucket.size();
                    combinedBatchCount += polledBucket.batchCount();
                }

                // Create combined bucket if we have multiple, otherwise use original
                Bucket<T, R> bucketToWrite;
                if (bucketsToCombine.size() > 1) {
                    bucketToWrite = combineBuckets(bucketsToCombine, minBucketSize, maxBatches, maxDelay);
                } else {
                    bucketToWrite = task.bucket();
                }

                var combinedTask = new WriteTask<>(task.taskId(), task.startTime(), bucketToWrite);
                runningWrite = combinedTask;

                try {
                    var start = clock.instant();
                    write(combinedTask);
                    var end = clock.instant();
                    totalWriteBatches.accumulate(bucketToWrite.batches().size());
                    totalWrite.accumulate(bucketToWrite.size());
                    totalWriteBuckets.accumulate(bucketsToCombine.size());
                    timeSpentWriting.accumulate(Duration.between(start, end).toMillis());
                } catch (Exception e) {
                    logger.error("Write failed for queue '{}': dropping bucket of {} batches / {} bytes",
                            identifier, bucketToWrite.batchCount(), bucketToWrite.size(), e);
                    // A failed bucket is no longer outstanding work: account it separately so
                    // pendingWrite()/getPendingBatches() (acceptedBytes - written - failed) don't
                    // count it as pending forever and eventually wedge add() at maxPendingWrite.
                    // Accumulate before completing the futures so a caller observing the failure
                    // already sees the corrected pending numbers.
                    failedWriteBytes.accumulate(bucketToWrite.size());
                    failedWriteBatches.accumulate(bucketToWrite.batchCount());
                    failedWriteBuckets.accumulate(1);
                    rollbackProducerSequences(bucketToWrite);
                    // Complete futures with exception but continue processing remaining tasks
                    for (var future : bucketToWrite.futures()) {
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

    /**
     * Rolls each producer's sequence entry back below the smallest id that failed in this bucket,
     * so the client can resubmit exactly the batches whose futures failed; without this a retry is
     * rejected as {@link OutOfSequenceBatch} and a recoverable write failure becomes data loss.
     * Runs under the queue lock because {@link #add} reads and writes {@link #inProgressBatchIds}
     * under the same lock, and before the futures are completed so a client observing the failure
     * can retry immediately. Ids of later batches from the same producer still queued behind the
     * failed bucket become re-submittable too; they either succeed (so dedup is only weakened for
     * batches the client was told failed) or fail and legitimately need the same retry window.
     */
    private synchronized void rollbackProducerSequences(Bucket<T, R> bucket) {
        var minFailedByProducer = new HashMap<String, Long>();
        for (var batch : bucket.batches()) {
            if (batch.producerId() != null) {
                minFailedByProducer.merge(batch.producerId(), batch.producerBatchId(), Math::min);
            }
        }
        minFailedByProducer.forEach((producerId, minFailedId) -> {
            var current = inProgressBatchIds.get(producerId);
            if (current != null && current >= minFailedId) {
                inProgressBatchIds.put(producerId, minFailedId - 1);
            }
        });
    }

    @Override
    public Stats getStats(){
        return new Stats(identifier, totalWrite.get(), totalWriteBatches.get(), totalWriteBuckets.get(),
                timeSpentWriting.get(), getPendingBatches(), getPendingBuckets(),
                failedWriteBytes.get(), failedWriteBatches.get(), failedWriteBuckets.get(),
                producerIdEvictions.get());
    }

    public long getTotalWriteBatches() {
        return totalWriteBatches.get();
    }

    public long getTotalWriteBuckets() {
        return totalWriteBuckets.get();
    }

    public long getTotalWriteBytes() {
        return totalWrite.get();
    }

    public long getTimeSpentWriting() {
        return timeSpentWriting.get();
    }

    public long getFailedWriteBytes() {
        return failedWriteBytes.get();
    }

    public long getFailedWriteBatches() {
        return failedWriteBatches.get();
    }

    public long getFailedWriteBuckets() {
        return failedWriteBuckets.get();
    }

    public long getProducerIdEvictions() {
        return producerIdEvictions.get();
    }

    public long getPendingBatches() {
        return acceptedBatches.get() - totalWriteBatches.get() - failedWriteBatches.get();
    }

    public long getPendingBuckets() {
        return writeQueue.size();
    }

    @Override
    public String identifier() {
        return identifier;
    }

    @Override
    public long pendingWrite() {
        return acceptedBytes.get() - totalWrite.get() - failedWriteBytes.get();
    }

    /**
     * Calculates an estimated retry time based on the current ingestion rate.
     * Returns the time in seconds it would take to drain the current pending bytes.
     *
     * @param currentPending the current pending bytes
     * @return estimated retry time in seconds, bounded between 1 and 60 seconds
     */
    private int calculateRetryAfterSeconds(long currentPending) {
        long writtenBytes = totalWrite.get();
        long writingTimeMs = timeSpentWriting.get();

        // If no data has been written yet, use default
        if (writtenBytes == 0 || writingTimeMs == 0) {
            return 5; // Default 5 seconds
        }

        // Calculate write rate in bytes per millisecond
        double bytesPerMs = (double) writtenBytes / writingTimeMs;

        // Calculate estimated time to drain pending bytes (in seconds)
        double estimatedDrainSeconds = currentPending / bytesPerMs / 1000.0;

        // Bound between 1 and 60 seconds
        int retryAfterSeconds = (int) Math.ceil(estimatedDrainSeconds);
        return Math.max(1, Math.min(60, retryAfterSeconds));
    }

    public synchronized CompletableFuture<R> add(Batch<T> batch) {
        if (terminating || draining) {
            throw new IllegalStateException("The queue is closed");
        }
        long currentPending = pendingWrite();
        if (currentPending + batch.totalSize() > maxPendingWrite) {
            int retryAfterSeconds = calculateRetryAfterSeconds(currentPending);
            return CompletableFuture.failedFuture(
                    new PendingWriteExceededException(currentPending, maxPendingWrite, retryAfterSeconds));
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
        acceptedBatches.accumulate(1);
        acceptedBytes.accumulate(batch.totalSize());
        if (batch.producerId() != null) {
            inProgressBatchIds.put(batch.producerId(), batch.producerBatchId());
        }
        if (currentBucket.isFull()) {
           submitWriteTask();
        }
        return result;
    }

    private synchronized void triggerWriteIfRequired() {
        // This run consumed the scheduled trigger; whichever path follows schedules the successor.
        triggerScheduled = false;
        if (terminating || draining) {
            // Once draining, drain() owns the final flush and the write thread is stopping; any
            // further scheduled trigger is a no-op so the executor stops rescheduling this task.
            return;
        }
        var now = clock.instant();
        if (currentBucket.isEmpty()) {
            // Nothing to flush — schedule the next check at a full maxDelay interval.
            // Using scheduleNextTrigger here is unsafe: when lastWrite == Instant.EPOCH
            // (initial state), nextTrigger is decades in the past, so timeRemaining is
            // deeply negative and Math.max(0, ...) collapses to 0, creating a tight
            // spin-loop that consumes 100% CPU and starves all other threads.
            scheduleTrigger(maxDelay.toMillis());
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
        scheduleTrigger(Math.max(0, timeRemaining.toMillis()));
    }

    /** Schedules the next {@link #triggerWriteIfRequired} run unless one is already pending. */
    private void scheduleTrigger(long delayMillis) {
        if (triggerScheduled) {
            return;
        }
        triggerScheduled = true;
        executorService.schedule(this::triggerWriteIfRequired, delayMillis, TimeUnit.MILLISECONDS);
    }

    private synchronized void submitWriteTask() {
        if (currentBucket.isEmpty()) {
            return;
        }
        var toWrite = currentBucket;
        toWrite.markFinalized();
        createNewBucket();
        var writeTask = new WriteTask<>(writeTaskId++, clock.instant(), toWrite);
        lastWrite = clock.instant();
        writeQueue.offer(writeTask);
        if (!terminating && !draining) {
            scheduleNextTrigger(clock.instant());
        }
    }
    /**
     * Initiates draining: marks the queue as draining (so {@link #add} is rejected and scheduled
     * triggers become no-ops), finalizes and enqueues the current bucket, then enqueues the poison
     * pill. FIFO ordering guarantees the write thread drains that bucket — and every task still
     * queued ahead of it — before it reaches the pill and stops. Idempotent and a no-op once
     * {@code terminating} (close already abandoned everything) or already draining.
     */
    private synchronized void initiateDrain() {
        if (!terminating && !draining) {
            draining = true;
            submitWriteTask();
            writeQueue.offer(poisonPill);
        }
    }

    @Override
    public void drain() throws InterruptedException {
        initiateDrain();
        writeThread.join();
    }

    @Override
    public boolean drain(Duration timeout) throws InterruptedException {
        initiateDrain();
        long millis = timeout.toMillis();
        // join(0) blocks forever, so only wait when the bound is positive; a non-positive
        // timeout means "don't wait" and we report whether the writer happens to be done.
        if (millis > 0) {
            writeThread.join(millis);
        }
        return !writeThread.isAlive();
    }

    @Override
    public synchronized void close() throws Exception {
        terminating = true;
        var c = runningWrite;
        if (c != null) {
            c.cancel();
        }

        // Interrupt and wait (bounded) for the write thread to finish processing. It exits the loop
        // when it sees terminating=true, or when interrupted while blocked waiting for a task. The
        // bound guarantees a forceful close cannot hang forever on a write that ignores cancellation;
        // a writer still stuck past it is left running (it is a daemon and cannot block JVM exit) and
        // only owns its own in-flight bucket, which the cleanup below deliberately does not touch.
        writeThread.interrupt();
        var joinTimeout = closeJoinTimeout();
        writeThread.join(joinTimeout.toMillis());
        if (writeThread.isAlive()) {
            logger.atWarn().log("Write thread {} did not terminate within {} of close(); abandoning it",
                    writeThread.getName(), joinTimeout);
        }

        // Fail any futures in the current bucket and release their resources
        var exception = new IllegalStateException("Server shutting down before batch could be written");
        if (!currentBucket.isEmpty()) {
            currentBucket.batches().forEach(this::onBatchAbandoned);
            currentBucket.futures().forEach(f -> f.completeExceptionally(exception));
        }

        // Fail any remaining tasks that weren't processed and release their resources. A timed-out
        // drain() can leave its poison pill (null bucket) in the queue, so skip it here.
        WriteTask<T, R> remaining;
        while ((remaining = writeQueue.poll()) != null) {
            if (remaining == poisonPill) {
                continue;
            }
            remaining.bucket().batches().forEach(this::onBatchAbandoned);
            remaining.bucket().futures().forEach(f -> f.completeExceptionally(exception));
        }
    }
}
