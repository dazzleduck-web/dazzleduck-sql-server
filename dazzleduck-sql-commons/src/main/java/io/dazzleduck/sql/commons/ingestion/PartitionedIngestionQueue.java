package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A {@link ParquetIngestionQueue} that splits one logical ingestion queue into {@code numPartitions}
 * hash-routed child queues. Each child is a fully independent {@link ParquetIngestionQueue} (its own
 * batching, backpressure, flush loop and write path). All children write into the parent's output
 * path — the on-disk layout is identical to an unpartitioned queue, so Hive partition pruning and
 * plain directory globs keep working — and share the parent's queue id so the transformation,
 * watermark spec and DuckLake catalog registration all resolve to the same table. Files never
 * collide: unpartitioned writes are named {@code dd_<uuid>} and {@code PARTITION_BY} writes use
 * {@code APPEND}, which generates unique names.
 *
 * <p><b>Routing.</b> On {@link #add}, the partition index of a batch is computed once as
 * {@code hash(partitionExpression) % numPartitions}, evaluated over the batch's raw input rows. A
 * batch whose rows all map to a single partition is forwarded to that child unchanged. A batch whose
 * rows span more than one partition is <b>rejected</b> (its future fails with an
 * {@link IllegalArgumentException}) and its staged input file is deleted — the batch is not written
 * anywhere. This makes the producer responsible for pre-partitioning its data; the server enforces
 * the invariant rather than silently splitting or dropping rows.
 *
 * <p>Because this type extends {@link ParquetIngestionQueue}, it drops into every place the codebase
 * already hands around a queue (the Flight producer, the HTTP adaptor and the OTLP collector all go
 * through {@code getOrCreateQueue(...) -> queue.add(batch)}), so no entry point needs to know whether
 * a queue is partitioned. The parent's own accept/flush machinery is unused — {@link #add} is
 * overridden to delegate to a child — so the metric accessors below aggregate across the children.
 */
public class PartitionedIngestionQueue extends ParquetIngestionQueue {

    private static final Logger logger = LoggerFactory.getLogger(PartitionedIngestionQueue.class);

    private final String queueId;
    private final String inputFormat;
    private final int numPartitions;
    private final String partitionExpression;
    private final List<ParquetIngestionQueue> children;
    private final java.util.concurrent.atomic.LongAccumulator rejectedMultiPartition =
            new java.util.concurrent.atomic.LongAccumulator(Long::sum, 0L);

    public PartitionedIngestionQueue(String applicationId,
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
                                     Clock clock,
                                     int numPartitions,
                                     String partitionExpression) {
        super(applicationId, inputFormat, outputPath, ingestionQueue, minBucketSize, maxBucketSize, maxBatches,
                maxPendingWrite, maxDelay, parquetCompression, postIngestionHandler, executorService, clock);
        if (numPartitions <= 1) {
            throw new IllegalArgumentException(
                    "Queue '%s': PartitionedIngestionQueue requires numPartitions > 1, got %d"
                            .formatted(ingestionQueue, numPartitions));
        }
        if (partitionExpression == null || partitionExpression.isBlank()) {
            throw new IllegalArgumentException(
                    "Queue '%s': PartitionedIngestionQueue requires a non-blank partitionExpression"
                            .formatted(ingestionQueue));
        }
        this.queueId = ingestionQueue;
        this.inputFormat = inputFormat;
        this.numPartitions = numPartitions;
        this.partitionExpression = partitionExpression;
        List<ParquetIngestionQueue> built = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            // Children are plain queues with the parent's exact configuration (same queue id, so the
            // handler resolves the same table/transform; same output path); they differ only in
            // their own batching/flush/backpressure state. Note min_bucket_size, max_delay and
            // max_pending_write therefore apply per child, not to the logical queue as a whole.
            built.add(new ParquetIngestionQueue(applicationId, inputFormat, outputPath, ingestionQueue,
                    minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, parquetCompression,
                    postIngestionHandler, executorService, clock));
        }
        this.children = List.copyOf(built);
    }

    @Override
    public CompletableFuture<IngestionResult> add(Batch<String> batch) {
        int partition;
        try {
            partition = resolvePartition(batch);
        } catch (Exception e) {
            // A failure to evaluate the expression is a server-side problem (transient read/connection
            // error, or a misconfigured expression), not the caller's fault — surface it as a retryable
            // error (HTTP 503 / Flight UNAVAILABLE) so the producer resends rather than dropping.
            deleteInput(batch);
            return CompletableFuture.failedFuture(new PartitionEvaluationException(
                    "Queue '%s': failed to evaluate partition expression '%s' for batch: %s"
                            .formatted(queueId, partitionExpression, e.getMessage()), e));
        }
        if (partition == MULTIPLE_PARTITIONS) {
            rejectedMultiPartition.accumulate(1);
            deleteInput(batch);
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    ("Queue '%s': batch rejected — its rows span more than one partition "
                            + "(num_partitions=%d, partition_expression='%s'). A batch must contain rows for "
                            + "exactly one partition; pre-partition the data on the producer.")
                            .formatted(queueId, numPartitions, partitionExpression)));
        }
        return children.get(partition).add(batch);
    }

    /** Sentinel returned by {@link #resolvePartition} when a batch's rows map to more than one partition. */
    private static final int MULTIPLE_PARTITIONS = -1;

    /**
     * Returns the single partition index every row of {@code batch} maps to, {@code 0} for an empty
     * batch, or {@link #MULTIPLE_PARTITIONS} when the rows span more than one partition.
     */
    private int resolvePartition(Batch<String> batch) throws Exception {
        String relation = "read_%s(['%s'])".formatted(inputFormat, batch.record());
        // hash() is unsigned; cast the modulo (0 .. numPartitions-1) to BIGINT for a clean getLong().
        String sql = ("SELECT count(DISTINCT part) AS distinct_count, min(part) AS partition "
                + "FROM (SELECT (hash(%s) %% %d)::BIGINT AS part FROM %s)")
                .formatted(partitionExpression, numPartitions, relation);
        try (var conn = ConnectionPool.getConnection();
             var stmt = conn.createStatement();
             var rs = stmt.executeQuery(sql)) {
            if (!rs.next()) {
                return 0; // no rows at all
            }
            long distinct = rs.getLong("distinct_count");
            if (distinct == 0) {
                return 0; // empty batch — route to partition 0 (writes nothing)
            }
            if (distinct > 1) {
                return MULTIPLE_PARTITIONS;
            }
            return (int) rs.getLong("partition");
        }
    }

    private void deleteInput(Batch<String> batch) {
        try {
            Files.deleteIfExists(Path.of(batch.record()));
        } catch (Exception e) {
            logger.warn("Queue '{}': failed to delete rejected batch input file {}", queueId, batch.record(), e);
        }
    }

    // -----------------------------------------------------------------------
    // Metric accessors — aggregate across children, since the parent's own
    // accept/flush machinery is bypassed by the overridden add().
    // -----------------------------------------------------------------------

    private long sum(java.util.function.ToLongFunction<ParquetIngestionQueue> f) {
        long total = 0;
        for (ParquetIngestionQueue child : children) {
            total += f.applyAsLong(child);
        }
        return total;
    }

    @Override public long getTotalWriteBatches()   { return sum(ParquetIngestionQueue::getTotalWriteBatches); }
    @Override public long getTotalWriteBuckets()   { return sum(ParquetIngestionQueue::getTotalWriteBuckets); }
    @Override public long getTotalWriteBytes()     { return sum(ParquetIngestionQueue::getTotalWriteBytes); }
    @Override public long getTimeSpentWriting()    { return sum(ParquetIngestionQueue::getTimeSpentWriting); }
    @Override public long getFailedWriteBytes()    { return sum(ParquetIngestionQueue::getFailedWriteBytes); }
    @Override public long getFailedWriteBatches()  { return sum(ParquetIngestionQueue::getFailedWriteBatches); }
    @Override public long getFailedWriteBuckets()  { return sum(ParquetIngestionQueue::getFailedWriteBuckets); }
    @Override public long getProducerIdEvictions() { return sum(ParquetIngestionQueue::getProducerIdEvictions); }
    @Override public long getPendingBatches()      { return sum(ParquetIngestionQueue::getPendingBatches); }
    @Override public long getPendingBuckets()      { return sum(ParquetIngestionQueue::getPendingBuckets); }
    @Override public long pendingWrite()           { return sum(ParquetIngestionQueue::pendingWrite); }
    @Override public long getDataPhaseNanos()      { return sum(ParquetIngestionQueue::getDataPhaseNanos); }
    @Override public long getPostIngestPhaseNanos(){ return sum(ParquetIngestionQueue::getPostIngestPhaseNanos); }

    @Override
    public Stats getStats() {
        // Fold each child's own Stats (relabeled p0..pN-1) into one aggregated row; the children
        // carry every per-queue counter already, so summing their Stats keeps this in lockstep with
        // whatever BulkIngestQueue/ParquetIngestionQueue expose without re-deriving each field here.
        List<Stats> childStats = new ArrayList<>(children.size());
        Stats.Builder agg = Stats.builder(queueId);
        long bytes = 0, batches = 0, buckets = 0, timeW = 0, pBatches = 0, pBuckets = 0, pBytes = 0, maxPend = 0;
        long fBytes = 0, fBatches = 0, fBuckets = 0, evict = 0, rows = 0, dataMs = 0, postMs = 0, r429 = 0, rOos = 0;
        long lastWrite = 0, lastReceive = 0, lastErrMs = 0;
        String lastErr = null;
        long[] rowsPerMin = new long[BulkIngestQueue.HISTORY_MINUTES];
        long[] batchesPerMin = new long[BulkIngestQueue.HISTORY_MINUTES];
        for (int i = 0; i < children.size(); i++) {
            Stats c = children.get(i).getStats();
            childStats.add(c.withIdentifier("p" + i));
            bytes += c.totalWriteBytes();       batches += c.totalWriteBatches();   buckets += c.totalWriteBuckets();
            timeW += c.timeSpentWriting();       pBatches += c.pendingBatches();     pBuckets += c.pendingBuckets();
            pBytes += c.pendingBytes();          maxPend += c.maxPendingWrite();     fBytes += c.failedWriteBytes();
            fBatches += c.failedWriteBatches();  fBuckets += c.failedWriteBuckets(); evict += c.producerIdEvictions();
            rows += c.rowsWritten();             dataMs += c.dataPhaseMillis();      postMs += c.postIngestMillis();
            r429 += c.rejected429();             rOos += c.rejectedOutOfSequence();
            lastWrite = Math.max(lastWrite, c.lastWriteEpochMs());
            lastReceive = Math.max(lastReceive, c.lastReceiveEpochMs());
            if (c.lastErrorEpochMs() > lastErrMs) { lastErrMs = c.lastErrorEpochMs(); lastErr = c.lastError(); }
            addElementwise(rowsPerMin, c.rowsWrittenPerMinute());
            addElementwise(batchesPerMin, c.batchesReceivedPerMinute());
        }
        return agg.totalWriteBytes(bytes).totalWriteBatches(batches).totalWriteBuckets(buckets)
                .timeSpentWriting(timeW).pendingBatches(pBatches).pendingBuckets(pBuckets)
                .pendingBytes(pBytes).maxPendingWrite(maxPend).failedWriteBytes(fBytes)
                .failedWriteBatches(fBatches).failedWriteBuckets(fBuckets).producerIdEvictions(evict)
                .rowsWritten(rows).dataPhaseMillis(dataMs).postIngestMillis(postMs)
                .rejected429(r429).rejectedOutOfSequence(rOos).rejectedMultiPartition(rejectedMultiPartition.get())
                .lastWriteEpochMs(lastWrite).lastReceiveEpochMs(lastReceive)
                .lastErrorEpochMs(lastErrMs).lastError(lastErr)
                .rowsWrittenPerMinute(rowsPerMin).batchesReceivedPerMinute(batchesPerMin)
                .partitions(childStats)
                .build();
    }

    /** Adds {@code src} into {@code dst} element-wise (aligned indices; ignores a length mismatch tail). */
    private static void addElementwise(long[] dst, long[] src) {
        int n = Math.min(dst.length, src.length);
        for (int i = 0; i < n; i++) {
            dst[i] += src[i];
        }
    }

    /** Batches rejected because their rows spanned more than one partition. */
    public long getRejectedMultiPartition() {
        return rejectedMultiPartition.get();
    }

    @Override
    public boolean drain(Duration timeout) throws InterruptedException {
        boolean allDrained = true;
        for (ParquetIngestionQueue child : children) {
            allDrained &= child.drain(timeout);
        }
        // Drain the (idle) parent last so its write thread also stops.
        allDrained &= super.drain(timeout);
        return allDrained;
    }

    @Override
    public void drain() throws InterruptedException {
        for (ParquetIngestionQueue child : children) {
            child.drain();
        }
        super.drain();
    }

    @Override
    public void close() throws Exception {
        Exception first = null;
        for (ParquetIngestionQueue child : children) {
            try {
                child.close();
            } catch (Exception e) {
                logger.warn("Queue '{}': failed to close child partition queue", queueId, e);
                if (first == null) first = e;
            }
        }
        try {
            super.close();
        } catch (Exception e) {
            if (first == null) first = e;
        }
        if (first != null) {
            throw first;
        }
    }

    /** Live child queues, in partition-index order. Exposed for tests and metrics. */
    public List<ParquetIngestionQueue> children() {
        return children;
    }

    public int numPartitions() {
        return numPartitions;
    }
}
