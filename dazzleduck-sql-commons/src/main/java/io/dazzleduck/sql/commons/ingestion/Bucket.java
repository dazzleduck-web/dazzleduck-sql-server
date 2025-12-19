package io.dazzleduck.sql.commons.ingestion;



import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * This is not a thread safe class and should only be used by BulkIngestionQueue.
 * @param <T>
 * @param <R>
 */
public class Bucket<T, R> {
    private final long capacity;
    private final List<Batch<T>> batches = new ArrayList<>();
    private final List<CompletableFuture<R>> futures = new ArrayList<>();
    private final Duration maxWriteDelay;
    private long size = 0;
    private Instant minReceiveInstance = Instant.MAX;
    private boolean scheduleWrite;
    private final Map<String, Long> producerMaxBatchId = new HashMap<>();

    private boolean finalized = false;

    public Bucket(long capacity, Duration maxWriteDelay) {
        this.capacity = capacity;
        this.maxWriteDelay = maxWriteDelay;
    }
    void add(Batch<T> batch, CompletableFuture<R> future) {
        if(finalized){
            throw new IllegalStateException("Bucket is already finalized: No add is allowed");
        }
        batches.add(batch);
        futures.add(future);
        if (batch.producerId() != null) {
            producerMaxBatchId.put(batch.producerId(), batch.producerBatchId());
        }
        size += batch.totalSize();
        if (batch.receivedTime().isBefore(minReceiveInstance)) {
            minReceiveInstance = batch.receivedTime();
        }
    }

    public long size() {
        return size;
    }

    public List<Batch<T>> batches() {
        return batches;
    }

    public List<CompletableFuture<R>> futures() {
        return futures;
    }

    boolean isFull() {
        return size >= capacity;
    }

    boolean isEmpty() {return size == 0;}

    public boolean timeExpired(Instant now) {
        return minReceiveInstance.plus(maxWriteDelay).isBefore(now);
    }

    public boolean readyForWrite(Instant now) {
        return !batches.isEmpty()
                && (minReceiveInstance.plus(maxWriteDelay).isBefore(now) || isFull());
    }

    public boolean isScheduleWrite(){
        return scheduleWrite;
    }

    public void writeScheduled() {
        scheduleWrite = true;
    }

    public Map<String, Long> getProducerMaxBatchId() {
        return Collections.unmodifiableMap(producerMaxBatchId);
    }

    public void markFinalized(){
        this.finalized =  true;
    }
}
