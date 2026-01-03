package io.dazzleduck.sql.collector.metricscollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe buffer for collected metrics.
 * Supports tailing mode - metrics are buffered until flushed based on threshold or interval.
 */
public class MetricsBuffer {

    private static final Logger log = LoggerFactory.getLogger(MetricsBuffer.class);

    private final ConcurrentLinkedQueue<CollectedMetric> buffer = new ConcurrentLinkedQueue<>();
    private final AtomicInteger size = new AtomicInteger(0);
    private final int maxBufferSize;

    public MetricsBuffer(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    /**
     * Add a metric to the buffer.
     * If buffer is full, oldest metrics are dropped.
     */
    public void add(CollectedMetric metric) {
        if (size.get() >= maxBufferSize) {
            CollectedMetric dropped = buffer.poll();
            if (dropped != null) {
                size.decrementAndGet();
                log.warn("Buffer full, dropped metric: {}", dropped.name());
            }
        }
        buffer.offer(metric);
        size.incrementAndGet();
    }

    /**
     * Add multiple metrics to the buffer.
     */
    public void addAll(List<CollectedMetric> metrics) {
        for (CollectedMetric metric : metrics) {
            add(metric);
        }
    }

    /**
     * Drain all metrics from the buffer.
     */
    public List<CollectedMetric> drain() {
        List<CollectedMetric> drained = new ArrayList<>();
        CollectedMetric metric;
        while ((metric = buffer.poll()) != null) {
            drained.add(metric);
            size.decrementAndGet();
        }
        return drained;
    }

    /**
     * Return metrics to buffer for retry.
     */
    public void returnForRetry(List<CollectedMetric> metrics) {
        for (CollectedMetric metric : metrics) {
            buffer.offer(metric);
            size.incrementAndGet();
        }
    }

    /**
     * Get current buffer size.
     */
    public int getSize() {
        return size.get();
    }

    /**
     * Check if buffer is empty.
     */
    public boolean isEmpty() {
        return size.get() == 0;
    }

    /**
     * Clear the buffer.
     */
    public void clear() {
        buffer.clear();
        size.set(0);
    }
}
