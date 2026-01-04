package io.dazzleduck.sql.logback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe buffer for log entries with size limits to prevent OOM.
 */
public class LogBuffer {

    private final ConcurrentLinkedQueue<LogEntry> queue;
    private final AtomicInteger size;
    private final int maxSize;

    // Secondary buffer for failed sends (retry queue)
    private final ConcurrentLinkedQueue<LogEntry> retryQueue;
    private final AtomicInteger retrySize;

    public LogBuffer(int maxSize) {
        this.queue = new ConcurrentLinkedQueue<>();
        this.retryQueue = new ConcurrentLinkedQueue<>();
        this.size = new AtomicInteger(0);
        this.retrySize = new AtomicInteger(0);
        this.maxSize = maxSize;
    }

    /**
     * Add a log entry to the buffer.
     * Returns false if buffer is full (entry dropped).
     */
    public boolean offer(LogEntry entry) {
        if (size.get() >= maxSize) {
            return false;
        }
        queue.offer(entry);
        size.incrementAndGet();
        return true;
    }

    /**
     * Drain all entries from the buffer for sending.
     * Returns entries from retry queue first, then main queue.
     */
    public List<LogEntry> drain() {
        List<LogEntry> entries = new ArrayList<>();

        // First drain retry queue (failed previous sends)
        LogEntry entry;
        while ((entry = retryQueue.poll()) != null) {
            entries.add(entry);
            retrySize.decrementAndGet();
        }

        // Then drain main queue
        while ((entry = queue.poll()) != null) {
            entries.add(entry);
            size.decrementAndGet();
        }

        return entries;
    }

    /**
     * Return entries to retry queue after failed send.
     */
    public void returnForRetry(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            if (retrySize.get() < maxSize) {
                retryQueue.offer(entry);
                retrySize.incrementAndGet();
            }
            // Drop if retry queue is also full
        }
    }

    /**
     * Get current buffer size (main + retry).
     */
    public int getSize() {
        return size.get() + retrySize.get();
    }

    /**
     * Check if buffer is empty.
     */
    public boolean isEmpty() {
        return queue.isEmpty() && retryQueue.isEmpty();
    }
}
