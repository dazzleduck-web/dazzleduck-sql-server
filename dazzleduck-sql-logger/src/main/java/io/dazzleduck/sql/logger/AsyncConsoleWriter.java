package io.dazzleduck.sql.logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Async console writer that offloads stderr writes to a dedicated thread.
 * Application threads do a non-blocking queue offer and never block on I/O.
 * Only the single writer thread calls System.err, eliminating lock contention.
 */
class AsyncConsoleWriter {

    private static final int DEFAULT_QUEUE_CAPACITY = 8192;
    private static final int DRAIN_BATCH_SIZE = 63;

    private final BlockingQueue<String> queue;
    private final Thread writerThread;
    private volatile boolean running = true;

    AsyncConsoleWriter() {
        this(DEFAULT_QUEUE_CAPACITY);
    }

    AsyncConsoleWriter(int queueCapacity) {
        this.queue = new LinkedBlockingQueue<>(queueCapacity);
        this.writerThread = new Thread(this::drainLoop, "async-console-writer");
        this.writerThread.setDaemon(true);
        this.writerThread.start();
    }

    /**
     * Queue a message for async writing. Non-blocking.
     * If the queue is full, the message is silently dropped
     * (preferable to blocking the application thread).
     */
    void write(String message) {
        queue.offer(message);
    }

    private void drainLoop() {
        List<String> batch = new ArrayList<>();
        while (running || !queue.isEmpty()) {
            try {
                String first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first != null) {
                    batch.add(first);
                    queue.drainTo(batch, DRAIN_BATCH_SIZE);
                    for (int i = 0; i < batch.size(); i++) {
                        System.err.println(batch.get(i));
                    }
                    batch.clear();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // Flush remaining on shutdown
        queue.drainTo(batch);
        for (int i = 0; i < batch.size(); i++) {
            System.err.println(batch.get(i));
        }
    }

    void shutdown() {
        running = false;
        writerThread.interrupt();
        try {
            writerThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
