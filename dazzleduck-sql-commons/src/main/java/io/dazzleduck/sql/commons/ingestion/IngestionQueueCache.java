package io.dazzleduck.sql.commons.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * The live {@link ParquetIngestionQueue}s of an {@link IngestionHandler}, one per queue ID, for
 * handlers to implement {@link IngestionHandler#getOrCreateQueue}, {@link
 * IngestionHandler#getQueueStats} and {@link IngestionHandler#closeQueues(Duration)} with.
 *
 * <p>A handler that hands out queues must keep them: every queue owns a writer thread and buffers
 * batches until it flushes. Creating one per request — what the stateless interface default does —
 * leaks a thread and a queue on every call (they are never closed), and stops requests from
 * sharing a batch, so each one waits the full {@code max_delay} and writes its own file.
 *
 * <p>Thread-safe.
 */
public final class IngestionQueueCache {

    private static final Logger logger = LoggerFactory.getLogger(IngestionQueueCache.class);

    private final ConcurrentHashMap<String, ParquetIngestionQueue> queues = new ConcurrentHashMap<>();

    /**
     * Returns the queue for {@code queueId}, creating it exactly once. A {@code null}
     * {@code targetPath} means the queue is gone (tombstone / deleted mapping): any cached queue
     * is evicted and closed, and {@code null} is returned.
     */
    public ParquetIngestionQueue getOrCreate(String queueId,
                                             String targetPath,
                                             IngestionHandler.QueueCreator creator,
                                             IngestionHandler.QueueEventListener listener) {
        if (targetPath == null) {
            if (evict(queueId)) {
                listener.onDeleted(queueId);
            }
            return null;
        }
        return queues.computeIfAbsent(queueId, id -> {
            ParquetIngestionQueue queue = creator.create(id, targetPath);
            listener.onCreated(id);
            return queue;
        });
    }

    /** Removes and closes the queue for {@code queueId}; returns whether there was one. */
    public boolean evict(String queueId) {
        ParquetIngestionQueue removed = queues.remove(queueId);
        if (removed == null) {
            return false;
        }
        try {
            removed.close();
        } catch (Exception e) {
            logger.atWarn().setCause(e).log("Failed to close evicted queue: {}", queueId);
        }
        return true;
    }

    /** Calls {@code action} with every cached queue and its ID. */
    public void forEach(java.util.function.BiConsumer<String, ParquetIngestionQueue> action) {
        queues.forEach(action);
    }

    /** Stats of every cached queue. */
    public List<Stats> stats() {
        return queues.values().stream().map(ParquetIngestionQueue::getStats).toList();
    }

    /** Number of cached queues. */
    public int size() {
        return queues.size();
    }

    /**
     * Drains every cached queue, waiting at most {@code drainTimeout} per queue, then closes it,
     * and empties the cache.
     */
    public void closeAll(Duration drainTimeout) {
        // Snapshot and clear first so nothing is routed to these queues while they shut down.
        var snapshot = new ArrayList<>(queues.values());
        queues.clear();
        if (snapshot.isEmpty()) {
            return;
        }
        // Drain queues concurrently: total shutdown time is bounded by the slowest queue
        // (~drainTimeout), not the sum across all queues. Virtual threads keep this cheap even with
        // many queues; the executor's close() blocks until every drain+close task has finished.
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var queue : snapshot) {
                executor.submit(() -> drainAndClose(queue, drainTimeout));
            }
        }
    }

    /**
     * Drains a single queue within {@code drainTimeout}, then closes it. Drains buffered-but-unwritten
     * batches gracefully and bounds the wait so a stalled write backend can't hang shutdown; the
     * following close releases resources (and cancels/abandons anything the drain didn't finish).
     * All failures are isolated so one bad queue cannot block the others.
     */
    private static void drainAndClose(ParquetIngestionQueue queue, Duration drainTimeout) {
        String id = queue.identifier();
        try {
            if (!queue.drain(drainTimeout)) {
                logger.atWarn().log("Drain timed out after {} for ingestion queue: {}; forcing close",
                        drainTimeout, id);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.atWarn().setCause(e).log("Interrupted while draining ingestion queue: {}", id);
        }
        try {
            queue.close();
        } catch (Exception e) {
            logger.atWarn().setCause(e).log("Failed to close ingestion queue: {}", id);
        }
    }
}
