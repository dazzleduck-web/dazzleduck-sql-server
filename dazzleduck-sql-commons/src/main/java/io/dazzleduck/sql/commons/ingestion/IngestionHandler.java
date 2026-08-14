package io.dazzleduck.sql.commons.ingestion;

public interface IngestionHandler {

    PostIngestionTask createPostIngestionTask(IngestionResult ingestionResult);

    String getTargetPath(String queueId);

    default String getTransformation(String queueId) { return null; }

    String[] getPartitionBy(String queueId);

    /**
     * Derived-column projections added to the COPY relation so the identifiers returned by
     * {@link #getPartitionBy} resolve — each a full expression, e.g. {@code day("timestamp") AS "day"}.
     * Empty when the table has no non-identity partition transforms.
     */
    default String[] getPartitionProjections(String queueId) { return new String[0]; }

    /**
     * Watermark configuration for the queue, or {@code null} when none is configured. When
     * non-null, {@link ParquetIngestionQueue} computes the watermark rows at write time (from the
     * same relation the output is written from) and carries them on the {@link IngestionResult}.
     */
    default WatermarkSpec getWatermarkSpec(String queueId) { return null; }

    default boolean supportPartitionByHeader() { return true; }

    // -----------------------------------------------------------------------
    // Queue lifecycle
    // -----------------------------------------------------------------------

    /** Creates a new {@link ParquetIngestionQueue} for the given queue ID and target path. */
    @FunctionalInterface
    interface QueueCreator {
        ParquetIngestionQueue create(String queueId, String targetPath);
    }

    /** Receives lifecycle events so callers can record metrics without the handler needing a recorder. */
    interface QueueEventListener {
        void onCreated(String queueId);
        default void onRefreshed(String queueId) {}
        default void onDeleted(String queueId) {}
    }

    /** Returns stats for all currently cached queues. */
    default java.util.List<Stats> getQueueStats() { return java.util.List.of(); }

    /** Closes and clears all cached queues on producer shutdown. */
    default void closeQueues() {}

    /** Default per-queue bound for flushing in-flight batches on shutdown before forcing the close. */
    java.time.Duration DEFAULT_DRAIN_TIMEOUT = java.time.Duration.ofSeconds(30);

    /**
     * Like {@link #closeQueues()}, but flushes each queue gracefully, waiting at most
     * {@code drainTimeout} per queue before forcing the close. Override in handlers that own
     * queues; the default delegates to {@link #closeQueues()}.
     */
    default void closeQueues(java.time.Duration drainTimeout) { closeQueues(); }

    /**
     * Returns the set of queue IDs this handler currently knows about — the single source of
     * truth for which queues are routable. Callers (e.g. the OTLP collector's signal services)
     * accept a request for a queue only when it is in this set; an unknown queue is rejected.
     * An empty set means "no queues are currently configured" (reject everything), which is a
     * real state, not "unknown". Static handlers return a fixed set (their configured mappings);
     * dynamic handlers (e.g. SQLite-backed) return a live set that changes as the registry is
     * reloaded.
     */
    default java.util.Set<String> getKnownQueues() { return java.util.Set.of(); }

    /**
     * Returns the live queue for {@code queueId}, creating or refreshing it as necessary.
     * Returns {@code null} when the queue's target path is gone (tombstone / deleted mapping).
     *
     * <p>The default implementation is stateless: it creates a fresh queue on every call.
     * Override in stateful handlers (e.g. {@link DuckLakeIngestionHandler}) to add
     * caching and lazy-refresh semantics.
     */
    default ParquetIngestionQueue getOrCreateQueue(String queueId,
                                                   QueueCreator creator,
                                                   QueueEventListener listener) {
        String path = getTargetPath(queueId);
        if (path == null) {
            listener.onDeleted(queueId);
            return null;
        }
        ParquetIngestionQueue q = creator.create(queueId, path);
        listener.onCreated(queueId);
        return q;
    }
}
