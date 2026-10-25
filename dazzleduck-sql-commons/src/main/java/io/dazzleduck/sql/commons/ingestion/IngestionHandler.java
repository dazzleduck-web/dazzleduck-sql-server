package io.dazzleduck.sql.commons.ingestion;

public interface IngestionHandler {

    /** Name of the per-row JWT-claims MAP column stamped when {@link #extractClaims} is on. */
    String CLAIMS_COLUMN = "claims";

    PostIngestionTask createPostIngestionTask(IngestionResult ingestionResult);

    String getTargetPath(String queueId);

    default String getTransformation(String queueId) { return null; }

    /**
     * DuckDB session variables to set on the connection that writes the queue's batch, so its
     * transformation can read them with {@code getvariable('name')}. Empty when none are
     * configured; never null.
     *
     * <p>Read on every write, like {@link #getTransformation}, so a handler that reloads them
     * (see {@link IngestionVariables}) needs no notification to take effect.
     */
    default java.util.Map<String, String> getVariables(String queueId) { return java.util.Map.of(); }

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

    /**
     * The queue's attribute type registry table ({@link QueueIdToTableMapping#attributeTypesTable()}),
     * or {@code null} when the queue records none. Unqualified: the post-ingestion task resolves it
     * in the queue's own catalog and schema. Queues naming the same table in the same schema share
     * it; that is the configuration's choice, not a rule of this interface.
     */
    default String getAttributeTypesTable(String queueId) { return null; }

    /**
     * The same table as {@link #getAttributeTypesTable}, fully qualified as
     * {@code catalog.schema.table}, or {@code null} when the queue records none. This is how a
     * producer finds the table to READ — to build its cache of what is already recorded at startup
     * — since the name is otherwise resolved only once a batch is in flight.
     */
    default String getAttributeTypesTableRef(String queueId) { return null; }

    /**
     * The directory that table's data files live in, or {@code null} when the queue records none.
     * A producer needs it because its rows are written as a Parquet file and registered in place
     * (see {@link SidecarRows}), so the file has to land under that table's own path. Resolvable
     * only once the table exists in the catalog.
     */
    default String getAttributeTypesTablePath(String queueId) { return null; }

    /**
     * The queue's own cap on held attribute type mappings, or null to use the process-wide default.
     * See {@code QueueIdToTableMapping#attributeTypesMaxEntries()}.
     */
    default Integer getAttributeTypesMaxEntries(String queueId) { return null; }

    /**
     * Re-reads, right now, whatever catalog-derived state the queue's writes depend on (its
     * transformation and partitioning), ignoring any refresh interval. Called by
     * {@link ParquetIngestionQueue} after a failed write, so a retry is built from the current
     * catalog instead of the cached state that may have caused the failure — for example a
     * partition spec changed by {@code ALTER TABLE ... SET PARTITIONED BY}, which makes
     * registering files written under the old spec fail. A no-op for handlers without cached state.
     */
    default void refreshNow(String queueId) {}

    default boolean supportPartitionByHeader() { return true; }

    /** Whether ingested rows carry a {@value #CLAIMS_COLUMN} MAP column from the caller's JWT. */
    default boolean extractClaims(String queueId) { return false; }

    /**
     * Number of hash-routed sub-queues {@code queueId} is split into ({@code >= 1}; {@code 1} means
     * not partitioned). When {@code > 1}, {@link #getOrCreateQueue}'s creator is expected to build a
     * {@link PartitionedIngestionQueue} that routes each batch by
     * {@code hash(getPartitionExpression) % numPartitions}.
     */
    default int getNumPartitions(String queueId) { return 1; }

    /**
     * SQL expression over the raw input row that the partition index is derived from (see
     * {@link #getNumPartitions}). Non-null only when the queue is partitioned.
     */
    default String getPartitionExpression(String queueId) { return null; }

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
     * <p>The default implementation is stateless: it creates a fresh queue on EVERY call and never
     * closes it. That is only right for a caller that owns and closes the queue it gets. A handler
     * given to a server (the Flight/HTTP producer, the OTLP collector), which calls this once per
     * request, MUST override it to return the same queue each time — otherwise every request leaks
     * a queue and its writer thread, and no two requests share a batch. Implement it, together with
     * {@link #getQueueStats} and {@link #closeQueues(java.time.Duration)}, with an
     * {@link IngestionQueueCache}, as {@link DuckLakeIngestionHandler} and the handler of
     * {@link NOOPIngestionTaskFactoryProvider} do.
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
