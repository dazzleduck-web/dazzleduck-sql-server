package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.util.HeaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * {@link IngestionHandler} backed by DuckLake metadata.
 *
 * <p>Two independent caches are maintained per queue:
 * <ul>
 *   <li><b>stateCache</b> — the refreshable state: target path, resolved transformation SQL,
 *       and partition columns. All three are resolved together at construction time and again
 *       lazily whenever {@link #getTargetPath}, {@link #getTransformation}, or
 *       {@link #getPartitionBy} are called and the cached state is older than
 *       {@code refreshInterval}. Read accessors call {@link #getOrRefreshState} which uses
 *       {@code stateCache.compute()} to atomically check and refresh — no DuckDB round-trip
 *       on the hot write path when state is fresh.</li>
 *   <li><b>queueCache</b> — the {@link ParquetIngestionQueue} instance. The queue is created
 *       exactly once per queue ID (via {@link #getOrCreateQueue}) and is never replaced unless
 *       the target path disappears and the queue is evicted.</li>
 * </ul>
 */
public class DuckLakeIngestionHandler implements IngestionHandler {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeIngestionHandler.class);

    private static final String TABLE_PATH_QUERY =
            """
            SELECT CASE WHEN s.path_is_relative
                        THEN concat(rtrim(m."value", '/'), '/', rtrim(s.path, '/'), '/', rtrim(t.path, '/'))
                        ELSE concat(rtrim(s.path, '/'), '/', rtrim(t.path, '/'))
                   END AS path
            FROM %s.ducklake_schema s
            JOIN %s.ducklake_table t ON (s.schema_id = t.schema_id)
            CROSS JOIN %s.ducklake_metadata m
            WHERE m.key = 'data_path'
              AND s.schema_name = '%s'
              AND t.table_name = '%s'
              AND s.end_snapshot IS NULL
              AND t.end_snapshot IS NULL
            """;

    // Mutable so subclasses (e.g. DynamicIngestionHandler) can reconcile the set at runtime via
    // updateMappings(); concurrent because read accessors run on request threads while a reload
    // thread may be updating it.
    private final Map<String, QueueIdToTableMapping> queueIdsToTableMappings;

    // -----------------------------------------------------------------------
    // Refreshable state cache — populated at init, refreshed lazily
    // -----------------------------------------------------------------------

    /**
     * {@code schemaChangeId} is MAX(schema_version) from ducklake_snapshot.
     * It increments only on DDL (CREATE/ALTER TABLE, view changes), not on data ingestion.
     * {@code refreshedAt} is the clock instant when this state was last confirmed/rebuilt.
     */
    private record QueueState(String targetPath, String transformation, String[] partitionColumns,
                               String[] partitionProjections, long schemaChangeId, Instant refreshedAt) {}

    /**
     * A resolved partition column: {@code token} is the identifier placed in COPY's
     * {@code PARTITION_BY} clause; {@code projection} is the derived-column expression that must be
     * added to the COPY relation for the token to resolve, or {@code null} for identity columns
     * (which already exist in the relation).
     */
    private record ResolvedPartition(String token, String projection) {}

    private final ConcurrentHashMap<String, QueueState> stateCache = new ConcurrentHashMap<>();

    // -----------------------------------------------------------------------
    // Queue lifecycle cache — queue created once, evicted only on tombstone
    // -----------------------------------------------------------------------

    private final ConcurrentHashMap<String, ParquetIngestionQueue> queueCache = new ConcurrentHashMap<>();

    private final Duration refreshInterval;
    private final Clock clock;

    public DuckLakeIngestionHandler(Map<String, QueueIdToTableMapping> mappings, Duration refreshInterval) {
        this(mappings, refreshInterval, Clock.systemUTC());
    }

    public DuckLakeIngestionHandler(Map<String, QueueIdToTableMapping> mappings, Duration refreshInterval, Clock clock) {
        this.queueIdsToTableMappings = new ConcurrentHashMap<>(mappings);
        this.refreshInterval = refreshInterval;
        this.clock = clock;
        mappings.forEach((id, mapping) -> stateCache.put(id, buildState(mapping, clock.instant())));
    }

    /**
     * Reconciles the queue→table mapping set to {@code fresh} (add/replace changed entries, drop
     * removed ones, evicting and closing their cached queues). Derived state for added/changed
     * queues is invalidated and rebuilt lazily on next access (which also defers the DuckLake
     * metadata read until the table actually exists). Intended for a dynamic source that detects
     * registry changes; call it from a single reconcile thread.
     */
    protected void updateMappings(Map<String, QueueIdToTableMapping> fresh) {
        fresh.forEach((id, mapping) -> {
            QueueIdToTableMapping previous = queueIdsToTableMappings.put(id, mapping);
            if (previous == null || !previous.equals(mapping)) {
                stateCache.remove(id); // force a lazy rebuild of the DuckLake-derived state
                onMappingReconciled(id, mapping);
            }
        });
        queueIdsToTableMappings.keySet().removeIf(id -> {
            if (fresh.containsKey(id)) return false;
            stateCache.remove(id);
            ParquetIngestionQueue removed = queueCache.remove(id);
            if (removed != null) {
                try { removed.close(); } catch (Exception e) {
                    logger.atWarn().setCause(e).log("Failed to close evicted queue: {}", id);
                }
            }
            return true;
        });
    }

    /**
     * Hook invoked from {@link #updateMappings} for each added or changed mapping (before its derived
     * state is rebuilt lazily). Default is a no-op; {@link DynamicIngestionHandler} overrides it to
     * create/evolve the backing DuckLake table when {@code manageTables} is enabled.
     */
    protected void onMappingReconciled(String queueId, QueueIdToTableMapping mapping) {}

    /**
     * Convenience constructor that defaults {@code refreshInterval} to 2 minutes.
     * Used by tests and legacy callers that do not need to tune the interval.
     */
    public DuckLakeIngestionHandler(Map<String, QueueIdToTableMapping> mappings) {
        this(mappings, Duration.ofMinutes(2));
    }

    // -----------------------------------------------------------------------
    // IngestionHandler — read accessors (refresh lazily when stale)
    // -----------------------------------------------------------------------

    @Override
    public String getTargetPath(String queueId) {
        QueueState s = getOrRefreshState(queueId);
        return s != null ? s.targetPath() : null;
    }

    @Override
    public String getTransformation(String queueId) {
        QueueState s = getOrRefreshState(queueId);
        return s != null ? s.transformation() : null;
    }

    @Override
    public String[] getPartitionBy(String queueId) {
        QueueState s = getOrRefreshState(queueId);
        return s != null ? s.partitionColumns() : new String[0];
    }

    @Override
    public String[] getPartitionProjections(String queueId) {
        QueueState s = getOrRefreshState(queueId);
        return s != null ? s.partitionProjections() : new String[0];
    }

    @Override
    public java.util.Set<String> getKnownQueues() {
        // The configured mappings are this handler's fixed, authoritative queue set.
        return queueIdsToTableMappings.keySet();
    }

    @Override
    public PostIngestionTask createPostIngestionTask(IngestionResult result) {
        QueueIdToTableMapping mapping = queueIdsToTableMappings.get(result.queueName());
        if (mapping == null) mapping = queueIdsToTableMappings.get(extractSuffix(result.queueName()));
        if (mapping == null) {
            // No DuckLake mapping for this queue — write-only mode, no catalog registration.
            logger.atDebug().log("No DuckLake mapping for queue '{}', skipping catalog registration", result.queueName());
            return PostIngestionTask.NOOP;
        }
        return new DuckLakePostIngestionTask(result, mapping.catalog(), mapping.table(), mapping.schema(),
                mapping.additionalParameters());
    }

    // -----------------------------------------------------------------------
    // Queue lifecycle — queue is created once; state is refreshed lazily
    // -----------------------------------------------------------------------

    /**
     * Returns the live queue for {@code queueId}, creating it on first call or evicting and
     * returning {@code null} when the target path has disappeared.
     *
     * <p>State is always up-to-date when this method returns: {@link #getTargetPath} delegates
     * to {@link #getOrRefreshState}, which uses {@code stateCache.compute()} to refresh atomically.
     */
    @Override
    public ParquetIngestionQueue getOrCreateQueue(String queueId,
                                                  QueueCreator creator,
                                                  QueueEventListener listener) {
        String path = getTargetPath(queueId); // triggers getOrRefreshState internally
        if (path == null) {
            // Path gone: evict any existing queue.
            ParquetIngestionQueue removed = queueCache.remove(queueId);
            if (removed != null) {
                listener.onDeleted(queueId);
                try { removed.close(); } catch (Exception e) {
                    logger.atWarn().setCause(e).log("Failed to close evicted queue: {}", queueId);
                }
            }
            return null;
        }
        // Create queue exactly once per ID.
        return queueCache.compute(queueId, (id, existing) -> {
            if (existing != null) return existing;
            ParquetIngestionQueue q = creator.create(id, path);
            listener.onCreated(id);
            return q;
        });
    }

    @Override
    public java.util.List<Stats> getQueueStats() {
        return queueCache.values().stream()
                .map(ParquetIngestionQueue::getStats)
                .toList();
    }

    @Override
    public void closeQueues() {
        closeQueues(DEFAULT_DRAIN_TIMEOUT);
    }

    @Override
    public void closeQueues(Duration drainTimeout) {
        // Snapshot and clear first so nothing is routed to these queues while they shut down.
        var queues = new ArrayList<>(queueCache.values());
        queueCache.clear();
        if (queues.isEmpty()) {
            return;
        }
        // Drain queues concurrently: total shutdown time is bounded by the slowest queue
        // (~drainTimeout), not the sum across all queues. Virtual threads keep this cheap even with
        // many queues; the executor's close() blocks until every drain+close task has finished.
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var queue : queues) {
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
    private void drainAndClose(ParquetIngestionQueue queue, Duration drainTimeout) {
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

    // -----------------------------------------------------------------------
    // Lazy refresh — single entry point used by all read accessors
    // -----------------------------------------------------------------------

    /**
     * Returns the current (possibly just-refreshed) {@link QueueState} for {@code queueId},
     * or {@code null} if the ID is unknown.
     *
     * <p>Uses {@code stateCache.compute()} to atomically check staleness and refresh in one
     * operation — no separate read-then-write, no double map lookup.
     */
    private QueueState getOrRefreshState(String queueId) {
        String key = resolveStateKey(queueId);
        if (key == null) return null;
        return stateCache.compute(key, (k, existing) -> {
            if (existing != null && !existing.refreshedAt().plus(refreshInterval).isBefore(clock.instant()))
                return existing; // still fresh — nothing to do
            return computeRefreshedState(k, existing);
        });
    }

    /**
     * Resolves the canonical mapping key for {@code queueId}: exact match first, then the last path
     * segment as a fallback for path-style IDs like {@code /some/path/tableName}.
     *
     * <p>Resolved against the mapping set (the authoritative source), not {@code stateCache}: a queue
     * may be known but have no cached state yet — at startup for the static handler, and after every
     * {@link #updateMappings} for the dynamic handler, which invalidates state for lazy rebuild.
     * {@link #getOrRefreshState} then builds the state on first access via {@code stateCache.compute}.
     */
    private String resolveStateKey(String queueId) {
        if (queueIdsToTableMappings.containsKey(queueId)) return queueId;
        String suffix = extractSuffix(queueId);
        return queueIdsToTableMappings.containsKey(suffix) ? suffix : null;
    }

    // -----------------------------------------------------------------------
    // State resolution
    // -----------------------------------------------------------------------

    /**
     * Compute function for {@code stateCache.compute()}: checks {@code schema_version} first
     * (cheap — one DuckDB round-trip). If unchanged, only bumps {@code refreshedAt}.
     * If changed (or no existing state), does a full rebuild of path, transformation, and
     * partition columns.
     */
    private QueueState computeRefreshedState(String key, QueueState existing) {
        QueueIdToTableMapping mapping = queueIdsToTableMappings.get(key);
        if (mapping == null) mapping = queueIdsToTableMappings.get(extractSuffix(key));
        if (mapping == null) return existing; // unknown queue — leave unchanged

        long currentSchemaChangeId = fetchSchemaChangeId(mapping.catalog(), mapping.schema(), mapping.table());
        if (existing != null && existing.schemaChangeId() == currentSchemaChangeId) {
            logger.atDebug().log("Schema unchanged (id={}) for {}.{}.{}, skipping full refresh",
                    currentSchemaChangeId, mapping.catalog(), mapping.schema(), mapping.table());
            return new QueueState(existing.targetPath(), existing.transformation(),
                    existing.partitionColumns(), existing.partitionProjections(),
                    existing.schemaChangeId(), clock.instant());
        }
        return buildState(mapping, currentSchemaChangeId, clock.instant());
    }

    /**
     * Resolves the full {@link QueueState} for a mapping by querying DuckLake metadata.
     * Accepts a pre-fetched {@code schemaChangeId} to avoid a redundant round-trip when
     * called from {@link #getOrRefreshState}.
     */
    private static QueueState buildState(QueueIdToTableMapping mapping, long schemaChangeId, Instant refreshedAt) {
        String path           = fetchPath(mapping.catalog(), mapping.schema(), mapping.table());
        String transformation = resolveTransformation(mapping);
        List<ResolvedPartition> partitions = fetchPartitions(mapping.catalog(), mapping.schema(), mapping.table());
        String[] tokens      = partitions.stream().map(ResolvedPartition::token).toArray(String[]::new);
        String[] projections = partitions.stream().map(ResolvedPartition::projection)
                .filter(java.util.Objects::nonNull).toArray(String[]::new);
        return new QueueState(path, transformation, tokens, projections, schemaChangeId, refreshedAt);
    }

    /** Convenience overload that fetches schema change ID itself (used at construction time). */
    private static QueueState buildState(QueueIdToTableMapping mapping, Instant refreshedAt) {
        long schemaChangeId = fetchSchemaChangeId(mapping.catalog(), mapping.schema(), mapping.table());
        return buildState(mapping, schemaChangeId, refreshedAt);
    }

    private static String resolveTransformation(QueueIdToTableMapping mapping) {
        if (mapping.hasViewTransformation()) {
            return resolveViewTransformationStatic(mapping.view(), mapping.inputTable());
        }
        return mapping.transformation();
    }

    // -----------------------------------------------------------------------
    // DuckLake metadata queries
    // -----------------------------------------------------------------------

    private static String fetchPath(String catalogName, String schema, String table) {
        String metadataDatabase = "__ducklake_metadata_" + catalogName;
        String query = TABLE_PATH_QUERY.formatted(metadataDatabase, metadataDatabase, metadataDatabase, schema, table);
        try {
            return ConnectionPool.collectFirst(query, String.class);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get path for table %s.%s.%s".formatted(catalogName, schema, table), e);
        }
    }

    public static String[] getPartitionColumns(String catalogName, String schema, String table) {
        return fetchPartitions(catalogName, schema, table).stream()
                .map(ResolvedPartition::token).toArray(String[]::new);
    }

    /**
     * Returns {@code MAX(schema_version)} from {@code ducklake_snapshot} for the catalog.
     * {@code schema_version} is a catalog-wide BIGINT that increments only on DDL operations
     * (CREATE/ALTER TABLE, view changes) — not on data ingestion — making it a cheap,
     * reliable schema-change detector with a single aggregation query.
     */
    static long fetchSchemaChangeId(String catalogName, String schema, String table) {
        String db = "__ducklake_metadata_" + catalogName;
        String query = "SELECT MAX(schema_version) FROM %s.ducklake_snapshot".formatted(db);
        try {
            Long id = ConnectionPool.collectFirst(query, Long.class);
            return id != null ? id : 0L;
        } catch (SQLException e) {
            logger.atDebug().setCause(e).log("Failed to get schema_version for catalog {}", catalogName);
            return 0L;
        }
    }

    private static List<ResolvedPartition> fetchPartitions(String catalogName, String schema, String table) {
        String metadataDatabase = "__ducklake_metadata_" + catalogName;
        String query = """
                SELECT
                    c.column_name,
                    pc.transform,
                    pc.partition_key_index
                FROM %1$s.ducklake_table t
                JOIN %1$s.ducklake_partition_info pi
                    ON t.table_id = pi.table_id
                JOIN %1$s.ducklake_partition_column pc
                    ON pi.partition_id = pc.partition_id
                JOIN %1$s.ducklake_column c
                    ON pc.column_id = c.column_id
                    AND c.table_id = t.table_id
                WHERE t.table_name = '%2$s'
                  AND t.end_snapshot IS NULL
                  AND pi.end_snapshot IS NULL
                  AND c.end_snapshot IS NULL
                ORDER BY pc.partition_key_index ASC
                """.formatted(metadataDatabase, table);
        try (var connection = ConnectionPool.getConnection()) {
            Iterable<ResolvedPartition> rows = ConnectionPool.collectAll(connection, query,
                    rs -> resolvePartition(rs.getString("column_name"), rs.getString("transform")));
            List<ResolvedPartition> partitions = new ArrayList<>();
            rows.forEach(partitions::add);
            return partitions;
        } catch (SQLException e) {
            logger.atDebug().setCause(e).log("Failed to get partition columns for table {}.{}.{}", catalogName, schema, table);
            return List.of();
        }
    }

    /**
     * Resolves a {@code ducklake_partition_column} row into a {@link ResolvedPartition}. Identity
     * columns partition by the column directly. Time transforms ({@code year}/{@code month}/{@code
     * day}/{@code hour}) map to the DuckDB scalar function of the same name; since COPY's
     * {@code PARTITION_BY} accepts only column names, the transform is projected as a derived column
     * aliased to the transform name and partitioned by that alias.
     */
    private static ResolvedPartition resolvePartition(String columnName, String transform) {
        if (transform == null || transform.isBlank() || transform.equalsIgnoreCase("identity")) {
            return new ResolvedPartition(columnName, null);
        }
        String fn = transform.toLowerCase(Locale.ROOT);
        String projection = "%s(%s) AS %s".formatted(fn,
                HeaderUtils.quoteIdentifier(columnName), HeaderUtils.quoteIdentifier(fn));
        return new ResolvedPartition(fn, projection);
    }

    // -----------------------------------------------------------------------
    // View-based transformation resolution
    // -----------------------------------------------------------------------

    static String resolveViewTransformationStatic(String fqView, String fqInputTable) {
        String[] viewParts  = splitFqName(fqView,       "view");
        String[] tableParts = splitFqName(fqInputTable, "input_table");
        String viewSql = fetchViewDefinition(viewParts[0], viewParts[1], viewParts[2]);
        try {
            return Transformations.rewriteTableAsThis(viewSql, tableParts[0], tableParts[1], tableParts[2]);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to derive transformation from view '%s' replacing '%s': %s"
                            .formatted(fqView, fqInputTable, e.getMessage()), e);
        }
    }

    private static String[] splitFqName(String fqName, String fieldLabel) {
        String[] parts = fqName.split("\\.", 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException(
                    "%s must be fully qualified as 'catalog.schema.name', got: '%s'"
                            .formatted(fieldLabel, fqName));
        }
        return parts;
    }

    private static String fetchViewDefinition(String catalog, String schema, String viewName) {
        String query = """
                SELECT sql
                FROM duckdb_views()
                WHERE database_name = '%s' AND schema_name = '%s' AND view_name = '%s'
                """.formatted(catalog, schema, viewName);
        try {
            String fullSql = ConnectionPool.collectFirst(query, String.class);
            if (fullSql == null) {
                throw new RuntimeException("View '%s.%s.%s' not found".formatted(catalog, schema, viewName));
            }
            String upperFull     = fullSql.toUpperCase();
            String upperViewName = viewName.toUpperCase();
            int nameIdx = upperFull.indexOf(upperViewName);
            int asIdx   = nameIdx >= 0
                    ? upperFull.indexOf(" AS ", nameIdx + upperViewName.length())
                    : -1;
            if (asIdx < 0) {
                throw new RuntimeException(
                        "Unexpected view definition format for '%s.%s.%s': %s"
                                .formatted(catalog, schema, viewName, fullSql));
            }
            return fullSql.substring(asIdx + 4).trim();
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Failed to fetch definition for view '%s.%s.%s'".formatted(catalog, schema, viewName), e);
        }
    }

    // -----------------------------------------------------------------------

    private String extractSuffix(String queueName) {
        String normalized = queueName.replace("\\", "/");
        int lastSlash = normalized.lastIndexOf('/');
        return lastSlash >= 0 ? normalized.substring(lastSlash + 1) : normalized;
    }
}
