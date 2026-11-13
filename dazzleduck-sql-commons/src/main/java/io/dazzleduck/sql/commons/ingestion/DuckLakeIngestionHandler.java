package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.util.HeaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link IngestionHandler} backed by DuckLake metadata.
 *
 * <p>Two independent caches are maintained per queue:
 * <ul>
 *   <li><b>stateCache</b> — the refreshable state: target path, resolved transformation SQL,
 *       partition columns and session variables. All are resolved together at construction time
 *       and again lazily whenever {@link #getTargetPath}, {@link #getTransformation},
 *       {@link #getVariables} or {@link #getPartitionBy} are called and the cached state is older
 *       than {@code refreshInterval}. Read accessors call {@link #getOrRefreshState} which uses
 *       {@code stateCache.compute()} to atomically check and refresh — no DuckDB round-trip
 *       on the hot write path when state is fresh. A queue whose variables come from a relation
 *       (see {@link IngestionVariables}) re-reads them on every refresh even when the schema has
 *       not changed, because rows are data, not schema. A view-based transformation is also
 *       re-read on every refresh: the view may live in a catalog other than the target table's,
 *       whose {@code schema_version} would not move when it changes. If a periodic re-read fails,
 *       the last resolved transformation is kept; only {@link #refreshNow} (after a failed write)
 *       or a first build records the failure and fails the queue.</li>
 *   <li><b>queueCache</b> — the {@link ParquetIngestionQueue} instance, in an
 *       {@link IngestionQueueCache}. The queue is created exactly once per queue ID (via
 *       {@link #getOrCreateQueue}) and is never replaced unless the target path disappears and
 *       the queue is evicted.</li>
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
     * {@code partitionError} is non-null when the table's partitioning cannot be reproduced by the
     * COPY path (see {@link UnsupportedPartitionTransformException}); only this queue is affected.
     * {@code transformationError} is non-null when the queue's view-based transformation cannot be
     * resolved (see {@link TransformationUnavailableException}); likewise scoped to this queue.
     */
    private record QueueState(String targetPath, String transformation, String[] partitionColumns,
                               String[] partitionProjections, Map<String, String> variables,
                               long schemaChangeId, Instant refreshedAt,
                               String partitionError, String transformationError,
                               List<SidecarTarget> sidecars, String sidecarError) {}

    /** A transformation resolved for a queue: the SQL, or the reason it could not be resolved. */
    private record ResolvedTransformation(String sql, String error) {}

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

    private final IngestionQueueCache queueCache = new IngestionQueueCache();

    private final Duration refreshInterval;
    private final Clock clock;

    public DuckLakeIngestionHandler(Map<String, QueueIdToTableMapping> mappings, Duration refreshInterval) {
        this(mappings, refreshInterval, Clock.systemUTC());
    }

    public DuckLakeIngestionHandler(Map<String, QueueIdToTableMapping> mappings, Duration refreshInterval, Clock clock) {
        this.queueIdsToTableMappings = new ConcurrentHashMap<>(mappings);
        this.refreshInterval = refreshInterval;
        this.clock = clock;
        mappings.forEach((id, mapping) -> stateCache.put(id, buildStateWithSidecarTables(mapping, clock.instant())));
    }

    /**
     * Builds a queue's first state. A queue whose sidecar tables cannot be created or do not match
     * their queries fails the construction, and so startup (see {@link SidecarTables#ensure}); later,
     * the same failure is scoped to the queue. Not while the queue's own transformation or
     * partitioning fails: that queue cannot be written anyway, and its sidecars are resolved once it
     * can.
     */
    private QueueState buildStateWithSidecarTables(QueueIdToTableMapping mapping, Instant refreshedAt) {
        QueueState state = buildState(mapping, refreshedAt);
        if (state.sidecarError() != null && state.transformationError() == null && state.partitionError() == null) {
            throw new IllegalArgumentException(state.sidecarError());
        }
        return state;
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
            queueCache.evict(id);
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
        if (s != null && s.transformationError() != null) {
            throw new TransformationUnavailableException(s.transformationError());
        }
        return s != null ? s.transformation() : null;
    }

    @Override
    public Map<String, String> getVariables(String queueId) {
        QueueState s = getOrRefreshState(queueId);
        return s != null ? s.variables() : Map.of();
    }

    /**
     * The queue's sidecars, resolved against their tables. Throws
     * {@link TransformationUnavailableException} while one of them cannot be resolved (its table was
     * dropped or partitioned), so the queue's writes fail with the reason instead of silently
     * leaving the sidecar out.
     */
    @Override
    public List<SidecarTarget> getSidecars(String queueId) {
        QueueState s = getOrRefreshState(queueId);
        if (s != null && s.sidecarError() != null) {
            throw new TransformationUnavailableException(s.sidecarError());
        }
        return s != null ? s.sidecars() : List.of();
    }

    @Override
    public String[] getPartitionBy(String queueId) {
        QueueState s = requirePartitionable(getOrRefreshState(queueId));
        return s != null ? s.partitionColumns() : new String[0];
    }

    @Override
    public String[] getPartitionProjections(String queueId) {
        QueueState s = requirePartitionable(getOrRefreshState(queueId));
        return s != null ? s.partitionProjections() : new String[0];
    }

    /**
     * Fails a queue whose table partitioning is unsupported, so its writes are rejected with a clear
     * message instead of registering unpartitioned (or wrongly partitioned) files. The state is
     * rebuilt on the next schema change, so fixing the table's partitioning recovers the queue.
     */
    private static QueueState requirePartitionable(QueueState s) {
        if (s != null && s.partitionError() != null) {
            throw new UnsupportedPartitionTransformException(s.partitionError());
        }
        return s;
    }

    @Override
    public java.util.Set<String> getKnownQueues() {
        // The configured mappings are this handler's fixed, authoritative queue set.
        return queueIdsToTableMappings.keySet();
    }

    @Override
    public boolean extractClaims(String queueId) {
        QueueIdToTableMapping mapping = mappingFor(queueId);
        return mapping != null && mapping.extractClaims();
    }

    @Override
    public int getNumPartitions(String queueId) {
        QueueIdToTableMapping mapping = mappingFor(queueId);
        return mapping == null ? 1 : mapping.numPartitions();
    }

    @Override
    public String getPartitionExpression(String queueId) {
        QueueIdToTableMapping mapping = mappingFor(queueId);
        return mapping == null ? null : mapping.partitionExpression();
    }

    /** Mapping for {@code queueId}, resolved exact-first with the path-suffix fallback. */
    private QueueIdToTableMapping mappingFor(String queueId) {
        String key = resolveStateKey(queueId);
        return key == null ? null : queueIdsToTableMappings.get(key);
    }

    @Override
    public PostIngestionTask createPostIngestionTask(IngestionResult result) {
        QueueIdToTableMapping mapping = mappingFor(result.queueName());
        if (mapping == null) {
            // No DuckLake mapping for this queue — write-only mode, no catalog registration.
            logger.atDebug().log("No DuckLake mapping for queue '{}', skipping catalog registration", result.queueName());
            return PostIngestionTask.NOOP;
        }
        return new DuckLakePostIngestionTask(result, mapping.catalog(), mapping.table(), mapping.schema());
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
        // Created exactly once per ID; a gone path evicts and closes the cached queue.
        return queueCache.getOrCreate(queueId, path, creator, listener);
    }

    @Override
    public java.util.List<Stats> getQueueStats() {
        return queueCache.stats();
    }

    @Override
    public void closeQueues() {
        closeQueues(DEFAULT_DRAIN_TIMEOUT);
    }

    @Override
    public void closeQueues(Duration drainTimeout) {
        queueCache.closeAll(drainTimeout);
    }

    // -----------------------------------------------------------------------
    // Lazy refresh — single entry point used by all read accessors
    // -----------------------------------------------------------------------

    /**
     * Refreshes the queue's state now, regardless of {@code refreshInterval}: re-checks
     * {@code schema_version} (rebuilding the state if it changed) and re-reads a view-based
     * transformation. Unlike a periodic refresh, a view that does not resolve is recorded as the
     * queue's failure instead of keeping the last resolved transformation — this runs after a write
     * failed, when that transformation is suspect.
     */
    @Override
    public void refreshNow(String queueId) {
        String key = resolveStateKey(queueId);
        if (key == null) return;
        stateCache.compute(key, (k, existing) -> computeRefreshedState(k, existing, true));
    }

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
            return computeRefreshedState(k, existing, false);
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
     * (cheap — one DuckDB round-trip). If unchanged, bumps {@code refreshedAt} and re-reads only
     * what can change without DDL in this catalog: variables from a relation and a view-based
     * transformation. If changed (or no existing state), does a full rebuild of path,
     * transformation, and partition columns. {@code forced} is true for {@link #refreshNow}; see
     * {@link #resolveTransformationIsolated} for how it treats a view that does not resolve.
     */
    private QueueState computeRefreshedState(String key, QueueState existing, boolean forced) {
        QueueIdToTableMapping mapping = mappingFor(key);
        if (mapping == null) return existing; // unknown queue — leave unchanged

        long currentSchemaChangeId = fetchSchemaChangeId(mapping.catalog(), mapping.schema(), mapping.table());
        if (existing != null && existing.schemaChangeId() == currentSchemaChangeId) {
            logger.atDebug().log("Schema unchanged (id={}) for {}.{}.{}, skipping full refresh",
                    currentSchemaChangeId, mapping.catalog(), mapping.schema(), mapping.table());
            // Variables from a relation are the exception: their rows change without any DDL, so
            // the schema-version shortcut would pin them forever. Re-read them on this tick (one
            // query, only for a queue that configures a relation) and keep everything else.
            Map<String, String> variables = mapping.variables().hasView()
                    ? mapping.variables().resolve(mapping.ingestionQueue())
                    : existing.variables();
            // So is a view-based transformation: the view may be in another catalog (or a plain
            // DuckDB one), whose changes do not move the target catalog's schema_version.
            ResolvedTransformation transformation = mapping.hasViewTransformation()
                    ? resolveTransformationIsolated(mapping, existing, forced)
                    : new ResolvedTransformation(existing.transformation(), existing.transformationError());
            // Sidecars are resolved again while they failed (a cause outside this catalog's schema,
            // such as a transient read error or the queue's view now resolving, would otherwise pin
            // the failure), and when the transformation they read changed.
            // Not on every forced refresh: each failed batch forces one, and a mismatch that DDL must fix
            // would re-run the whole check per batch; that DDL triggers a full rebuild anyway.
            boolean recheckSidecars = (existing.sidecarError() != null && !forced)
                    || !Objects.equals(transformation.sql(), existing.transformation());
            ResolvedSidecars sidecars = recheckSidecars
                    ? resolveSidecars(mapping, transformation, existing.partitionProjections(), variables, existing)
                    : new ResolvedSidecars(existing.sidecars(), existing.sidecarError());
            // A forced refresh that kept a sidecar failure without checking it again leaves the
            // refresh time alone, so the periodic refresh still comes round to re-check it while
            // failed batches keep forcing refreshes.
            Instant refreshedAt = forced && !recheckSidecars && existing.sidecarError() != null
                    ? existing.refreshedAt() : clock.instant();
            return new QueueState(existing.targetPath(), transformation.sql(),
                    existing.partitionColumns(), existing.partitionProjections(), variables,
                    schemaChangeIdAfter(mapping, existing.schemaChangeId(), sidecars), refreshedAt,
                    existing.partitionError(), transformation.error(), sidecars.targets(), sidecars.error());
        }
        return buildState(mapping, currentSchemaChangeId, clock.instant(), existing, forced);
    }

    /**
     * Resolves the full {@link QueueState} for a mapping by querying DuckLake metadata.
     * Accepts a pre-fetched {@code schemaChangeId} to avoid a redundant round-trip when
     * called from {@link #getOrRefreshState}.
     */
    private static QueueState buildState(QueueIdToTableMapping mapping, long schemaChangeId, Instant refreshedAt,
                                         QueueState previous, boolean forced) {
        boolean firstBuild = previous == null;
        String path           = fetchPath(mapping.catalog(), mapping.schema(), mapping.table());
        ResolvedTransformation resolved = resolveTransformationIsolated(mapping, previous, forced);
        String transformation = resolved.sql();
        Map<String, String> variables = mapping.variables().resolve(mapping.ingestionQueue());
        if (firstBuild && resolved.error() == null) {
            warnIfClaimsColumnMissing(mapping, transformation);
        }
        List<ResolvedPartition> partitions;
        try {
            partitions = fetchPartitions(mapping.catalog(), mapping.schema(), mapping.table());
        } catch (UnsupportedPartitionTransformException e) {
            // Isolate to this queue: the handler (and every other queue) must keep working.
            logger.error("Queue '{}' ({}.{}.{}) cannot be ingested until its partitioning is changed: {}",
                    mapping.ingestionQueue(), mapping.catalog(), mapping.schema(), mapping.table(), e.getMessage());
            ResolvedSidecars sidecars = resolveSidecars(mapping, resolved, new String[0], variables, previous);
            return new QueueState(path, transformation, new String[0], new String[0], variables,
                    schemaChangeIdAfter(mapping, schemaChangeId, sidecars), refreshedAt, e.getMessage(), resolved.error(),
                    sidecars.targets(), sidecars.error());
        }
        String[] tokens      = partitions.stream().map(ResolvedPartition::token).toArray(String[]::new);
        String[] projections = partitions.stream().map(ResolvedPartition::projection)
                .filter(java.util.Objects::nonNull).toArray(String[]::new);
        ResolvedSidecars sidecars = resolveSidecars(mapping, resolved, projections, variables, previous);
        return new QueueState(path, transformation, tokens, projections, variables,
                schemaChangeIdAfter(mapping, schemaChangeId, sidecars), refreshedAt, null, resolved.error(),
                sidecars.targets(), sidecars.error());
    }

    /**
     * The schema version to record once the queue's sidecars are resolved. Creating a sidecar table
     * moves the catalog's version by one; recording the version read before it would make the next
     * refresh rebuild again. The later version is taken only when it moved by exactly the tables
     * created, so no other process's DDL in between (which this state may not reflect) is marked as
     * seen; otherwise the earlier one is kept, and the next refresh rebuilds.
     */
    private static long schemaChangeIdAfter(QueueIdToTableMapping mapping, long schemaChangeId, ResolvedSidecars sidecars) {
        if (sidecars.tablesCreated() == 0) {
            return schemaChangeId;
        }
        long now = fetchSchemaChangeId(mapping.catalog(), mapping.schema(), mapping.table());
        return now == schemaChangeId + sidecars.tablesCreated() ? now : schemaChangeId;
    }

    /** A queue's sidecars resolved for writing, or why they cannot be. */
    private record ResolvedSidecars(List<SidecarTarget> targets, String error, int tablesCreated) {
        ResolvedSidecars(List<SidecarTarget> targets, String error) {
            this(targets, error, 0);
        }
    }

    /**
     * Creates the queue's missing sidecar tables, checks the existing ones, and resolves them all for
     * writing. A failure is returned, not thrown, so it stays with this queue; so is a queue whose
     * transformation does not resolve, whose sidecars cannot be checked until it does. A table whose
     * partitioning is unsupported does not stop them: a batch that names its partitioning in its
     * header is still written, with its sidecars.
     */
    private static ResolvedSidecars resolveSidecars(QueueIdToTableMapping mapping, ResolvedTransformation transformation,
                                                    String[] partitionProjections,
                                                    Map<String, String> variables, QueueState previous) {
        if (mapping.sidecars().isEmpty()) {
            return new ResolvedSidecars(List.of(), null);
        }
        String error;
        if (transformation.error() != null) {
            error = "Queue '%s': its sidecars are resolved once the queue's own transformation is"
                    .formatted(mapping.ingestionQueue());
        } else {
            try {
                SidecarTables.Ensured ensured = SidecarTables.ensure(mapping, transformation.sql(), partitionProjections, variables);
                int created = ensured.created();
                List<SidecarTarget> targets = new ArrayList<>();
                for (Sidecar sidecar : mapping.sidecars()) {
                    targets.add(resolveSidecar(mapping, sidecar, ensured.produced().get(sidecar.table())));
                }
                if (previous != null && previous.sidecarError() != null) {
                    logger.info("Queue '{}': its sidecar tables can be written again", mapping.ingestionQueue());
                }
                return new ResolvedSidecars(List.copyOf(targets), null, created);
            } catch (RuntimeException e) {
                error = "Queue '%s' cannot be ingested until its sidecar tables can be written: %s"
                        .formatted(mapping.ingestionQueue(), e.getMessage());
                if (previous == null || !error.equals(previous.sidecarError())) {
                    logger.error(error);
                }
            }
        }
        return new ResolvedSidecars(List.of(), error);
    }

    /**
     * Resolves one sidecar against its table, for writing. {@link SidecarTables#ensure}, run just
     * before, has checked the table exists, matches and is not partitioned.
     */
    private static SidecarTarget resolveSidecar(QueueIdToTableMapping mapping, Sidecar sidecar, java.util.Set<String> produced) {
        return SidecarTables.resolve(mapping, sidecar, fetchPath(mapping.catalog(), mapping.schema(), sidecar.table()), produced);
    }

    /** Convenience overload that fetches schema change ID itself (used at construction time). */
    private static QueueState buildState(QueueIdToTableMapping mapping, Instant refreshedAt) {
        long schemaChangeId = fetchSchemaChangeId(mapping.catalog(), mapping.schema(), mapping.table());
        return buildState(mapping, schemaChangeId, refreshedAt, null, false);
    }

    /**
     * Resolves the queue's transformation, isolating a view that cannot be resolved to this queue:
     * the failure is returned instead of thrown, so the handler and every other queue keep working.
     *
     * <p>How a failure is applied depends on why the state is being read:
     * <ul>
     *   <li>A periodic refresh ({@code forced == false}) with a previously resolved transformation
     *       keeps that transformation and logs a warning on every tick. The failure may be a brief
     *       catalog error, and the kept SQL reads only the batch, so it still writes correctly.</li>
     *   <li>A forced refresh ({@link #refreshNow}: a write failed, or the queue is already unusable)
     *       or a first build records the failure, so this queue's writes are rejected with the
     *       reason until the view resolves again.</li>
     * </ul>
     */
    private static ResolvedTransformation resolveTransformationIsolated(QueueIdToTableMapping mapping,
                                                                        QueueState previous,
                                                                        boolean forced) {
        if (!mapping.hasViewTransformation()) {
            return new ResolvedTransformation(mapping.transformation(), null);
        }
        try {
            String sql = resolveViewTransformationStatic(mapping.view(), mapping.inputTable());
            if (previous != null && previous.transformationError() != null) {
                logger.info("Queue '{}': view '{}' resolves again; ingestion resumes",
                        mapping.ingestionQueue(), mapping.view());
            } else if (previous != null && !sql.equals(previous.transformation())) {
                logger.info("Queue '{}': view '{}' changed; transformation is now: {}",
                        mapping.ingestionQueue(), mapping.view(), sql);
            }
            return new ResolvedTransformation(sql, null);
        } catch (RuntimeException e) {
            boolean hasLastGood = previous != null && previous.transformationError() == null
                    && previous.transformation() != null;
            if (!forced && hasLastGood) {
                // Logged every tick while it lasts; the message is enough, the stack is at debug.
                logger.warn("Queue '{}': could not re-read view '{}'; keeping its last resolved "
                                + "transformation until a refresh succeeds or a write fails: {}",
                        mapping.ingestionQueue(), mapping.view(), e.getMessage());
                logger.atDebug().setCause(e).log("Queue '{}': view '{}' re-read failure",
                        mapping.ingestionQueue(), mapping.view());
                return new ResolvedTransformation(previous.transformation(), null);
            }
            String error = "Queue '%s' cannot be ingested until its view '%s' resolves: %s"
                    .formatted(mapping.ingestionQueue(), mapping.view(), e.getMessage());
            if (previous == null || !error.equals(previous.transformationError())) {
                logger.error(error, e);
            }
            return new ResolvedTransformation(null, error);
        }
    }

    // -----------------------------------------------------------------------
    // DuckLake metadata queries
    // -----------------------------------------------------------------------

    private static String fetchPath(String catalogName, String schema, String table) {
        String metadataDatabase = "__ducklake_metadata_" + catalogName;
        String query = TABLE_PATH_QUERY.formatted(metadataDatabase, metadataDatabase, metadataDatabase,
                schema.replace("'", "''"), table.replace("'", "''"));
        try {
            return ConnectionPool.collectFirst(query, String.class);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get path for table %s.%s.%s".formatted(catalogName, schema, table), e);
        }
    }

    /**
     * Whether the table is partitioned. Unlike {@link #getPartitionColumns}, a metadata read that
     * fails is thrown, not read as "unpartitioned".
     */
    static boolean isPartitioned(String catalogName, String schema, String table) throws SQLException {
        String metadataDatabase = "__ducklake_metadata_" + catalogName;
        String query = """
                SELECT count(*)
                FROM %1$s.ducklake_table t
                JOIN %1$s.ducklake_schema s ON t.schema_id = s.schema_id
                JOIN %1$s.ducklake_partition_info pi ON t.table_id = pi.table_id
                WHERE s.schema_name = '%3$s' AND t.table_name = '%2$s'
                  AND s.end_snapshot IS NULL AND t.end_snapshot IS NULL AND pi.end_snapshot IS NULL
                """.formatted(metadataDatabase, table.replace("'", "''"), schema.replace("'", "''"));
        Long count = ConnectionPool.collectFirst(query, Long.class);
        return count != null && count > 0;
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

    /**
     * DuckLake registration tolerates extra file columns, so with {@code extract_claims} on
     * a target table missing the claims column silently drops the data — warn loudly instead.
     * A transformation controls the output shape (it may consume claims without persisting
     * them), so only raw pass-through mappings are checked, and only on the queue's first
     * state build.
     */
    private static void warnIfClaimsColumnMissing(QueueIdToTableMapping mapping, String transformation) {
        if (!mapping.extractClaims() || transformation != null) return;
        try (var conn = ConnectionPool.getConnection()) {
            if (!DuckLakeTableManager.hasColumn(conn, mapping, CLAIMS_COLUMN)) {
                logger.warn("Queue '{}' has extract_claims enabled but table {}.{}.{} has no '{}' column — "
                                + "claims will be silently dropped at registration. "
                                + "Run: ALTER TABLE {}.{}.{} ADD COLUMN {} MAP(VARCHAR, VARCHAR)",
                        mapping.ingestionQueue(), mapping.catalog(), mapping.schema(), mapping.table(),
                        CLAIMS_COLUMN, mapping.catalog(), mapping.schema(), mapping.table(), CLAIMS_COLUMN);
            }
        } catch (SQLException e) {
            logger.atDebug().setCause(e).log("Could not verify '{}' column on {}.{}.{}",
                    CLAIMS_COLUMN, mapping.catalog(), mapping.schema(), mapping.table());
        }
    }

    private static List<ResolvedPartition> fetchPartitions(String catalogName, String schema, String table) {
        String metadataDatabase = "__ducklake_metadata_" + catalogName;
        String query = """
                SELECT
                    c.column_name,
                    pc.transform,
                    c.column_type,
                    pc.partition_key_index
                FROM %1$s.ducklake_table t
                JOIN %1$s.ducklake_schema s
                    ON t.schema_id = s.schema_id
                    AND s.schema_name = '%3$s'
                    AND s.end_snapshot IS NULL
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
                """.formatted(metadataDatabase, table.replace("'", "''"), schema.replace("'", "''"));
        try (var connection = ConnectionPool.getConnection()) {
            // Collect the raw (column, transform, type) rows first — resolving a bucket() column's hive
            // key needs to know how many earlier columns already used the "bucket" key (disambiguation),
            // and the column type decides how a bucket() value is encoded for hashing.
            List<String[]> raw = new ArrayList<>();
            ConnectionPool.collectAll(connection, query,
                    rs -> new String[]{rs.getString("column_name"), rs.getString("transform"),
                            rs.getString("column_type")})
                    .forEach(raw::add);
            return resolvePartitions(raw);
        } catch (SQLException e) {
            logger.atDebug().setCause(e).log("Failed to get partition columns for table {}.{}.{}", catalogName, schema, table);
            return List.of();
        }
    }

    /**
     * Resolves the ordered {@code (column, transform)} rows into partition tokens/projections. When
     * any column uses a {@code bucket(N)} transform, the murmur3 bucket macros are registered once
     * (they are needed by the projection). Hive keys are disambiguated the way DuckLake's own writer
     * names partition directories, because {@code ducklake_add_data_files} matches files to the table's
     * partitioning by hive key <em>name</em>.
     */
    private static List<ResolvedPartition> resolvePartitions(List<String[]> raw) {
        boolean hasBucket = raw.stream().anyMatch(r ->
                r[1] != null && BUCKET_TRANSFORM.matcher(r[1].toLowerCase(Locale.ROOT)).matches());
        if (hasBucket) {
            ensureIcebergBucketMacros();
        }
        Map<String, Integer> keyUses = new HashMap<>();
        List<ResolvedPartition> partitions = new ArrayList<>(raw.size());
        for (String[] r : raw) {
            partitions.add(resolvePartition(r[0], r[1], r[2], keyUses));
        }
        return partitions;
    }

    /**
     * Resolves a {@code ducklake_partition_column} row into a {@link ResolvedPartition}. COPY's
     * {@code PARTITION_BY} accepts only column names, so every non-identity transform is projected as
     * a derived column aliased to a hive key, and partitioned by that key:
     * <ul>
     *   <li><b>identity</b> — the column itself (no projection).</li>
     *   <li><b>year/month/day/hour</b> — DuckDB's scalar of the same name (calendar-component
     *       extraction, matching DuckLake): {@code day("ts") AS "day"}.</li>
     *   <li><b>bucket(N)</b> — the murmur3 bucket DuckLake computes natively, with the column encoded
     *       the way DuckLake encodes it for hashing (see {@link #bucketExpression}):
     *       {@code _dd_iceberg_bucket("group_id", 4) AS "bucket"}.</li>
     * </ul>
     * The hive key follows DuckLake's own directory-naming: the transform's base name on first use
     * ({@code bucket}), suffixed with the column on any later collision ({@code bucket_user_id}), so
     * {@code ducklake_add_data_files} (which matches by hive key name) accepts the written files.
     */
    private static ResolvedPartition resolvePartition(String columnName, String transform,
                                                      String columnType, Map<String, Integer> keyUses) {
        if (transform == null || transform.isBlank() || transform.equalsIgnoreCase("identity")) {
            return new ResolvedPartition(columnName, null);
        }
        String fn = transform.toLowerCase(Locale.ROOT);

        Matcher bucket = BUCKET_TRANSFORM.matcher(fn);
        if (bucket.matches()) {
            int numBuckets = Integer.parseInt(bucket.group(1));
            String expression = bucketExpression(columnName, columnType, numBuckets);
            String key = hiveKey("bucket", columnName, keyUses);
            return new ResolvedPartition(key, expression + " AS " + HeaderUtils.quoteIdentifier(key));
        }

        // Time transforms: the transform string is the bare function name (year/month/day/hour).
        String key = hiveKey(fn, columnName, keyUses);
        String projection = "%s(%s) AS %s".formatted(fn,
                HeaderUtils.quoteIdentifier(columnName), HeaderUtils.quoteIdentifier(key));
        return new ResolvedPartition(key, projection);
    }

    /** DuckLake names the first dir for a transform by its base key, later collisions {@code base_column}. */
    private static String hiveKey(String baseKey, String columnName, Map<String, Integer> keyUses) {
        int use = keyUses.merge(baseKey, 1, Integer::sum);
        return use == 1 ? baseKey : baseKey + "_" + columnName;
    }

    /**
     * SQL computing DuckLake's {@code bucket(N)} value for a column of the given DuckLake
     * {@code column_type}. DuckLake hashes murmur3_x86_32 over one of three encodings, and the value
     * written to the hive path must equal it exactly, so each type is mapped to the encoding DuckLake
     * uses (derived from, and verified against, DuckLake's own bucketing — see the handler tests):
     * <ul>
     *   <li><b>64-bit integer</b> ({@link #ICEBERG_BUCKET_FN}) — int8..int64; boolean (1/0); date (days
     *       since epoch); time (µs since midnight); timetz (DuckDB's packed µs/offset bits); timestamp
     *       and timestamptz (epoch µs); timestamp_s/_ms/_ns (epoch in their own unit); decimal with
     *       precision ≤ 18 (the unscaled value).</li>
     *   <li><b>UTF-8 of the string form</b> ({@link #ICEBERG_BUCKET_HEX_FN}) — varchar, uuid, interval,
     *       uint8..uint64, int128/uint128, decimal with precision &gt; 18.</li>
     *   <li><b>raw bytes</b> — blob.</li>
     * </ul>
     * float32/float64 are rejected: DuckLake hashes their IEEE-754 bits, which DuckDB SQL cannot
     * reinterpret exactly (Iceberg disallows bucketing floats for the same reason). Nested and other
     * types are rejected as well — fail fast rather than register files under the wrong partition.
     */
    static String bucketExpression(String columnName, String columnType, int numBuckets) {
        String col = HeaderUtils.quoteIdentifier(columnName);
        String type = columnType == null ? "" : columnType.toLowerCase(Locale.ROOT).trim();
        Matcher decimal = DECIMAL_TYPE.matcher(type);
        boolean isDecimal = decimal.matches();

        String asLong = switch (type) {
            case "int8", "int16", "int32", "int64" -> col;
            case "boolean" -> col + "::BIGINT";
            case "date" -> "(%s - DATE '1970-01-01')".formatted(col);
            case "time" -> "epoch_us(DATE '1970-01-01' + %s)".formatted(col);
            case "timetz" -> ("((epoch_us(DATE '1970-01-01' + %1$s::TIME) << 24)"
                    + " | (57599 - date_part('timezone', %1$s)))").formatted(col);
            case "timestamp", "timestamptz" -> "epoch_us(%s)".formatted(col);
            case "timestamp_s" -> "(epoch_ms(%s) // 1000)".formatted(col);
            case "timestamp_ms" -> "epoch_ms(%s)".formatted(col);
            case "timestamp_ns" -> "epoch_ns(%s)".formatted(col);
            default -> null;
        };
        if (asLong == null && isDecimal && Integer.parseInt(decimal.group(1)) <= 18) {
            int scale = Integer.parseInt(decimal.group(2));
            asLong = scale == 0 ? col + "::BIGINT"
                    : "(%s * %s)::BIGINT".formatted(col, BigInteger.TEN.pow(scale));
        }
        if (asLong != null) {
            return "%s(%s, %d)".formatted(ICEBERG_BUCKET_FN, asLong, numBuckets);
        }

        String hex = switch (type) {
            case "blob" -> "hex(%s)".formatted(col);
            case "varchar", "uuid", "interval", "uint8", "uint16", "uint32", "uint64", "int128", "uint128" ->
                    "hex(encode(%s::VARCHAR))".formatted(col);
            default -> isDecimal ? "hex(encode(%s::VARCHAR))".formatted(col) : null;
        };
        if (hex != null) {
            return "%s(%s, %d)".formatted(ICEBERG_BUCKET_HEX_FN, hex, numBuckets);
        }
        throw new UnsupportedPartitionTransformException(("bucket() partitioning is not supported on column '%s' of type "
                + "'%s'. Supported: integers, boolean, decimal, date/time/timestamp types, varchar, uuid, "
                + "interval, blob. Floating-point types are hashed over IEEE-754 bits, which cannot be "
                + "reproduced exactly in SQL.").formatted(columnName, columnType));
    }

    // -----------------------------------------------------------------------
    // bucket(N) partition transform — murmur3, reproduced as DuckDB SQL macros
    // -----------------------------------------------------------------------

    /** Bucket of a value DuckLake hashes as a 64-bit integer: {@code _dd_iceberg_bucket(long, N)}. */
    static final String ICEBERG_BUCKET_FN = "_dd_iceberg_bucket";

    /** Bucket of a value DuckLake hashes as bytes, given as a hex string: {@code _dd_iceberg_bucket_hex(hex, N)}. */
    static final String ICEBERG_BUCKET_HEX_FN = "_dd_iceberg_bucket_hex";

    /** Matches DuckLake's stored {@code bucket(N)} transform, capturing the bucket count. */
    private static final Pattern BUCKET_TRANSFORM = Pattern.compile("bucket\\((\\d+)\\)");

    /** Matches a DuckLake {@code decimal(p,s)} column type, capturing precision and scale. */
    private static final Pattern DECIMAL_TYPE = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)");

    private static final AtomicBoolean ICEBERG_BUCKET_MACROS_READY = new AtomicBoolean(false);

    /**
     * Iceberg/DuckLake {@code bucket(N, v)} = {@code (murmur3_x86_32(bytes(v)) & 0x7fffffff) % N}.
     * DuckDB has no murmur3 builtin and COPY's {@code PARTITION_BY} needs the value as a real column,
     * so murmur3 is expressed as SQL macros: {@link #ICEBERG_BUCKET_FN} over a long's 8 little-endian
     * bytes, and {@link #ICEBERG_BUCKET_HEX_FN} over arbitrary bytes (a hex string, folded 4 bytes at a
     * time with {@code list_reduce}, plus the tail). All arithmetic is in {@code UBIGINT}: every
     * intermediate is a 32-bit value times a 32-bit constant, which fits unsigned 64-bit, and unsigned
     * 64-bit math is several times faster in DuckDB than {@code HUGEINT}. Results are masked to 32
     * bits; NULL in gives NULL out (DuckLake's NULL partition).
     * Constants: c1=0xcc9e2d51, c2=0x1b873593, 0xe6546b64, fmix 0x85ebca6b / 0xc2b2ae35.
     */
    private static final String[] ICEBERG_BUCKET_MACRO_DDL = {
            "CREATE OR REPLACE MACRO _dd_m32(x) AS (x & 4294967295::UBIGINT)",
            "CREATE OR REPLACE MACRO _dd_rotl32(x, r) AS _dd_m32((x << r) | (x >> (32 - r)))",
            "CREATE OR REPLACE MACRO _dd_kmix(k) AS _dd_m32(_dd_rotl32(_dd_m32(k::UBIGINT * 3432918353::UBIGINT), 15) * 461845907::UBIGINT)",
            "CREATE OR REPLACE MACRO _dd_hstep(h, k) AS _dd_m32(_dd_rotl32(xor(h::UBIGINT, _dd_kmix(k)), 13) * 5::UBIGINT + 3864292196::UBIGINT)",
            "CREATE OR REPLACE MACRO _dd_fm1(h) AS _dd_m32(xor(h, h >> 16) * 2246822507::UBIGINT)",
            "CREATE OR REPLACE MACRO _dd_fm2(h) AS _dd_m32(xor(h, h >> 13) * 3266489909::UBIGINT)",
            "CREATE OR REPLACE MACRO _dd_fm3(h) AS xor(h, h >> 16)",
            "CREATE OR REPLACE MACRO " + ICEBERG_BUCKET_FN + "(val, n) AS "
                    + "(_dd_fm3(_dd_fm2(_dd_fm1(xor("
                    + "_dd_hstep(_dd_hstep(0, (val::BIGINT & 4294967295)::UBIGINT), ((val::BIGINT >> 32) & 4294967295)::UBIGINT), "
                    + "8::UBIGINT)))) & 2147483647) % n",
            // byte i (0-based) of a hex string
            "CREATE OR REPLACE MACRO _dd_byte(hx, i) AS ('0x' || substr(hx, 2 * i + 1, 2))::UBIGINT",
            "CREATE OR REPLACE MACRO _dd_murmur3_hex(hx) AS _dd_fm3(_dd_fm2(_dd_fm1(xor(xor("
                    + "list_reduce(list_transform(range(length(hx) // 8), lambda j: "
                    + "_dd_byte(hx, 4 * j) | (_dd_byte(hx, 4 * j + 1) << 8) | (_dd_byte(hx, 4 * j + 2) << 16) "
                    + "| (_dd_byte(hx, 4 * j + 3) << 24)), lambda h, k: _dd_hstep(h, k), 0::UBIGINT), "
                    + "_dd_kmix(COALESCE(list_reduce(list_transform(range((length(hx) // 2) % 4), lambda t: "
                    + "_dd_byte(hx, (length(hx) // 8) * 4 + t) << (8 * t)), lambda a, b: a | b, 0::UBIGINT), 0::UBIGINT))), "
                    + "(length(hx) // 2)::UBIGINT)))) & 2147483647",
            "CREATE OR REPLACE MACRO " + ICEBERG_BUCKET_HEX_FN + "(hx, n) AS _dd_murmur3_hex(hx) % n"
    };

    /**
     * Registers the {@code bucket()} macros once. They are persistent (non-{@code TEMP}) macros, and
     * {@link ConnectionPool} hands out {@code duplicate()}s of one in-process DuckDB instance, so a
     * single registration is visible to every ingestion connection — including the one that runs the
     * COPY later. {@code CREATE OR REPLACE} keeps it idempotent under races.
     */
    private static void ensureIcebergBucketMacros() {
        if (ICEBERG_BUCKET_MACROS_READY.get()) {
            return;
        }
        synchronized (ICEBERG_BUCKET_MACROS_READY) {
            if (ICEBERG_BUCKET_MACROS_READY.get()) {
                return;
            }
            try (var connection = ConnectionPool.getConnection()) {
                for (String ddl : ICEBERG_BUCKET_MACRO_DDL) {
                    ConnectionPool.execute(connection, ddl);
                }
                ICEBERG_BUCKET_MACROS_READY.set(true);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to register iceberg bucket() partition macros", e);
            }
        }
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
        // Scalar subquery: one row, NULL when the view is missing (collectFirst throws on no rows).
        String query = """
                SELECT (SELECT sql
                        FROM duckdb_views()
                        WHERE database_name = '%s' AND schema_name = '%s' AND view_name = '%s')
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
