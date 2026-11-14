package io.dazzleduck.sql.commons.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link DuckLakeIngestionHandler} whose queue→table mapping set is sourced from a SQLite registry
 * and refreshed at runtime. Everything else — resolving each queue's target path, partition columns
 * and transformation from DuckLake metadata, caching, and queue lifecycle — is inherited from the
 * superclass. The registry only supplies, per queue, the {@code catalog}/{@code schema}/{@code table}
 * (plus optional {@code transformation}/{@code view}/{@code input_table}); the write location and
 * partitioning come from the DuckLake table itself.
 *
 * <p>A background thread polls {@code schema_version.version} (the writer's sync cursor, bumped in
 * the same transaction as every change — see {@link DynamicQueueRepository}) at {@code checkInterval}
 * via the persistent DuckDB connection, and on a change reloads the registry and feeds it to
 * {@link #updateMappings}.
 *
 * <p>Deliberately <strong>not</strong> file-mtime-based: a long-lived JVM polling
 * {@code Files.getLastModifiedTime} on the same path can see a stale cached value indefinitely on
 * FUSE-backed bind mounts (observed on Docker Desktop for Mac/virtiofs) — file attribute caching,
 * not file content caching, is what goes stale. Reading the counter through the persistent
 * connection forces a real read every time.
 */
public class DynamicIngestionHandler extends DuckLakeIngestionHandler {

    private static final Logger logger = LoggerFactory.getLogger(DynamicIngestionHandler.class);

    private final String dbPath;
    private final Connection readConn;
    /** When true, create/evolve the backing DuckLake table for each reconciled queue. */
    private final boolean manageTables;
    /** Prepared once and reused by the single poller thread to avoid re-parsing on every poll. */
    private final PreparedStatement schemaVersionStmt;
    private final AtomicLong lastSeenVersion = new AtomicLong(-1L);

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "dynamic-ingestion-reload");
                t.setDaemon(true);
                return t;
            });

    /** Backward-compatible constructor: table management disabled. */
    public DynamicIngestionHandler(String dbPath,
                                   Connection readConn,
                                   Map<String, QueueIdToTableMapping> initialMappings,
                                   Duration checkInterval) {
        this(dbPath, readConn, initialMappings, checkInterval, false);
    }

    public DynamicIngestionHandler(String dbPath,
                                   Connection readConn,
                                   Map<String, QueueIdToTableMapping> initialMappings,
                                   Duration checkInterval,
                                   boolean manageTables) {
        // Start with no mappings so the superclass does no eager DuckLake reads, then feed the
        // initial set via updateMappings (which rebuilds derived state lazily, deferring metadata
        // reads until the tables actually exist). checkInterval doubles as the DuckLake-state
        // refresh interval for already-known queues.
        super(Map.of(), checkInterval, Clock.systemUTC());
        this.dbPath       = dbPath;
        this.readConn     = readConn;
        this.manageTables = manageTables;
        // Prepare the change-detection query once; the poller reuses it. If preparing fails, fall
        // back to per-poll statements (see currentSchemaVersion) so detection still works.
        PreparedStatement stmt = null;
        try {
            stmt = DynamicQueueRepository.prepareSchemaVersionQuery(readConn);
        } catch (SQLException e) {
            logger.warn("Could not prepare schema_version query; falling back to per-poll statements", e);
        }
        this.schemaVersionStmt = stmt;

        updateMappings(initialMappings);
        lastSeenVersion.set(currentSchemaVersion());

        long intervalMs = checkInterval.toMillis();
        scheduler.scheduleWithFixedDelay(this::checkAndReload,
                intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /** Reads schema_version via the reused prepared statement, falling back to a per-call statement. */
    private long currentSchemaVersion() {
        return schemaVersionStmt != null
                ? DynamicQueueRepository.readSchemaVersion(schemaVersionStmt)
                : DynamicQueueRepository.readSchemaVersion(readConn);
    }

    private void checkAndReload() {
        try {
            long currentVersion = currentSchemaVersion();
            if (currentVersion == lastSeenVersion.get()) return;

            logger.debug("schema_version changed ({} → {}), reloading ingestion queues",
                    lastSeenVersion.get(), currentVersion);

            updateMappings(DynamicQueueRepository.loadAll(readConn, currentMappings()));
            lastSeenVersion.set(currentVersion);
        } catch (Exception e) {
            logger.warn("Failed to reload ingestion queue registry from {}", dbPath, e);
        }
    }

    /**
     * When {@code manageTables} is enabled, create/evolve the backing DuckLake table to match the
     * queue's transformation output over its {@code input_schema}.
     * Failure-isolated inside {@link DuckLakeTableManager#ensureTable}.
     */
    @Override
    protected void onMappingReconciled(String queueId, QueueIdToTableMapping mapping) {
        if (!manageTables) return;
        DuckLakeTableManager.ensureTable(mapping);
    }

    // Override the Duration overload (the one shutdown callers invoke) rather than the no-arg form,
    // so this handler's extra cleanup runs no matter which entry point is used. The inherited
    // no-arg closeQueues() delegates here via DuckLakeIngestionHandler.
    @Override
    public void closeQueues(Duration drainTimeout) {
        scheduler.shutdownNow(); // stop the reconcile thread first so no new queues are created mid-drain
        super.closeQueues(drainTimeout); // drain + close + clear the cached queues
        if (schemaVersionStmt != null) {
            try { schemaVersionStmt.close(); } catch (SQLException e) {
                logger.warn("Failed to close schema_version statement", e);
            }
        }
        try { readConn.close(); } catch (SQLException e) {
            logger.warn("Failed to close SQLite read connection", e);
        }
    }
}
