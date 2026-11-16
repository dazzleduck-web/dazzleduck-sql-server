package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DuckLakeHousekeeper implements Housekeeper {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeHousekeeper.class);

    private final String startupScript;
    private final Duration snapshotRetention;
    private final List<String> connectionSettings;
    private final boolean rewriteDeletesEnabled;
    private final Double rewriteDeleteThreshold;
    private final CompactionState metrics;
    // null: rewrite without leases, for callers (tests) that run nothing beside it
    private final TableLeases leases;
    // Where its instances spill, per database; set by useSpill before first use
    private volatile SpillBudget.Slot spill = SpillBudget.Slot.NONE;

    /** Where its instances spill: one directory per database. Call before first use. */
    void useSpill(SpillBudget.Slot value) {
        this.spill = value;
    }
    /** A table the rewrite skipped this many times in a row, because another job held it, is waited for. */
    static final int SKIPS_BEFORE_WAIT = 3;
    // "db.schema.table" -> consecutive runs that skipped the table; touched only by the database's housekeeping
    private final ConcurrentHashMap<String, Integer> skipped = new ConcurrentHashMap<>();

    // One real DuckDB instance per database — see RawConnections. Keyed by database, not shared:
    // CompactionService sizes its housekeeping thread pool so different databases' housekeeping runs
    // concurrently, and a plain JDBC Connection is not safe for concurrent use by more than one
    // thread. Opened lazily on first use and reused for that database's whole lifetime.
    private final ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();

    public DuckLakeHousekeeper(String startupScript, Duration snapshotRetention, List<String> connectionSettings, CompactionState metrics) {
        this(startupScript, snapshotRetention, connectionSettings, false, null, metrics);
    }

    /**
     * @param rewriteDeletesEnabled  run {@code ducklake_rewrite_data_files} before expiry, rewriting
     *                               data files whose rows are mostly deleted so their delete files go away
     * @param rewriteDeleteThreshold fraction of a file's rows that must be deleted before it is rewritten,
     *                               or {@code null} to use the catalog's {@code rewrite_delete_threshold}
     */
    public DuckLakeHousekeeper(String startupScript, Duration snapshotRetention, List<String> connectionSettings,
                               boolean rewriteDeletesEnabled, Double rewriteDeleteThreshold, CompactionState metrics) {
        this(startupScript, snapshotRetention, connectionSettings, rewriteDeletesEnabled, rewriteDeleteThreshold,
                metrics, null);
    }

    /**
     * @param leases the leases the tier loops and data retention take: the delete-file rewrite runs
     *               per table under its lease, never beside a merge on the same table
     */
    public DuckLakeHousekeeper(String startupScript, Duration snapshotRetention, List<String> connectionSettings,
                               boolean rewriteDeletesEnabled, Double rewriteDeleteThreshold, CompactionState metrics,
                               TableLeases leases) {
        this.leases = leases;
        this.startupScript = startupScript;
        this.snapshotRetention = snapshotRetention;
        this.connectionSettings = connectionSettings;
        this.rewriteDeletesEnabled = rewriteDeletesEnabled;
        this.rewriteDeleteThreshold = rewriteDeleteThreshold;
        this.metrics = metrics;
    }

    @Override
    public void housekeep(String database) throws Exception {
        long retentionSeconds = snapshotRetention.toSeconds();
        Connection connection = connectionFor(database);

        // Run steps independently so a failure in one does not silently skip the rest. Rewriting
        // first means the files it replaces are retired in this cycle and deleted by cleanup once
        // they fall out of snapshot retention, like any other retired file.
        if (rewriteDeletesEnabled) {
            try {
                time("housekeeping", "rewrite_deletes", database, () -> {
                    long rewritten = rewriteDeletes(connection, database);
                    metrics.addFilesRewritten(database, rewritten);
                    logger.debug("Delete-file rewrite for {} rewrote {} data file(s)", database, rewritten);
                });
            } catch (Exception e) {
                if (e.getCause() instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    return;
                }
                logger.error("Delete-file rewrite failed for {}, expiry and cleanup will still run", database, e);
            }
        }
        try {
            time("housekeeping", "expire", database, () -> execute(connection,
                    "CALL ducklake_expire_snapshots('%s', older_than => now() - INTERVAL '%d seconds')"
                            .formatted(database, retentionSeconds)));
        } catch (Exception e) {
            logger.error("Snapshot expiry failed for {}, cleanup will still run", database, e);
        }
        try {
            metrics.updateUnexpiredSnapshots(database, countSnapshots(connection, database));
        } catch (Exception e) {
            logger.warn("Could not count the unexpired snapshots of {}", database, e);
        }
        time("housekeeping", "cleanup", database, () -> execute(connection,
                "CALL ducklake_cleanup_old_files('%s', older_than => now() - INTERVAL '%d seconds')"
                        .formatted(database, retentionSeconds)));
    }

    /**
     * Rewrites each live table's data files with enough deleted rows, one table at a time under its
     * lease, so the rewrite never runs beside a merge or a retention DELETE on the same table. A table
     * another job holds is skipped until the next run, so a long merge does not hold up the rest of
     * housekeeping; after {@link #SKIPS_BEFORE_WAIT} skips in a row the rewrite waits for it, so a busy
     * table is not passed over for good. A table that fails is logged and the others still run.
     *
     * @return data files rewritten
     */
    private long rewriteDeletes(Connection connection, String database) {
        long rewritten = 0;
        List<String[]> tables = liveTables(connection, database);
        // forget the skip counts of tables that no longer exist
        java.util.Set<String> live = new java.util.HashSet<>();
        tables.forEach(t -> live.add(TableLeases.key(database, t[0], t[1])));
        String prefix = TableLeases.key(database, "", "").replaceAll("\\.+$", "") + ".";
        skipped.keySet().removeIf(key -> key.startsWith(prefix) && !live.contains(key));
        for (String[] table : tables) {
            String key = TableLeases.key(database, table[0], table[1]);
            if (leases != null && !leases.tryAcquire(key)) {
                int skips = skipped.merge(key, 1, Integer::sum);
                if (skips < SKIPS_BEFORE_WAIT) {
                    logger.debug("Delete-file rewrite skips {}.{}.{}: another job holds it", database, table[0], table[1]);
                    continue;
                }
                try {
                    leases.acquire(key);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            skipped.remove(key);
            try {
                rewritten += sumFilesProcessed(connection,
                        rewriteDataFilesSql(database, table[0], table[1], rewriteDeleteThreshold));
            } catch (RuntimeException e) {
                logger.error("Delete-file rewrite failed for {}.{}.{}, the other tables still run",
                        database, table[0], table[1], e);
            } finally {
                if (leases != null) {
                    leases.release(key);
                }
            }
        }
        return rewritten;
    }

    /** Each live table's schema and name. */
    private static List<String[]> liveTables(Connection connection, String database) {
        String md = Sql.metadataCatalog(database);
        String sql = ("SELECT s.schema_name, t.table_name FROM %1$s.ducklake_table t JOIN %1$s.ducklake_schema s"
                + " ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL WHERE t.end_snapshot IS NULL"
                + " ORDER BY 1, 2").formatted(md);
        List<String[]> tables = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                tables.add(new String[]{rs.getString(1), rs.getString(2)});
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return tables;
    }

    private static long countSnapshots(Connection connection, String database) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT count(*) FROM %s.ducklake_snapshot"
                     .formatted(Sql.metadataCatalog(database)))) {
            return rs.next() ? rs.getLong(1) : -1;
        }
    }

    static String rewriteDataFilesSql(String database, String schema, String table, Double deleteThreshold) {
        String sql = "CALL ducklake_rewrite_data_files(%s, %s, schema := %s".formatted(
                Sql.literal(database), Sql.literal(table), Sql.literal(schema));
        return deleteThreshold == null ? sql + ")" : sql + ", delete_threshold => " + deleteThreshold + ")";
    }


    private Connection connectionFor(String database) throws SQLException {
        Connection existing = connections.get(database);
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = connections.get(database);
            if (existing != null) {
                return existing;
            }
            Connection opened = RawConnections.open(startupScript, spill.apply(database, connectionSettings));
            connections.put(database, opened);
            return opened;
        }
    }

    /** Sums {@code files_processed} over the one-row-per-table result of a DuckLake compaction call. */
    private static long sumFilesProcessed(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            long processed = 0;
            if (statement.execute(sql)) {
                try (var rs = statement.getResultSet()) {
                    while (rs.next()) {
                        processed += rs.getLong("files_processed");
                    }
                }
            }
            return processed;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void execute(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void time(String type, String step, String database, Runnable action) {
        Timer.Sample sample = metrics.startTimer();
        try {
            action.run();
        } finally {
            metrics.stopTimer(sample, type, step, database);
        }
        logger.debug("{} step '{}' completed for {}", type, step, database);
    }

    @Override
    public void close() {
        connections.values().forEach(connection -> {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("Failed to close a housekeeping connection", e);
            }
        });
    }
}
