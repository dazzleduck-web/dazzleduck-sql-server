package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
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
                    long rewritten = sumFilesProcessed(connection, rewriteDataFilesSql(database, rewriteDeleteThreshold));
                    metrics.addFilesRewritten(database, rewritten);
                    logger.debug("Delete-file rewrite for {} rewrote {} data file(s)", database, rewritten);
                });
            } catch (Exception e) {
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
        time("housekeeping", "cleanup", database, () -> execute(connection,
                "CALL ducklake_cleanup_old_files('%s', older_than => now() - INTERVAL '%d seconds')"
                        .formatted(database, retentionSeconds)));
    }

    static String rewriteDataFilesSql(String database, Double deleteThreshold) {
        if (deleteThreshold == null) {
            return "CALL ducklake_rewrite_data_files('%s')".formatted(database);
        }
        return "CALL ducklake_rewrite_data_files('%s', delete_threshold => %s)".formatted(database, deleteThreshold);
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
            Connection opened = RawConnections.open(startupScript, connectionSettings);
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
