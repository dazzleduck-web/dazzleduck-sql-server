package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;

public class DuckDbTierCompactor implements TierCompactor {

    private static final Logger logger = LoggerFactory.getLogger(DuckDbTierCompactor.class);

    private final String startupScript;
    private final CompactionState metrics;

    // One real DuckDB instance per (database, tier) pair — see RawConnections for why this can't be
    // a duplicated/shared connection (GLOBAL-scoped settings like memory_limit would leak across
    // tiers). Keyed by BOTH database and tier, not tier alone: with multiple databases configured,
    // CompactionService deliberately sizes its thread pool so the same tier can run concurrently for
    // different databases, and a plain JDBC Connection is not safe for concurrent use by more than
    // one thread — keying by tier name only would hand two such threads the same Connection object.
    // Opened lazily on first use and reused for the pair's whole lifetime.
    private final ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();

    public DuckDbTierCompactor(String startupScript, CompactionState metrics) {
        this.startupScript = startupScript;
        this.metrics = metrics;
    }

    @Override
    public MergeOutcome compact(String database, CompactionTier tier) throws Exception {
        Connection connection = connectionFor(database, tier);
        String sql = mergeAdjacentFilesSql(database, tier.minFileSize(), tier.maxFileSize(), tier.maxCompactedFiles());
        Timer.Sample sample = metrics.startTimer();
        long start = System.nanoTime();
        // ducklake_merge_adjacent_files returns one row per compacted table:
        // (schema_name, table_name, files_processed, files_created). Sum across tables; null when the
        // call returned no rows (nothing merged). Confirmed against DuckLake on DuckDB v1.5.x.
        Long filesProcessed = null;
        Long filesCreated = null;
        try (Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute(sql);
            if (hasResultSet) {
                try (var rs = statement.getResultSet()) {
                    long processed = 0, created = 0;
                    boolean anyRows = false;
                    while (rs.next()) {
                        processed += rs.getLong("files_processed");
                        created += rs.getLong("files_created");
                        anyRows = true;
                    }
                    if (anyRows) {
                        filesProcessed = processed;
                        filesCreated = created;
                    }
                }
            }
        } finally {
            metrics.stopTimer(sample, tier.name(), "merge", database);
        }
        long durationMergeMs = (System.nanoTime() - start) / 1_000_000;
        logger.debug("Tier '{}' merge completed for {} in {}ms (filesProcessed={}, filesCreated={})",
                tier.name(), database, durationMergeMs, filesProcessed, filesCreated);
        // No separate commit time: merge + catalog commit are one atomic CALL here (see CompactionRun).
        return new MergeOutcome(durationMergeMs, filesProcessed, filesCreated);
    }

    private Connection connectionFor(String database, CompactionTier tier) throws SQLException {
        String key = connectionKey(database, tier.name());
        Connection existing = connections.get(key);
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = connections.get(key);
            if (existing != null) {
                return existing;
            }
            Connection opened = RawConnections.open(startupScript, tier.connectionSettings());
            connections.put(key, opened);
            return opened;
        }
    }

    private static String connectionKey(String database, String tierName) {
        return database.length() + ":" + database + "|" + tierName;
    }

    /**
     * {@code min_file_size} is what keeps tiers from racing each other — every tier passes one
     * (always required, including {@code 0} for the lowest tier), fencing it to its own range so
     * tiers can run concurrently with no lock (their ranges are validated disjoint at startup in
     * {@link CompactionConfig#from}). {@code max_compacted_files} is appended only when positive,
     * matching that parameter's own unbounded-at-zero convention.
     */
    static String mergeAdjacentFilesSql(String database, long minFileSizeBytes, long maxFileSizeBytes, long maxCompactedFiles) {
        StringBuilder sql = new StringBuilder("CALL ducklake_merge_adjacent_files('%s', min_file_size := %d, max_file_size := %d"
                .formatted(database, minFileSizeBytes, maxFileSizeBytes));
        if (maxCompactedFiles > 0) {
            sql.append(", max_compacted_files := ").append(maxCompactedFiles);
        }
        return sql.append(")").toString();
    }

    @Override
    public void close() {
        connections.values().forEach(connection -> {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("Failed to close a tier connection", e);
            }
        });
    }
}
