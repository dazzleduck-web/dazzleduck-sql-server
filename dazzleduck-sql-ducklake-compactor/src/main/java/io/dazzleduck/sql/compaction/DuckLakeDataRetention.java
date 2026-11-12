package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deletes expired rows from DuckLake tables, as listed in a policy table:
 *
 * <pre>{@code
 * CREATE TABLE lake.main.retention_policy (
 *     table_name  VARCHAR,   -- catalog.schema.table
 *     time_column VARCHAR,   -- the column whose value is the age of a row
 *     retention   VARCHAR    -- '30 days', '12h', or 'unlimited'
 * );
 * }</pre>
 *
 * <p>The policy table is read on every cycle, so a change applies from the next cycle without a
 * restart. A database's cycle handles the rows whose catalog is that database.
 *
 * <h2>Whole files only</h2>
 *
 * A retention is a lower bound, not an exact cut: deleting less now is fine, since the rest goes in a
 * later cycle. So the cutoff is moved down to a file boundary, read from the min/max statistics of
 * the time column in the catalog: no live data file has rows on both sides of it. Each table then
 * gets one {@code DELETE ... WHERE time_column < boundary}, which matches only whole files. DuckLake
 * drops a file all of whose rows are deleted, without writing a delete file or rewriting anything.
 * The delete still reads the time column of the files it drops, to find their rows; the other
 * files are pruned by their statistics.
 *
 * <p>The boundary is a constant of the column's own type, so it is pushed into the scan, and with a
 * build that has the duckdb/ducklake#1895 fix the delete only conflicts with concurrently inserted
 * files that could hold rows below it. Rows whose time column is NULL are kept. A file holding
 * NULLs can never be dropped whole, so it does not move the boundary - it would hold retention back
 * forever - and its expired rows are deleted with a delete file instead. A file without statistics
 * for the column could hold any time, so a table with one is skipped. The dropped files are removed from storage by housekeeping once they fall out
 * of snapshot retention.
 */
public class DuckLakeDataRetention implements DataRetention {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeDataRetention.class);

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    private final String startupScript;
    private final List<String> databases;
    private final String policyTable;
    private final List<String> connectionSettings;
    private final CompactionState metrics;
    private final Clock clock;

    // One raw DuckDB instance per database, as in DuckLakeHousekeeper: different databases'
    // retention cycles run concurrently, and a JDBC Connection is not safe across threads.
    private final ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();

    /**
     * @param databases the configured databases, to report policy rows that no cycle handles
     */
    public DuckLakeDataRetention(String startupScript, List<String> databases, String policyTable,
                                 List<String> connectionSettings, CompactionState metrics) {
        this(startupScript, databases, policyTable, connectionSettings, metrics, Clock.systemUTC());
    }

    DuckLakeDataRetention(String startupScript, List<String> databases, String policyTable,
                          List<String> connectionSettings, CompactionState metrics, Clock clock) {
        this.startupScript = startupScript;
        this.databases = List.copyOf(databases);
        this.policyTable = policyTable;
        this.connectionSettings = connectionSettings;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public Outcome enforce(String database) throws Exception {
        Connection connection = connectionFor(database);
        List<RetentionPolicy> policies;
        try {
            policies = readPolicies(connection, database);
        } catch (SQLException e) {
            // The table may be missing, or the connection broken (catalog backend restarted): open a
            // new one next cycle rather than failing on the same dead connection until a restart.
            evict(database, connection);
            throw e;
        }
        Instant now = clock.instant();
        int tables = 0;
        int failed = 0;
        long rowsDeleted = 0;
        for (RetentionPolicy policy : policies) {
            if (policy.unlimited()) {
                continue;
            }
            tables++;
            Timer.Sample sample = metrics.startTimer();
            try {
                long deleted = deleteExpired(connection, policy, now.minus(policy.retention()));
                rowsDeleted += deleted;
                metrics.addRowsDeleted(database, policy.name(), deleted);
                logger.info("Retention deleted {} row(s) older than {} from {}", deleted, policy.retention(), policy.name());
            } catch (Exception e) {
                failed++;
                logger.error("Retention failed for {}, the other tables still run", policy.name(), e);
            } finally {
                metrics.stopTimer(sample, "retention", "delete", database);
            }
        }
        return new Outcome(tables, rowsDeleted, failed);
    }

    /**
     * The database's policies, one per table. A row that cannot be used is logged and skipped, not
     * fatal. When a table has more than one row the longest retention wins, so a conflicting row
     * never deletes more than the operator meant to keep.
     */
    private List<RetentionPolicy> readPolicies(Connection connection, String database) throws SQLException {
        Map<String, RetentionPolicy> policies = new LinkedHashMap<>();
        String sql = "SELECT table_name, time_column, retention FROM " + policyTable;
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                RetentionPolicy policy;
                try {
                    policy = RetentionPolicy.parse(rs.getString(1), rs.getString(2), rs.getString(3));
                } catch (IllegalArgumentException e) {
                    logger.error("Skipping retention policy row ({}, {}, {}): {}",
                            rs.getString(1), rs.getString(2), rs.getString(3), e.getMessage());
                    continue;
                }
                // DuckDB names are case-insensitive
                if (!policy.catalog().equalsIgnoreCase(database)) {
                    // Reported by one database's cycle only, so it is logged once per cycle.
                    if (database.equals(databases.get(0)) && databases.stream().noneMatch(policy.catalog()::equalsIgnoreCase)) {
                        logger.warn("Retention policy for {} is not applied: {} is not one of the configured databases {}",
                                policy.name(), policy.catalog(), databases);
                    }
                    continue;
                }
                policies.merge(policy.name().toLowerCase(Locale.ROOT), policy, (first, second) -> {
                    logger.warn("Retention policy table lists {} more than once; the longest retention is used",
                            policy.name());
                    return longer(first, second);
                });
            }
        }
        return List.copyOf(policies.values());
    }

    private static RetentionPolicy longer(RetentionPolicy a, RetentionPolicy b) {
        if (a.unlimited()) {
            return a;
        }
        if (b.unlimited()) {
            return b;
        }
        return a.retention().compareTo(b.retention()) >= 0 ? a : b;
    }

    private long deleteExpired(Connection connection, RetentionPolicy policy, Instant cutoff) throws SQLException {
        Optional<String> sql = deleteStatement(connection, policy, cutoff);
        if (sql.isEmpty()) {
            return 0;
        }
        logger.debug("Retention for {}: {}", policy.name(), sql.get());
        try (Statement statement = connection.createStatement()) {
            // long: the first run on a table that holds years of data can delete more than 2^31 rows
            return statement.executeLargeUpdate(sql.get());
        }
    }

    /**
     * The DELETE that drops the table's data files entirely older than {@code cutoff}, or empty when
     * there are none (or a file lacks statistics, so no boundary is known).
     */
    static Optional<String> deleteStatement(Connection connection, RetentionPolicy policy, Instant cutoff)
            throws SQLException {
        String type = columnType(connection, policy);
        List<FileRange> files = fileRanges(connection, policy, type);
        if (files.stream().anyMatch(f -> !f.hasStats())) {
            logger.warn("Retention skipped {}: a data file has no statistics for {}, so no file boundary is known",
                    policy.name(), policy.timeColumn());
            return Optional.empty();
        }
        OptionalLong boundary = fileBoundary(files, toMicros(type, cutoff));
        if (boundary.isEmpty()) {
            logger.debug("Retention for {}: no data file is entirely older than {}", policy.name(), cutoff);
            return Optional.empty();
        }
        Instant at = Instant.EPOCH.plus(boundary.getAsLong(), ChronoUnit.MICROS);
        return Optional.of("DELETE FROM %s WHERE %s < %s".formatted(
                policy.quotedTable(), policy.quotedTimeColumn(), cutoffLiteral(type, at)));
    }

    private static String columnType(Connection connection, RetentionPolicy policy) throws SQLException {
        // Case-insensitive, as DuckDB resolves the same names in the DELETE
        String sql = "SELECT data_type FROM duckdb_columns() WHERE lower(database_name) = lower(?) "
                + "AND lower(schema_name) = lower(?) AND lower(table_name) = lower(?) AND lower(column_name) = lower(?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, policy.catalog());
            statement.setString(2, policy.schema());
            statement.setString(3, policy.table());
            statement.setString(4, policy.timeColumn());
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalArgumentException("%s has no column %s".formatted(policy.name(), policy.timeColumn()));
                }
                return rs.getString(1);
            }
        }
    }

    /** The time-column range of one live data file, in UTC epoch microseconds. */
    record FileRange(Long min, Long max, boolean hasNulls, boolean hasStats) {
    }

    /** The live data files of the table, with the range of the time column each one holds. */
    private static List<FileRange> fileRanges(Connection connection, RetentionPolicy policy, String type)
            throws SQLException {
        String metadata = "\"__ducklake_metadata_" + policy.catalog() + "\"";
        String min = toMicrosSql(type, "s.min_value");
        String max = toMicrosSql(type, "s.max_value");
        String sql = """
                SELECT %2$s AS min_us, %3$s AS max_us,
                       coalesce(s.null_count, 1) > 0 AS has_nulls,
                       s.data_file_id IS NOT NULL
                         AND (s.min_value IS NULL) = (s.max_value IS NULL)
                         AND (s.min_value IS NULL OR %2$s IS NOT NULL)
                         AND (s.max_value IS NULL OR %3$s IS NOT NULL) AS has_stats
                FROM %1$s.ducklake_data_file d
                JOIN %1$s.ducklake_table t ON t.table_id = d.table_id AND t.end_snapshot IS NULL
                JOIN %1$s.ducklake_schema sc ON sc.schema_id = t.schema_id AND sc.end_snapshot IS NULL
                JOIN %1$s.ducklake_column c ON c.table_id = t.table_id AND c.end_snapshot IS NULL
                     AND c.parent_column IS NULL
                LEFT JOIN %1$s.ducklake_file_column_stats s
                     ON s.data_file_id = d.data_file_id AND s.column_id = c.column_id
                WHERE d.end_snapshot IS NULL AND lower(sc.schema_name) = lower(?)
                  AND lower(t.table_name) = lower(?) AND lower(c.column_name) = lower(?)
                """.formatted(metadata, min, max);
        List<FileRange> files = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, policy.schema());
            statement.setString(2, policy.table());
            statement.setString(3, policy.timeColumn());
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    long minUs = rs.getLong(1);
                    Long minValue = rs.wasNull() ? null : minUs;
                    long maxUs = rs.getLong(2);
                    Long maxValue = rs.wasNull() ? null : maxUs;
                    files.add(new FileRange(minValue, maxValue, rs.getBoolean(3), rs.getBoolean(4)));
                }
            }
        }
        return files;
    }

    /**
     * The highest boundary at or below {@code cutoff} that no file straddles, or empty when no file
     * without NULLs lies entirely below it. A file straddles a boundary when it holds rows on both
     * sides of it. Lowering the boundary to such a file's minimum can make another file straddle, so
     * it repeats. A straddling file only holds retention back until its newest row expires; a file
     * holding NULLs never could be dropped whole, so it is left out rather than hold it back forever.
     */
    static OptionalLong fileBoundary(List<FileRange> files, long cutoff) {
        long boundary = cutoff;
        while (true) {
            long lowest = boundary;
            for (FileRange file : files) {
                // a file of NULLs only has no rows below any boundary
                if (file.min() != null && file.min() < boundary && file.max() >= boundary) {
                    lowest = Math.min(lowest, file.min());
                }
            }
            if (lowest == boundary) {
                break;
            }
            boundary = lowest;
        }
        long at = boundary;
        boolean anyExpired = files.stream()
                .anyMatch(f -> f.min() != null && f.max() < at && !f.hasNulls());
        return anyExpired ? OptionalLong.of(boundary) : OptionalLong.empty();
    }

    /** A statistics value of the column, as UTC epoch microseconds, or NULL if it does not parse. */
    private static String toMicrosSql(String columnType, String value) {
        String type = columnType.toUpperCase(Locale.ROOT);
        return switch (type) {
            case "DATE" -> "epoch_us(TRY_CAST(%s AS DATE)::TIMESTAMP)".formatted(value);
            case "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS" ->
                    "epoch_us(TRY_CAST(%s AS %s)::TIMESTAMP)".formatted(value, type);
            case "TIMESTAMP WITH TIME ZONE" -> "epoch_us(TRY_CAST(%s AS TIMESTAMPTZ))".formatted(value);
            default -> throw new IllegalArgumentException(
                    "a retention time column must be a DATE or TIMESTAMP, got " + columnType);
        };
    }

    /** The cutoff in UTC epoch microseconds; for a DATE column, the start of the cutoff's day. */
    static long toMicros(String columnType, Instant cutoff) {
        Instant at = columnType.equalsIgnoreCase("DATE") ? cutoff.truncatedTo(ChronoUnit.DAYS) : cutoff;
        return ChronoUnit.MICROS.between(Instant.EPOCH, at);
    }

    /**
     * The cutoff as a constant of the column's own type. Comparing the column with a constant of
     * another type would cast the column instead, and a cast column is not pushed into the scan, so
     * DuckLake could neither prune files nor scope the delete's conflict check to them.
     *
     * <p>Times are UTC. A DATE column keeps the day the cutoff falls in and deletes the days before it.
     */
    static String cutoffLiteral(String columnType, Instant cutoff) {
        String type = columnType.toUpperCase(Locale.ROOT);
        return switch (type) {
            case "DATE" -> "DATE '%s'".formatted(DATE.format(cutoff));
            case "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS" ->
                    "CAST('%s' AS %s)".formatted(TIMESTAMP.format(cutoff), type);
            case "TIMESTAMP WITH TIME ZONE" -> "TIMESTAMPTZ '%s+00'".formatted(TIMESTAMP.format(cutoff));
            default -> throw new IllegalArgumentException(
                    "a retention time column must be a DATE or TIMESTAMP, got " + columnType);
        };
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

    private void evict(String database, Connection connection) {
        if (connections.remove(database, connection)) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("Failed to close a retention connection", e);
            }
        }
    }

    @Override
    public void close() {
        connections.values().forEach(connection -> {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("Failed to close a retention connection", e);
            }
        });
    }
}
