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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

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
 * <p>The statistics are read and the DELETE runs in one transaction, so the DELETE sees exactly the
 * files the boundary was computed from: a file committed in between makes the commit conflict rather
 * than be partly deleted. The boundary is a constant of the column's own type, so it is pushed into
 * the scan, and with a build that has the duckdb/ducklake#1895 fix the delete only conflicts with
 * concurrently inserted files that could hold rows below it.
 *
 * <p>Rows whose time column is NULL are kept. A file holding NULLs can never be dropped whole, so it
 * does not move the boundary - it would hold retention back forever - and its expired rows are
 * deleted with a delete file instead. So are those of a file without statistics for the column,
 * such as one written before the column was added. The dropped files are removed from storage by
 * housekeeping once they fall out of snapshot retention.
 */
public class DuckLakeDataRetention implements DataRetention {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeDataRetention.class);

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    private final List<String> databases;
    private final String policyTable;
    private final CompactionState metrics;
    private final Clock clock;
    // Different databases' retention cycles run concurrently, hence one connection per database.
    private final RawConnectionCache connections;

    /**
     * @param databases the configured databases, to report policy rows that no cycle handles
     */
    public DuckLakeDataRetention(String startupScript, List<String> databases, String policyTable,
                                 List<String> connectionSettings, CompactionState metrics) {
        this(startupScript, databases, policyTable, connectionSettings, metrics, Clock.systemUTC());
    }

    DuckLakeDataRetention(String startupScript, List<String> databases, String policyTable,
                          List<String> connectionSettings, CompactionState metrics, Clock clock) {
        this.databases = List.copyOf(databases);
        this.policyTable = policyTable;
        this.connections = new RawConnectionCache(startupScript, connectionSettings, "retention");
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public Outcome enforce(String database) throws Exception {
        Connection connection = connections.get(database);
        List<RetentionPolicy> policies;
        try {
            policies = readPolicies(connection, database);
        } catch (SQLException e) {
            // The table may be missing, or the connection broken (catalog backend restarted): open a
            // new one next cycle rather than failing on the same dead connection until a restart.
            connections.evict(database, connection);
            throw e;
        }
        Instant now = clock.instant();
        int tables = 0;
        int failed = 0;
        long rowsDeleted = 0;
        boolean sqlFailure = false;
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
                sqlFailure |= e instanceof SQLException;
                logger.error("Retention failed for {}, the other tables still run", policy.name(), e);
            } finally {
                metrics.stopTimer(sample, "retention", "delete", database);
            }
        }
        if (sqlFailure) {
            // Most likely a conflict, but it may be a broken connection to the DuckLake catalog
            // while the policy table, elsewhere, is still readable: start fresh next cycle.
            connections.evict(database, connection);
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

    /** Deletes the policy's whole expired files in one transaction; returns the rows deleted. */
    static long deleteExpired(Connection connection, RetentionPolicy policy, Instant cutoff) throws SQLException {
        // BEGIN/COMMIT statements rather than setAutoCommit(false): the transaction must end with
        // this call, not stay open on a cached connection until the next cycle.
        execute(connection, "BEGIN TRANSACTION");
        try {
            long deleted = deleteInTransaction(connection, policy, cutoff);
            execute(connection, "COMMIT");
            return deleted;
        } catch (SQLException | RuntimeException e) {
            try {
                execute(connection, "ROLLBACK");
            } catch (SQLException rollback) {
                // a failed COMMIT has already ended the transaction
                e.addSuppressed(rollback);
            }
            throw e;
        }
    }

    /**
     * Reads the file statistics and runs the DELETE, in the caller's transaction. Looking up the
     * column's type goes through the DuckLake catalog, which fixes the transaction's snapshot before
     * the statistics are read: they show every file the DELETE sees (and perhaps newer ones, which
     * only lower the boundary), and a file committed later makes the commit conflict if it could
     * hold rows below the boundary, rather than be partly deleted.
     */
    static long deleteInTransaction(Connection connection, RetentionPolicy policy, Instant cutoff)
            throws SQLException {
        return deleteInTransaction(connection, policy, cutoff, () -> { });
    }

    /** For tests: {@code beforeDelete} runs between reading the statistics and the DELETE. */
    static long deleteInTransaction(Connection connection, RetentionPolicy policy, Instant cutoff, Runnable beforeDelete)
            throws SQLException {
        Optional<String> sql = deleteStatement(connection, policy, cutoff);
        if (sql.isEmpty()) {
            return 0;
        }
        beforeDelete.run();
        logger.debug("Retention for {}: {}", policy.name(), sql.get());
        try (Statement statement = connection.createStatement()) {
            // long: the first run on a table that holds years of data can delete more than 2^31 rows
            return statement.executeLargeUpdate(sql.get());
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    /**
     * The DELETE that drops the table's data files entirely older than {@code cutoff}, or empty when
     * there are none.
     */
    static Optional<String> deleteStatement(Connection connection, RetentionPolicy policy, Instant cutoff)
            throws SQLException {
        // first: it fixes the DuckLake snapshot (see deleteInTransaction)
        String type = columnType(connection, policy);
        List<FileRange> files = fileRanges(connection, policy, type);
        long unknown = files.stream().filter(f -> !f.hasStats()).count();
        if (unknown > 0) {
            logger.warn("Retention for {}: {} data file(s) have no statistics for {}, so they are never dropped whole",
                    policy.name(), unknown, policy.timeColumn());
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

        /** Whether the file can be dropped whole: its rows all have a time, and their range is known. */
        boolean droppable() {
            return hasStats && !hasNulls && min != null && max != null;
        }
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
     * lies entirely below it. A file straddles a boundary when it holds rows on both sides of it.
     * Lowering the boundary to such a file's minimum can make another file straddle, so it repeats;
     * a straddling file only holds retention back until its newest row expires. A file holding NULLs,
     * or without statistics, can never be dropped whole, so it is left out rather than hold
     * retention back forever.
     *
     * <p>One sweep in order of descending maximum: once a file's maximum is below the boundary, so
     * is every later file's, and a file passed over had its minimum at or above a boundary that has
     * only gone down since.
     */
    static OptionalLong fileBoundary(List<FileRange> files, long cutoff) {
        List<FileRange> droppable = files.stream()
                .filter(FileRange::droppable)
                .sorted(Comparator.comparingLong(FileRange::max).reversed())
                .toList();
        long boundary = cutoff;
        for (FileRange file : droppable) {
            if (file.max() < boundary) {
                break;
            }
            if (file.min() < boundary) {
                boundary = file.min();
            }
        }
        long at = boundary;
        return droppable.stream().anyMatch(f -> f.max() < at) ? OptionalLong.of(boundary) : OptionalLong.empty();
    }

    /** A statistics value of the column, as UTC epoch microseconds, or NULL if it does not parse. */
    static String toMicrosSql(String columnType, String value) {
        String type = columnType.toUpperCase(Locale.ROOT);
        return switch (type) {
            case "DATE" -> "epoch_us(TRY_CAST(%s AS DATE)::TIMESTAMP)".formatted(value);
            case "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS" ->
                    "epoch_us(TRY_CAST(%s AS %s)::TIMESTAMP)".formatted(value, type);
            // Floored to microseconds: a cast to TIMESTAMP truncates toward zero, which before 1970
            // would raise a file's minimum above its first row.
            case "TIMESTAMP_NS" -> ("(epoch_ns(TRY_CAST(%1$s AS TIMESTAMP_NS)) // 1000 - "
                    + "CASE WHEN epoch_ns(TRY_CAST(%1$s AS TIMESTAMP_NS)) %% 1000 < 0 THEN 1 ELSE 0 END)").formatted(value);
            case "TIMESTAMP WITH TIME ZONE" -> "epoch_us(TRY_CAST(%s AS TIMESTAMPTZ))".formatted(value);
            default -> throw new IllegalArgumentException(
                    "a retention time column must be a DATE or TIMESTAMP, got " + columnType);
        };
    }

    /**
     * The cutoff in UTC epoch microseconds, truncated to what the column can hold (for a DATE, the
     * start of the cutoff's day), so that {@link #cutoffLiteral} writes it exactly: a cast to a
     * coarser timestamp rounds, and rounding down would keep rows of a file counted as expired.
     */
    static long toMicros(String columnType, Instant cutoff) {
        ChronoUnit precision = switch (columnType.toUpperCase(Locale.ROOT)) {
            case "DATE" -> ChronoUnit.DAYS;
            case "TIMESTAMP_S" -> ChronoUnit.SECONDS;
            case "TIMESTAMP_MS" -> ChronoUnit.MILLIS;
            default -> ChronoUnit.MICROS;
        };
        return ChronoUnit.MICROS.between(Instant.EPOCH, cutoff.truncatedTo(precision));
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

    @Override
    public void close() {
        connections.close();
    }
}
