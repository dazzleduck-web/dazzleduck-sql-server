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

    // uuuu, not yyyy (year of era): no year 0 or BC year may print as an AD one
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("uuuu-MM-dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter TIMESTAMP =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    /** The earliest boundary written as a literal; a lower one, from a corrupt time, skips the table. */
    static final Instant EARLIEST_BOUNDARY = Instant.parse("0001-01-01T00:00:00Z");

    private final List<String> databases;
    private final String policyTable;
    private final CompactionState metrics;
    private final Clock clock;
    // Different databases' retention cycles run concurrently, hence one connection per database.
    private final RawConnectionCache connections;
    /**
     * Per database and table, the highest boundary a DELETE has run below, so a file holding NULLs
     * only starts one again for rows it may still have above it (see {@link #fileBoundary}).
     */
    private final ConcurrentHashMap<String, Long> deletedBelow = new ConcurrentHashMap<>();

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
            // The connection may be broken (catalog backend restarted): open a new one next cycle
            // rather than fail on the same dead connection until a restart.
            evictIfBroken(database, connection);
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
                String key = database + "|" + policy.name().toLowerCase(Locale.ROOT);
                Deletion deletion = deleteExpired(connection, policy, now.minus(policy.retention()),
                        deletedBelow.getOrDefault(key, Long.MIN_VALUE));
                deletion.boundary().ifPresent(boundary -> deletedBelow.merge(key, boundary, Math::max));
                long deleted = deletion.rows();
                rowsDeleted += deleted;
                // one series per table however the policy row spells it
                metrics.addRowsDeleted(database, policy.name().toLowerCase(Locale.ROOT), deleted);
                if (deleted > 0) {
                    logger.info("Retention deleted {} row(s) older than {} from {}", deleted, policy.retention(), policy.name());
                }
            } catch (Exception e) {
                failed++;
                logger.error("Retention failed for {}, the other tables still run", policy.name(), e);
            } finally {
                metrics.stopTimer(sample, "retention", "delete", database);
            }
        }
        if (failed > 0) {
            // The policy table may live elsewhere and still be readable while the DuckLake
            // catalog's connection is dead.
            evictIfBroken(database, connection);
        }
        return new Outcome(tables, rowsDeleted, failed);
    }

    /**
     * After a failure, drops the connection if it can no longer read the DuckLake catalog (its
     * backend restarted, say), so the next cycle opens a new one. Not for a conflict or a missing
     * table, which a new connection would not fix: reconnecting reruns the whole startup script.
     */
    private void evictIfBroken(String database, Connection connection) {
        if (!catalogReachable(connection, database)) {
            logger.warn("Retention connection for {} cannot read its catalog, reopening it next cycle", database);
            connections.evict(database, connection);
        }
    }

    static boolean catalogReachable(Connection connection, String database) {
        String sql = "SELECT 1 FROM %s.ducklake_snapshot LIMIT 1".formatted(DuckLakeCatalogs.metadataCatalog(database));
        try (Statement statement = connection.createStatement(); ResultSet ignored = statement.executeQuery(sql)) {
            return true;
        } catch (SQLException e) {
            return false;
        }
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
                    // every database's cycle reads the row; one of them reports it
                    if (database.equals(databases.get(0))) {
                        logger.error("Skipping retention policy row ({}, {}, {}): {}",
                                rs.getString(1), rs.getString(2), rs.getString(3), e.getMessage());
                    }
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

    /** What one table's DELETE did: rows deleted, and the boundary it ran below, if it ran. */
    record Deletion(long rows, OptionalLong boundary) {
        static final Deletion NONE = new Deletion(0, OptionalLong.empty());
    }

    /**
     * Deletes the policy's whole expired files in one transaction.
     *
     * @param deletedBelow the highest boundary a DELETE has already run below for this table
     */
    static Deletion deleteExpired(Connection connection, RetentionPolicy policy, Instant cutoff, long deletedBelow)
            throws SQLException {
        // Before the transaction, whose first read must be of the table (see deleteInTransaction)
        String type = columnType(connection, policy);
        // BEGIN/COMMIT statements rather than setAutoCommit(false): the transaction must end with
        // this call, not stay open on a cached connection until the next cycle.
        execute(connection, "BEGIN TRANSACTION");
        try {
            Deletion deletion = deleteInTransaction(connection, policy, type, cutoff, deletedBelow, () -> { });
            execute(connection, "COMMIT");
            return deletion;
        } catch (Throwable e) {
            // any failure, an Error too: a transaction left open would fail every later BEGIN
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
     * Reads the file statistics and runs the DELETE, in a transaction the caller has just begun.
     * Its first statement reads the table, which fixes the transaction's DuckLake snapshot before
     * the statistics are read: they show every file the DELETE sees (and perhaps newer ones, which
     * only lower the boundary), and a file committed later makes the commit conflict if it could
     * hold rows below the cutoff, rather than be partly deleted.
     *
     * <p>That read is filtered on the cutoff: DuckLake scopes a transaction's conflict check by the
     * filters of everything it read, and an unfiltered read - even {@code DESCRIBE}, a filter such
     * as {@code IS NULL}, or one that prunes every file, so the scan never starts - makes the DELETE
     * conflict with every concurrent insert. So the commit conflicts with inserted files that could
     * hold rows older than the cutoff: late data, older than the retention.
     *
     * @param type         the time column's type, from {@link #columnType}
     * @param beforeDelete for tests: runs between reading the statistics and the DELETE
     */
    static Deletion deleteInTransaction(Connection connection, RetentionPolicy policy, String type, Instant cutoff,
                                        long deletedBelow, Runnable beforeDelete) throws SQLException {
        String pin = "SELECT 1 FROM %s WHERE %s < %s LIMIT 1".formatted(policy.quotedTable(), policy.quotedTimeColumn(),
                cutoffLiteral(type, Instant.EPOCH.plus(toMicros(type, cutoff), ChronoUnit.MICROS)));
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(pin)) {
            // Fetched, so the scan has certainly started even with streamed results (RawConnections):
            // a read that never started scopes nothing, and the DELETE would conflict with every insert.
            rs.next();
        }
        OptionalLong boundary = boundary(connection, policy, type, cutoff, deletedBelow);
        if (boundary.isEmpty()) {
            return Deletion.NONE;
        }
        String sql = deleteSql(policy, type, boundary.getAsLong());
        beforeDelete.run();
        logger.debug("Retention for {}: {}", policy.name(), sql);
        try (Statement statement = connection.createStatement()) {
            // long: the first run on a table that holds years of data can delete more than 2^31 rows
            return new Deletion(statement.executeLargeUpdate(sql), boundary);
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    /**
     * The boundary below which the table has something to delete (see {@link #fileBoundary}), or
     * empty when it has nothing.
     */
    static OptionalLong boundary(Connection connection, RetentionPolicy policy, String type, Instant cutoff,
                                 long deletedBelow) throws SQLException {
        long cutoffMicros = toMicros(type, cutoff);
        List<FileRange> files = fileRanges(connection, policy, type, cutoffMicros);
        long unknown = files.stream().filter(f -> !f.hasStats()).count();
        if (unknown > 0) {
            logger.warn("Retention for {}: {} data file(s) have no statistics for {}, so they are never dropped whole",
                    policy.name(), unknown, policy.timeColumn());
        }
        OptionalLong boundary = fileBoundary(files, cutoffMicros, deletedBelow);
        if (boundary.isEmpty()) {
            logger.debug("Retention for {}: nothing to delete below {}", policy.name(), cutoff);
            return boundary;
        }
        if (Instant.EPOCH.plus(boundary.getAsLong(), ChronoUnit.MICROS).isBefore(EARLIEST_BOUNDARY)) {
            logger.warn("Retention skipped {}: a data file holds a time before year 1 in {}", policy.name(), policy.timeColumn());
            return OptionalLong.empty();
        }
        return boundary;
    }

    static String deleteSql(RetentionPolicy policy, String type, long boundary) {
        return "DELETE FROM %s WHERE %s < %s".formatted(policy.quotedTable(), policy.quotedTimeColumn(),
                cutoffLiteral(type, Instant.EPOCH.plus(boundary, ChronoUnit.MICROS)));
    }

    /**
     * The time column's type, as DuckDB names it, from the DuckLake metadata rather than
     * {@code duckdb_columns()}, which would list every attached catalog.
     */
    static String columnType(Connection connection, RetentionPolicy policy) throws SQLException {
        String sql = """
                SELECT c.column_type
                FROM %1$s.ducklake_column c
                JOIN %1$s.ducklake_table t ON t.table_id = c.table_id AND t.end_snapshot IS NULL
                JOIN %1$s.ducklake_schema sc ON sc.schema_id = t.schema_id AND sc.end_snapshot IS NULL
                WHERE c.end_snapshot IS NULL AND c.parent_column IS NULL AND lower(sc.schema_name) = lower(?)
                  AND lower(t.table_name) = lower(?) AND lower(c.column_name) = lower(?)
                """.formatted(metadataCatalog(policy));
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, policy.schema());
            statement.setString(2, policy.table());
            statement.setString(3, policy.timeColumn());
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalArgumentException("%s has no column %s".formatted(policy.name(), policy.timeColumn()));
                }
                // DuckLake's type names; timestamptz is the only one DuckDB spells differently
                String type = rs.getString(1).toUpperCase(Locale.ROOT);
                return type.equals("TIMESTAMPTZ") ? "TIMESTAMP WITH TIME ZONE" : type;
            }
        }
    }

    private static String metadataCatalog(RetentionPolicy policy) {
        return DuckLakeCatalogs.metadataCatalog(policy.catalog());
    }

    /** The time-column range of one live data file, in UTC epoch microseconds. */
    record FileRange(Long min, Long max, boolean hasNulls, boolean hasStats) {

        /** Whether the file can be dropped whole: its rows all have a time, and their range is known. */
        boolean droppable() {
            return hasStats && !hasNulls && min != null && max != null;
        }
    }

    /**
     * The live data files of the table that could hold rows below {@code cutoff} (or are without
     * statistics), with the range of the time column each one holds.
     */
    private static List<FileRange> fileRanges(Connection connection, RetentionPolicy policy, String type, long cutoff)
            throws SQLException {
        String metadata = metadataCatalog(policy);
        String min = toMicrosSql(type, "s.min_value");
        String max = toMicrosSql(type, "s.max_value");
        // Only files that can matter: a file from the cutoff on never moves the boundary or expires
        String sql = """
                SELECT min_us, max_us, has_nulls, has_stats FROM (
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
                ) WHERE NOT has_stats OR min_us < ?
                """.formatted(metadata, min, max);
        List<FileRange> files = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, policy.schema());
            statement.setString(2, policy.table());
            statement.setString(3, policy.timeColumn());
            statement.setLong(4, cutoff);
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
     * The highest boundary at or below {@code cutoff} that no droppable file straddles, or empty when
     * there is nothing to delete below it. A file straddles a boundary when it holds rows on both
     * sides of it. Lowering the boundary to such a file's minimum can make another file straddle, so
     * it repeats; a straddling file only holds retention back until its newest row expires. A file
     * holding NULLs, or without statistics, can never be dropped whole, so it does not move the
     * boundary rather than hold retention back forever.
     *
     * <p>There is something to delete when a droppable file lies entirely below the boundary, or a
     * file holding NULLs may have rows between {@code deletedBelow} and the boundary; those are
     * deleted with a delete file. Its statistics do not change after that, so without
     * {@code deletedBelow} it would start a DELETE that finds nothing every cycle. A file without
     * statistics never starts a delete on its own: it could hold anything, and would be scanned in
     * full every cycle.
     *
     * <p>One sweep in order of descending maximum: once a file's maximum is below the boundary, so
     * is every later file's, and a file passed over had its minimum at or above a boundary that has
     * only gone down since.
     */
    static OptionalLong fileBoundary(List<FileRange> files, long cutoff) {
        return fileBoundary(files, cutoff, Long.MIN_VALUE);
    }

    static OptionalLong fileBoundary(List<FileRange> files, long cutoff, long deletedBelow) {
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
        boolean anyToDelete = droppable.stream().anyMatch(f -> f.max() < at)
                || files.stream().anyMatch(f -> f.hasStats() && f.hasNulls() && f.min() != null
                        && f.min() < at && f.max() >= deletedBelow);
        return anyToDelete ? OptionalLong.of(boundary) : OptionalLong.empty();
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
