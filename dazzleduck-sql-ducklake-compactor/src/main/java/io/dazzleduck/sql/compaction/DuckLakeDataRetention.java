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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

    /**
     * The earliest time a file may hold to be dropped whole. A file with an earlier one, from a
     * corrupt value, is treated like one without statistics, so it cannot drag the boundary below
     * what a literal can express.
     */
    static final Instant EARLIEST_BOUNDARY = Instant.parse("0001-01-01T00:00:00Z");
    private static final long EARLIEST_MICROS = ChronoUnit.MICROS.between(Instant.EPOCH, EARLIEST_BOUNDARY);

    private final List<String> databases;
    private final String policyTable;
    private final CompactionState metrics;
    private final Clock clock;
    private final String startupScript;
    private final List<String> connectionSettings;
    private final int maxFilesPerCycle;
    /** Tables already warned about for files without statistics; those stay so until rewritten. */
    private final Set<String> warnedNoStatistics = ConcurrentHashMap.newKeySet();
    /** The connection of each database's running cycle, so {@link #close} can abort it. */
    private final ConcurrentHashMap<String, Connection> running = new ConcurrentHashMap<>();
    private volatile boolean closed;

    /**
     * @param databases the configured databases, to report policy rows that no cycle handles
     */
    public DuckLakeDataRetention(String startupScript, List<String> databases, String policyTable,
                                 List<String> connectionSettings, int maxFilesPerCycle, CompactionState metrics) {
        this(startupScript, databases, policyTable, connectionSettings, maxFilesPerCycle, metrics, Clock.systemUTC());
    }

    /** @param maxFilesPerCycle the most data files one table's DELETE drops in a cycle; 0 for no cap */
    DuckLakeDataRetention(String startupScript, List<String> databases, String policyTable,
                          List<String> connectionSettings, int maxFilesPerCycle, CompactionState metrics, Clock clock) {
        this.maxFilesPerCycle = maxFilesPerCycle;
        this.databases = List.copyOf(databases);
        this.policyTable = policyTable;
        this.startupScript = startupScript;
        this.connectionSettings = List.copyOf(connectionSettings);
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public Outcome enforce(String database) throws Exception {
        if (closed) {
            throw new IllegalStateException("data retention is closed");
        }
        // A new connection each cycle, closed at its end: it runs at most hourly by default, so
        // there is no stale connection to detect or replace, and nothing to keep between cycles.
        try (Connection connection = RawConnections.open(startupScript, connectionSettings)) {
            running.put(database, connection);
            try {
                return enforce(connection, database);
            } finally {
                running.remove(database, connection);
            }
        }
    }

    private Outcome enforce(Connection connection, String database) throws SQLException {
        List<RetentionPolicy> policies = readPolicies(connection, database);
        Instant now = clock.instant();
        int tables = 0;
        int failed = 0;
        long rowsDeleted = 0;
        for (RetentionPolicy policy : policies) {
            if (policy.unlimited()) {
                continue;
            }
            tables++;
            String table = policy.name().toLowerCase(Locale.ROOT); // one series per table however it is spelled
            Timer.Sample sample = metrics.startTimer();
            try {
                Deletion deletion = deleteExpired(connection, policy, now.minus(policy.retention()), maxFilesPerCycle);
                rowsDeleted += deletion.rows();
                metrics.addRowsDeleted(database, table, deletion.rows());
                metrics.setBoundaryLag(database, table, deletion.lag());
                if (deletion.rows() > 0) {
                    logger.info("Retention deleted {} row(s) older than {} from {}", deletion.rows(), policy.retention(), policy.name());
                }
                if (deletion.lag().compareTo(policy.retention()) > 0) {
                    logger.warn("Retention for {} is held back by {}: a data file holds rows from both sides of the cutoff, "
                                    + "so rows older than the retention are kept until its newest row expires",
                            policy.name(), deletion.lag());
                }
                if (deletion.filesWithoutStatistics() > 0 && warnedNoStatistics.add(database + "|" + table)) {
                    logger.warn("Retention for {}: {} data file(s) have no statistics for {}, so they are never dropped whole",
                            policy.name(), deletion.filesWithoutStatistics(), policy.timeColumn());
                }
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
     * Aborts the cycles still running, by closing their connections: shutdown must not wait for a
     * long DELETE, and an aborted one rolls back.
     */
    @Override
    public void close() {
        closed = true;
        running.values().forEach(connection -> {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("Failed to close a running retention connection", e);
            }
        });
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

    /**
     * What one table's cycle did: the rows deleted, how far files straddling the cutoff held the
     * boundary back, and how many files have no statistics for the time column.
     */
    record Deletion(long rows, Duration lag, long filesWithoutStatistics) {
        static final Deletion NONE = new Deletion(0, Duration.ZERO, 0);
    }

    /**
     * Deletes the policy's expired files (see the class comment) in one transaction.
     *
     * @param maxFiles the most data files to drop; 0 for no cap
     */
    static Deletion deleteExpired(Connection connection, RetentionPolicy policy, Instant cutoff, int maxFiles)
            throws SQLException {
        // Before the transaction, whose first read must be of the table (see deleteInTransaction)
        String type = columnType(connection, policy);
        // BEGIN/COMMIT statements rather than setAutoCommit(false): the transaction must end with
        // this call, not stay open for the cycle's next table.
        execute(connection, "BEGIN TRANSACTION");
        try {
            Deletion deletion = deleteInTransaction(connection, policy, type, cutoff, maxFiles, () -> { });
            execute(connection, "COMMIT");
            return deletion;
        } catch (Throwable e) {
            // any failure, an Error too: a transaction left open would fail the next table's BEGIN
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
     *
     * <ol>
     *   <li>A read of rows older than the cutoff fixes the transaction's DuckLake snapshot, so the
     *       statistics read next show every file the DELETE sees (and perhaps newer ones, which only
     *       lower the boundary); a file committed later makes the commit conflict if it could hold
     *       rows below the cutoff, rather than be partly deleted. It must be filtered: DuckLake
     *       scopes a transaction's conflict check by the filters of everything it read, and an
     *       unfiltered read - even {@code DESCRIBE}, a filter such as {@code IS NULL}, or one that
     *       prunes every file, so the scan never starts - makes the DELETE conflict with every
     *       concurrent insert. So the commit conflicts only with late data, older than the
     *       retention. When it finds no row, there is nothing to delete.</li>
     *   <li>The boundary, from the statistics (see {@link #fileBoundary}), lowered further so that
     *       at most {@code maxFiles} files are dropped: the first DELETE on a table with a long
     *       backlog would otherwise scan all of it in one transaction, which may outlast the
     *       catalog's idle-transaction timeout and fail every cycle.</li>
     *   <li>When the boundary is below the cutoff, a read of a row below the boundary: the rows
     *       older than the cutoff may all lie between the two. It also covers what statistics
     *       cannot tell: rows in files holding NULLs or without statistics, which may already have
     *       been deleted by an earlier cycle, and rows still inlined in the catalog.</li>
     *   <li>The DELETE, only when there is a row to delete.</li>
     * </ol>
     *
     * @param type         the time column's type, from {@link #columnType}
     * @param maxFiles     the most data files to drop; 0 for no cap
     * @param beforeDelete for tests: runs between reading the statistics and the DELETE
     */
    static Deletion deleteInTransaction(Connection connection, RetentionPolicy policy, String type, Instant cutoff,
                                        int maxFiles, Runnable beforeDelete) throws SQLException {
        long cutoffMicros = toMicros(type, cutoff);
        if (!anyRowBelow(connection, policy, type, cutoffMicros)) {
            return Deletion.NONE;
        }
        List<FileRange> files = fileRanges(connection, policy, type, cutoffMicros);
        long withoutStatistics = files.stream().filter(f -> !f.hasStats()).count();
        long uncapped = fileBoundary(files, cutoffMicros, 0);
        Duration lag = Duration.of(cutoffMicros - uncapped, ChronoUnit.MICROS);
        long boundary = fileBoundary(files, cutoffMicros, maxFiles);
        if (boundary < cutoffMicros && !anyRowBelow(connection, policy, type, boundary)) {
            logger.debug("Retention for {}: nothing to delete below the file boundary", policy.name());
            return new Deletion(0, lag, withoutStatistics);
        }
        String sql = deleteSql(policy, type, boundary);
        beforeDelete.run();
        logger.debug("Retention for {}: {}", policy.name(), sql);
        try (Statement statement = connection.createStatement()) {
            // long: the first run on a table that holds years of data can delete more than 2^31 rows
            return new Deletion(statement.executeLargeUpdate(sql), lag, withoutStatistics);
        }
    }

    /** Whether the table has a row whose time is below {@code micros}. */
    private static boolean anyRowBelow(Connection connection, RetentionPolicy policy, String type, long micros)
            throws SQLException {
        String sql = "SELECT 1 FROM %s WHERE %s < %s LIMIT 1".formatted(policy.quotedTable(), policy.quotedTimeColumn(),
                cutoffLiteral(type, Instant.EPOCH.plus(micros, ChronoUnit.MICROS)));
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            // Fetched, so the scan has certainly started even with streamed results (RawConnections):
            // a read that never started scopes nothing, and the DELETE would conflict with every insert.
            return rs.next();
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
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
                type = type.equals("TIMESTAMPTZ") ? "TIMESTAMP WITH TIME ZONE" : type;
                toMicrosSql(type, "NULL"); // rejects a type other than DATE or TIMESTAMP, before any transaction
                return type;
            }
        }
    }

    /** The metadata catalog under DuckLake's default name, as the rest of the compactor reads it. */
    private static String metadataCatalog(RetentionPolicy policy) {
        return "\"__ducklake_metadata_" + policy.catalog() + "\"";
    }

    /** The time-column range of one live data file, in UTC epoch microseconds. */
    record FileRange(Long min, Long max, boolean hasNulls, boolean hasStats) {

        /** Whether the file can be dropped whole: its rows all have a time, and their range is known. */
        boolean droppable() {
            return hasStats && !hasNulls && min != null && max != null && min >= EARLIEST_MICROS;
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
                SELECT min_us, max_us, has_nulls,
                       has_row AND (min_value IS NULL) = (max_value IS NULL)
                         AND (min_value IS NULL OR min_us IS NOT NULL)
                         AND (max_value IS NULL OR max_us IS NOT NULL) AS has_stats
                FROM (
                SELECT %2$s AS min_us, %3$s AS max_us, s.min_value, s.max_value,
                       coalesce(s.null_count, 1) > 0 AS has_nulls,
                       s.data_file_id IS NOT NULL AS has_row
                FROM %1$s.ducklake_data_file d
                JOIN %1$s.ducklake_table t ON t.table_id = d.table_id AND t.end_snapshot IS NULL
                JOIN %1$s.ducklake_schema sc ON sc.schema_id = t.schema_id AND sc.end_snapshot IS NULL
                JOIN %1$s.ducklake_column c ON c.table_id = t.table_id AND c.end_snapshot IS NULL
                     AND c.parent_column IS NULL
                LEFT JOIN %1$s.ducklake_file_column_stats s
                     ON s.data_file_id = d.data_file_id AND s.column_id = c.column_id
                WHERE d.end_snapshot IS NULL AND lower(sc.schema_name) = lower(?)
                  AND lower(t.table_name) = lower(?) AND lower(c.column_name) = lower(?)
                )) WHERE NOT has_stats OR min_us < ?
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
     * The highest boundary at or below {@code cutoff} that no droppable file straddles, with at most
     * {@code maxFiles} droppable files entirely below it (0 for no cap). A file straddles a boundary
     * when it holds rows on both sides of it. Lowering the boundary to such a file's minimum can make
     * another file straddle, so it repeats; a straddling file only holds retention back until its
     * newest row expires. A file holding NULLs, without statistics, or with a time before year 1 can
     * never be dropped whole, so it does not move the boundary rather than hold retention back
     * forever; its expired rows are deleted with a delete file.
     *
     * <p>Over the cap, the boundary is lowered to the minimum of the oldest file past it, and swept
     * again; files that overlap so that no lower boundary drops any of them whole are still dropped
     * together, past the cap, rather than never.
     */
    static long fileBoundary(List<FileRange> files, long cutoff, int maxFiles) {
        List<FileRange> droppable = files.stream().filter(FileRange::droppable).toList();
        long boundary = sweep(droppable, cutoff);
        if (maxFiles <= 0) {
            return boundary;
        }
        long at = boundary;
        List<FileRange> below = droppable.stream()
                .filter(f -> f.max() < at)
                .sorted(Comparator.comparingLong(FileRange::min))
                .toList();
        if (below.size() <= maxFiles) {
            return boundary;
        }
        long capped = sweep(droppable, below.get(maxFiles).min());
        return droppable.stream().anyMatch(f -> f.max() < capped) ? capped : boundary;
    }

    /**
     * One sweep in order of descending maximum: once a file's maximum is below the boundary, so is
     * every later file's, and a file passed over had its minimum at or above a boundary that has
     * only gone down since.
     */
    private static long sweep(List<FileRange> droppable, long from) {
        List<FileRange> byMax = droppable.stream()
                .sorted(Comparator.comparingLong(FileRange::max).reversed())
                .toList();
        long boundary = from;
        for (FileRange file : byMax) {
            if (file.max() < boundary) {
                break;
            }
            if (file.min() < boundary) {
                boundary = file.min();
            }
        }
        return boundary;
    }

    /** A statistics value of the column, as UTC epoch microseconds, or NULL if it does not parse. */
    static String toMicrosSql(String columnType, String value) {
        String type = columnType.toUpperCase(Locale.ROOT);
        // TRY: a value that parses but has no microsecond count, such as 'infinity', is NULL too
        return "TRY(" + switch (type) {
            case "DATE" -> "epoch_us(TRY_CAST(%s AS DATE)::TIMESTAMP)".formatted(value);
            case "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS" ->
                    "epoch_us(TRY_CAST(%s AS %s)::TIMESTAMP)".formatted(value, type);
            // Floored to microseconds: a cast to TIMESTAMP truncates toward zero, which before 1970
            // would raise a file's minimum above its first row.
            case "TIMESTAMP_NS" -> ("epoch_ns(TRY_CAST(%1$s AS TIMESTAMP_NS)) // 1000 - "
                    + "CASE WHEN epoch_ns(TRY_CAST(%1$s AS TIMESTAMP_NS)) %% 1000 < 0 THEN 1 ELSE 0 END").formatted(value);
            // A value without an offset is UTC, as the cutoff is, whatever the session's TimeZone
            // An offset follows a time of day; a date's own '-01' is not one.
            case "TIMESTAMP WITH TIME ZONE" -> ("CASE WHEN regexp_matches(%1$s, '[0-9]:[0-9]{2}(:[0-9]{2}(\\.[0-9]+)?)? ?([+-][0-9]{2}(?::?[0-9]{2})?|Z)$') "
                    + "THEN epoch_us(TRY_CAST(%1$s AS TIMESTAMPTZ)) ELSE epoch_us(TRY_CAST(%1$s AS TIMESTAMP)) END").formatted(value);
            default -> throw new IllegalArgumentException(
                    "a retention time column must be a DATE or TIMESTAMP, got " + columnType);
        } + ")";
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

}
