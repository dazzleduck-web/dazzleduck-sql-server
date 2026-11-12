package io.dazzleduck.sql.compaction;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs writers, readers, both compaction tiers, housekeeping (delete-file rewrite, snapshot expiry,
 * old-file cleanup) and orphaned-file deletion against one table, all at once, then checks that no
 * row was lost, duplicated or resurrected and that the catalog is still consistent.
 *
 * <p>{@code events} takes inserts, deletes and updates. {@code ducklake_merge_adjacent_files} skips any
 * file with delete files, so on that table most of the work falls to the housekeeping rewrite.
 * {@code appends} only takes inserts, so its files grow through both compaction tiers.
 *
 * <p>Every writer owns the ids {@code id % writers == writer} and keeps an in-memory model of what
 * its slice should contain, applied only after a statement commits. Transaction conflicts are
 * expected under this much concurrency and are counted, not failed; any other error is a failure.
 *
 * <p>Excluded from the default build (see the {@code stress} profile in this module's pom). Knobs,
 * all system properties:
 * <ul>
 *   <li>{@code compaction.stress.duration} (ISO-8601, default {@code PT3M})</li>
 *   <li>{@code compaction.stress.writers} (default 4)</li>
 *   <li>{@code compaction.stress.readers} (default 2)</li>
 *   <li>{@code compaction.stress.seed} (default: random, printed; fixes each thread's random choices, though thread timing still varies between runs)</li>
 *   <li>{@code ducklake.extension.path}: a DuckLake extension file to {@code LOAD} instead of
 *       installing the stock one, e.g. a locally built or patched DuckLake. The file must be named
 *       {@code ducklake.duckdb_extension}: DuckDB derives the entry point from it.</li>
 * </ul>
 *
 * <p>Stock DuckLake 1.5 failed this test: two concurrent deletes on one data file left it with two
 * active delete files, and deleted rows came back (duckdb/ducklake#1482, carried by the
 * {@code dazzleduck-web/ducklake} 1.5 patch). DuckLake 2.0 checks deletes and compactions per file
 * upstream, so the stock extension is run.
 */
@Tag("stress")
class CompactionStressTest {

    static final Duration DURATION = Duration.parse(System.getProperty("compaction.stress.duration", "PT3M"));
    static final int WRITERS = Integer.getInteger("compaction.stress.writers", 4);
    static final int READERS = Integer.getInteger("compaction.stress.readers", 2);
    static final long SEED = Long.getLong("compaction.stress.seed", new Random().nextLong());
    static final String EXTENSION_PATH = System.getProperty("ducklake.extension.path", "");

    static final String CATALOG = "stress_lake";
    static final String MD_DATABASE = "__ducklake_metadata_" + CATALOG;
    static final String TABLE = CATALOG + ".main.events";
    static final String APPENDS = CATALOG + ".main.appends";

    /** Short so housekeeping really expires snapshots and deletes retired files during the run. */
    static final Duration SNAPSHOT_RETENTION = Duration.ofSeconds(3);
    /**
     * {@code ducklake_delete_orphaned_files} deletes any Parquet file under DATA_PATH that the catalog
     * does not know and that is older than this, including files an unfinished transaction has
     * written but not yet committed. So it must exceed the longest a commit can take: every write
     * here is one DuckLake INSERT/DELETE/UPDATE, but its commit retries up to
     * {@link #COMMIT_RETRIES} times, waiting 100 ms x 1.5^n (each wait capped at 60 s), which is up
     * to about 15 minutes. A 20-second age once deleted the file of a commit that retried for 28
     * seconds, and the commit then landed pointing at it. Longer than the run, so the cleaner scans
     * concurrently but deletes nothing here. Ingestion that writes with COPY first and registers
     * later with {@code ducklake_add_data_files} (the otel collector) needs hours.
     */
    static final Duration ORPHAN_AGE = Duration.ofMinutes(16);

    /** DuckLake's commit retries, as the compactor and the otel collector configure them. */
    static final int COMMIT_RETRIES = 30;

    // Small bands so files move through both tiers within one run.
    static final CompactionTier MINOR = new CompactionTier(
            "minor", true, Duration.ofSeconds(1), 0, 32 * 1024L, 0, List.of());
    static final CompactionTier MAJOR = new CompactionTier(
            "major", true, Duration.ofSeconds(1), 32 * 1024L, 4 * 1024 * 1024L, 0, List.of());

    /** Housekeeping's delete-file rewrite threshold; the post-run check uses the same rule. */
    static final double REWRITE_DELETE_THRESHOLD = 0.3;

    @TempDir
    static Path tempDir;

    static Path dataPath;
    static DuckLakeHousekeeper housekeeper;
    static CompactionState state;

    static PostgreSQLContainer<?> postgres;
    static String startupScript;
    static CompactionRunLog runLog;
    static CompactionService service;
    static ListAppender<ILoggingEvent> errorLog;

    /** Unexpected errors from any thread; the test fails if this is not empty. */
    static final List<String> unexpectedErrors = new CopyOnWriteArrayList<>();
    static final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();

    @BeforeAll
    static void setUp() throws Exception {
        postgres = new PostgreSQLContainer<>("postgres:15-alpine")
                .withDatabaseName("ducklake")
                .withUsername("duck")
                .withPassword("duck");
        postgres.start();

        dataPath = tempDir.resolve("data");
        Files.createDirectories(dataPath);
        String connectionString = "host=%s port=%d dbname=ducklake user=duck password=duck".formatted(
                postgres.getHost(), postgres.getFirstMappedPort());
        String load = EXTENSION_PATH.isBlank()
                ? "INSTALL ducklake; LOAD ducklake; SET ducklake_max_retry_count = %d;".formatted(COMMIT_RETRIES)
                : "LOAD '%s'; SET ducklake_max_retry_count = %d;".formatted(EXTENSION_PATH, COMMIT_RETRIES);
        startupScript = "%s\nATTACH 'ducklake:postgres:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0);"
                .formatted(load, connectionString, CATALOG, dataPath);

        // The first connection installs the extension, so later concurrent opens never race on it.
        try (Connection connection = RawConnections.open(startupScript, List.of());
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE %s (id BIGINT, writer INTEGER, version INTEGER, payload VARCHAR)".formatted(TABLE));
            statement.execute("CREATE TABLE %s (id BIGINT, payload VARCHAR)".formatted(APPENDS));
            try (ResultSet rs = statement.executeQuery(
                    "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'")) {
                rs.next();
                System.out.printf("CompactionStressTest: duration=%s writers=%d readers=%d seed=%d ducklake=%s (%s)%n",
                        DURATION, WRITERS, READERS, SEED, rs.getString(1),
                        EXTENSION_PATH.isBlank() ? "stock" : EXTENSION_PATH);
            }
        }

        CompactionConfig config = new CompactionConfig(
                List.of(CATALOG), List.of(MINOR, MAJOR), Duration.ofSeconds(1), SNAPSHOT_RETENTION,
                List.of(), 0, 100_000, true, REWRITE_DELETE_THRESHOLD);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        state = new CompactionState(registry, config.databases(), List.of("minor", "major"));
        // filesMerged sums every run of the test, so keep them all, not the last run_history_size:
        // a longer run would otherwise drop a tier's early merges and report it never merged.
        runLog = new CompactionRunLog(Integer.MAX_VALUE);
        housekeeper = new DuckLakeHousekeeper(startupScript, config.snapshotRetention(), List.of(),
                config.rewriteDeletesEnabled(), config.rewriteDeleteThreshold(), state);
        service = new CompactionService(config, startupScript,
                new DuckDbTierCompactor(startupScript, state), housekeeper, state, runLog);

        // The service and housekeeper log failures instead of throwing, so capture them here.
        errorLog = new ListAppender<>();
        errorLog.start();
        for (Class<?> source : List.of(CompactionService.class, DuckLakeHousekeeper.class)) {
            ((Logger) LoggerFactory.getLogger(source)).addAppender(errorLog);
        }
    }

    @AfterAll
    static void tearDown() {
        for (Class<?> source : List.of(CompactionService.class, DuckLakeHousekeeper.class)) {
            ((Logger) LoggerFactory.getLogger(source)).detachAppender(errorLog);
        }
        if (service != null) service.close();
        if (postgres != null) postgres.stop();
    }

    @Test
    void everythingRunsConcurrentlyWithoutLosingOrDuplicatingRows() throws Exception {
        Instant deadline = Instant.now().plus(DURATION);
        List<Writer> writers = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int w = 0; w < WRITERS; w++) {
            Writer writer = new Writer(w, new Random(SEED + w));
            writers.add(writer);
            threads.add(worker("writer-" + w, deadline, writer::step));
        }
        Appender appender = new Appender(new Random(SEED - 1));
        threads.add(worker("appender", deadline, appender::step));
        for (int r = 0; r < READERS; r++) {
            Reader reader = new Reader();
            threads.add(worker("reader-" + r, deadline, reader::step));
        }
        threads.add(worker("tier-minor", deadline, () -> {
            service.runTier(CATALOG, MINOR);
            pause(200);
        }));
        threads.add(worker("tier-major", deadline, () -> {
            service.runTier(CATALOG, MAJOR);
            pause(1_000);
        }));
        threads.add(worker("housekeeping", deadline, () -> {
            service.runHousekeeping(CATALOG);
            pause(1_000);
        }));
        OrphanCleaner orphans = new OrphanCleaner();
        threads.add(worker("orphans", deadline, orphans::step));

        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join(DURATION.plusMinutes(2).toMillis());
            assertFalse(thread.isAlive(), thread.getName() + " did not stop");
        }
        writers.forEach(Writer::close);
        appender.close();
        orphans.close();

        // Quiesce: with no writers left, compaction and housekeeping can finish without conflicts.
        for (int i = 0; i < 3; i++) {
            service.runTier(CATALOG, MINOR);
            service.runTier(CATALOG, MAJOR);
            service.runHousekeeping(CATALOG);
        }

        // Every check runs and is reported together, so one failure does not hide the others.
        List<String> failures = new ArrayList<>();
        try (Connection connection = RawConnections.open(startupScript, List.of())) {
            // With writers stopped, the rewrite has had its chance at every file: none should still
            // carry enough deleted rows to qualify. Same rule as DuckLake's REWRITE_DELETES filter.
            long unrewritten = scalar(connection, ("SELECT COUNT(*) FROM %1$s.ducklake_delete_file d "
                    + "JOIN %1$s.ducklake_data_file f USING (data_file_id) "
                    + "WHERE d.end_snapshot IS NULL AND f.end_snapshot IS NULL AND f.record_count > 0 "
                    + "AND d.delete_count::DOUBLE / f.record_count >= %2$s").formatted(MD_DATABASE, REWRITE_DELETE_THRESHOLD));
            if (unrewritten != 0) {
                failures.add(unrewritten + " data files still have at least " + REWRITE_DELETE_THRESHOLD
                        + " of their rows deleted after quiescing");
            }

            // Retired delete files: still referenced by an older snapshot, or already scheduled for
            // deletion. Once they are older than snapshot_retention, expiry and cleanup must remove
            // them from storage.
            List<String> retiredDeleteFiles = strings(connection, ("SELECT path FROM %1$s.ducklake_delete_file "
                    + "WHERE end_snapshot IS NOT NULL UNION ALL SELECT path FROM %1$s.ducklake_files_scheduled_for_deletion "
                    + "WHERE path LIKE '%%-delete.parquet'").formatted(MD_DATABASE)).stream()
                    .map(path -> Path.of(path).getFileName().toString())
                    .toList();
            // First pass expires the snapshots that still reference them, second pass deletes them.
            for (int i = 0; i < 2; i++) {
                pause(SNAPSHOT_RETENTION.plusSeconds(1).toMillis());
                service.runHousekeeping(CATALOG);
            }
            if (retiredDeleteFiles.isEmpty()) {
                failures.add("no retired delete files to check, so storage cleanup went untested");
            }
            List<String> stillOnDisk = filesOnDisk().stream().filter(retiredDeleteFiles::contains).toList();
            if (!stillOnDisk.isEmpty()) {
                failures.add("%d of %d retired delete files are still in storage, e.g. %s".formatted(
                        stillOnDisk.size(), retiredDeleteFiles.size(), stillOnDisk.subList(0, Math.min(3, stillOnDisk.size()))));
            }
            long overdue = scalar(connection, ("SELECT COUNT(*) FROM %s.ducklake_files_scheduled_for_deletion "
                    + "WHERE schedule_start < now() - INTERVAL '%d seconds'").formatted(MD_DATABASE, SNAPSHOT_RETENTION.toSeconds()));
            if (overdue != 0) {
                failures.add(overdue + " files scheduled for deletion are older than snapshot_retention but were not deleted");
            }
            count("retired_delete_files.checked", retiredDeleteFiles.size());
        }
        if (state.getFilesRewritten(CATALOG) == 0) {
            failures.add("the housekeeping rewrite never rewrote a file");
        }

        classifyLoggedErrors();
        report();
        failures.addAll(0, unexpectedErrors);
        try (Connection connection = RawConnections.open(startupScript, List.of())) {
            // Reads every column of every live file, so a data file deleted while still referenced fails here.
            query(connection, "SELECT SUM(hash(id, writer, version, payload)) FROM " + TABLE);

            long duplicates = scalar(connection, "SELECT COUNT(*) - COUNT(DISTINCT id) FROM " + TABLE);
            if (duplicates != 0) {
                failures.add(duplicates + " duplicate ids in " + TABLE);
            }
            for (Writer writer : writers) {
                String diff = diff(writer.expected(), actualRows(connection, writer.id));
                if (diff != null) {
                    failures.add("writer " + writer.id + ": " + diff);
                }
            }
            long appendRows = scalar(connection, "SELECT COUNT(*) FROM " + APPENDS);
            long appendIdSum = scalar(connection, "SELECT COALESCE(SUM(id), 0) FROM " + APPENDS);
            if (appendRows != appender.committedRows || appendIdSum != appender.committedIdSum) {
                failures.add("%s has %d rows (id sum %d), expected %d (id sum %d)".formatted(
                        APPENDS, appendRows, appendIdSum, appender.committedRows, appender.committedIdSum));
            }
            long multipleDeleteFiles = scalar(connection, ("SELECT COUNT(*) FROM (SELECT data_file_id FROM %s.ducklake_delete_file "
                    + "WHERE end_snapshot IS NULL GROUP BY data_file_id HAVING COUNT(*) > 1)").formatted(MD_DATABASE));
            if (multipleDeleteFiles != 0) {
                failures.add(multipleDeleteFiles + " data files have more than one active delete file");
            }
        }
        for (CompactionTier tier : List.of(MINOR, MAJOR)) {
            if (filesMerged(tier) == 0) {
                failures.add("tier '" + tier.name() + "' never merged a file");
            }
        }
        if (!failures.isEmpty()) {
            fail("seed " + SEED + ":\n  " + String.join("\n  ", failures));
        }
    }

    /** Null when equal; otherwise counts of missing, unexpected and wrong-version ids with a few examples. */
    private static String diff(Map<Long, Integer> expected, Map<Long, Integer> actual) {
        if (expected.equals(actual)) {
            return null;
        }
        List<Long> missing = expected.keySet().stream().filter(id -> !actual.containsKey(id)).toList();
        List<Long> unexpected = actual.keySet().stream().filter(id -> !expected.containsKey(id)).toList();
        List<Long> wrongVersion = expected.keySet().stream()
                .filter(id -> actual.containsKey(id) && !actual.get(id).equals(expected.get(id))).toList();
        return "%d missing %s, %d unexpected %s, %d wrong version %s".formatted(
                missing.size(), examples(missing), unexpected.size(), examples(unexpected),
                wrongVersion.size(), examples(wrongVersion));
    }

    private static List<Long> examples(List<Long> ids) {
        return ids.subList(0, Math.min(5, ids.size()));
    }

    /** Runs {@code step} until the deadline; a throwable ends only that thread and is recorded. */
    private static Thread worker(String name, Instant deadline, ThrowingRunnable step) {
        return new Thread(() -> {
            while (Instant.now().isBefore(deadline)) {
                try {
                    step.run();
                } catch (Throwable t) {
                    unexpectedErrors.add(name + ": " + t);
                    return;
                }
            }
        }, "stress-" + name);
    }

    /**
     * One writer's slice of the table. Each statement runs in autocommit, so the model changes only
     * after DuckDB reports the commit succeeded.
     */
    static final class Writer {
        final int id;
        final Random random;
        final Connection connection;
        /** id → version, for every row this writer committed and has not deleted. */
        final Map<Long, Integer> rows = new HashMap<>();
        /** The same ids, for sampling in O(1). */
        final List<Long> ids = new ArrayList<>();
        long nextSequence;

        Writer(int id, Random random) {
            this.id = id;
            this.random = random;
            try {
                this.connection = RawConnections.open(startupScript, List.of());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        void step() throws SQLException {
            int dice = random.nextInt(100);
            if (dice < 50 || ids.size() < 50) {
                insert();
            } else if (dice < 70) {
                delete();
            } else {
                update();
            }
        }

        void insert() throws SQLException {
            int count = 20 + random.nextInt(200);
            List<Long> batch = new ArrayList<>(count);
            StringJoiner values = new StringJoiner(",");
            for (int i = 0; i < count; i++) {
                long rowId = (nextSequence++) * WRITERS + id;
                batch.add(rowId);
                values.add("(%d, %d, 0, repeat('x', %d))".formatted(rowId, id, 50 + random.nextInt(200)));
            }
            if (run("insert", "INSERT INTO %s VALUES %s".formatted(TABLE, values))) {
                for (long rowId : batch) {
                    rows.put(rowId, 0);
                    ids.add(rowId);
                }
            }
        }

        void delete() throws SQLException {
            List<Long> victims = sample(1 + random.nextInt(30));
            if (run("delete", "DELETE FROM %s WHERE id IN (%s)".formatted(TABLE, join(victims)))) {
                for (long rowId : victims) {
                    rows.remove(rowId);
                }
                ids.removeAll(new java.util.HashSet<>(victims));
            }
        }

        void update() throws SQLException {
            List<Long> targets = sample(1 + random.nextInt(30));
            if (run("update", "UPDATE %s SET version = version + 1 WHERE id IN (%s)".formatted(TABLE, join(targets)))) {
                for (long rowId : targets) {
                    rows.merge(rowId, 1, Integer::sum);
                }
            }
        }

        /** True when the statement committed; false on a transaction conflict, which changes nothing. */
        boolean run(String kind, String sql) throws SQLException {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
                count(kind + ".ok");
                return true;
            } catch (SQLException e) {
                if (isConflict(e.getMessage())) {
                    count(kind + ".conflict");
                    return false;
                }
                throw e;
            }
        }

        List<Long> sample(int n) {
            List<Long> picked = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                Long candidate = ids.get(random.nextInt(ids.size()));
                if (!picked.contains(candidate)) {
                    picked.add(candidate);
                }
            }
            return picked;
        }

        Map<Long, Integer> expected() {
            return new TreeMap<>(rows);
        }

        void close() {
            try {
                connection.close();
            } catch (SQLException ignored) {
            }
        }
    }

    /** Insert-only writer for {@code appends}; tracks what it committed as a row count and id sum. */
    static final class Appender {
        final Random random;
        final Connection connection;
        long nextId;
        long committedRows;
        long committedIdSum;

        Appender(Random random) {
            this.random = random;
            try {
                this.connection = RawConnections.open(startupScript, List.of());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        void step() throws SQLException {
            int count = 50 + random.nextInt(250);
            long first = nextId;
            nextId += count;
            String sql = "INSERT INTO %s SELECT range, repeat('y', 100) FROM range(%d, %d)".formatted(APPENDS, first, first + count);
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
                committedRows += count;
                // sum of first .. first + count - 1
                committedIdSum += count * first + (long) count * (count - 1) / 2;
                count("append.ok");
            } catch (SQLException e) {
                if (!isConflict(e.getMessage())) {
                    throw e;
                }
                count("append.conflict");
            }
            pause(50);
        }

        void close() {
            try {
                connection.close();
            } catch (SQLException ignored) {
            }
        }
    }

    /** Full scans; every snapshot must be readable and free of duplicate ids. */
    static final class Reader {
        Connection connection;

        void step() throws SQLException {
            if (connection == null) {
                connection = RawConnections.open(startupScript, List.of());
            }
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(
                         "SELECT COUNT(*), COUNT(DISTINCT id), SUM(hash(payload)) FROM " + TABLE)) {
                rs.next();
                if (rs.getLong(1) != rs.getLong(2)) {
                    unexpectedErrors.add("reader saw %d rows but %d distinct ids".formatted(rs.getLong(1), rs.getLong(2)));
                }
                count("read.ok");
            }
            pause(100);
        }
    }

    /** Deletes files the catalog does not reference, as an operator's scheduled job would. */
    static final class OrphanCleaner {
        Connection connection;

        void step() throws SQLException {
            if (connection == null) {
                connection = RawConnections.open(startupScript, List.of());
            }
            try (Statement statement = connection.createStatement()) {
                statement.execute("CALL ducklake_delete_orphaned_files('%s', older_than => now() - INTERVAL '%d seconds')"
                        .formatted(CATALOG, ORPHAN_AGE.toSeconds()));
                count("orphans.ok");
            } catch (SQLException e) {
                if (isConflict(e.getMessage())) {
                    count("orphans.conflict");
                } else if (isVanishedFile(e.getMessage())) {
                    // The scan of DATA_PATH listed a file that housekeeping's cleanup, or a transaction
                    // rolling back after a conflict, deleted before the scan opened it. The call fails
                    // before deleting anything, so this is a retryable race, not damage.
                    count("orphans.file_vanished");
                } else {
                    throw e;
                }
            }
            pause(2_000);
        }

        void close() {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    /** Sorts captured ERROR logs into expected conflicts and failures. */
    private static void classifyLoggedErrors() {
        for (ILoggingEvent event : errorLog.list) {
            if (event.getLevel() != Level.ERROR) {
                continue;
            }
            String messages = causeMessages(event.getThrowableProxy());
            if (isConflict(messages)) {
                count("logged.conflict");
            } else {
                unexpectedErrors.add(event.getFormattedMessage() + " — " + messages);
            }
        }
    }

    private static String causeMessages(IThrowableProxy proxy) {
        StringJoiner joined = new StringJoiner(" | ");
        for (IThrowableProxy p = proxy; p != null; p = p.getCause()) {
            joined.add(p.getClassName() + ": " + p.getMessage());
        }
        return joined.toString();
    }

    static boolean isVanishedFile(String message) {
        return message != null && message.contains("Cannot open file") && message.contains("No such file or directory");
    }

    static boolean isConflict(String message) {
        return message != null && message.toLowerCase(Locale.ROOT).contains("transaction conflict");
    }

    private static long filesMerged(CompactionTier tier) {
        return runLog.recent(new CompactionRunLog.Key(CATALOG, tier.name())).stream()
                .filter(run -> run.filesProcessed() != null)
                .mapToLong(CompactionRun::filesProcessed)
                .sum();
    }

    private static List<CompactionRun> allRuns() {
        return runLog.keys().stream().flatMap(key -> runLog.recent(key).stream()).toList();
    }

    private static void report() {
        Map<String, Long> outcomes = allRuns().stream().collect(Collectors.groupingBy(
                run -> run.tierName() + "." + run.outcome() + (run.failureClass() == CompactionRun.FailureClass.NONE
                        ? "" : "." + run.failureClass()),
                TreeMap::new, Collectors.counting()));
        StringBuilder report = new StringBuilder("CompactionStressTest report (seed " + SEED + ")\n");
        new TreeMap<>(counters).forEach((k, v) -> report.append("  ").append(k).append(" = ").append(v).append('\n'));
        outcomes.forEach((k, v) -> report.append("  runs.").append(k).append(" = ").append(v).append('\n'));
        report.append("  files merged: minor = ").append(filesMerged(MINOR))
                .append(", major = ").append(filesMerged(MAJOR))
                .append(", files rewritten by housekeeping = ").append(state.getFilesRewritten(CATALOG));
        System.out.println(report);
    }

    private static Map<Long, Integer> actualRows(Connection connection, int writer) throws SQLException {
        Map<Long, Integer> actual = new TreeMap<>();
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT id, version FROM %s WHERE writer = %d".formatted(TABLE, writer))) {
            while (rs.next()) {
                actual.put(rs.getLong(1), rs.getInt(2));
            }
        }
        return actual;
    }

    private static long scalar(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void query(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                // drain
            }
        }
    }

    private static String join(List<Long> ids) {
        return ids.stream().map(String::valueOf).collect(Collectors.joining(","));
    }

    private static void count(String key) {
        count(key, 1);
    }

    private static void count(String key, long delta) {
        counters.computeIfAbsent(key, k -> new AtomicLong()).addAndGet(delta);
    }

    private static List<String> strings(Connection connection, String sql) throws SQLException {
        List<String> values = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                values.add(rs.getString(1));
            }
        }
        return values;
    }

    /** File names (not paths) of every file under the catalog's DATA_PATH; DuckLake names are UUIDs. */
    private static java.util.Set<String> filesOnDisk() throws java.io.IOException {
        try (var paths = Files.walk(dataPath)) {
            return paths.filter(Files::isRegularFile)
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toSet());
        }
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }
}
