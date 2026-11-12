package io.dazzleduck.sql.compaction;

import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Retention's DELETE running alongside ingestion's {@code ducklake_add_data_files}, on the patched
 * DuckLake extension this module ships ({@code ducklake.patch.release}). Needs duckdb/ducklake#1895:
 * on the stock extension a DELETE conflicts with every concurrent insert into the table, so each of
 * these tests fails there with a "Transaction conflict" error.
 *
 * <p>The extension is downloaded from the release once per platform into {@code target/}; the tests
 * are skipped when it cannot be downloaded. {@code -Dducklake.extension.path} uses a local build
 * instead (the file must be named {@code ducklake.duckdb_extension}).
 *
 * <p>All connections are duplicates of one DuckDB instance attached to a DuckDB-file catalog, so
 * this needs no Postgres; the conflict check under test is the same for every catalog backend.
 */
class RetentionConcurrentIngestionTest {

    static final String CATALOG = "lake";
    static final String TABLE = CATALOG + ".main.events";
    static final RetentionPolicy POLICY = RetentionPolicy.parse(TABLE, "ts", "30 days");
    /** Logical time of the data: batch {@code h} holds rows stamped {@code EPOCH + h hours}. */
    static final Instant EPOCH = Instant.parse("2026-01-01T00:00:00Z");
    static final int ROWS_PER_FILE = 10;

    @TempDir
    Path tempDir;

    Connection instance;
    Path staging;
    final AtomicInteger fileSequence = new AtomicInteger();

    @BeforeEach
    void setUp() throws Exception {
        Path extension = patchedExtension();
        Properties properties = new Properties();
        properties.setProperty("allow_unsigned_extensions", "true");
        instance = DriverManager.getConnection("jdbc:duckdb:", properties);
        staging = Files.createDirectories(tempDir.resolve("staging"));
        try (Statement statement = instance.createStatement()) {
            statement.execute("LOAD '%s'".formatted(extension));
            // Inlining off, as the ingestion path never inlines: every batch is its own data file.
            statement.execute("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0)"
                    .formatted(tempDir.resolve("meta.ducklake"), CATALOG, tempDir.resolve("data")));
            statement.execute("CREATE TABLE %s (ts TIMESTAMP, id BIGINT)".formatted(TABLE));
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (instance != null) {
            instance.close();
        }
    }

    @Test
    void ingestionCommitsWhileARetentionDeleteIsOpen() throws Exception {
        try (Connection ingest = duplicate(); Connection retention = duplicate()) {
            for (int day = 0; day < 3; day++) {
                addFile(ingest, day * 24);
            }
            int recent = 60 * 24;
            addFile(ingest, recent);

            execute(retention, "BEGIN");
            assertEquals(3 * ROWS_PER_FILE, deleteInOpenTransaction(retention, cutoffAtHour(recent)));
            // Ingestion keeps landing data while the DELETE is uncommitted.
            addFile(ingest, recent + 1);
            execute(retention, "COMMIT");

            assertEquals(2 * ROWS_PER_FILE, count(ingest, "SELECT count(*) FROM " + TABLE));
            assertEquals(0, count(ingest, "SELECT count(*) FROM %s WHERE ts < '2026-03-01'".formatted(TABLE)));
            assertEquals(0, activeDeleteFiles(ingest));
        }
    }

    @Test
    void retentionDeleteCommitsWhileAnIngestionBatchIsOpen() throws Exception {
        try (Connection ingest = duplicate(); Connection retention = duplicate()) {
            addFile(ingest, 0);
            addFile(ingest, 24);
            int recent = 60 * 24;

            // A batch of several files, registered in one transaction like DuckLakePostIngestionTask.
            execute(ingest, "BEGIN");
            addFile(ingest, recent);
            addFile(ingest, recent + 1);
            assertEquals(2 * ROWS_PER_FILE, executeDelete(retention, cutoffAtHour(recent)));
            execute(ingest, "COMMIT");

            assertEquals(2 * ROWS_PER_FILE, count(ingest, "SELECT count(*) FROM " + TABLE));
            assertEquals(0, activeDeleteFiles(ingest));
        }
    }

    /**
     * Late data the DELETE would have matched must still conflict: committing the DELETE would
     * otherwise leave those rows behind without anyone noticing. The next cycle removes them.
     */
    @Test
    void lateDataTheDeleteWouldMatchStillConflicts() throws Exception {
        try (Connection ingest = duplicate(); Connection retention = duplicate()) {
            addFile(ingest, 0);
            addFile(ingest, 24);
            int recent = 60 * 24;
            addFile(ingest, recent);

            execute(retention, "BEGIN");
            assertEquals(2 * ROWS_PER_FILE, deleteInOpenTransaction(retention, cutoffAtHour(recent)));
            addFile(ingest, 12);
            SQLException conflict = assertThrows(SQLException.class, () -> execute(retention, "COMMIT"));
            assertTrue(conflict.getMessage().toLowerCase().contains("conflict"), conflict.getMessage());
            try {
                execute(retention, "ROLLBACK");
            } catch (SQLException alreadyRolledBack) {
                // a failed COMMIT may already have ended the transaction
            }

            assertEquals(3 * ROWS_PER_FILE, executeDelete(retention, cutoffAtHour(recent)));
            assertEquals(ROWS_PER_FILE, count(ingest, "SELECT count(*) FROM " + TABLE));
        }
    }

    /**
     * The statistics and the DELETE share one snapshot. A file committed after the statistics were
     * read, here one straddling the boundary, would otherwise be seen by the DELETE and partly
     * deleted, writing a delete file. The DELETE does not see it, and the commit conflicts instead.
     */
    @Test
    void aFileCommittedMidTransactionIsNeverPartlyDeleted() throws Exception {
        try (Connection ingest = duplicate(); Connection retention = duplicate()) {
            addFile(ingest, 0);
            int recent = 60 * 24;
            addFile(ingest, recent);

            execute(retention, "BEGIN");
            assertEquals(ROWS_PER_FILE, deleteInOpenTransaction(retention, cutoffAtHour(recent), () -> {
                try {
                    addRange(ingest, 24, recent + 24);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }), "only the file the snapshot shows");
            assertThrows(SQLException.class, () -> execute(retention, "COMMIT"));
            try {
                execute(retention, "ROLLBACK");
            } catch (SQLException alreadyRolledBack) {
                // a failed COMMIT may already have ended the transaction
            }

            assertEquals(0, activeDeleteFiles(ingest));
            assertEquals(ROWS_PER_FILE, executeDelete(retention, cutoffAtHour(recent)),
                    "the next cycle sees the straddling file, lowers the boundary below it, and drops the old file");
            assertEquals(0, activeDeleteFiles(ingest));
        }
    }

    /**
     * Several ingestion threads and one retention thread, all as fast as they can go. Every batch is
     * stamped later than the one claimed before it, as with live ingestion, and retention keeps a
     * window below the oldest batch still being registered. Nothing may conflict, and every row is
     * either still there or was deleted.
     *
     * <p>The window is anchored to the oldest batch in flight, not the newest claimed: when commits
     * collide, DuckLake retries with backoff, and one ingester can stall for seconds behind the
     * others. Its batch is then late data the DELETE matches, and that conflict is correct (see
     * {@link #lateDataTheDeleteWouldMatchStillConflicts}). Every thread pauses between commits, so
     * such stalls stay short and the window keeps moving.
     */
    @Test
    void continuousIngestionAndRetentionNeverConflict() throws Exception {
        int ingesters = 3;
        int windowHours = 24;
        Duration runFor = Duration.ofSeconds(5);
        InFlight inFlight = new InFlight();
        AtomicLong rowsIngested = new AtomicLong();
        AtomicLong rowsDeleted = new AtomicLong();
        AtomicInteger deletes = new AtomicInteger();
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        Instant deadline = Instant.now().plus(runFor);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < ingesters; i++) {
            threads.add(new Thread(() -> {
                try (Connection ingest = duplicate()) {
                    while (Instant.now().isBefore(deadline)) {
                        int hour = inFlight.claim();
                        try {
                            addFile(ingest, hour);
                        } finally {
                            inFlight.done(hour);
                        }
                        rowsIngested.addAndGet(ROWS_PER_FILE);
                        Thread.sleep(20);
                    }
                } catch (Throwable t) {
                    errors.add(t);
                }
            }, "ingest-" + i));
        }
        threads.add(new Thread(() -> {
            try (Connection retention = duplicate()) {
                while (Instant.now().isBefore(deadline)) {
                    long deleted = executeDelete(retention, cutoffAtHour(inFlight.oldest() - windowHours));
                    if (deleted > 0) {
                        rowsDeleted.addAndGet(deleted);
                        deletes.incrementAndGet();
                    }
                    Thread.sleep(50);
                }
            } catch (Throwable t) {
                errors.add(t);
            }
        }, "retention"));

        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join(runFor.plusMinutes(1).toMillis());
            assertFalse(thread.isAlive(), thread.getName() + " did not stop");
        }

        assertEquals(List.of(), errors.stream().map(Throwable::toString).toList());
        assertTrue(deletes.get() >= 5, "retention deleted only " + deletes.get() + " time(s)");
        try (Connection check = duplicate()) {
            assertEquals(rowsIngested.get() - rowsDeleted.get(), count(check, "SELECT count(*) FROM " + TABLE));
            assertEquals(0, activeDeleteFiles(check));
        }
    }

    /** Hands out increasing batch hours and knows the oldest one not yet registered. */
    private static final class InFlight {
        private final TreeSet<Integer> hours = new TreeSet<>();
        private int next;

        synchronized int claim() {
            hours.add(next);
            return next++;
        }

        synchronized void done(int hour) {
            hours.remove(hour);
        }

        synchronized int oldest() {
            return hours.isEmpty() ? next : hours.first();
        }
    }

    /** Writes one Parquet file of rows stamped hour {@code hour} and registers it, as ingestion does. */
    private void addFile(Connection connection, int hour) throws SQLException {
        Path file = staging.resolve("batch-%d.parquet".formatted(fileSequence.incrementAndGet()));
        execute(connection, ("COPY (SELECT TIMESTAMP '2026-01-01' + INTERVAL (%d) HOUR + INTERVAL (range) SECOND AS ts, "
                + "range AS id FROM range(%d)) TO '%s' (FORMAT parquet)").formatted(hour, ROWS_PER_FILE, file));
        execute(connection, ("CALL ducklake_add_data_files('%s', 'events', '%s', schema => 'main', "
                + "ignore_extra_columns => true, allow_missing => true)").formatted(CATALOG, file));
    }

    /** One file of two rows, stamped hour {@code from} and hour {@code to}. */
    private void addRange(Connection connection, int from, int to) throws SQLException {
        Path file = staging.resolve("batch-%d.parquet".formatted(fileSequence.incrementAndGet()));
        execute(connection, ("COPY (SELECT TIMESTAMP '2026-01-01' + INTERVAL (h) HOUR AS ts, h::BIGINT AS id "
                + "FROM (VALUES (%d), (%d)) v(h)) TO '%s' (FORMAT parquet)").formatted(from, to, file));
        execute(connection, ("CALL ducklake_add_data_files('%s', 'events', '%s', schema => 'main', "
                + "ignore_extra_columns => true, allow_missing => true)").formatted(CATALOG, file));
    }

    /** Retention's delete for the cutoff, in its own transaction as the job runs it. */
    private static long executeDelete(Connection connection, Instant cutoff) throws SQLException {
        return DuckLakeDataRetention.deleteExpired(connection, POLICY, cutoff);
    }

    /** Retention's delete, in a transaction the caller has begun and commits. */
    private static long deleteInOpenTransaction(Connection connection, Instant cutoff) throws SQLException {
        return deleteInOpenTransaction(connection, cutoff, () -> { });
    }

    /** The same, running {@code beforeDelete} between the statistics read and the DELETE. */
    private static long deleteInOpenTransaction(Connection connection, Instant cutoff, Runnable beforeDelete)
            throws SQLException {
        return DuckLakeDataRetention.deleteInTransaction(connection, POLICY, cutoff, beforeDelete);
    }

    private static Instant cutoffAtHour(int hour) {
        return EPOCH.plus(hour, ChronoUnit.HOURS);
    }

    private Connection duplicate() throws SQLException {
        return ((DuckDBConnection) instance).duplicate();
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static long count(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static long activeDeleteFiles(Connection connection) throws SQLException {
        return count(connection, "SELECT count(*) FROM __ducklake_metadata_%s.ducklake_delete_file WHERE end_snapshot IS NULL"
                .formatted(CATALOG));
    }

    /** The patched extension for this platform: a local build, or the pinned release, downloaded once. */
    private static Path patchedExtension() throws Exception {
        String local = System.getProperty("ducklake.extension.path", "");
        if (!local.isBlank()) {
            return Path.of(local);
        }
        Properties pins = new Properties();
        try (InputStream in = RetentionConcurrentIngestionTest.class.getResourceAsStream("/ducklake-patch-version.properties")) {
            pins.load(in);
        }
        String release = pins.getProperty("ducklake.patch.release");
        String platform;
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("PRAGMA platform")) {
            rs.next();
            platform = rs.getString(1);
        }
        Path target = Path.of("target", "ducklake-patch", release, platform, "ducklake.duckdb_extension");
        if (Files.exists(target)) {
            return target.toAbsolutePath();
        }
        URI uri = URI.create("https://github.com/dazzleduck-web/ducklake/releases/download/%s/ducklake-%s.duckdb_extension"
                .formatted(release, platform));
        Files.createDirectories(target.getParent());
        Path partial = target.resolveSibling(target.getFileName() + ".part");
        try {
            HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL)
                    .connectTimeout(Duration.ofSeconds(20)).build();
            HttpResponse<Path> response = client.send(
                    HttpRequest.newBuilder(uri).timeout(Duration.ofMinutes(5)).build(),
                    HttpResponse.BodyHandlers.ofFile(partial));
            Assumptions.assumeTrue(response.statusCode() == 200,
                    () -> "could not download the patched DuckLake extension " + uri + ": HTTP " + response.statusCode());
        } catch (IOException e) {
            Assumptions.abort("could not download the patched DuckLake extension " + uri + ": " + e);
        }
        Files.move(partial, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        return target.toAbsolutePath();
    }
}
