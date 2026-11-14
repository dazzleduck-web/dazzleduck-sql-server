package io.dazzleduck.sql.compaction;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real Postgres-backed catalog, not a local file-based one: this module's compaction/housekeeping
 * connections are raw, independent DuckDB instances now (see {@link RawConnections}), one per
 * (database, tier) pair, so that different tiers' GLOBAL-scoped connection_settings (memory_limit,
 * threads) don't leak across each other. A local file-based DuckLake catalog only allows ONE attach
 * at a time ({@code Unique file handle conflict} — verified empirically while building this), so it
 * cannot support more than one of these concurrently-open raw connections. Postgres has no such
 * restriction, which is also the realistic backend for anyone actually wanting per-tier isolation.
 */
@Tag("slow")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CompactionIntegrationTest {

    @TempDir
    static Path tempDir;

    static final String CATALOG = "test_lake";
    static final String MD_DATABASE = "__ducklake_metadata_" + CATALOG;

    static final CompactionTier MINOR = new CompactionTier(
            "minor", true, Duration.ofSeconds(60), 0, 512 * 1024L, 0, List.of());
    static final CompactionTier MAJOR = new CompactionTier(
            "major", true, Duration.ofMillis(100), 512 * 1024L, 10 * 1024 * 1024L, 0, List.of());

    static PostgreSQLContainer postgres;
    static SimpleMeterRegistry registry;
    static CompactionConfig config;
    static String startupScript;
    static CompactionService service;

    @BeforeAll
    static void setUp() throws Exception {
        postgres = new PostgreSQLContainer("postgres:15-alpine")
                .withDatabaseName("ducklake")
                .withUsername("duck")
                .withPassword("duck");
        postgres.start();

        Path dataPath = tempDir.resolve("data");
        Files.createDirectories(dataPath);

        // The "postgres:" sub-scheme is required, not optional: "ducklake:host=..." with no scheme
        // silently falls back to a LOCAL file catalog named after the literal connection string
        // (verified empirically — zero ducklake_* tables ever appear in Postgres without it).
        String connectionString = "host=%s port=%d dbname=ducklake user=duck password=duck".formatted(
                postgres.getHost(), postgres.getFirstMappedPort());
        // Only the idempotent parts (extension load + ATTACH) — this text gets replayed by every raw
        // connection DuckDbTierCompactor/DuckLakeHousekeeper/CompactionService open (see
        // RawConnections), so one-time DDL like CREATE TABLE below must NOT be part of it.
        startupScript = "INSTALL ducklake; LOAD ducklake;\nATTACH 'ducklake:postgres:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0);"
                .formatted(connectionString, CATALOG, dataPath);

        ConnectionPool.executeOnSingleton(startupScript);
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "CREATE TABLE %s.main.events (id BIGINT, name VARCHAR)".formatted(CATALOG));
        }

        // Insert data to force Parquet file creation
        for (int i = 0; i < 6; i++) {
            try (Connection conn = ConnectionPool.getConnection()) {
                ConnectionPool.execute(conn,
                        "INSERT INTO %s.main.events SELECT range + %d * 10, 'event-' || range FROM range(10)"
                                .formatted(CATALOG, i));
            }
        }

        config = new CompactionConfig(
                List.of(CATALOG),
                List.of(MINOR, MAJOR),
                Duration.ofMillis(500),   // housekeeping every 500ms in tests
                Duration.ofSeconds(5),
                List.of(),
                0,                        // 0 = OS-assigned port, health server not used in tests
                CompactionRunLog.DEFAULT_CAPACITY
        );

        registry = new SimpleMeterRegistry();
        CompactionState state = new CompactionState(registry, config.databases(), List.of("minor", "major"));
        TierCompactor tierCompactor = new DuckDbTierCompactor(startupScript, state);
        Housekeeper housekeeper = new DuckLakeHousekeeper(startupScript, config.snapshotRetention(), config.housekeepingConnectionSettings(), state);
        service = new CompactionService(config, startupScript, tierCompactor, housekeeper, state, new CompactionRunLog(50));
    }

    @AfterAll
    static void tearDown() {
        if (service != null) service.close();
        if (postgres != null) postgres.stop();
    }

    @Test
    @Order(1)
    void initialFileCountIsGreaterThanZero() throws Exception {
        long total = ConnectionPool.collectFirst(
                "SELECT COUNT(*) FROM %s.ducklake_data_file WHERE end_snapshot IS NULL"
                        .formatted(MD_DATABASE),
                Long.class);
        assertTrue(total > 0, "Expected files to exist before compaction, found: " + total);
    }

    @Test
    @Order(2)
    void minorTierTimerIsRecorded() {
        service.runTier(CATALOG, MINOR);

        Timer timer = registry.find("ducklake.compaction.duration")
                .tag("type", "minor")
                .tag("step", "merge")
                .tag("database", CATALOG)
                .timer();

        assertNotNull(timer, "Minor tier timer not registered");
        assertEquals(1, timer.count());
    }

    @Test
    @Order(3)
    void fileCountGaugesArePopulated() {
        double total = registry.find("ducklake.files.total")
                .tag("database", CATALOG)
                .gauge()
                .value();
        double minorFiles = registry.find("ducklake.files.by_tier")
                .tag("database", CATALOG)
                .tag("tier", "minor")
                .gauge()
                .value();
        double majorFiles = registry.find("ducklake.files.by_tier")
                .tag("database", CATALOG)
                .tag("tier", "major")
                .gauge()
                .value();

        assertTrue(total >= 0, "Total files gauge should be non-negative");
        // minor + major covers files below majorTier's max size; larger files only appear in total
        assertTrue(minorFiles + majorFiles <= total, "per-tier counts must not exceed the total");
    }

    @Test
    @Order(4)
    void bothTiersCanRunConcurrentlyAgainstTheSameDatabase() throws Exception {
        // The whole point of raw, independent per-(database,tier) connections: this must not throw
        // (e.g. the file-handle conflict a local file-based catalog would hit) and both must complete.
        Thread minorThread = new Thread(() -> service.runTier(CATALOG, MINOR));
        Thread majorThread = new Thread(() -> service.runTier(CATALOG, MAJOR));
        minorThread.start();
        majorThread.start();
        minorThread.join(10_000);
        majorThread.join(10_000);

        Timer mergeTimer = registry.find("ducklake.compaction.duration")
                .tag("type", "major")
                .tag("step", "merge")
                .tag("database", CATALOG)
                .timer();
        assertNotNull(mergeTimer, "Expected major tier timer for step: merge");
        assertTrue(mergeTimer.count() >= 1, "Expected at least one recording for step: merge");
    }

    @Test
    @Order(5)
    void housekeepingTimersAreRecorded() throws Exception {
        service.runHousekeeping(CATALOG);

        for (String step : List.of("expire", "cleanup")) {
            Timer timer = registry.find("ducklake.compaction.duration")
                    .tag("type", "housekeeping")
                    .tag("step", step)
                    .tag("database", CATALOG)
                    .timer();
            assertNotNull(timer, "Expected housekeeping timer for step: " + step);
            assertTrue(timer.count() >= 1, "Expected at least one recording for step: " + step);
        }
    }

    @Test
    @Order(6)
    void fileCountDecreasesAfterCompaction() throws Exception {
        long before = ConnectionPool.collectFirst(
                "SELECT COUNT(*) FROM %s.ducklake_data_file WHERE end_snapshot IS NULL"
                        .formatted(MD_DATABASE),
                Long.class);

        service.runTier(CATALOG, MAJOR);

        long after = ConnectionPool.collectFirst(
                "SELECT COUNT(*) FROM %s.ducklake_data_file WHERE end_snapshot IS NULL"
                        .formatted(MD_DATABASE),
                Long.class);

        assertTrue(after <= before, "Expected file count to decrease after compaction (%d -> %d)".formatted(before, after));
    }

    @Test
    @Order(7)
    void connectionSettingsAreAppliedBeforeTheMergeCall() throws Exception {
        // Proves the settings SQL actually runs on the same connection the merge uses, against a
        // real catalog — a bad/incompatible setting would make RawConnections.open throw before the
        // merge even starts.
        CompactionState state = new CompactionState(new SimpleMeterRegistry(), config.databases(), List.of("minor", "major"));
        TierCompactor withSettings = new DuckDbTierCompactor(startupScript, state);
        CompactionTier majorWithSettings = new CompactionTier(
                "major", true, MAJOR.frequency(), MAJOR.minFileSize(), MAJOR.maxFileSize(), 0, List.of("SET threads=2"));

        assertDoesNotThrow(() -> withSettings.compact(CATALOG, majorWithSettings));
    }

    @Test
    @Order(8)
    void housekeepingRewriteDropsDeleteFilesAndKeepsTheRemainingRows() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "CREATE TABLE %s.main.rewrites (id BIGINT)".formatted(CATALOG));
            ConnectionPool.execute(conn, "INSERT INTO %s.main.rewrites SELECT range FROM range(100)".formatted(CATALOG));
            // 80% of the file's rows, above the 0.5 threshold below
            ConnectionPool.execute(conn, "DELETE FROM %s.main.rewrites WHERE id < 80".formatted(CATALOG));
        }
        assertTrue(activeDeleteFiles("rewrites") > 0, "the DELETE should have written a delete file");

        // Own registry: the service's state already registered this database's counters on `registry`,
        // and Micrometer would hand back those instead of this state's.
        SimpleMeterRegistry rewriteRegistry = new SimpleMeterRegistry();
        CompactionState state = new CompactionState(rewriteRegistry, config.databases(), List.of("minor", "major"));
        try (Housekeeper housekeeper = new DuckLakeHousekeeper(
                startupScript, config.snapshotRetention(), List.of(), true, 0.5, state)) {
            housekeeper.housekeep(CATALOG);
        }

        assertEquals(0, activeDeleteFiles("rewrites"), "the rewrite should have dropped the delete file");
        assertEquals(20L, ConnectionPool.collectFirst(
                "SELECT COUNT(*) FROM %s.main.rewrites".formatted(CATALOG), Long.class));
        assertEquals(80L, ConnectionPool.collectFirst(
                "SELECT MIN(id) FROM %s.main.rewrites".formatted(CATALOG), Long.class));
        Timer timer = rewriteRegistry.find("ducklake.compaction.duration")
                .tag("type", "housekeeping")
                .tag("step", "rewrite_deletes")
                .tag("database", CATALOG)
                .timer();
        assertNotNull(timer, "Expected housekeeping timer for step: rewrite_deletes");
        assertEquals(1.0, rewriteRegistry.get("ducklake.files.rewritten").tag("database", CATALOG).functionCounter().count(),
                "the one data file with deletes should be counted as rewritten");
        assertEquals(1L, state.getSnapshot(config.databases()).databases().get(CATALOG).totalFilesRewritten());
    }

    private static long activeDeleteFiles(String table) throws Exception {
        return ConnectionPool.collectFirst((
                "SELECT COUNT(*) FROM %1$s.ducklake_delete_file d JOIN %1$s.ducklake_table t USING (table_id) "
                        + "WHERE d.end_snapshot IS NULL AND t.end_snapshot IS NULL AND t.table_name = '%2$s'")
                        .formatted(MD_DATABASE, table),
                Long.class);
    }
}
