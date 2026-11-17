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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

    static final CompactionTier MINOR = CompactionTier.of("minor", 0, 512 * 1024L, 1024 * 1024L, 12);
    static final CompactionTier MAJOR = CompactionTier.of("major", 512 * 1024L, 10 * 1024 * 1024L, 10 * 1024 * 1024L, 16);

    static PostgreSQLContainer postgres;
    static SimpleMeterRegistry registry;
    static CompactionConfig config;
    static String startupScript;
    static String connectionString;
    static Path dataPath;
    static CompactionService service;

    @BeforeAll
    static void setUp() throws Exception {
        postgres = new PostgreSQLContainer("postgres:15-alpine")
                .withDatabaseName("ducklake")
                .withUsername("duck")
                .withPassword("duck");
        postgres.start();

        dataPath = tempDir.resolve("data");
        Files.createDirectories(dataPath);

        // The "postgres:" sub-scheme is required, not optional: "ducklake:host=..." with no scheme
        // silently falls back to a LOCAL file catalog named after the literal connection string
        // (verified empirically — zero ducklake_* tables ever appear in Postgres without it).
        connectionString = "host=%s port=%d dbname=ducklake user=duck password=duck".formatted(
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
    @Order(1)
    void discoveryAndFileCountsRunInsidePostgresWithTheSameResults() throws Exception {
        // Review of #544: through the postgres scanner, discovery copied every table's catalog rows into
        // DuckDB and ran out of memory at ~1.5M files; inside PostgreSQL only the result comes back
        try (Connection setup = ConnectionPool.getConnection()) {
            ConnectionPool.execute(setup, "CREATE TABLE %s.main.parted (day INTEGER, v VARCHAR)".formatted(CATALOG));
            ConnectionPool.execute(setup, "ALTER TABLE %s.main.parted SET PARTITIONED BY (day)".formatted(CATALOG));
            ConnectionPool.execute(setup, "CREATE TABLE %s.main.deleted (id BIGINT)".formatted(CATALOG));
            for (int i = 0; i < 4; i++) {
                ConnectionPool.execute(setup, "INSERT INTO %s.main.parted SELECT range %% 2, 'v' FROM range(4)"
                        .formatted(CATALOG));
                ConnectionPool.execute(setup, "INSERT INTO %s.main.deleted SELECT range FROM range(5)".formatted(CATALOG));
            }
            ConnectionPool.execute(setup, "ALTER TABLE %s.main.parted ADD COLUMN w INTEGER".formatted(CATALOG));
            ConnectionPool.execute(setup, "INSERT INTO %s.main.parted SELECT range %% 2, 'v', 1 FROM range(4)"
                    .formatted(CATALOG));
            ConnectionPool.execute(setup, "DELETE FROM %s.main.deleted WHERE id = 1".formatted(CATALOG));
        }
        try {
            compareDiscoveryAndCounts();
        } finally {
            try (Connection drop = ConnectionPool.getConnection()) {
                ConnectionPool.execute(drop, "DROP TABLE %s.main.parted".formatted(CATALOG));
                ConnectionPool.execute(drop, "DROP TABLE %s.main.deleted".formatted(CATALOG));
            }
        }
    }

    private static void compareDiscoveryAndCounts() throws Exception {
        try (Connection connection = RawConnections.open(startupScript, List.of())) {
            String schema = CatalogSql.postgresSchema(connection, CATALOG);
            assertEquals("public", schema);
            for (CompactionTier tier : List.of(MINOR, MINOR.withAdmission(100, Duration.ofSeconds(1)), MAJOR)) {
                List<String> inDuckDb = rows(connection, DuckDbTierCompactor.discoverySql(CATALOG, tier));
                List<String> inPostgres = rows(connection, CatalogSql.viaPostgres(CATALOG,
                        DuckDbTierCompactor.discoverySqlPostgres(schema, tier)));
                assertEquals(inDuckDb, inPostgres, "tier " + tier.name() + " " + tier.minFiles());
                if (tier == MINOR) {
                    // every insert into deleted holds id 1, so the DELETE gave all four files a delete file
                    assertEquals(2, inDuckDb.size(), "events and parted; deleted's files all have deletes: " + inDuckDb);
                    assertTrue(inDuckDb.stream().anyMatch(r -> r.contains("table_name=parted files_waiting=8")
                            && r.contains("busy_partitions=2")), "two partitions, two schema versions: " + inDuckDb);
                }
            }
            assertEquals(rows(connection, CompactionService.fileCountsSql(Sql.metadataCatalog(CATALOG), List.of(MINOR, MAJOR))),
                    rows(connection, CatalogSql.viaPostgres(CATALOG,
                            CompactionService.fileCountsSql(Sql.identifier(schema), List.of(MINOR, MAJOR)))));
        }
    }

    /** Every row as text, oldest_waiting_ms to the second (DuckDB truncates to ms, PostgreSQL rounds). */
    private static List<String> rows(Connection connection, String sql) throws SQLException {
        List<String> rows = new java.util.ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int c = 1; c <= columns; c++) {
                    String name = rs.getMetaData().getColumnName(c);
                    Object value = rs.getObject(c);
                    if (name.equals("oldest_waiting_ms") && value != null) {
                        value = ((Number) value).longValue() / 1000;
                    } else if (value instanceof Number n) {
                        value = n.longValue();
                    }
                    row.append(name).append('=').append(value).append(' ');
                }
                rows.add(row.toString());
            }
        }
        java.util.Collections.sort(rows);
        return rows;
    }

    @Test
    @Order(2)
    void aMinorPassListsTheTableAndMergesIt() {
        TierLoop.PassResult result = service.runPass(CATALOG, MINOR);
        assertEquals(1, result.listed(), "events has six small files");
        assertEquals(1, result.merged());

        for (String step : List.of("discover", "merge")) {
            Timer timer = registry.find("ducklake.compaction.duration")
                    .tag("type", "minor").tag("step", step).tag("database", CATALOG).timer();
            assertNotNull(timer, "Minor tier timer not registered for " + step);
            assertEquals(1, timer.count());
        }
        assertEquals(0, service.getStats().databases().get(CATALOG).tiers().get("minor").idleTxnTimeoutMs(),
                "Postgres's default idle-in-transaction timeout is 0: no limit");

        TierLoop.PassResult again = service.runPass(CATALOG, MINOR);
        assertEquals(0, again.listed(), "one file left: nothing to merge");
    }

    @Test
    @Order(3)
    void fileCountGaugesArePopulated() {
        service.refreshFileCounts(CATALOG);
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
        try (Connection conn = ConnectionPool.getConnection()) {
            for (int i = 0; i < 3; i++) {
                ConnectionPool.execute(conn, "INSERT INTO %s.main.events SELECT range, 'more' FROM range(10)".formatted(CATALOG));
            }
        }
        Thread minorThread = new Thread(() -> service.runPass(CATALOG, MINOR));
        Thread majorThread = new Thread(() -> service.runPass(CATALOG, MAJOR));
        minorThread.start();
        majorThread.start();
        minorThread.join(10_000);
        majorThread.join(10_000);

        Timer discoverTimer = registry.find("ducklake.compaction.duration")
                .tag("type", "major")
                .tag("step", "discover")
                .tag("database", CATALOG)
                .timer();
        assertNotNull(discoverTimer, "Expected major tier timer for step: discover");
        assertTrue(discoverTimer.count() >= 1, "Expected at least one recording for step: discover");
        Timer minorMerge = registry.find("ducklake.compaction.duration")
                .tag("type", "minor").tag("step", "merge").tag("database", CATALOG).timer();
        assertEquals(2, minorMerge.count(), "minor merged the new files beside major");
    }

    @Test
    @Order(5)
    void housekeepingTimersAreRecorded() throws Exception {
        service.runHousekeeping(CATALOG);

        assertTrue(service.getStats().databases().get(CATALOG).unexpiredSnapshots() > 0,
                "housekeeping counts the snapshots left after expiry");
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

        service.runPass(CATALOG, MAJOR);

        long after = ConnectionPool.collectFirst(
                "SELECT COUNT(*) FROM %s.ducklake_data_file WHERE end_snapshot IS NULL"
                        .formatted(MD_DATABASE),
                Long.class);

        assertTrue(after <= before, "Expected file count to decrease after compaction (%d -> %d)".formatted(before, after));
    }

    @Test
    @Order(7)
    void connectionSettingsAreAppliedWhenTheTierConnectionOpens() throws Exception {
        // Proves the settings SQL actually runs on the connection the calls use, against a real
        // catalog — a bad setting would make RawConnections.open throw before any call.
        CompactionState state = new CompactionState(new SimpleMeterRegistry(), config.databases(), List.of("minor", "major"));
        try (TierCompactor withSettings = new DuckDbTierCompactor(startupScript, state)) {
            assertFalse(withSettings.prepare(CATALOG, MAJOR.withConnectionSettings(List.of("SET threads=2"))).isRefused());
            assertDoesNotThrow(() -> withSettings.discover(CATALOG, MAJOR));
        }
        try (TierCompactor bad = new DuckDbTierCompactor(startupScript, state)) {
            assertThrows(Exception.class, () -> bad.prepare(CATALOG, MAJOR.withConnectionSettings(List.of("SET no_such_setting=1"))));
        }
    }

    @Test
    @Order(9)
    void theIdleTransactionTimeoutIsReadFromPostgresAndRefusesATierOfWhichNotTwoFilesFit() throws Exception {
        // The timeout comes from the catalog's own session, so an option in the ATTACH string counts.
        String withTimeout = ("INSTALL ducklake; LOAD ducklake;\nATTACH 'ducklake:postgres:%s options=-cidle_in_transaction_session_timeout=%d'"
                + " AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0);");
        CompactionState state = new CompactionState(new SimpleMeterRegistry(), config.databases(), List.of("minor"));
        // The spec's minor tier: a full group of 5 KB files is 3,200 files, 144 s, so it needs 180 s.
        CompactionTier specMinor = CompactionTier.of("minor", 0, 8_000_000, 16_000_000, 12);
        try (TierCompactor compactor = new DuckDbTierCompactor(
                withTimeout.formatted(connectionString, 600_000, CATALOG, dataPath), state)) {
            TierCompactor.Readiness ready = compactor.prepare(CATALOG, specMinor);
            assertFalse(ready.isRefused(), String.valueOf(ready.refused()));
            assertEquals(600_000, ready.idleTxnTimeoutMs());
        }
        try (TierCompactor compactor = new DuckDbTierCompactor(
                withTimeout.formatted(connectionString, 100_000, CATALOG, dataPath), state)) {
            TierCompactor.Readiness lowered = compactor.prepare(CATALOG, specMinor);
            assertFalse(lowered.isRefused(), "80 s cannot hold one 144 s group, but its calls run with a lower target");
            assertEquals(100_000, lowered.idleTxnTimeoutMs());
        }
        try (TierCompactor compactor = new DuckDbTierCompactor(
                withTimeout.formatted(connectionString, 100, CATALOG, dataPath), state)) {
            TierCompactor.Readiness refused = compactor.prepare(CATALOG, specMinor);
            assertTrue(refused.isRefused(), "not even two files fit in 80 ms");
            assertTrue(refused.refused().contains("not even two files"), refused.refused());
        }
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

    @Test
    @Order(10)
    void theStartedServiceMergesNewFilesOnItsOwnAndReportsItsLoops() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "CREATE TABLE %s.main.live (id BIGINT, name VARCHAR)".formatted(CATALOG));
        }
        CompactionTier minor = MINOR.withIdleSleep(Duration.ofMillis(200));
        CompactionTier major = MAJOR.withIdleSleep(Duration.ofMillis(200));
        CompactionConfig liveConfig = new CompactionConfig(List.of(CATALOG), List.of(minor, major),
                Duration.ofHours(1), Duration.ofSeconds(5), List.of(), 0, CompactionRunLog.DEFAULT_CAPACITY);
        SimpleMeterRegistry liveRegistry = new SimpleMeterRegistry();
        CompactionState state = new CompactionState(liveRegistry, liveConfig.databases(), List.of("minor", "major"));
        try (CompactionService live = new CompactionService(liveConfig, startupScript,
                new DuckDbTierCompactor(startupScript, state), new DuckLakeHousekeeper(startupScript,
                liveConfig.snapshotRetention(), List.of(), state), state, new CompactionRunLog(50))) {
            live.start();
            for (int i = 0; i < 5; i++) {
                try (Connection conn = ConnectionPool.getConnection()) {
                    ConnectionPool.execute(conn, "INSERT INTO %s.main.live SELECT range, 'x' FROM range(10)".formatted(CATALOG));
                }
            }
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (liveFiles("live") > 1) {
                assertTrue(System.nanoTime() < deadline, "the minor loop should merge the five files");
                Thread.sleep(100);
            }
            assertEquals(50L, ConnectionPool.collectFirst("SELECT count(*) FROM %s.main.live".formatted(CATALOG), Long.class));
            CompactionStats stats = live.getStats();
            CompactionStats.TierStatus minorLoop = stats.databases().get(CATALOG).tiers().get("minor");
            assertEquals("RUNNING", minorLoop.state());
            assertTrue(minorLoop.passes() >= 1);
            assertTrue(minorLoop.threads() >= 1);
            assertTrue(liveRegistry.get("ducklake.compaction.commits").tag("tier", "minor").functionCounter().count() >= 1);
            assertTrue(stats.alerts().isEmpty(), stats.alerts().toString());
        }
    }

    private static long liveFiles(String table) throws Exception {
        return ConnectionPool.collectFirst((
                "SELECT COUNT(*) FROM %1$s.ducklake_data_file d JOIN %1$s.ducklake_table t USING (table_id) "
                        + "WHERE d.end_snapshot IS NULL AND t.end_snapshot IS NULL AND t.table_name = '%2$s'")
                        .formatted(MD_DATABASE, table),
                Long.class);
    }

    private static long activeDeleteFiles(String table) throws Exception {
        return ConnectionPool.collectFirst((
                "SELECT COUNT(*) FROM %1$s.ducklake_delete_file d JOIN %1$s.ducklake_table t USING (table_id) "
                        + "WHERE d.end_snapshot IS NULL AND t.end_snapshot IS NULL AND t.table_name = '%2$s'")
                        .formatted(MD_DATABASE, table),
                Long.class);
    }
}
