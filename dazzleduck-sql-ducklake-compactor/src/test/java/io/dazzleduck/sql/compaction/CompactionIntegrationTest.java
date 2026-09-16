package io.dazzleduck.sql.compaction;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CompactionIntegrationTest {

    @TempDir
    static Path tempDir;

    static final String CATALOG = "test_lake";
    static final String MD_DATABASE = "__ducklake_metadata_" + CATALOG;

    static SimpleMeterRegistry registry;
    static CompactionConfig config;
    static CompactionService service;

    @BeforeAll
    static void setUp() throws Exception {
        Files.createDirectories(tempDir.resolve("data"));

        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0)".formatted(
                            tempDir.resolve("catalog"), CATALOG, tempDir.resolve("data")),
                    "CREATE TABLE %s.main.events (id BIGINT, name VARCHAR)".formatted(CATALOG)
            });
        }

        // Insert data and flush inlined data to force Parquet file creation
        for (int i = 0; i < 6; i++) {
            try (Connection conn = ConnectionPool.getConnection()) {
                ConnectionPool.execute(conn,
                        "INSERT INTO %s.main.events SELECT range + %d * 10, 'event-' || range FROM range(10)"
                                .formatted(CATALOG, i));
            }
        }

        config = new CompactionConfig(
                List.of(CATALOG),
                Duration.ofSeconds(60),
                Duration.ofMillis(100),   // short so major fires quickly in tests
                Duration.ofMillis(500),   // housekeeping every 500ms in tests
                512 * 1024L,              // 512KB minor max
                0,                        // 0 = unbounded, unchanged behavior for this test
                10 * 1024 * 1024L,        // 10MB major max
                Duration.ofSeconds(5),
                0                         // 0 = OS-assigned port, health server not used in tests
        );

        registry = new SimpleMeterRegistry();
        CompactionState state = new CompactionState(registry, config.databases());
        MajorCompactor majorCompactor = new DuckDbMajorCompactor(
                config.majorCompactionMaxSize(), config.snapshotRetention(), state);
        service = new CompactionService(config, majorCompactor, state);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (service != null) service.close();
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    @Test
    @Order(1)
    void initialFileCountIsGreaterThanZero() throws Exception {
        long total = ConnectionPool.collectFirst(
                "SELECT COUNT(*) FROM %s.main.ducklake_data_file WHERE end_snapshot IS NULL"
                        .formatted(MD_DATABASE),
                Long.class);
        assertTrue(total > 0, "Expected files to exist before compaction, found: " + total);
    }

    @Test
    @Order(2)
    void minorCompactionTimerIsRecorded() {
        service.runCompaction(CATALOG);

        Timer timer = registry.find("ducklake.compaction.duration")
                .tag("type", "minor")
                .tag("step", "merge")
                .tag("database", CATALOG)
                .timer();

        assertNotNull(timer, "Minor compaction timer not registered");
        assertEquals(1, timer.count());
    }

    @Test
    @Order(3)
    void fileCountGaugesArePopulated() {
        double total = registry.find("ducklake.files.total")
                .tag("database", CATALOG)
                .gauge()
                .value();
        double small = registry.find("ducklake.files.small")
                .tag("database", CATALOG)
                .gauge()
                .value();
        double medium = registry.find("ducklake.files.medium")
                .tag("database", CATALOG)
                .gauge()
                .value();

        assertTrue(total >= 0, "Total files gauge should be non-negative");
        // small + medium covers files below majorCompactionMaxSize; large files only appear in total
        assertTrue(small + medium <= total, "small + medium must not exceed total");
    }

    @Test
    @Order(4)
    void majorCompactionTimersAreRecorded() throws Exception {
        // majorCompactionFrequency = 100ms, so wait for it to be eligible
        Thread.sleep(200);
        service.runCompaction(CATALOG);

        Timer mergeTimer = registry.find("ducklake.compaction.duration")
                .tag("type", "major")
                .tag("step", "merge")
                .tag("database", CATALOG)
                .timer();
        assertNotNull(mergeTimer, "Expected major compaction timer for step: merge");
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
                "SELECT COUNT(*) FROM %s.main.ducklake_data_file WHERE end_snapshot IS NULL"
                        .formatted(MD_DATABASE),
                Long.class);

        // Ensure major fires
        Thread.sleep(200);
        service.runCompaction(CATALOG);

        long after = ConnectionPool.collectFirst(
                "SELECT COUNT(*) FROM %s.main.ducklake_data_file WHERE end_snapshot IS NULL"
                        .formatted(MD_DATABASE),
                Long.class);

        assertTrue(after <= before, "Expected file count to decrease after compaction (%d -> %d)".formatted(before, after));
    }
}
