package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.MutableClock;
import io.dazzleduck.sql.commons.util.TestUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests for the DuckLake ingestion pipeline:
 * ParquetIngestionQueue → DuckLakeIngestionTaskFactory → DuckLakePostIngestionTask → DuckLake catalog.
 */
class DuckLakeIngestionIntegrationTest {

    @TempDir
    Path tempDir;

    static final String CATALOG = "ingest_test_lake";
    static final String SCHEMA = "main";
    static final String TABLE = "events";

    Path parquetSrc1;
    Path parquetSrc2;
    Path outputDir;

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(tempDir.resolve("data"));
        outputDir = tempDir.resolve("output");
        Files.createDirectories(outputDir);

        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "LOAD arrow",
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), CATALOG, tempDir.resolve("data")),
                    "CREATE TABLE %s.%s.%s (id BIGINT, value BIGINT, category VARCHAR)".formatted(CATALOG, SCHEMA, TABLE)
            });
        }

        parquetSrc1 = writeParquet("src1.parquet", 0, 100);
        parquetSrc2 = writeParquet("src2.parquet", 100, 50);
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    // -----------------------------------------------------------------------
    // Single batch → DuckLake table
    // -----------------------------------------------------------------------

    @Test
    void shouldRegisterParquetFileInDuckLakeAfterIngestion() throws Exception {
        var factory = factoryFor(TABLE, null);
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        try (var queue = queue(factory, scheduler, clock)) {
            var future = queue.add(batch(parquetSrc1, 0, Files.size(parquetSrc1) + 1));
            scheduler.tick(1, TimeUnit.MILLISECONDS);
            var result = future.get(5, TimeUnit.SECONDS);

            assertFalse(result.filesCreated().isEmpty(), "Expected at least one Parquet file written");
            assertEquals(100, result.rowCount());
        }

        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator alloc = new RootAllocator()) {
            ConnectionPool.execute(conn, "USE " + CATALOG);
            TestUtils.isEqual(conn, alloc,
                    "SELECT * FROM %s ORDER BY id".formatted(TABLE),
                    "SELECT i AS id, i*2 AS value, 'cat'||(i%3) AS category FROM range(0,100) t(i) ORDER BY id");
        }
    }

    // -----------------------------------------------------------------------
    // Multiple batches batched into one bucket
    // -----------------------------------------------------------------------

    @Test
    void shouldRegisterAllFilesWhenMultipleBatchesMergeIntoOneBucket() throws Exception {
        var factory = factoryFor(TABLE, null);
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        long src1Size = Files.size(parquetSrc1);
        long src2Size = Files.size(parquetSrc2);
        // minBucketSize > src1 alone but <= combined, so first batch doesn't flush,
        // second batch pushes total over threshold and triggers one merged write.
        long minBucket = src1Size + src2Size;

        try (var queue = new ParquetIngestionQueue(
                "test-app", "parquet",
                outputDir.toString(), TABLE,
                minBucket, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                Duration.ofSeconds(5), factory, scheduler, clock)) {

            var f1 = queue.add(batch(parquetSrc1, 0, src1Size));
            var f2 = queue.add(batch(parquetSrc2, 1, src2Size));
            scheduler.tick(1, TimeUnit.MILLISECONDS);
            var r1 = f1.get(5, TimeUnit.SECONDS);
            var r2 = f2.get(5, TimeUnit.SECONDS);

            // Both futures resolve to the same merged bucket result
            assertEquals(150, r1.rowCount());
            assertEquals(150, r2.rowCount());
        }

        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator alloc = new RootAllocator()) {
            ConnectionPool.execute(conn, "USE " + CATALOG);
            Long count = ConnectionPool.collectFirst(conn, "SELECT COUNT(*) FROM " + TABLE, Long.class);
            assertEquals(150L, count);
        }
    }

    // -----------------------------------------------------------------------
    // Transformation applied before writing to DuckLake
    // -----------------------------------------------------------------------

    @Test
    void shouldApplyTransformationBeforeRegisteringInDuckLake() throws Exception {
        // Table has (id, value, category); transformation drops category
        String transformation = "SELECT id, value FROM __this";

        // Re-create the table with only the columns the transformation produces
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "DROP TABLE %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE),
                    "CREATE TABLE %s.%s.%s (id BIGINT, value BIGINT)".formatted(CATALOG, SCHEMA, TABLE)
            });
        }

        var factory = factoryFor(TABLE, transformation);
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        try (var queue = queueWithTransformation(factory, transformation, scheduler, clock)) {
            var future = queue.add(batch(parquetSrc1, 0, Files.size(parquetSrc1) + 1));
            scheduler.tick(1, TimeUnit.MILLISECONDS);
            future.get(5, TimeUnit.SECONDS);
        }

        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator alloc = new RootAllocator()) {
            ConnectionPool.execute(conn, "USE " + CATALOG);
            TestUtils.isEqual(conn, alloc,
                    "SELECT * FROM %s ORDER BY id".formatted(TABLE),
                    "SELECT i AS id, i*2 AS value FROM range(0,100) t(i) ORDER BY id");
        }
    }

    // -----------------------------------------------------------------------
    // Suffix-based queue name mapping
    // -----------------------------------------------------------------------

    @Test
    void shouldResolveTableMappingViaSuffixWhenQueueNameIsOutputPath() throws Exception {
        // Mapping key matches the last segment of the output path ("events")
        var mappings = Map.of(TABLE,
                new QueueIdToTableMapping(TABLE, CATALOG, SCHEMA, TABLE, Map.of(), null));
        var factory = new DuckLakeIngestionHandler(mappings);
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        // Queue name is the full output path; DuckLakeIngestionTaskFactory uses suffix fallback
        String queueName = outputDir.toString(); // ends with "output", not "events"
        // Use a queueName that ends in the mapping key
        Path tableOutputDir = tempDir.resolve(TABLE);
        Files.createDirectories(tableOutputDir);

        try (var queue = new ParquetIngestionQueue(
                "test-app", "parquet",
                tableOutputDir.toString(),
                tableOutputDir.toString(),        // queueName = full path ending in "events"
                1L, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                Duration.ofSeconds(5), factory, scheduler, clock)) {

            var future = queue.add(batch(parquetSrc1, 0, Files.size(parquetSrc1) + 1));
            scheduler.tick(1, TimeUnit.MILLISECONDS);
            var result = future.get(5, TimeUnit.SECONDS);
            assertEquals(100, result.rowCount());
        }

        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "USE " + CATALOG);
            Long count = ConnectionPool.collectFirst(conn, "SELECT COUNT(*) FROM " + TABLE, Long.class);
            assertEquals(100L, count);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private Path writeParquet(String name, int startId, int rows) throws Exception {
        Path file = tempDir.resolve(name);
        int endId = startId + rows;
        ConnectionPool.execute(
                "COPY (SELECT i AS id, i*2 AS value, 'cat'||(i%3) AS category " +
                "FROM range(" + startId + ", " + endId + ") t(i)) TO '" + file + "' (FORMAT PARQUET)");
        return file;
    }

    private DuckLakeIngestionHandler factoryFor(String table, String transformation) {
        var mapping = new QueueIdToTableMapping(TABLE, CATALOG, SCHEMA, table, Map.of(), transformation);
        return new DuckLakeIngestionHandler(Map.of(TABLE, mapping));
    }

    private Batch<String> batch(Path file, long batchId, long size) {
        return new Batch<>(null, null, file.toString(),
                "producer1", batchId, size, "parquet", Instant.now());
    }

    private ParquetIngestionQueue queue(DuckLakeIngestionHandler factory,
                                        DeterministicScheduler scheduler,
                                        MutableClock clock) {
        return new ParquetIngestionQueue(
                "test-app", "parquet",
                outputDir.toString(), TABLE,
                1L, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                Duration.ofSeconds(5), factory, scheduler, clock);
    }

    private ParquetIngestionQueue queueWithTransformation(DuckLakeIngestionHandler factory,
                                                          String transformation,
                                                          DeterministicScheduler scheduler,
                                                          MutableClock clock) {
        return new ParquetIngestionQueue(
                "test-app", "parquet",
                outputDir.toString(), TABLE,
                1L, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                Duration.ofSeconds(5), factory, scheduler, clock, transformation);
    }
}
