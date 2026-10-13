package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.MutableClock;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A table's partitioning changed under a running queue. The handler caches the partition spec for
 * its refresh interval (an hour here, so the cache is stale for the whole test), so the next batch is
 * written under the old spec and {@code ducklake_add_data_files} rejects it. The queue must re-read
 * the catalog and retry that batch once, delete the rejected files, and count what it did.
 */
class CatalogChangeRetryTest {

    @TempDir
    Path tempDir;

    static final String CATALOG = "catalog_retry_lake";
    static final String SCHEMA = "main";
    static final String TABLE = "events";

    Path outputDir;
    Path src1;
    Path src2;

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(tempDir.resolve("data"));
        outputDir = Files.createDirectories(tempDir.resolve("output"));
        ConnectionPool.execute("INSTALL arrow FROM community");
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "LOAD arrow",
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), CATALOG, tempDir.resolve("data")),
                    "CREATE TABLE %s.%s.%s (id BIGINT, value BIGINT, category VARCHAR)".formatted(CATALOG, SCHEMA, TABLE)
            });
        }
        src1 = writeParquet("src1.parquet", 0, 100);
        src2 = writeParquet("src2.parquet", 100, 50);
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    @Test
    void partitioningAddedUnderARunningQueueIsAppliedByRetryingTheBatch() throws Exception {
        var handler = handler(null);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            assertEquals(100, write(queue, scheduler, src1, 0).rowCount(), "first batch: table unpartitioned");

            alter("SET PARTITIONED BY (category)");
            IngestionResult result = write(queue, scheduler, src2, 1);

            assertEquals(50, result.rowCount());
            assertFalse(result.filesCreated().isEmpty());
            assertTrue(result.filesCreated().stream().allMatch(f -> f.contains("category=")),
                    "the retry must be written under the new spec: " + result.filesCreated());
            assertEquals(1, queue.getCatalogRetries());
            assertEquals(0, queue.getCatalogRetryFailures());
            assertEquals(1, queue.getOrphanFilesDeleted(), "the rejected unpartitioned file is deleted");
            assertEquals(0, queue.getOrphanFilesLeft());
            assertEquals(List.of(), unregisteredFilesUnder(outputDir), "no file is left behind unregistered");

            Stats stats = queue.getStats();
            assertEquals(1, stats.catalogRetries());
            assertEquals(1, stats.orphanFilesDeleted());
            assertEquals(0, stats.failedWriteBatches(), "the client never saw a failure");
        }
        assertEquals(150L, count(""));
        assertEquals(17L, count("WHERE category = 'cat1' AND id >= 100"));
    }

    @Test
    void partitioningRemovedUnderARunningQueueNeedsNoRetry() throws Exception {
        alter("SET PARTITIONED BY (category)");
        var handler = handler(null);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, src1, 0);
            alter("RESET PARTITIONED BY");
            write(queue, scheduler, src2, 1);   // still written partitioned; DuckLake accepts it
            assertEquals(0, queue.getCatalogRetries());
            assertEquals(0, queue.getOrphanFilesDeleted());
        }
        assertEquals(150L, count(""));
        assertEquals(17L, count("WHERE category = 'cat1' AND id >= 100"), "values are read from the path");
    }

    @Test
    void aFailureWithoutACatalogChangeIsNotRetriedAndItsRejectedFilesAreDeleted() throws Exception {
        // category comes out as INTEGER, the table column is VARCHAR: registration rejects every batch.
        var handler = handler("SELECT id, value, 7 AS category FROM __this");
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            var failure = assertThrows(ExecutionException.class, () -> write(queue, scheduler, src1, 0));
            assertInstanceOf(FilesNotRegisteredException.class, rootRegistrationFailure(failure),
                    "the original registration failure reaches the client, not the queue's wrapper");
            assertEquals(0, queue.getCatalogRetries(), "nothing changed in the catalog, so no retry");
            assertEquals(1, queue.getOrphanFilesDeleted());
            assertEquals(List.of(), unregisteredFilesUnder(outputDir));
            assertEquals(1, queue.getStats().failedWriteBatches());
        }
        assertEquals(0L, count(""));
    }

    @Test
    void aRetryThatFailsAgainFailsTheBatchAndIsCounted() throws Exception {
        // First attempt: rejected for the stale (unpartitioned) layout. Retry: written partitioned,
        // but still rejected for the INTEGER category.
        var handler = handler("SELECT id, value, (id % 3)::INTEGER AS category FROM __this");
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            alter("SET PARTITIONED BY (value)");
            assertThrows(ExecutionException.class, () -> write(queue, scheduler, src1, 0));
            assertEquals(1, queue.getCatalogRetries());
            assertEquals(1, queue.getCatalogRetryFailures());
            assertTrue(queue.getOrphanFilesDeleted() >= 2, "files of both attempts are deleted");
            assertEquals(List.of(), unregisteredFilesUnder(outputDir));
        }
    }

    @Test
    void onlyLocalOutputFilesAreDeleted() throws Exception {
        Path local = Files.createFile(tempDir.resolve("orphan.parquet"));
        assertTrue(ParquetIngestionQueue.deleteLocalFile(local.toString()));
        assertFalse(Files.exists(local));

        Path viaUri = Files.createFile(tempDir.resolve("orphan2.parquet"));
        assertTrue(ParquetIngestionQueue.deleteLocalFile("file://" + viaUri));
        assertFalse(Files.exists(viaUri));

        assertFalse(ParquetIngestionQueue.deleteLocalFile("s3://bucket/data/orphan.parquet"),
                "object-store files are left for ducklake_delete_orphaned_files");
    }

    // -----------------------------------------------------------------------

    private DuckLakeIngestionHandler handler(String transformation) {
        var mapping = new QueueIdToTableMapping(TABLE, CATALOG, SCHEMA, TABLE, Map.of(), transformation);
        return new DuckLakeIngestionHandler(Map.of(TABLE, mapping), Duration.ofHours(1));
    }

    private ParquetIngestionQueue queue(DuckLakeIngestionHandler handler, DeterministicScheduler scheduler) {
        return new ParquetIngestionQueue("test-app", "parquet", outputDir.toString(), TABLE,
                1L, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, Duration.ofSeconds(5),
                handler, scheduler, new MutableClock(Instant.now(), ZoneId.systemDefault()));
    }

    private IngestionResult write(ParquetIngestionQueue queue, DeterministicScheduler scheduler,
                                  Path src, long batchId) throws Exception {
        // The queue deletes its input file after the write, so hand it a copy.
        Path input = Files.copy(src, tempDir.resolve("in-" + batchId + "-" + System.nanoTime() + ".parquet"));
        var future = queue.add(new Batch<>(null, null, input.toString(), "producer1", batchId,
                Files.size(input), "parquet", Instant.now()));
        scheduler.tick(1, TimeUnit.MILLISECONDS);
        return future.get(10, TimeUnit.SECONDS);
    }

    private void alter(String change) throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "ALTER TABLE %s.%s.%s %s".formatted(CATALOG, SCHEMA, TABLE, change));
        }
    }

    private long count(String where) throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            return ConnectionPool.collectFirst(conn,
                    "SELECT count(*) FROM %s.%s.%s %s".formatted(CATALOG, SCHEMA, TABLE, where), Long.class);
        }
    }

    /** Parquet files under {@code dir} that the table does not reference. */
    private List<String> unregisteredFilesUnder(Path dir) throws Exception {
        List<String> registered = new java.util.ArrayList<>();
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.collectAll(conn,
                    "SELECT data_file FROM ducklake_list_files('%s', '%s')".formatted(CATALOG, TABLE),
                    rs -> rs.getString(1)).forEach(registered::add);
        }
        try (var files = Files.walk(dir)) {
            return files.filter(p -> p.toString().endsWith(".parquet"))
                    .map(Path::toString)
                    .filter(p -> registered.stream().noneMatch(r -> r.endsWith(p) || p.endsWith(r)))
                    .toList();
        }
    }

    private static Throwable rootRegistrationFailure(ExecutionException e) {
        Throwable t = e.getCause();
        while (t != null && !(t instanceof FilesNotRegisteredException)) {
            t = t.getCause();
        }
        return t;
    }

    private Path writeParquet(String name, int startId, int rows) throws Exception {
        Path file = tempDir.resolve(name);
        ConnectionPool.execute("COPY (SELECT i AS id, i*2 AS value, 'cat'||(i%%3) AS category FROM range(%d, %d) t(i)) TO '%s' (FORMAT PARQUET)"
                .formatted(startId, startId + rows, file));
        return file;
    }
}
