package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.MutableClock;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A queue's own {@code max_delay_ms}, from the static mapping or the dynamic registry, overriding the
 * global {@code ingestion.max_delay_ms}, and applied to a running queue without recreating it.
 */
class PerQueueMaxDelayTest {

    private static final long MIN_BUCKET = 1_000_000;
    private static final String CATALOG = "per_queue_delay_lake";

    @TempDir
    Path tempDir;

    @BeforeAll
    static void loadExtensions() throws Exception {
        ConnectionPool.executeBatch(new String[]{"INSTALL ducklake", "LOAD ducklake"});
    }

    @AfterEach
    void detach() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH DATABASE IF EXISTS " + CATALOG);
        }
    }

    // -----------------------------------------------------------------------
    // The queue
    // -----------------------------------------------------------------------

    private static Batch<String> small(long id) {
        return new Batch<>(new String[0], new String[0], "", "p", id, 10, "parquet", Instant.now());
    }

    /** Moves the scheduler and the queue's clock together. */
    private static void advance(DeterministicScheduler service, MutableClock clock, long millis) {
        clock.advanceBy(Duration.ofMillis(millis));
        service.tick(millis, TimeUnit.MILLISECONDS);
    }

    @Test
    void aShorterDelayFlushesTheBucketSooner() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var queue = new MockBulkIngestQueue("q", MIN_BUCKET, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                Duration.ofSeconds(10), service, clock);
        queue.setMaxDelayOverride(Duration.ofSeconds(1));
        assertEquals(Duration.ofSeconds(1), queue.getMaxDelay());

        Future<MockWriteResult> result = queue.add(small(0));
        advance(service, clock, 1_100);
        assertNotNull(result.get(5, TimeUnit.SECONDS), "written after 1 s, not the global 10 s");
        queue.close();
    }

    @Test
    void aLongerDelayHoldsTheBucketAndClearingItGoesBackToTheGlobalDelay() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var queue = new MockBulkIngestQueue("q", MIN_BUCKET, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                Duration.ofSeconds(1), service, clock);
        queue.setMaxDelayOverride(Duration.ofSeconds(10));

        Future<MockWriteResult> result = queue.add(small(0));
        advance(service, clock, 1_100);
        Thread.sleep(50);
        assertFalse(result.isDone(), "held past the global 1 s");

        queue.setMaxDelayOverride(null); // the registry row's value is removed
        assertEquals(Duration.ofSeconds(1), queue.getMaxDelay());
        advance(service, clock, 1);
        assertNotNull(result.get(5, TimeUnit.SECONDS), "the pending check is brought forward to the global delay");
        queue.close();
    }

    @Test
    void theFirstBucketOfAQueueWaitsItsOwnDelayFromCreation() throws Exception {
        // The handler applies a queue's own delay right after creating it, while the queue's first
        // flush check was scheduled at the global one: that check must not flush early.
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var queue = new MockBulkIngestQueue("q", MIN_BUCKET, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                Duration.ofSeconds(2), service, clock);
        queue.setMaxDelayOverride(Duration.ofSeconds(30));
        Future<MockWriteResult> result = queue.add(small(0));
        advance(service, clock, 2_100);
        Thread.sleep(50);
        assertFalse(result.isDone(), "not at the global 2 s");
        advance(service, clock, 28_000);
        assertNotNull(result.get(5, TimeUnit.SECONDS), "at its own 30 s");
        queue.close();
    }

    @Test
    void everyPartitionOfAPartitionedQueueTakesTheDelay() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        IngestionHandler handler = new IngestionHandler() {
            @Override public PostIngestionTask createPostIngestionTask(IngestionResult r) { return PostIngestionTask.NOOP; }
            @Override public String getTargetPath(String queueId) { return tempDir.toString(); }
            @Override public String[] getPartitionBy(String queueId) { return new String[0]; }
        };
        try (var queue = new PartitionedIngestionQueue("app", "parquet", tempDir.toString(), "q",
                MIN_BUCKET, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, Duration.ofSeconds(2), null,
                handler, scheduler, Clock.systemUTC(), 3, "id")) {
            queue.setMaxDelayOverride(Duration.ofMillis(250));
            assertEquals(Duration.ofMillis(250), queue.getMaxDelay());
            queue.children().forEach(child -> assertEquals(Duration.ofMillis(250), child.getMaxDelay()));
            assertEquals(250, queue.getStats().maxDelayMs());
            assertTrue(queue.getStats().partitions().stream().allMatch(p -> p.maxDelayMs() == 250));
            queue.setMaxDelayOverride(null);
            queue.children().forEach(child -> assertEquals(Duration.ofSeconds(2), child.getMaxDelay()));
        } finally {
            scheduler.shutdownNow();
        }
    }

    // -----------------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------------

    @Test
    void theStaticMappingReadsMaxDelayMsPerQueue() {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [
                    { ingestion_queue = "fast", catalog = "c", schema = "s", table = "t1", max_delay_ms = 250 }
                    { ingestion_queue = "global", catalog = "c", schema = "s", table = "t2" }
                ]
                """));
        Map<String, QueueIdToTableMapping> mappings = provider.loadMappings();
        assertEquals(Duration.ofMillis(250), mappings.get("fast").maxDelay());
        assertNull(mappings.get("global").maxDelay(), "unset: the global ingestion.max_delay_ms");
    }

    @Test
    void aNonPositiveMaxDelayInTheStaticMappingFailsStartup() {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [
                    { ingestion_queue = "bad", catalog = "c", schema = "s", table = "t", max_delay_ms = 0 }
                ]
                """));
        var e = assertThrows(IllegalArgumentException.class, provider::loadMappings);
        assertTrue(e.getMessage().contains("max_delay_ms"), e.getMessage());
    }

    /**
     * Writes to the registry, retrying while it is locked: a handler under test polls the same file,
     * and a write that lands during its read comes back "database is locked", a busy signal a real
     * writer copes with too (as {@code DynamicQueueRepositoryTest} does).
     */
    private static void writeToDb(String dbPath, String sql) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (true) {
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
                s.execute("LOAD sqlite");
                s.execute("ATTACH '" + dbPath + "' AS " + DynamicQueueRepository.ATTACHMENT + " (TYPE sqlite)");
                s.execute(sql);
                return;
            } catch (java.sql.SQLException e) {
                boolean locked = e.getMessage() != null && e.getMessage().contains("database is locked");
                if (!locked || System.nanoTime() > deadline) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }
    }

    private static final String QUEUES = DynamicQueueRepository.ATTACHMENT + ".ingestion_queues";

    @Test
    void theRegistryReadsMaxDelayMsAndIgnoresANonPositiveOne() throws Exception {
        String dbPath = tempDir.resolve("registry.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            writeToDb(dbPath, "INSERT INTO " + QUEUES + " (ingestion_queue, catalog, schema_name, table_name, max_delay_ms)"
                    + " VALUES ('fast', 'c', 's', 't1', 250), ('global', 'c', 's', 't2', NULL), ('bad', 'c', 's', 't3', 0)");
            try (Connection conn = repo.openReadOnlyConnection()) {
                Map<String, QueueIdToTableMapping> mappings = DynamicQueueRepository.loadAll(conn);
                assertEquals(Duration.ofMillis(250), mappings.get("fast").maxDelay());
                assertNull(mappings.get("global").maxDelay());
                assertNull(mappings.get("bad").maxDelay(), "a non-positive value falls back, without failing the reload");
                assertEquals(3, mappings.size());
            }
        }
    }

    @Test
    void aRegistryWithoutTheColumnGetsIt() throws Exception {
        String dbPath = tempDir.resolve("old.db").toString();
        writeToDb(dbPath, "CREATE TABLE " + QUEUES + " (ingestion_queue TEXT PRIMARY KEY, catalog TEXT NOT NULL,"
                + " schema_name TEXT NOT NULL, table_name TEXT NOT NULL, transformation TEXT, view_name TEXT,"
                + " input_table TEXT, input_schema TEXT, partition_by TEXT, min_bucket_size INTEGER)");
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            writeToDb(dbPath, "INSERT INTO " + QUEUES + " (ingestion_queue, catalog, schema_name, table_name, max_delay_ms)"
                    + " VALUES ('q', 'c', 's', 't', 500)");
            try (Connection conn = repo.openReadOnlyConnection()) {
                assertEquals(Duration.ofMillis(500), DynamicQueueRepository.loadAll(conn).get("q").maxDelay());
            }
        }
    }

    // -----------------------------------------------------------------------
    // The handler applies it to the running queue
    // -----------------------------------------------------------------------

    private void createCatalog() throws Exception {
        Path dataPath = tempDir.resolve("data");
        Files.createDirectories(dataPath);
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(tempDir.resolve("catalog"), CATALOG, dataPath),
                    "CREATE TABLE %s.main.events (id BIGINT)".formatted(CATALOG),
                    "CREATE TABLE %s.main.other (id BIGINT)".formatted(CATALOG)
            });
        }
    }

    private static IngestionHandler.QueueCreator creator(IngestionHandler handler, ScheduledExecutorService scheduler,
                                                         Duration global) {
        return (id, path) -> new ParquetIngestionQueue("test", "arrow", path, id, MIN_BUCKET, Long.MAX_VALUE,
                Integer.MAX_VALUE, Long.MAX_VALUE, global, handler, scheduler, Clock.systemUTC());
    }

    private static final IngestionHandler.QueueEventListener NO_EVENTS = new IngestionHandler.QueueEventListener() {
        @Override public void onCreated(String id) {}
    };

    @Test
    void theHandlerGivesEachQueueItsOwnDelayOrTheGlobalOne() throws Exception {
        createCatalog();
        var handler = new DuckLakeIngestionHandler(Map.of(
                "fast", new QueueIdToTableMapping("fast", CATALOG, "main", "events", Map.of(), null)
                        .withMaxDelay(Duration.ofMillis(250)),
                "global", new QueueIdToTableMapping("global", CATALOG, "main", "other", Map.of(), null)));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            var create = creator(handler, scheduler, Duration.ofSeconds(10));
            var fast = handler.getOrCreateQueue("fast", create, NO_EVENTS);
            assertEquals(Duration.ofMillis(250), fast.getMaxDelay());
            assertEquals(250, fast.getStats().maxDelayMs(), "the dashboard shows the queue's own delay");
            assertEquals(Duration.ofSeconds(10), handler.getOrCreateQueue("global", create, NO_EVENTS).getMaxDelay());
        } finally {
            handler.closeQueues();
            scheduler.shutdownNow();
        }
    }

    @Test
    void aReloadAppliesTheDelayToAnIdleQueueWithoutWaitingForABatch() throws Exception {
        // A batch waiting under a long delay must not wait all of it when the registry shortens the
        // delay and no further batch arrives: the reload itself applies the new value.
        createCatalog();
        String dbPath = tempDir.resolve("idle.db").toString();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            writeToDb(dbPath, "INSERT INTO " + QUEUES + " (ingestion_queue, catalog, schema_name, table_name, max_delay_ms)"
                    + " VALUES ('q', '" + CATALOG + "', 'main', 'events', 60000)");
            writeToDb(dbPath, "UPDATE " + DynamicQueueRepository.ATTACHMENT + ".schema_version SET version = version + 1");
            Connection readConn = repo.openReadOnlyConnection();
            var handler = new DynamicIngestionHandler(dbPath, readConn, DynamicQueueRepository.loadAll(readConn),
                    Duration.ofMillis(50));
            try {
                ParquetIngestionQueue queue = handler.getOrCreateQueue("q", creator(handler, scheduler,
                        Duration.ofSeconds(10)), NO_EVENTS);
                assertEquals(Duration.ofSeconds(60), queue.getMaxDelay());

                writeToDb(dbPath, "UPDATE " + QUEUES + " SET max_delay_ms = 200 WHERE ingestion_queue = 'q'");
                writeToDb(dbPath, "UPDATE " + DynamicQueueRepository.ATTACHMENT + ".schema_version SET version = version + 1");
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                while (!Duration.ofMillis(200).equals(queue.getMaxDelay()) && System.nanoTime() < deadline) {
                    Thread.sleep(20); // no getOrCreateQueue, no batch: only the reload can change it
                }
                assertEquals(Duration.ofMillis(200), queue.getMaxDelay());
            } finally {
                handler.closeQueues();
            }
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void aRegistryChangeAppliesToTheRunningQueueWithoutRecreatingIt() throws Exception {
        createCatalog();
        String dbPath = tempDir.resolve("live.db").toString();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            writeToDb(dbPath, "INSERT INTO " + QUEUES + " (ingestion_queue, catalog, schema_name, table_name, max_delay_ms)"
                    + " VALUES ('q', '" + CATALOG + "', 'main', 'events', 250)");
            writeToDb(dbPath, "UPDATE " + DynamicQueueRepository.ATTACHMENT + ".schema_version SET version = version + 1");
            Connection readConn = repo.openReadOnlyConnection();
            var handler = new DynamicIngestionHandler(dbPath, readConn, DynamicQueueRepository.loadAll(readConn),
                    Duration.ofMillis(50));
            try {
                var create = creator(handler, scheduler, Duration.ofSeconds(10));
                ParquetIngestionQueue queue = handler.getOrCreateQueue("q", create, NO_EVENTS);
                assertEquals(Duration.ofMillis(250), queue.getMaxDelay());

                writeToDb(dbPath, "UPDATE " + QUEUES + " SET max_delay_ms = 5000 WHERE ingestion_queue = 'q'");
                writeToDb(dbPath, "UPDATE " + DynamicQueueRepository.ATTACHMENT + ".schema_version SET version = version + 1");
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                while (!Duration.ofMillis(5000).equals(handler.getMaxDelay("q")) && System.nanoTime() < deadline) {
                    Thread.sleep(20);
                }
                assertSame(queue, handler.getOrCreateQueue("q", create, NO_EVENTS), "the same queue, not a new one");
                assertEquals(Duration.ofMillis(5000), queue.getMaxDelay());

                writeToDb(dbPath, "UPDATE " + QUEUES + " SET max_delay_ms = NULL WHERE ingestion_queue = 'q'");
                writeToDb(dbPath, "UPDATE " + DynamicQueueRepository.ATTACHMENT + ".schema_version SET version = version + 1");
                deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                while (handler.getMaxDelay("q") != null && System.nanoTime() < deadline) {
                    Thread.sleep(20);
                }
                handler.getOrCreateQueue("q", create, NO_EVENTS);
                assertEquals(Duration.ofSeconds(10), queue.getMaxDelay(), "back to the global delay");
            } finally {
                handler.closeQueues();
            }
        } finally {
            scheduler.shutdownNow();
        }
    }
}
