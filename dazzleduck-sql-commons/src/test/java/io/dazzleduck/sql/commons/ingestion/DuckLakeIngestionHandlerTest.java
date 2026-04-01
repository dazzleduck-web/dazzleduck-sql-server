package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DuckLakeIngestionHandlerTest {

    @TempDir
    Path tempDir;

    static final String CATALOG = "test_factory_lake";
    static final String SCHEMA = "main";
    static final String TABLE = "events";
    static final String QUEUE_ID = "events";

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(tempDir.resolve("data"));
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "LOAD arrow",
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), CATALOG, tempDir.resolve("data")),
                    "CREATE TABLE %s.%s.%s (id BIGINT, msg VARCHAR)".formatted(CATALOG, SCHEMA, TABLE)
            });
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    private QueueIdToTableMapping mapping(String queueId, String transformation) {
        return new QueueIdToTableMapping(queueId, CATALOG, SCHEMA, TABLE, Map.of(), transformation);
    }

    // -----------------------------------------------------------------------
    // getTargetPath
    // -----------------------------------------------------------------------

    @Test
    void shouldResolveTargetPathFromCatalogOnConstruction() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        String path = factory.getTargetPath(QUEUE_ID);
        assertNotNull(path, "Expected resolved path, got null");
        assertFalse(path.isBlank());
        assertTrue(path.contains(tempDir.resolve("data").toString()),
                "Expected path under data dir, got: " + path);
    }

    @Test
    void shouldReturnNullTargetPathForUnknownQueueId() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        assertNull(factory.getTargetPath("unknown-queue"));
    }

    // -----------------------------------------------------------------------
    // createPostIngestionTask — direct match
    // -----------------------------------------------------------------------

    @Test
    void shouldCreateTaskForDirectQueueIdMatch() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        var result = new IngestionResult(QUEUE_ID, 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task);
        assertInstanceOf(DuckLakePostIngestionTask.class, task);
    }

    // -----------------------------------------------------------------------
    // createPostIngestionTask — suffix fallback
    // -----------------------------------------------------------------------

    @Test
    void shouldCreateTaskUsingSuffixFallbackWhenQueueNameIsAPath() {
        // Mapping key is "events"; result queueName is a path whose last segment is "events"
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        String pathQueueName = "/tmp/otel-output/" + QUEUE_ID;
        var result = new IngestionResult(pathQueueName, 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task);
        assertInstanceOf(DuckLakePostIngestionTask.class, task);
    }

    @Test
    void shouldCreateTaskUsingSuffixFallbackWithBackslashPath() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        String windowsPath = "C:\\output\\" + QUEUE_ID;
        var result = new IngestionResult(windowsPath, 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task);
        assertInstanceOf(DuckLakePostIngestionTask.class, task);
    }

    // -----------------------------------------------------------------------
    // createPostIngestionTask — no mapping found
    // -----------------------------------------------------------------------

    @Test
    void shouldThrowForUnknownQueueIdWithNoSuffixMatch() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        var result = new IngestionResult("completely-unknown", 1L, "app", Map.of(), 0L, List.of());
        var ex = assertThrows(IllegalArgumentException.class,
                () -> factory.createPostIngestionTask(result));
        assertTrue(ex.getMessage().contains("completely-unknown"),
                "Error should contain the unknown queue name");
        assertTrue(ex.getMessage().contains(QUEUE_ID),
                "Error should list available mappings");
    }

    @Test
    void shouldThrowWhenPathSuffixDoesNotMatchAnyMappingKey() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        // last segment is "other", not "events"
        var result = new IngestionResult("/tmp/output/other", 1L, "app", Map.of(), 0L, List.of());
        assertThrows(IllegalArgumentException.class, () -> factory.createPostIngestionTask(result));
    }

    // -----------------------------------------------------------------------
    // getTransformation
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnTransformationForKnownQueue() {
        String sql = "SELECT id, upper(msg) AS msg FROM __this";
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, sql)));
        assertEquals(sql, factory.getTransformation(QUEUE_ID));
    }

    @Test
    void shouldReturnNullTransformationWhenNotConfigured() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        assertNull(factory.getTransformation(QUEUE_ID));
    }

    @Test
    void shouldReturnNullTransformationForUnknownQueue() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        assertNull(factory.getTransformation("no-such-queue"));
    }

    @Test
    void shouldReturnTransformationViaSuffixFallback() {
        String sql = "SELECT * FROM __this WHERE id > 0";
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, sql)));
        assertEquals(sql, factory.getTransformation("/var/data/" + QUEUE_ID));
    }

    // -----------------------------------------------------------------------
    // Multiple mappings
    // -----------------------------------------------------------------------

    @Test
    void shouldSupportMultipleMappings() throws Exception {
        String table2 = "metrics";
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "CREATE TABLE %s.%s.%s (id BIGINT, val DOUBLE)".formatted(CATALOG, SCHEMA, table2));
        }

        var mappings = Map.of(
                "events", new QueueIdToTableMapping("events", CATALOG, SCHEMA, TABLE, Map.of(), null),
                "metrics", new QueueIdToTableMapping("metrics", CATALOG, SCHEMA, table2, Map.of(), null)
        );
        var factory = new DuckLakeIngestionHandler(mappings);

        assertNotNull(factory.getTargetPath("events"));
        assertNotNull(factory.getTargetPath("metrics"));
        assertNotEquals(factory.getTargetPath("events"), factory.getTargetPath("metrics"));

        assertInstanceOf(DuckLakePostIngestionTask.class,
                factory.createPostIngestionTask(new IngestionResult("events", 1L, "app", Map.of(), 0L, List.of())));
        assertInstanceOf(DuckLakePostIngestionTask.class,
                factory.createPostIngestionTask(new IngestionResult("metrics", 1L, "app", Map.of(), 0L, List.of())));
    }
}
