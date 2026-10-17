package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DynamicDuckLakeIngestionTaskFactoryProviderTest {

    @TempDir
    Path tempDir;

    private DynamicDuckLakeIngestionTaskFactoryProvider providerFor(String dbPath) {
        var provider = new DynamicDuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString(
                "db_path = \"" + dbPath + "\"\nconfig_load_interval_ms = 5000"));
        return provider;
    }

    private void initDb(String dbPath) throws Exception {
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
        }
    }

    private void insertQueue(String dbPath, String queueId,
                             String catalog, String schema, String table,
                             String transformation) throws Exception {
        String safePath = dbPath.replace("'", "''");
        String transVal = transformation == null ? "NULL" : "'" + transformation + "'";
        String att = DynamicQueueRepository.ATTACHMENT;
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement st = conn.createStatement()) {
            st.execute("LOAD sqlite");
            st.execute("ATTACH '" + safePath + "' AS " + att + " (TYPE sqlite)");
            st.execute("INSERT INTO " + att + ".ingestion_queues " +
                       "(ingestion_queue, catalog, schema_name, table_name, transformation) " +
                       "VALUES ('" + queueId + "', '" +
                       catalog + "', '" + schema + "', '" + table + "', " + transVal + ")");
        }
    }

    // -----------------------------------------------------------------------
    // loadMappings
    // -----------------------------------------------------------------------

    @Test
    void loadMappingsReturnsEmptyWhenConfigNotSet() {
        var provider = new DynamicDuckLakeIngestionTaskFactoryProvider();
        assertTrue(provider.loadMappings().isEmpty());
    }

    @Test
    void loadMappingsInitialisesNewDatabase() {
        String dbPath = tempDir.resolve("new.db").toString();
        var provider = providerFor(dbPath);
        // DB doesn't exist — loadMappings must create it and return empty map
        Map<String, QueueIdToTableMapping> mappings = provider.loadMappings();
        assertTrue(mappings.isEmpty());
    }

    @Test
    void loadMappingsReturnsInsertedRows() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs", "loglake", "main", "logs", null);

        var mappings = providerFor(dbPath).loadMappings();

        assertEquals(1, mappings.size());
        QueueIdToTableMapping m = mappings.get("logs");
        assertNotNull(m);
        assertNull(m.outputPath(), "output path is derived from DuckLake, not read from the registry");
        assertEquals("loglake", m.catalog());
        assertEquals("main", m.schema());
        assertEquals("logs", m.table());
        assertNull(m.transformation());
    }

    @Test
    void loadMappingsReturnsMultipleRows() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs",    "loglake", "main", "logs",    null);
        insertQueue(dbPath, "traces",  "loglake", "main", "traces",  null);
        insertQueue(dbPath, "metrics", "loglake", "main", "metrics", null);

        assertEquals(3, providerFor(dbPath).loadMappings().size());
    }

    // -----------------------------------------------------------------------
    // getIngestionHandler
    // -----------------------------------------------------------------------

    @Test
    void getIngestionHandlerThrowsWhenConfigNotSet() {
        var provider = new DynamicDuckLakeIngestionTaskFactoryProvider();
        assertThrows(IllegalStateException.class, provider::getIngestionHandler);
    }

    @Test
    void getIngestionHandlerReturnsDynamicHandler() {
        String dbPath = tempDir.resolve("test.db").toString();
        IngestionHandler handler = providerFor(dbPath).getIngestionHandler();
        try {
            assertInstanceOf(DynamicIngestionHandler.class, handler);
        } finally {
            handler.closeQueues();
        }
    }

    @Test
    void getIngestionHandlerInitialisesSchemaOnNewDatabase() throws Exception {
        String dbPath = tempDir.resolve("new.db").toString();
        IngestionHandler handler = providerFor(dbPath).getIngestionHandler();
        try {
            try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath);
                 Connection conn = repo.openReadOnlyConnection()) {
                assertEquals(0L, DynamicQueueRepository.readSchemaVersion(conn));
            }
        } finally {
            handler.closeQueues();
        }
    }

    @Test
    void getIngestionHandlerLoadsInitialQueues() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs", "loglake", "main", "logs", null);

        IngestionHandler handler = providerFor(dbPath).getIngestionHandler();
        try {
            // The target path is derived from DuckLake metadata (not set up here); the loaded
            // registry is reflected in the known-queue set.
            assertTrue(handler.getKnownQueues().contains("logs"));
        } finally {
            handler.closeQueues();
        }
    }

    // -----------------------------------------------------------------------
    // validate
    // -----------------------------------------------------------------------

    @Test
    void validatePassesWithNoRows() {
        String dbPath = tempDir.resolve("test.db").toString();
        assertDoesNotThrow(() -> providerFor(dbPath).validate());
    }

    @Test
    void validatePassesWithValidTransformation() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs", "loglake", "main", "logs",
                "SELECT id, ts FROM __this");

        assertDoesNotThrow(() -> providerFor(dbPath).validate());
    }

    /**
     * The inlining rule's instance-wide {@code SET GLOBAL} must not fire when nothing here writes
     * with SQL. A dynamic queue cannot configure a watermark — its registry stores no
     * {@code additional_parameters} — so every write of this provider's queues is a registered
     * file, which inlining cannot touch. Zeroing the setting anyway would change every small
     * {@code INSERT} on every DuckLake attached to the instance, including a user's own, for no
     * benefit on this path.
     */
    @Test
    void validateLeavesTheInliningSettingAloneWhenNoQueueWritesWatermarkRows() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs", "loglake", "main", "logs", null);

        String before = ConnectionPool.collectFirst(
                "SELECT current_setting('%s')::VARCHAR".formatted(DuckLakeInlining.SETTING), String.class);
        try {
            ConnectionPool.execute("SET GLOBAL %s = 7".formatted(DuckLakeInlining.SETTING));
            assertDoesNotThrow(() -> providerFor(dbPath).validate());
            assertEquals("7", ConnectionPool.collectFirst(
                    "SELECT current_setting('%s')::VARCHAR".formatted(DuckLakeInlining.SETTING), String.class),
                    "no watermark table is configured, so the rule has nothing here to protect");
        } finally {
            ConnectionPool.execute("SET GLOBAL %s = %s".formatted(DuckLakeInlining.SETTING, before));
        }
    }

    @Test
    void validatePassesWithNoTransformation() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs", "loglake", "main", "logs", null);

        assertDoesNotThrow(() -> providerFor(dbPath).validate());
    }

    @Test
    void validateFailsWhenTransformationDoesNotReferenceThis() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs", "loglake", "main", "logs",
                "SELECT * FROM wrong_table");

        var ex = assertThrows(IllegalArgumentException.class,
                () -> providerFor(dbPath).validate());
        assertTrue(ex.getMessage().contains("__this"));
        assertTrue(ex.getMessage().contains("logs"));
    }

    @Test
    void validateChecksAllQueues() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs",    "loglake", "main", "logs",    "SELECT * FROM __this");
        insertQueue(dbPath, "metrics", "loglake", "main", "metrics", "SELECT * FROM wrong_table");

        var ex = assertThrows(IllegalArgumentException.class,
                () -> providerFor(dbPath).validate());
        assertTrue(ex.getMessage().contains("metrics"));
    }
}
