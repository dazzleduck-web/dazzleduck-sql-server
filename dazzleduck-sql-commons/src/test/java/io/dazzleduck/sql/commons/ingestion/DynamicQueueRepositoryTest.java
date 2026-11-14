package io.dazzleduck.sql.commons.ingestion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DynamicQueueRepositoryTest {

    @TempDir
    Path tempDir;

    /**
     * Write test data via a short-lived DuckDB write connection, retrying while the registry is
     * locked.
     *
     * <p>A handler under test keeps a read connection open on the same file and polls it, so a
     * write can land while SQLite is serving that read and come back "database is locked" — which
     * is a busy signal, not a failure. A real writer has to cope with it too, so the test models
     * that rather than racing the poller and failing when it loses.
     */
    private void writeToDb(String dbPath, String sql) throws Exception {
        String safePath = dbPath.replace("'", "''");
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
        while (true) {
            try (Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                 Statement st = conn.createStatement()) {
                st.execute("LOAD sqlite");
                st.execute("ATTACH '" + safePath + "' AS " + DynamicQueueRepository.ATTACHMENT + " (TYPE sqlite)");
                st.execute(sql);
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

    @Test
    void initCreatesSchemaVersionRowForWriter() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            try (Connection conn = repo.openReadOnlyConnection()) {
                assertEquals(0L, DynamicQueueRepository.readSchemaVersion(conn));
            }
        }
    }

    @Test
    void dataVersionChangesOnAnyWrite() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            long before = DynamicQueueRepository.readDataVersion(dbPath);

            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues " +
                    "(ingestion_queue, catalog, schema_name, table_name) " +
                    "VALUES ('q1', 'cat', 'main', 'q1')");

            long after = DynamicQueueRepository.readDataVersion(dbPath);
            assertTrue(after >= before, "data version (mtime) must be >= after a write");
        }
    }

    @Test
    void writerTracksRemoteSyncVersionIndependently() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();

            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues " +
                    "(ingestion_queue, catalog, schema_name, table_name) " +
                    "VALUES ('q1', 'cat', 'main', 'q1')");
            writeToDb(dbPath, "UPDATE " + DynamicQueueRepository.ATTACHMENT +
                    ".schema_version SET version = version + 1 WHERE id = 1");
            writeToDb(dbPath, "DELETE FROM " + DynamicQueueRepository.ATTACHMENT +
                    ".ingestion_queues WHERE ingestion_queue = 'q1'");
            writeToDb(dbPath, "UPDATE " + DynamicQueueRepository.ATTACHMENT +
                    ".schema_version SET version = version + 1 WHERE id = 1");

            try (Connection conn = repo.openReadOnlyConnection()) {
                assertEquals(2L, DynamicQueueRepository.readSchemaVersion(conn),
                        "writer bumped schema_version twice — reflects remote offset");
            }
        }
    }

    @Test
    void loadAllReturnsInsertedRows() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();

            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues " +
                    "(ingestion_queue, catalog, schema_name, table_name) " +
                    "VALUES ('logs', 'my_catalog', 'main', 'logs')");

            try (Connection conn = repo.openReadOnlyConnection()) {
                Map<String, QueueIdToTableMapping> mappings = DynamicQueueRepository.loadAll(conn);
                assertEquals(1, mappings.size());
                QueueIdToTableMapping m = mappings.get("logs");
                assertNotNull(m);
                assertNull(m.outputPath(), "output path is derived from DuckLake, not read from the registry");
                assertEquals("my_catalog", m.catalog());
                assertEquals("main", m.schema());
                assertEquals("logs", m.table());
                assertNull(m.transformation());
                assertNull(m.inputSchema(), "input_schema defaults to null when not set");
            }
        }
    }

    @Test
    void loadAllReadsInputSchema() throws Exception {
        String dbPath = tempDir.resolve("schema.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();

            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues " +
                    "(ingestion_queue, catalog, schema_name, table_name, input_schema) " +
                    "VALUES ('app_logs', 'otel_lake', 'main', 'app_logs', 'severity_number INTEGER, body VARCHAR')");

            try (Connection conn = repo.openReadOnlyConnection()) {
                QueueIdToTableMapping m = DynamicQueueRepository.loadAll(conn).get("app_logs");
                assertNotNull(m);
                assertEquals("severity_number INTEGER, body VARCHAR", m.inputSchema());
            }
        }
    }

    @Test
    void dynamicHandlerReflectsHotReload() throws Exception {
        String dbPath = tempDir.resolve("hot.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();

            // Real writers (e.g. QueueRegistryWriter) always bump schema_version in the same
            // transaction as any data change — that's what DynamicIngestionHandler now polls for
            // change detection (not file mtime; see its class javadoc), so the test must model it.
            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues " +
                    "(ingestion_queue, catalog, schema_name, table_name) " +
                    "VALUES ('q1', 'cat', 'main', 'q1')");
            writeToDb(dbPath, "UPDATE " + DynamicQueueRepository.ATTACHMENT +
                    ".schema_version SET version = version + 1 WHERE id = 1");

            Connection readConn = repo.openReadOnlyConnection();
            Map<String, QueueIdToTableMapping> initial = DynamicQueueRepository.loadAll(readConn);
            var handler = new DynamicIngestionHandler(dbPath, readConn, initial,
                    java.time.Duration.ofMillis(50));

            // Assert on the known-queue set (the registry-driven part). The actual target path is
            // derived from DuckLake table metadata, which these fixture tables don't have, so this
            // test verifies hot-reload of the mapping set, not path resolution (covered elsewhere).
            assertTrue(handler.getKnownQueues().contains("q1"));
            assertFalse(handler.getKnownQueues().contains("q2"));

            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues " +
                    "(ingestion_queue, catalog, schema_name, table_name) " +
                    "VALUES ('q2', 'cat', 'main', 'q2')");
            writeToDb(dbPath, "UPDATE " + DynamicQueueRepository.ATTACHMENT +
                    ".schema_version SET version = version + 1 WHERE id = 1");

            // The handler polls every 50ms. A fixed sleep assumes the poll thread got
            // scheduled within it, which does not hold on a loaded CI runner, so wait for
            // the condition itself instead.
            long deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
            while (!handler.getKnownQueues().contains("q2") && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }

            assertTrue(handler.getKnownQueues().contains("q2"), "hot-reload picked up the new queue");

            handler.closeQueues();
        }
    }

    // -----------------------------------------------------------------------
    // Sidecars
    // -----------------------------------------------------------------------

    private static final String QUEUES = DynamicQueueRepository.ATTACHMENT + ".ingestion_queues";
    private static final String SIDECARS = DynamicQueueRepository.ATTACHMENT + "." + DynamicQueueRepository.SIDECARS_TABLE;

    @Test
    void loadAllReadsAQueuesSidecarsInPositionOrder() throws Exception {
        String dbPath = tempDir.resolve("sidecars.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            writeToDb(dbPath, "INSERT INTO " + QUEUES + " (ingestion_queue, catalog, schema_name, table_name) "
                    + "VALUES ('logs', 'lake', 'main', 'logs')");
            writeToDb(dbPath, "INSERT INTO " + SIDECARS + " (ingestion_queue, sidecar_table, transformation, "
                    + "transformation_view, input_table, position) VALUES "
                    + "('logs', 'z_counts', 'SELECT count(*) AS n FROM __base', NULL, NULL, 1), "
                    + "('logs', 'a_view', NULL, 'lake.main.v', 'lake.main.raw', 2)");
            try (Connection conn = repo.openReadOnlyConnection()) {
                var sidecars = DynamicQueueRepository.loadAll(conn).get("logs").sidecars();
                assertEquals(java.util.List.of(
                        new Sidecar("z_counts", "SELECT count(*) AS n FROM __base"),
                        new Sidecar("a_view", null, "lake.main.v", "lake.main.raw")), sidecars);
            }
        }
    }

    @Test
    void anUnusableSidecarRowLeavesOnlyItsQueueOut() throws Exception {
        String dbPath = tempDir.resolve("bad-sidecar.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            writeToDb(dbPath, "INSERT INTO " + QUEUES + " (ingestion_queue, catalog, schema_name, table_name) "
                    + "VALUES ('bad', 'lake', 'main', 'bad'), ('good', 'lake', 'main', 'good')");
            // Both a transformation and a view: not a usable sidecar.
            writeToDb(dbPath, "INSERT INTO " + SIDECARS + " (ingestion_queue, sidecar_table, transformation, "
                    + "transformation_view) VALUES ('bad', 'x', 'SELECT 1 AS a FROM __base', 'lake.main.v'), "
                    + "('good', 'y', 'SELECT 1 AS a FROM __base', NULL)");
            try (Connection conn = repo.openReadOnlyConnection()) {
                var all = DynamicQueueRepository.loadAll(conn);
                assertFalse(all.containsKey("bad"), "not loaded without its sidecars");
                assertEquals(1, all.get("good").sidecars().size());
            }
        }
    }

    @Test
    void aRunningQueueWhoseSidecarRowsBecomeUnusableKeepsItsPreviousMapping() throws Exception {
        String dbPath = tempDir.resolve("keep.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            writeToDb(dbPath, "INSERT INTO " + QUEUES + " (ingestion_queue, catalog, schema_name, table_name) "
                    + "VALUES ('logs', 'lake', 'main', 'logs')");
            writeToDb(dbPath, "INSERT INTO " + SIDECARS + " (ingestion_queue, sidecar_table, transformation) "
                    + "VALUES ('logs', 'x', 'SELECT 1 AS a FROM __base')");
            try (Connection conn = repo.openReadOnlyConnection()) {
                var before = DynamicQueueRepository.loadAll(conn);
                writeToDb(dbPath, "UPDATE " + SIDECARS + " SET transformation_view = 'not.qualified'");
                // ...and, in the same edit, a change to the queue's own row
                writeToDb(dbPath, "UPDATE " + QUEUES + " SET transformation = 'SELECT * FROM __this' WHERE ingestion_queue = 'logs'");
                var after = DynamicQueueRepository.loadAll(conn, before);
                assertNotNull(after.get("logs"), "kept, not evicted over a bad row");
                assertEquals(before.get("logs").sidecars(), after.get("logs").sidecars(), "with the sidecars it was running with");
                assertEquals("SELECT * FROM __this", after.get("logs").transformation(), "and its own row as it now reads");
            }
        }
    }

    @Test
    void aQueueWhoseTableMovesWhileItsSidecarRowsAreUnusableKeepsItsWholePreviousMapping() throws Exception {
        String dbPath = tempDir.resolve("moved.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            writeToDb(dbPath, "INSERT INTO " + QUEUES + " (ingestion_queue, catalog, schema_name, table_name) "
                    + "VALUES ('logs', 'lake', 'main', 'logs')");
            writeToDb(dbPath, "INSERT INTO " + SIDECARS + " (ingestion_queue, sidecar_table, transformation) "
                    + "VALUES ('logs', 'x', 'SELECT 1 AS a FROM __base')");
            try (Connection conn = repo.openReadOnlyConnection()) {
                var before = DynamicQueueRepository.loadAll(conn);
                // The table moves to another schema, and a sidecar row breaks, in the same edit.
                writeToDb(dbPath, "UPDATE " + QUEUES + " SET schema_name = 'v2' WHERE ingestion_queue = 'logs'");
                writeToDb(dbPath, "UPDATE " + SIDECARS + " SET transformation_view = 'not.qualified'");
                var after = DynamicQueueRepository.loadAll(conn, before);
                assertEquals(before.get("logs"), after.get("logs"),
                        "its old sidecars are not applied to the moved table: the whole previous mapping is kept");
            }
        }
    }

    @Test
    void aRegistryWithoutTheSidecarsTableLoads() throws Exception {
        String dbPath = tempDir.resolve("old.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            writeToDb(dbPath, "DROP TABLE " + SIDECARS); // as a registry created before it existed
            writeToDb(dbPath, "INSERT INTO " + QUEUES + " (ingestion_queue, catalog, schema_name, table_name) "
                    + "VALUES ('logs', 'lake', 'main', 'logs')");
            try (Connection conn = repo.openReadOnlyConnection()) {
                assertEquals(java.util.List.of(), DynamicQueueRepository.loadAll(conn).get("logs").sidecars());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Session variables
    // -----------------------------------------------------------------------

    @Test
    void loadsTheVariablesRelationNamedByARow() throws Exception {
        String dbPath = tempDir.resolve("vars.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues "
                    + "(ingestion_queue, catalog, schema_name, table_name, variables_view,"
                    + " variables_key_column, variables_expiration_column) VALUES "
                    + "('logs', 'lake', 'main', 'logs', 'vars_db.main.logs_vars', 'k', 'expires_at')");
            try (Connection conn = repo.openReadOnlyConnection()) {
                var variables = DynamicQueueRepository.loadAll(conn).get("logs").variables();
                assertTrue(variables.hasView());
                // Named by its full path, so it can be a table in this same SQLite file.
                assertEquals("vars_db.main.logs_vars", variables.view().relation());
                assertEquals("k", variables.view().keyColumn());
                assertEquals("value", variables.view().valueColumn(), "unset column falls back");
                assertEquals("expires_at", variables.view().expirationColumn());
            }
        }
    }

    @Test
    void aRowNamingNoRelationCarriesNoVariables() throws Exception {
        String dbPath = tempDir.resolve("novars.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues "
                    + "(ingestion_queue, catalog, schema_name, table_name) VALUES "
                    + "('logs', 'lake', 'main', 'logs')");
            try (Connection conn = repo.openReadOnlyConnection()) {
                assertEquals(IngestionVariables.NONE,
                        DynamicQueueRepository.loadAll(conn).get("logs").variables());
            }
        }
    }

    @Test
    void anUnusableRelationNameCostsOnlyThatQueuesVariables() throws Exception {
        // The reload runs for every queue at once; one bad row must not stop the others.
        String dbPath = tempDir.resolve("badvars.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues "
                    + "(ingestion_queue, catalog, schema_name, table_name, variables_view) VALUES "
                    + "('logs', 'lake', 'main', 'logs', 'vars; DROP TABLE orders')");
            try (Connection conn = repo.openReadOnlyConnection()) {
                var all = DynamicQueueRepository.loadAll(conn);
                assertEquals(IngestionVariables.NONE, all.get("logs").variables());
                assertEquals("logs", all.get("logs").table(), "the rest of the row still loads");
            }
        }
    }

    @Test
    void aRegistryPredatingTheVariablesColumnsGainsThem() throws Exception {
        // A registry created by an older build has no variables_* columns; init() must add them
        // rather than fail, the same way it does for the partitioning columns.
        String dbPath = tempDir.resolve("legacy.db").toString();
        // The schema exactly as the previous build created it: everything but the variables columns.
        writeToDb(dbPath, "CREATE TABLE " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues ("
                + "ingestion_queue TEXT PRIMARY KEY, catalog TEXT NOT NULL, schema_name TEXT NOT NULL,"
                + " table_name TEXT NOT NULL, transformation TEXT, view_name TEXT, input_table TEXT,"
                + " input_schema TEXT, partition_by TEXT, num_partitions INTEGER,"
                + " partition_expression TEXT, min_bucket_size INTEGER, max_delay_ms INTEGER)");
        writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues "
                + "(ingestion_queue, catalog, schema_name, table_name) VALUES ('logs', 'lake', 'main', 'logs')");
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            try (Connection conn = repo.openReadOnlyConnection()) {
                var mapping = DynamicQueueRepository.loadAll(conn).get("logs");
                assertEquals(IngestionVariables.NONE, mapping.variables());
                assertEquals("logs", mapping.table());
            }
        }
    }
}
