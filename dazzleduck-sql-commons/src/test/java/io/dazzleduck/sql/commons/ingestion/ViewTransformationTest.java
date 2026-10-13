package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.util.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for view-based transformation derivation:
 * - Transformations.rewriteTableAsThis()
 * - DuckLakeIngestionHandler.resolveViewTransformation()
 * - DuckLakeIngestionTaskFactoryProvider validation rules
 */
class ViewTransformationTest {

    @TempDir
    Path tempDir;

    private static final String CATALOG = "view_test_lake";

    @BeforeEach
    void setupDuckLake() throws Exception {
        ConnectionPool.executeBatch(new String[]{
                "INSTALL ducklake",
                "LOAD ducklake",
                "INSTALL arrow FROM community",
                "LOAD arrow"
        });

        Path dataPath = tempDir.resolve("data");
        Files.createDirectories(dataPath);

        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')"
                            .formatted(tempDir.resolve("catalog"), CATALOG, dataPath),
                    "CREATE TABLE %s.main.logs (ts TIMESTAMP, level VARCHAR, msg VARCHAR)".formatted(CATALOG),
                    "CREATE VIEW  %s.main.v_logs AS SELECT ts, upper(level) AS level, msg FROM %s.main.logs"
                            .formatted(CATALOG, CATALOG)
            });
        }
    }

    @AfterEach
    void detach() {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        } catch (Exception ignored) {}
    }

    // -------------------------------------------------------------------------
    // Transformations.rewriteTableAsThis
    // -------------------------------------------------------------------------

    @Test
    void rewrite_simpleFullyQualified_replacesTableAndClearsQualifiers() throws Exception {
        String sql    = "SELECT ts, upper(level) AS level, msg FROM %s.main.logs".formatted(CATALOG);
        String result = Transformations.rewriteTableAsThis(sql, CATALOG, "main", "logs");
        // __this reference should have no catalog/schema qualifiers
        assertFalse(result.contains(CATALOG), "catalog qualifier should be removed");
        assertTrue(result.contains("__this"), "result must reference __this");
    }

    @Test
    void rewrite_preservesAlias() throws Exception {
        String sql    = "SELECT * FROM %s.main.logs AS raw".formatted(CATALOG);
        String result = Transformations.rewriteTableAsThis(sql, CATALOG, "main", "logs");
        assertTrue(result.contains("__this"), "should contain __this");
        assertTrue(result.contains("raw"), "alias 'raw' should be preserved");
    }

    @Test
    void rewrite_tableNotFound_throws() {
        String sql = "SELECT * FROM %s.main.logs".formatted(CATALOG);
        var ex = assertThrows(IllegalArgumentException.class,
                () -> Transformations.rewriteTableAsThis(sql, CATALOG, "main", "no_such_table"));
        assertTrue(ex.getMessage().contains("not found"), ex.getMessage());
    }

    @Test
    void rewrite_multipleOccurrences_throws() throws Exception {
        String sql = "SELECT * FROM %1$s.main.logs a JOIN %1$s.main.logs b ON a.ts = b.ts".formatted(CATALOG);
        var ex = assertThrows(IllegalArgumentException.class,
                () -> Transformations.rewriteTableAsThis(sql, CATALOG, "main", "logs"));
        assertTrue(ex.getMessage().contains("multiple") || ex.getMessage().contains("times"), ex.getMessage());
    }

    @Test
    void rewrite_differentCatalogNotMatched() throws Exception {
        String sql    = "SELECT * FROM %s.main.logs".formatted(CATALOG);
        // wrong catalog — should not find a match
        var ex = assertThrows(IllegalArgumentException.class,
                () -> Transformations.rewriteTableAsThis(sql, "other_catalog", "main", "logs"));
        assertTrue(ex.getMessage().contains("not found"), ex.getMessage());
    }

    // -------------------------------------------------------------------------
    // DuckLakeIngestionHandler.resolveViewTransformation
    // -------------------------------------------------------------------------

    @Test
    void resolveView_derivesCorrectTransformation() {
        // View body: SELECT ts, upper(level) AS level, msg FROM CATALOG.main.logs
        // input_table = CATALOG.main.logs  →  should become __this
        String fqView       = CATALOG + ".main.v_logs";
        String fqInputTable = CATALOG + ".main.logs";

        String transformation = DuckLakeIngestionHandler.resolveViewTransformationStatic(fqView, fqInputTable);

        assertNotNull(transformation);
        assertTrue(transformation.contains("__this"), "derived transformation must reference __this");
        assertFalse(transformation.contains(CATALOG + ".main.logs"),
                "original qualified reference should be replaced");
        assertTrue(transformation.contains("upper"), "view logic (upper) must be preserved");
    }

    @Test
    void resolveView_viewWithCte_derivesValidTransformation() throws Exception {
        // input_table is referenced only inside the view's CTE body
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn,
                    "CREATE VIEW %1$s.main.v_logs_cte AS WITH lv AS (SELECT ts, upper(level) AS level, msg FROM %1$s.main.logs) "
                            .formatted(CATALOG)
                            + "SELECT * FROM lv WHERE level <> 'DEBUG'");
        }

        String transformation = DuckLakeIngestionHandler.resolveViewTransformationStatic(
                CATALOG + ".main.v_logs_cte", CATALOG + ".main.logs");

        assertTrue(transformation.contains("__this"), "derived transformation must reference __this");
        assertFalse(transformation.contains(CATALOG + ".main.logs"),
                "original qualified reference should be replaced");
        assertDoesNotThrow(() -> AbstractIngestionTaskFactoryProvider.validateTransformation("q", transformation));
        // The derived transformation runs over a __this relation once wrapped
        try (Connection conn = ConnectionPool.getConnection()) {
            assertEquals(1L, ConnectionPool.collectFirst(conn,
                    "SELECT count(*) FROM (%s)".formatted(ParquetIngestionQueue.applyTransformation(
                            "SELECT TIMESTAMP '2026-01-01' AS ts, 'info' AS level, 'm' AS msg "
                                    + "UNION ALL SELECT TIMESTAMP '2026-01-01', 'debug', 'm2'", transformation)),
                    Long.class));
        }
    }

    @Test
    void resolveView_viewDoesNotExist_throws() {
        var ex = assertThrows(RuntimeException.class,
                () -> DuckLakeIngestionHandler.resolveViewTransformationStatic(
                        CATALOG + ".main.nonexistent_view", CATALOG + ".main.logs"));
        assertTrue(ex.getMessage().contains("not found") || ex.getMessage().contains("nonexistent_view"),
                ex.getMessage());
    }

    @Test
    void resolveView_inputTableNotInView_throws() {
        var ex = assertThrows(RuntimeException.class,
                () -> DuckLakeIngestionHandler.resolveViewTransformationStatic(
                        CATALOG + ".main.v_logs", CATALOG + ".main.other_table"));
        assertTrue(ex.getMessage().contains("not found"), ex.getMessage());
    }

    @Test
    void resolveView_badlyQualifiedName_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> DuckLakeIngestionHandler.resolveViewTransformationStatic(
                        "v_logs",          // not FQ — only 1 segment
                        CATALOG + ".main.logs"));
        assertThrows(IllegalArgumentException.class,
                () -> DuckLakeIngestionHandler.resolveViewTransformationStatic(
                        CATALOG + ".main.v_logs",
                        "logs"));           // not FQ
    }

    // -------------------------------------------------------------------------
    // fetchSchemaChangeId — DDL-only change detection
    // -------------------------------------------------------------------------

    @Test
    void schemaChangeId_unchangedAfterDataInsert() throws Exception {
        long idBefore = DuckLakeIngestionHandler.fetchSchemaChangeId(CATALOG, "main", "logs");
        assertTrue(idBefore > 0, "schema change id must be positive after table creation");

        ConnectionPool.execute("INSERT INTO %s.main.logs VALUES (NOW(), 'INFO', 'hello')".formatted(CATALOG));

        long idAfter = DuckLakeIngestionHandler.fetchSchemaChangeId(CATALOG, "main", "logs");
        assertEquals(idBefore, idAfter, "schema change id must not change after data insert");
    }

    @Test
    void schemaChangeId_incrementsAfterColumnAdd() throws Exception {
        long idBefore = DuckLakeIngestionHandler.fetchSchemaChangeId(CATALOG, "main", "logs");

        ConnectionPool.execute("ALTER TABLE %s.main.logs ADD COLUMN extra VARCHAR".formatted(CATALOG));

        long idAfter = DuckLakeIngestionHandler.fetchSchemaChangeId(CATALOG, "main", "logs");
        assertTrue(idAfter > idBefore,
                "schema change id must increase after ALTER TABLE ADD COLUMN (before=%d, after=%d)"
                        .formatted(idBefore, idAfter));
    }

    // -------------------------------------------------------------------------
    // View refresh — transformation updated when view definition changes
    // -------------------------------------------------------------------------

    /**
     * Verifies the full refresh cycle for view-based transformations:
     * 1. Handler resolves transformation from the original view.
     * 2. View is replaced with a different SQL body.
     * 3. Clock advances past the refresh interval → getOrCreateQueue() triggers refreshIfStale().
     * 4. getTransformation() returns the updated SQL.
     *
     * Also verifies the schema_version short-circuit: a data INSERT between step 1 and 2
     * does NOT trigger a spurious full refresh.
     */
    @Test
    void refresh_updatesTransformation_whenViewDefinitionChanges() throws Exception {
        var mapping = new QueueIdToTableMapping("q", CATALOG, "main", "logs",
                Map.of(), null,
                CATALOG + ".main.v_logs",   // view
                CATALOG + ".main.logs");     // input_table
        var clock = new MutableClock(Instant.now(), ZoneId.of("UTC"));
        Duration refreshInterval = Duration.ofMinutes(1);
        var handler = new DuckLakeIngestionHandler(Map.of("q", mapping), refreshInterval, clock);

        // Initial transformation derived from v_logs (upper(level))
        String initial = handler.getTransformation("q");
        assertNotNull(initial);
        assertTrue(initial.contains("upper"), "initial transformation must use upper()");

        // A data insert must NOT change the cached transformation (schema_version unchanged)
        ConnectionPool.execute("INSERT INTO %s.main.logs VALUES (NOW(), 'INFO', 'msg')".formatted(CATALOG));
        assertEquals(initial, handler.getTransformation("q"),
                "data insert must not change cached transformation");

        // Replace the view — schema_version increments
        ConnectionPool.execute(
                "CREATE OR REPLACE VIEW %s.main.v_logs AS SELECT ts, lower(level) AS level, msg FROM %s.main.logs"
                        .formatted(CATALOG, CATALOG));

        Path outDir = tempDir.resolve("out");
        Files.createDirectories(outDir);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            IngestionHandler.QueueCreator creator = (id, _path) -> new ParquetIngestionQueue(
                    "test", "arrow", outDir.toString(), id,
                    1L, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                    Duration.ofSeconds(10), handler, scheduler, clock);
            IngestionHandler.QueueEventListener noOp = new IngestionHandler.QueueEventListener() {
                @Override public void onCreated(String id) {}
            };

            // First call — creates queue; state was already stamped at construction time
            handler.getOrCreateQueue("q", creator, noOp);

            // Advance past refresh interval so the cached state becomes stale
            clock.advanceBy(refreshInterval.plusSeconds(1));

            // Second call — getTargetPath() triggers refreshIfStale(); schema_version changed → full rebuild
            handler.getOrCreateQueue("q", creator, noOp);

            String updated = handler.getTransformation("q");
            assertNotNull(updated);
            assertFalse(updated.contains("upper"),
                    "updated transformation must NOT use upper() after view change");
            assertTrue(updated.contains("lower"),
                    "updated transformation must use lower() after view change");
            assertTrue(updated.contains("__this"),
                    "updated transformation must still reference __this");
        } finally {
            scheduler.shutdown();
            handler.closeQueues();
        }
    }

    /**
     * Verifies the schema_version short-circuit: when schema_version has NOT changed
     * (data-only writes), refreshState() bumps refreshedAt but does NOT re-fetch path,
     * transformation, or partitions from DuckLake.
     */
    @Test
    void refresh_skipsFullRebuild_whenSchemaVersionUnchanged() throws Exception {
        var mapping = new QueueIdToTableMapping("q", CATALOG, "main", "logs",
                Map.of(), null,
                CATALOG + ".main.v_logs",
                CATALOG + ".main.logs");
        var clock = new MutableClock(Instant.now(), ZoneId.of("UTC"));
        Duration refreshInterval = Duration.ofMinutes(1);
        var handler = new DuckLakeIngestionHandler(Map.of("q", mapping), refreshInterval, clock);

        String before = handler.getTransformation("q");
        assertNotNull(before);

        // Only data inserts — schema_version stays the same
        ConnectionPool.execute("INSERT INTO %s.main.logs VALUES (NOW(), 'DEBUG', 'x')".formatted(CATALOG));
        ConnectionPool.execute("INSERT INTO %s.main.logs VALUES (NOW(), 'WARN',  'y')".formatted(CATALOG));

        Path outDir = tempDir.resolve("out2");
        Files.createDirectories(outDir);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            IngestionHandler.QueueCreator creator = (id, _path) -> new ParquetIngestionQueue(
                    "test", "arrow", outDir.toString(), id,
                    1L, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                    Duration.ofSeconds(10), handler, scheduler, clock);
            IngestionHandler.QueueEventListener noOp = new IngestionHandler.QueueEventListener() {
                @Override public void onCreated(String id) {}
            };

            handler.getOrCreateQueue("q", creator, noOp);
            clock.advanceBy(refreshInterval.plusSeconds(1));
            handler.getOrCreateQueue("q", creator, noOp);

            // Transformation must be identical — no view re-fetch occurred
            assertEquals(before, handler.getTransformation("q"),
                    "transformation must be unchanged after data-only writes");
        } finally {
            scheduler.shutdown();
            handler.closeQueues();
        }
    }

    // -------------------------------------------------------------------------
    // QueueIdToTableMapping validation rules
    // -------------------------------------------------------------------------

    @Test
    void mapping_viewAndInputTable_hasViewTransformation() {
        var m = new QueueIdToTableMapping("q", CATALOG, "main", "logs",
                Map.of(), null, CATALOG + ".main.v_logs", CATALOG + ".main.logs");
        assertTrue(m.hasViewTransformation());
        assertNull(m.transformation());
    }

    @Test
    void mapping_sixArgConstructor_noViewTransformation() {
        var m = new QueueIdToTableMapping("q", CATALOG, "main", "logs",
                Map.of(), "SELECT * FROM __this");
        assertFalse(m.hasViewTransformation());
        assertNull(m.view());
        assertNull(m.inputTable());
    }

    // -------------------------------------------------------------------------
    // DuckLakeIngestionTaskFactoryProvider validation
    // -------------------------------------------------------------------------

    @Test
    void provider_viewWithoutInputTable_failsValidation() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> DuckLakeIngestionTaskFactoryProvider.validateViewTransformation(
                        "test_queue",
                        CATALOG + ".main.v_logs",
                        CATALOG + ".main.nonexistent")); // input_table not in view
        assertNotNull(ex.getMessage());
    }

    @Test
    void provider_validViewAndInputTable_passesValidation() {
        // Should not throw — view exists and input_table appears exactly once
        assertDoesNotThrow(() ->
                DuckLakeIngestionTaskFactoryProvider.validateViewTransformation(
                        "test_queue",
                        CATALOG + ".main.v_logs",
                        CATALOG + ".main.logs"));
    }
}
