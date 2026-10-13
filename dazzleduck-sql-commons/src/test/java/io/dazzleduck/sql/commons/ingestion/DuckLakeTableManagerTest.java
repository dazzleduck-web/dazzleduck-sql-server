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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DuckLakeTableManagerTest {

    @TempDir
    Path tempDir;

    static final String CATALOG = "table_mgr_lake";
    static final String SCHEMA = "main";

    /** Minimal logs-like input column spec (DuckDB column-definition fragment). */
    static final String LOGS = "severity_number INTEGER, severity_text VARCHAR, body VARCHAR";

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(tempDir.resolve("data"));
        ConnectionPool.execute("INSTALL arrow FROM community");
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "LOAD arrow",
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), CATALOG, tempDir.resolve("data"))
            });
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    private QueueIdToTableMapping mapping(String table, String transformation, String inputSchema) {
        return new QueueIdToTableMapping(table, CATALOG, SCHEMA, table, Map.of(), transformation)
                .withInputSchema(inputSchema);
    }

    private List<String> columns(String table) throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            List<String> cols = new ArrayList<>();
            ConnectionPool.collectAll(conn,
                    "SELECT column_name FROM information_schema.columns WHERE table_catalog = '%s' "
                            .formatted(CATALOG)
                            + "AND table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position"
                            .formatted(SCHEMA, table),
                    rs -> rs.getString("column_name")).forEach(cols::add);
            return cols;
        }
    }

    @Test
    void createsTableWithTransformationColumns() throws Exception {
        DuckLakeTableManager.ensureTable(
                mapping("app_logs", "SELECT *, severity_text AS level FROM __this", LOGS));

        assertEquals(List.of("severity_number", "severity_text", "body", "level"), columns("app_logs"));
    }

    @Test
    void createsTableWithCteTransformationColumns() throws Exception {
        DuckLakeTableManager.ensureTable(mapping("cte_logs",
                "WITH t AS (SELECT *, severity_text AS level FROM __this) SELECT * FROM t", LOGS));

        assertEquals(List.of("severity_number", "severity_text", "body", "level"), columns("cte_logs"));
    }

    @Test
    void createsTableWhenTransformationEndsWithSemicolonOrComment() throws Exception {
        DuckLakeTableManager.ensureTable(
                mapping("semi_logs", "SELECT *, severity_text AS level FROM __this;", LOGS));
        DuckLakeTableManager.ensureTable(
                mapping("comment_logs", "SELECT *, severity_text AS level FROM __this -- add level", LOGS));

        assertEquals(List.of("severity_number", "severity_text", "body", "level"), columns("semi_logs"));
        assertEquals(List.of("severity_number", "severity_text", "body", "level"), columns("comment_logs"));
    }

    @Test
    void createsTableWithRawSchemaWhenNoTransformation() throws Exception {
        DuckLakeTableManager.ensureTable(mapping("raw_logs", null, LOGS));

        assertEquals(List.of("severity_number", "severity_text", "body"), columns("raw_logs"));
    }

    @Test
    void supportsComplexInputColumnTypes() throws Exception {
        // MAP / TIMESTAMP input columns must survive derivation and table creation.
        String schema = "ts TIMESTAMP, body VARCHAR, attributes MAP(VARCHAR, VARCHAR)";
        DuckLakeTableManager.ensureTable(
                mapping("complex", "SELECT *, upper(body) AS body_upper FROM __this", schema));

        assertEquals(List.of("ts", "body", "attributes", "body_upper"), columns("complex"));
    }

    @Test
    void altersTableAddingAndDroppingColumnsOnDiff() throws Exception {
        // Start: SELECT * + level  ->  severity_number, severity_text, body, level
        DuckLakeTableManager.ensureTable(
                mapping("evolving", "SELECT *, severity_text AS level FROM __this", LOGS));
        assertEquals(List.of("severity_number", "severity_text", "body", "level"), columns("evolving"));

        // Change: drop body & severity_text from projection, keep severity_number + level, add msg
        DuckLakeTableManager.ensureTable(
                mapping("evolving", "SELECT severity_number, severity_text AS level, body AS msg FROM __this", LOGS));

        // severity_text & body dropped; msg added (appended); severity_number & level retained in place.
        assertEquals(List.of("severity_number", "level", "msg"), columns("evolving"));
    }

    @Test
    void evolvesTableWhenInputSchemaChanges() throws Exception {
        // No transformation: the table mirrors input_schema (SELECT * over the raw columns).
        DuckLakeTableManager.ensureTable(
                mapping("schema_evolving", null, "severity_number INTEGER, body VARCHAR"));
        assertEquals(List.of("severity_number", "body"), columns("schema_evolving"));

        // input_schema gains 'trace_id' → the table gains the column on next reconcile.
        DuckLakeTableManager.ensureTable(
                mapping("schema_evolving", null, "severity_number INTEGER, body VARCHAR, trace_id VARCHAR"));
        assertEquals(List.of("severity_number", "body", "trace_id"), columns("schema_evolving"));

        // input_schema drops 'body' → the column is removed from the table.
        DuckLakeTableManager.ensureTable(
                mapping("schema_evolving", null, "severity_number INTEGER, trace_id VARCHAR"));
        assertEquals(List.of("severity_number", "trace_id"), columns("schema_evolving"));
    }

    @Test
    void reReconcileWithSameTransformationIsNoOp() throws Exception {
        var m = mapping("stable", "SELECT *, severity_text AS level FROM __this", LOGS);
        DuckLakeTableManager.ensureTable(m);
        List<String> first = columns("stable");
        DuckLakeTableManager.ensureTable(m);
        assertEquals(first, columns("stable"));
    }

    @Test
    void nullInputSchemaSkipsManagement() throws Exception {
        DuckLakeTableManager.ensureTable(mapping("never_created", null, null));
        assertTrue(columns("never_created").isEmpty(), "no table should be created without an input schema");
    }
}
