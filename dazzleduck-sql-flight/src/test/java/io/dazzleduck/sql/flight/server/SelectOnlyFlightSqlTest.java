package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.authorization.SelectOnlyAuthorizer;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for READ_ONLY mode.
 * Validates that only SELECT queries are allowed and all DML/DDL operations are blocked.
 */
public class SelectOnlyFlightSqlTest {

    private static final String TEST_USER = "admin";
    private static final String TEST_CATALOG = "memory";
    private static final String TEST_SCHEMA = "main";
    private static ServerClient serverClient;
    private static Location serverLocation;

    @BeforeAll
    public static void setup() throws Exception {
        // Create test data directory
        var testDataPath = Files.createTempDirectory("readonly_test_").toString();

        // Create test parquet file
        Path testFile = Path.of(testDataPath, "data.parquet");
        ConnectionPool.execute("COPY (SELECT 1 as id, 'test' as name) TO '%s' (FORMAT parquet)".formatted(testFile.toString()));

        // Setup READ_ONLY server
        serverLocation = FlightTestUtils.findNextLocation();
        var flightTestUtils = FlightTestUtils.createForDatabaseSchema(TEST_USER, "password", TEST_CATALOG, TEST_SCHEMA);
        serverClient = flightTestUtils.createReadOnlyServerClient(serverLocation);

        ConnectionPool.executeBatch(new String[]{
                "CREATE TABLE t_select_only AS SELECT * FROM range(100)"
        });
    }

    @AfterAll
    public static void cleanup() throws Exception {
        if (serverClient != null) {
            serverClient.close();
        }
        ConnectionPool.execute("DROP TABLE IF EXISTS t_select_only");
    }

    // ===== SELECT Operations (Should be ALLOWED) =====

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testLimitHeader() throws Exception {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(Headers.HEADER_DATA_LIMIT, "5");
        HeaderCallOption opt = new HeaderCallOption(headers);

        FlightInfo info = serverClient.flightSqlClient().execute("SELECT * FROM t_select_only", opt);
        try (FlightStream stream = serverClient.flightSqlClient().getStream(info.getEndpoints().get(0).getTicket())) {
            int count = 0;
            while (stream.next()) {
                count += stream.getRoot().getRowCount();
            }
            assertEquals(5, count);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testLimitAndOffsetHeader() throws Exception {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(Headers.HEADER_DATA_LIMIT, "5");
        headers.insert(Headers.HEADER_DATA_OFFSET, "90");
        HeaderCallOption opt = new HeaderCallOption(headers);

        FlightInfo info = serverClient.flightSqlClient().execute("SELECT * FROM t_select_only", opt);
        try (FlightStream stream = serverClient.flightSqlClient().getStream(info.getEndpoints().get(0).getTicket())) {
            int count = 0;
            while (stream.next()) {
                count += stream.getRoot().getRowCount();
            }
            assertEquals(5, count);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectAllowed() throws Exception {
        // Basic SELECT should work
        String query = "SELECT 1";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithWhereAllowed() throws Exception {
        // SELECT with WHERE clause should work
        String query = "SELECT * FROM generate_series(10) AS t(value) WHERE value > 5";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithJoinAllowed() throws Exception {
        // SELECT with JOIN should work
        String query = "SELECT * FROM generate_series(5) AS a(value) JOIN generate_series(5) AS b(value) ON a.value = b.value";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithUnionAllowed() throws Exception {
        // SELECT with UNION should work
        String query = "SELECT 1 UNION ALL SELECT 2";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithCTEAllowed() throws Exception {
        // SELECT with CTE should work
        String query = "WITH cte AS (SELECT 1) SELECT * FROM cte";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithAggregateAllowed() throws Exception {
        // SELECT with aggregate functions should work
        String query = "SELECT COUNT(*) FROM generate_series(10) AS t(value)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithGroupByAllowed() throws Exception {
        // SELECT with GROUP BY should work
        String query = "SELECT value % 2, COUNT(*) FROM generate_series(10) AS t(value) GROUP BY (value % 2)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithWindowFunctionAllowed() throws Exception {
        // SELECT with window function should work
        String query = "SELECT value, ROW_NUMBER() OVER (ORDER BY value) as rn FROM generate_series(10) AS t(value)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    // ===== DML Operations (Should be BLOCKED) =====

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testExecuteUpdateBlocked() throws Exception {
        // executeUpdate goes through acceptPutStatement, not the query path the tests below use.
        assertThrows(FlightRuntimeException.class, () ->
                serverClient.flightSqlClient().executeUpdate("DROP TABLE t_select_only"));
        assertThrows(FlightRuntimeException.class, () ->
                serverClient.flightSqlClient().executeUpdate("DELETE FROM t_select_only"));
        assertEquals(100L, ConnectionPool.collectFirst("SELECT count(*) FROM t_select_only", Long.class),
                "the table is still there with its rows");
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testDatabaseHeaderCannotInjectStatements() throws Exception {
        // Before quoting, this header made the connection setup run
        // "USE memory.main; DELETE FROM t_select_only; USE memory.main" ahead of any authorization.
        var location = FlightTestUtils.findNextLocation();
        var utils = FlightTestUtils.createForDatabaseSchema(TEST_USER, "password", TEST_CATALOG, TEST_SCHEMA);
        try (var injected = utils.createReadOnlyServerClient(location, java.util.Map.of(
                Headers.HEADER_DATABASE, "memory.main; DELETE FROM t_select_only; USE memory"))) {
            assertThrows(FlightRuntimeException.class, () -> {
                FlightInfo info = injected.flightSqlClient().execute("SELECT 1");
                try (FlightStream stream = injected.flightSqlClient().getStream(info.getEndpoints().get(0).getTicket())) {
                    while (stream.next()) {
                        // drain
                    }
                }
            }, "the header names no catalog, so the query fails instead of running extra statements");
        }
        assertEquals(100L, ConnectionPool.collectFirst("SELECT count(*) FROM t_select_only", Long.class),
                "nothing was deleted");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testBulkIngestBlocked() throws Exception {
        // executeIngest writes to an ingestion queue without going through SQL, so the
        // SELECT-only check never sees it; it must be refused by the write-access check.
        var refused = assertThrows(FlightRuntimeException.class, () -> FlightTestUtils.bulkIngest(serverClient, "ro_queue"));
        assertTrue(refused.getMessage().contains("No write access"), "refused by authorization: " + refused.getMessage());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testInsertBlocked() {
        // INSERT should be blocked
        String query = "INSERT INTO test_table VALUES (1, 'test')";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testUpdateBlocked() {
        // UPDATE should be blocked
        String query = "UPDATE test_table SET name = 'updated' WHERE id = 1";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDeleteBlocked() {
        // DELETE should be blocked
        String query = "DELETE FROM test_table WHERE id = 1";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCreateTableBlocked() {
        // CREATE TABLE should be blocked
        String query = "CREATE TABLE new_table (id INT, name VARCHAR)";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDropTableBlocked() {
        // DROP TABLE should be blocked
        String query = "DROP TABLE IF EXISTS test_table";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testAlterTableBlocked() {
        // ALTER TABLE should be blocked
        String query = "ALTER TABLE test_table ADD COLUMN new_col VARCHAR";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testTruncateBlocked() {
        // TRUNCATE TABLE should be blocked
        String query = "TRUNCATE TABLE test_table";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCreateSchemaBlocked() {
        // CREATE SCHEMA should be blocked
        String query = "CREATE SCHEMA IF NOT EXISTS new_schema";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDropSchemaBlocked() {
        // DROP SCHEMA should be blocked
        String query = "DROP SCHEMA IF EXISTS new_schema";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCreateViewBlocked() {
        // CREATE VIEW should be blocked
        String query = "CREATE VIEW test_view AS SELECT 1";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDropViewBlocked() {
        // DROP VIEW should be blocked
        String query = "DROP VIEW IF EXISTS test_view";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testGrantBlocked() {
        // GRANT should be blocked
        String query = "GRANT SELECT ON test_table TO user";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testRevokeBlocked() {
        // REVOKE should be blocked
        String query = "REVOKE SELECT ON test_table FROM user";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCreateIndexBlocked() {
        // CREATE INDEX should be blocked
        String query = "CREATE INDEX idx_name ON test_table (column_name)";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDropIndexBlocked() {
        // DROP INDEX should be blocked
        String query = "DROP INDEX IF EXISTS idx_name ON test_table";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCopyFromBlocked() {
        // COPY FROM should be blocked (data import)
        String query = "COPY test_table FROM '/path/to/file.csv'";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testExportToBlocked() {
        // EXPORT TO should be blocked (data export)
        String query = "EXPORT DATABASE 'memory' TO '/path/to/export.sql'";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    // ===== Mixed Operations (Should be BLOCKED) =====

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testInsertSelectBlocked() {
        // INSERT INTO ... SELECT should be blocked
        String query = "INSERT INTO target_table SELECT * FROM source_table";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCreateTableAsSelectBlocked() {
        // CREATE TABLE AS SELECT should be blocked
        String query = "CREATE TABLE new_table AS SELECT * FROM source_table";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testUpdateWithSubqueryBlocked() {
        // UPDATE with subquery should be blocked
        String query = "UPDATE test_table SET col = (SELECT MAX(col) FROM test_table) WHERE id = 1";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDeleteWithSubqueryBlocked() {
        // DELETE with subquery should be blocked
        String query = "DELETE FROM test_table WHERE id IN (SELECT id FROM other_table)";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    // ===== Error Message Tests =====

    /**
     * Queries that DuckDB cannot parse to AST (e.g. INSERT, UPDATE, DELETE) must return
     * the user-friendly message "This query is not supported." rather than an internal
     * DuckDB error string.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testUnsupportedQueryErrorMessage() {
        // INSERT cannot be serialized by DuckDB's json_serialize_sql and triggers the
        // error-node path in SelectOnlyAuthorizer — the message must be user-friendly.
        FlightRuntimeException ex = assertThrows(FlightRuntimeException.class, () ->
                serverClient.flightSqlClient().execute("INSERT INTO test_table VALUES (1, 'test')"));

        assertEquals(FlightStatusCode.UNAUTHORIZED, ex.status().code());
        assertEquals(SelectOnlyAuthorizer.UNSUPPORTED_QUERY_TYPE_MESSAGE, ex.status().description());
    }

    // ===== Complex SELECT Operations (Should be ALLOWED) =====

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithSubqueryAllowed() throws Exception {
        // SELECT with subquery should work
        String query = "SELECT * FROM (SELECT 1 AS id, 'test' AS name)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithInSubqueryAllowed() throws Exception {
        // SELECT with IN subquery should work
        String query = "SELECT * FROM generate_series(10) AS t(value) WHERE value IN (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithCaseAllowed() throws Exception {
        // SELECT with CASE should work
        String query = "SELECT value, CASE WHEN value > 5 THEN 'high' ELSE 'low' END as level FROM generate_series(10) AS t(value)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithDistinctAllowed() throws Exception {
        // SELECT with DISTINCT should work
        String query = "SELECT DISTINCT value % 2 FROM generate_series(10) AS t(value)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithLimitAllowed() throws Exception {
        // SELECT with LIMIT should work
        String query = "SELECT * FROM generate_series(10) AS t(value) LIMIT 5";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithOffsetAllowed() throws Exception {
        // SELECT with OFFSET should work
        String query = "SELECT * FROM generate_series(10) AS t(value) OFFSET 5";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithOrderByAllowed() throws Exception {
        // SELECT with ORDER BY should work
        String query = "SELECT * FROM generate_series(10) AS t(value) ORDER BY value DESC";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    // ===== EXPLAIN Support Tests =====

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testExplainSelect() throws Exception {
        String query = "EXPLAIN SELECT 1";
        FlightTestUtils.testQuerySuccess(query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testExplainSelectWithWhere() throws Exception {
        String query = "EXPLAIN SELECT 1 WHERE 1=1";
        FlightTestUtils.testQuerySuccess(query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testExplainSelectWithJoin() throws Exception {
        String query = "EXPLAIN SELECT 1 t1_col, 2 t2_col FROM (SELECT 1) t1 JOIN (SELECT 2) t2 ON TRUE";
        FlightTestUtils.testQuerySuccess(query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testExplainAnalyzeSelect() throws Exception {
        String query = "EXPLAIN ANALYZE SELECT 1";
        FlightTestUtils.testQuerySuccess(query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testExplainAnalyzeSelectWithJoin() throws Exception {
        String query = "EXPLAIN ANALYZE SELECT 1 FROM (SELECT 1) JOIN (SELECT 2) ON TRUE";
        FlightTestUtils.testQuerySuccess(query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    // ===== Prepared Statement Tests =====

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testPreparedStatement_selectAllowed() throws Exception {
        try (var ps = serverClient.flightSqlClient().prepare("SELECT 1 AS n")) {
            var info = ps.execute();
            try (var stream = serverClient.flightSqlClient()
                    .getStream(info.getEndpoints().get(0).getTicket())) {
                assertTrue(stream.next());
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testPreparedStatement_insertRejected() {
        assertThrows(FlightRuntimeException.class,
                () -> serverClient.flightSqlClient().prepare(
                        "INSERT INTO t_select_only VALUES (99, 'x')"));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testPreparedStatement_dmlUpdateBlocked() throws Exception {
        try (var ps = serverClient.flightSqlClient().prepare("SELECT 1")) {
            assertThrows(FlightRuntimeException.class, () -> ps.executeUpdate());
        }
    }

    // ===== hasWriteAccess Test =====

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testHasWriteAccessDenied() throws Exception {
        // In READ_ONLY mode, hasWriteAccess should return false
        // This test verifies the authorizer correctly denies write access
        String query = "SELECT 1";

        // The query should work (it's a SELECT)
        FlightTestUtils.testQuery(query, serverClient.flightSqlClient(), serverClient.clientAllocator());

        // But any write operation should fail
        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute("INSERT INTO (VALUES (1)) VALUES (1)"));
    }
}
