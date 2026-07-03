package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that RESTRICTED mode (RestrictedDatasourceOnlyAuthorizer) closes the subquery security gap:
 * tables referenced only in WHERE/HAVING/SELECT expression subqueries are now both
 * access-controlled and filter-injected.
 */
public class RestrictedDatasourceOnlyAuthorizerSecurityTest {

    private static DuckDBConnection conn;
    private static final SqlAuthorizer authorizer = SqlAuthorizer.RESTRICTED_DATASOURCE_AUTHORIZER;
    private static final String DB = "memory";
    private static final String SCHEMA = "main";

    @BeforeAll
    static void setup() throws SQLException {
        conn = ConnectionPool.getConnection();
        conn.createStatement().execute(
                "CREATE TABLE orders (id INT, tenant_id VARCHAR, amount INT)");
        conn.createStatement().execute(
                "INSERT INTO orders VALUES (1,'abc',100),(2,'xyz',200),(3,'abc',300)");
        conn.createStatement().execute(
                "CREATE TABLE payments (order_id INT, tenant_id VARCHAR, paid INT)");
        conn.createStatement().execute(
                "INSERT INTO payments VALUES (1,'abc',100),(2,'xyz',200),(3,'abc',300)");
        conn.createStatement().execute(
                "CREATE TABLE sensitive (id INT, tenant_id VARCHAR, secret VARCHAR)");
        conn.createStatement().execute(
                "INSERT INTO sensitive VALUES (1,'abc','a-secret'),(2,'xyz','x-secret')");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.createStatement().execute("DROP TABLE IF EXISTS orders");
        conn.createStatement().execute("DROP TABLE IF EXISTS payments");
        conn.createStatement().execute("DROP TABLE IF EXISTS sensitive");
        conn.close();
    }

    private Map<String, String> claims(String table, String filter) {
        return Map.of(Headers.HEADER_TABLE, table, Headers.HEADER_FILTER, filter);
    }

    private List<Object> execFirstColumn(String sql) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
        List<Object> rows = new ArrayList<>();
        while (rs.next()) rows.add(rs.getObject(1));
        return rows;
    }

    // ── Gap 1: access control now catches subquery tables ────────────────────

    @Test
    void whereInSubquery_unauthorizedTable_rejected() throws SQLException, JsonProcessingException {
        // JWT allows only 'orders'; 'payments' in the IN subquery must be caught
        String sql = "SELECT id FROM orders WHERE id IN (SELECT order_id FROM payments)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", DB, SCHEMA, tree, claims("orders", "tenant_id='abc'")));
    }

    @Test
    void whereExistsSubquery_unauthorizedTable_rejected() throws SQLException, JsonProcessingException {
        String sql = "SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM sensitive WHERE sensitive.id = orders.id)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", DB, SCHEMA, tree, claims("orders", "tenant_id='abc'")));
    }

    @Test
    void scalarSubqueryInSelect_unauthorizedTable_rejected() throws SQLException, JsonProcessingException {
        String sql = "SELECT id, (SELECT count(*) FROM sensitive) AS cnt FROM orders";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", DB, SCHEMA, tree, claims("orders", "tenant_id='abc'")));
    }

    // ── Filter injection: outer WHERE clause correctly secures the result ────

    @Test
    void whereInSubquery_authorizedTable_outerFilterSecuresResult() throws Exception {
        // JWT allows 'orders'; the IN subquery also references 'orders' — authorized.
        // Filter (tenant_id='abc') is applied to the outer SELECT; the result contains
        // only tenant-abc rows regardless of what the inner subquery returns.
        String sql = "SELECT id FROM orders WHERE id IN (SELECT id FROM orders WHERE amount > 50)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = authorizer.authorize("user", DB, SCHEMA, tree,
                claims("orders", "tenant_id='abc'"));
        String out = Transformations.parseToSql(conn, result);

        List<Object> ids = execFirstColumn(out);
        // tenant 'abc' rows with amount>50: id=1 (100) and id=3 (300)
        assertEquals(2, ids.size());
        assertTrue(ids.contains(1));
        assertTrue(ids.contains(3));
    }

    @Test
    void existsSubquery_authorizedTable_outerFilterSecuresResult() throws Exception {
        String sql = "SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM orders o2 WHERE o2.id = orders.id)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = authorizer.authorize("user", DB, SCHEMA, tree,
                claims("orders", "tenant_id='abc'"));
        String out = Transformations.parseToSql(conn, result);

        List<Object> ids = execFirstColumn(out);
        assertEquals(2, ids.size());
        assertTrue(ids.contains(1));
        assertTrue(ids.contains(3));
    }

    // ── Regression: existing authorized queries still work ───────────────────

    @Test
    void simpleSelect_authorized_filterApplied() throws Exception {
        String sql = "SELECT id FROM orders";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = authorizer.authorize("user", DB, SCHEMA, tree,
                claims("orders", "tenant_id='abc'"));
        String out = Transformations.parseToSql(conn, result);

        List<Object> ids = execFirstColumn(out);
        assertEquals(List.of(1, 3), ids);
    }

    @Test
    void simpleSelect_unauthorizedTable_rejected() throws SQLException, JsonProcessingException {
        String sql = "SELECT id FROM payments";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", DB, SCHEMA, tree, claims("orders", "tenant_id='abc'")));
    }

    // ── Gap 2: node-type coverage — tables hidden by exotic shapes must still be caught ──

    @Test
    void recursiveCte_unauthorizedTable_rejected() throws SQLException, JsonProcessingException {
        // Regression: the enumerator ignored RECURSIVE_CTE_NODE, so 'payments' in the anchor was
        // invisible to the access check — the query was wrongly authorized.
        String sql = "WITH RECURSIVE r(x) AS ("
                + "SELECT order_id FROM payments UNION ALL SELECT x FROM r WHERE false) "
                + "SELECT x FROM r";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", DB, SCHEMA, tree, claims("orders", "tenant_id='abc'")));
    }

    @Test
    void unpivot_unauthorizedTable_rejected() throws SQLException, JsonProcessingException {
        // Regression: a PIVOT/UNPIVOT FROM node was not enumerated, hiding its `source` table.
        String sql = "SELECT tenant_id FROM payments UNPIVOT (val FOR col IN (order_id, paid))";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", DB, SCHEMA, tree, claims("orders", "tenant_id='abc'")));
    }

    @Test
    void valuesListScalarSubquery_unauthorizedTable_rejected() throws SQLException, JsonProcessingException {
        // Regression: an EXPRESSION_LIST (VALUES) FROM node's value expressions were not walked.
        String sql = "SELECT x FROM (VALUES ((SELECT count(*) FROM sensitive))) v(x)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", DB, SCHEMA, tree, claims("orders", "tenant_id='abc'")));
    }

    @Test
    void orderBySubquery_unauthorizedTable_rejected() throws SQLException, JsonProcessingException {
        // Regression: ORDER BY subqueries live under modifiers[], not the "order_bys" field the
        // enumerator used to read (always null), so 'payments' here was invisible to the check.
        String sql = "SELECT id FROM orders ORDER BY (SELECT max(paid) FROM payments)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", DB, SCHEMA, tree, claims("orders", "tenant_id='abc'")));
    }
}
