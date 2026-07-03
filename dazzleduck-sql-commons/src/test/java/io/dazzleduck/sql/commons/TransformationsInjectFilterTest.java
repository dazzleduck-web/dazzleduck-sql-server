package io.dazzleduck.sql.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.authorization.SqlAuthorizer;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class TransformationsInjectFilterTest {

    private static DuckDBConnection conn;

    @BeforeAll
    static void setup() throws SQLException {
        conn = ConnectionPool.getConnection();
        conn.createStatement().execute("CREATE TABLE orders (id INT, tenant_id VARCHAR, amount INT)");
        conn.createStatement().execute("INSERT INTO orders VALUES (1,'abc',100),(2,'xyz',200),(3,'abc',300)");
        conn.createStatement().execute("CREATE TABLE items (order_id INT, tenant_id VARCHAR, name VARCHAR)");
        conn.createStatement().execute("INSERT INTO items VALUES (1,'abc','widget'),(2,'xyz','gadget'),(3,'abc','thing')");
        // Second schema with a same-named table — used for cross-schema security tests
        conn.createStatement().execute("CREATE SCHEMA other_schema");
        conn.createStatement().execute("CREATE TABLE other_schema.orders (id INT, tenant_id VARCHAR, amount INT)");
        conn.createStatement().execute("INSERT INTO other_schema.orders VALUES (10,'abc',999),(11,'xyz',888)");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.createStatement().execute("DROP TABLE IF EXISTS orders");
        conn.createStatement().execute("DROP TABLE IF EXISTS items");
        conn.createStatement().execute("DROP TABLE IF EXISTS other_schema.orders");
        conn.createStatement().execute("DROP SCHEMA IF EXISTS other_schema");
        conn.close();
    }

    private JsonNode filter(String expr) {
        return SqlAuthorizer.compileFilterString(expr);
    }

    private List<Object> execFirstColumn(String sql) throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery(sql);
        List<Object> rows = new ArrayList<>();
        while (rs.next()) rows.add(rs.getObject(1));
        return rows;
    }

    // --- single table ---

    @Test
    void singleTable_filterInjected() throws SQLException, JsonProcessingException {
        String sql = "SELECT id FROM orders";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"), "CTE name should appear in output SQL");

        List<Object> ids = execFirstColumn(out);
        assertEquals(List.of(1, 3), ids);
    }

    @Test
    void singleTable_noFilterClause_cteMapEmpty_whenNoTables() throws SQLException, JsonProcessingException {
        // Constant query — no base tables — cte_map should remain empty
        String sql = "SELECT 1 as x";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        JsonNode cteMap = result.get("statements").get(0).get("node").get("cte_map").get("map");
        assertEquals(0, cteMap.size());
    }

    // --- JOIN ---

    @Test
    void join_filterInjectedIntoEachArm() throws SQLException, JsonProcessingException {
        String sql = "SELECT o.id FROM orders o JOIN items i ON o.id = i.order_id";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"), "orders CTE should be injected");
        assertTrue(out.contains("___items"), "items CTE should be injected");

        // Both tables filtered to tenant 'abc': ids 1 and 3
        List<Object> ids = execFirstColumn(out);
        assertEquals(2, ids.size());
        assertTrue(ids.contains(1));
        assertTrue(ids.contains(3));
    }

    @Test
    void join_cteMapHasTwoEntries() throws SQLException, JsonProcessingException {
        String sql = "SELECT * FROM orders JOIN items ON orders.id = items.order_id";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        JsonNode cteMap = result.get("statements").get(0).get("node").get("cte_map").get("map");
        assertEquals(2, cteMap.size());
    }

    // --- schema-qualified table ---

    @Test
    void schemaQualified_cteKeyIncludesSchema() throws SQLException, JsonProcessingException {
        String sql = "SELECT id FROM main.orders";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        JsonNode cteMap = result.get("statements").get(0).get("node").get("cte_map").get("map");
        assertEquals(1, cteMap.size());
        String cteKey = cteMap.get(0).get("key").asText();
        assertTrue(cteKey.startsWith("___main_"), "CTE key should include schema: " + cteKey);

        String out = Transformations.parseToSql(conn, result);
        List<Object> ids = execFirstColumn(out);
        assertEquals(List.of(1, 3), ids);
    }

    // --- subquery in FROM ---

    @Test
    void subqueryInFrom_filterInjectedIntoSubquery() throws SQLException, JsonProcessingException {
        String sql = "SELECT id FROM (SELECT id, tenant_id FROM orders) sub";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        List<Object> ids = execFirstColumn(out);
        assertEquals(List.of(1, 3), ids);
    }

    // --- user CTE referencing a base table ---

    @Test
    void userCte_filterInjectedIntoBody() throws SQLException, JsonProcessingException {
        String sql = "WITH user_cte AS (SELECT id, tenant_id FROM orders) SELECT id FROM user_cte";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        // The filter CTE for orders should precede user_cte
        JsonNode cteMap = result.get("statements").get(0).get("node").get("cte_map").get("map");
        assertTrue(cteMap.size() >= 2);
        // First entry should be the injected filter CTE for orders
        String firstKey = cteMap.get(0).get("key").asText();
        assertTrue(firstKey.startsWith("___"), "Filter CTE should be first: " + firstKey);

        List<Object> ids = execFirstColumn(out);
        assertEquals(List.of(1, 3), ids);
    }

    // --- same table referenced twice (self-join) ---

    @Test
    void selfJoin_onlyOneCteCreated() throws SQLException, JsonProcessingException {
        String sql = "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        JsonNode cteMap = result.get("statements").get(0).get("node").get("cte_map").get("map");
        // Only one CTE should be created for 'orders' even though it appears twice
        assertEquals(1, cteMap.size());

        String out = Transformations.parseToSql(conn, result);
        List<Object> ids = execFirstColumn(out);
        assertEquals(2, ids.size());
    }

    // --- WHERE subquery (IN, EXISTS, scalar) — security gap fixed ---

    @Test
    void whereInSubquery_innerTableFiltered() throws SQLException, JsonProcessingException {
        // items in the IN subquery must get the filter — otherwise tenant xyz rows leak
        String sql = "SELECT id FROM orders WHERE id IN (SELECT order_id FROM items)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___items"), "items must get a filter CTE");
        assertTrue(out.contains("___orders"), "orders must get a filter CTE");

        List<Object> ids = execFirstColumn(out);
        // orders filtered to {1,3}; items filtered to {1,3}; intersection = {1,3}
        assertEquals(List.of(1, 3), ids);
    }

    @Test
    void whereNotInSubquery_innerTableFiltered() throws SQLException, JsonProcessingException {
        String sql = "SELECT id FROM orders WHERE id NOT IN (SELECT order_id FROM items WHERE name = 'widget')";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___items"), "items must get a filter CTE");
        List<Object> ids = execFirstColumn(out);
        // orders {1,3}, items filtered to {1} (widget+abc); NOT IN removes id=1 → {3}
        assertEquals(List.of(3), ids);
    }

    @Test
    void whereExistsSubquery_innerTableFiltered() throws SQLException, JsonProcessingException {
        // Correlated EXISTS — items in the subquery must be filtered
        String sql = "SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___items"), "items must get a filter CTE");
        assertTrue(out.contains("___orders"), "orders must get a filter CTE");

        List<Object> ids = execFirstColumn(out);
        // orders {1,3} both have matching items in tenant abc
        assertEquals(2, ids.size());
        assertTrue(ids.contains(1));
        assertTrue(ids.contains(3));
    }

    @Test
    void scalarSubqueryInSelectList_innerTableFiltered() throws SQLException, JsonProcessingException {
        String sql = "SELECT id, (SELECT count(*) FROM items WHERE items.order_id = orders.id) AS cnt FROM orders";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___items"), "items in scalar subquery must get a filter CTE");
        assertTrue(out.contains("___orders"), "orders must get a filter CTE");

        // orders for tenant abc: ids 1 and 3; each has one matching item in tenant abc
        List<Object> cnts = new ArrayList<>();
        var rs = conn.createStatement().executeQuery(out);
        while (rs.next()) cnts.add(rs.getObject(2)); // cnt column
        assertEquals(List.of(1L, 1L), cnts);
    }

    // --- UNION ---

    @Test
    void union_filterInjectedIntoBothSides() throws SQLException, JsonProcessingException {
        // Both arms reference the same table; after filter each arm returns 2 rows (tenant 'abc')
        // UNION ALL → 4 rows total (no dedup)
        String sql = "SELECT id FROM orders WHERE amount > 0 UNION ALL SELECT id FROM orders WHERE amount < 1000";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));

        // Top-level is SET_OPERATION_NODE; cte_map is on it
        JsonNode cteMap = result.get("statements").get(0).get("node").get("cte_map").get("map");
        assertEquals(1, cteMap.size(), "One filter CTE for orders");

        String out = Transformations.parseToSql(conn, result);
        List<Object> ids = execFirstColumn(out);
        assertEquals(4, ids.size(), "UNION ALL: 2 rows × 2 arms");
    }

    // --- set operation nested inside a derived table (regression: RLS bypass) ---

    @Test
    void unionInsideDerivedTable_filterInjectedIntoBothArms() throws SQLException, JsonProcessingException {
        // Regression: a UNION nested inside a FROM-subquery (derived table). Previously the SUBQUERY
        // branch of renameBaseTablesInFromNode dispatched a set-operation body to the SELECT-only
        // handler, so base tables inside the UNION were never wrapped — an RLS bypass that leaked
        // every row. They must now be filtered.
        String sql = "SELECT id FROM ("
                + "SELECT id, tenant_id FROM orders WHERE amount > 0 "
                + "UNION ALL "
                + "SELECT id, tenant_id FROM orders WHERE amount < 1000) sub";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"), "orders inside the nested UNION must get a filter CTE");
        List<Object> ids = execFirstColumn(out);
        // Each arm filtered to tenant abc {1,3}; UNION ALL → 4 rows, all abc.
        assertEquals(4, ids.size(), "both arms return only tenant abc rows: " + ids);
        assertTrue(ids.stream().allMatch(v -> v.equals(1) || v.equals(3)),
                "tenant xyz (id 2) must not leak: " + ids);
    }

    // --- lateral VALUES unpivot (single-scan distinct-values pattern) ---

    @Test
    void lateralValuesUnpivot_filterInjected() throws SQLException, JsonProcessingException {
        // Mirrors the single-scan distinct-values query: the fact table is scanned once and each row
        // is unpivoted into (column, value) tuples via a lateral VALUES list. The base table must
        // still be filtered, and only authorized rows' values may appear.
        String sql = "SELECT DISTINCT v.col, v.val FROM orders, (VALUES "
                + "('id', CAST(id AS VARCHAR)), "
                + "('amount', CAST(amount AS VARCHAR))"
                + ") AS v(col, val) WHERE v.val IS NOT NULL";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"), "orders must get a filter CTE in the lateral-VALUES query");

        Set<String> pairs = new HashSet<>();
        try (ResultSet rs = conn.createStatement().executeQuery(out)) {
            while (rs.next()) {
                pairs.add(rs.getString(1) + ":" + rs.getString(2));
            }
        }
        // tenant abc rows: (id 1, amount 100), (id 3, amount 300). xyz (id 2 / amount 200) excluded.
        assertEquals(Set.of("id:1", "id:3", "amount:100", "amount:300"), pairs,
                "only tenant abc values may appear: " + pairs);
    }

    // --- per-table map with fully-qualified keys (security tests) ---

    @Test
    void perTableMap_qualifiedKey_bareReference_matchesFilter() throws SQLException, JsonProcessingException {
        // JWT authorizes "main.orders" with a filter; query uses bare "orders" — should match
        Map<String, JsonNode> filters = new LinkedHashMap<>();
        filters.put("main.orders", filter("tenant_id = 'abc'"));

        String sql = "SELECT id FROM orders";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filters);
        String out = Transformations.parseToSql(conn, result);

        List<Object> ids = execFirstColumn(out);
        assertEquals(List.of(1, 3), ids, "Filter from 'main.orders' key must apply to bare 'orders' reference");
    }

    @Test
    void perTableMap_qualifiedKey_schemaQualifiedReference_matchesFilter() throws SQLException, JsonProcessingException {
        // JWT authorizes "main.orders"; query uses "main.orders" — should match
        Map<String, JsonNode> filters = new LinkedHashMap<>();
        filters.put("main.orders", filter("tenant_id = 'abc'"));

        String sql = "SELECT id FROM main.orders";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filters);
        String out = Transformations.parseToSql(conn, result);

        List<Object> ids = execFirstColumn(out);
        assertEquals(List.of(1, 3), ids);
    }

    @Test
    void perTableMap_wrongSchema_returnsNoRows() throws SQLException, JsonProcessingException {
        // Security: JWT authorizes only "main.orders"; query references "other_schema.orders".
        // The filter for main.orders must NOT apply to other_schema.orders — it must get false.
        Map<String, JsonNode> filters = new LinkedHashMap<>();
        filters.put("main.orders", filter("tenant_id = 'abc'"));

        String sql = "SELECT id FROM other_schema.orders";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filters);
        String out = Transformations.parseToSql(conn, result);

        List<Object> ids = execFirstColumn(out);
        assertTrue(ids.isEmpty(), "other_schema.orders must receive false filter, not the main.orders filter");
    }

    @Test
    void perTableMap_multiTable_eachGetsOwnFilter() throws SQLException, JsonProcessingException {
        // Two tables in the map; each reference in the query must get its own filter
        Map<String, JsonNode> filters = new LinkedHashMap<>();
        filters.put("main.orders", filter("tenant_id = 'abc'"));
        filters.put("main.items",  filter("tenant_id = 'abc'"));

        String sql = "SELECT o.id FROM orders o JOIN items i ON o.id = i.order_id";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filters);
        String out = Transformations.parseToSql(conn, result);

        List<Object> ids = execFirstColumn(out);
        assertEquals(2, ids.size());
        assertTrue(ids.contains(1));
        assertTrue(ids.contains(3));
    }

    @Test
    void perTableMap_unauthorizedTable_returnsNoRows() throws SQLException, JsonProcessingException {
        // Map only covers "orders"; "items" is not listed → must get false filter (no rows leak)
        Map<String, JsonNode> filters = new LinkedHashMap<>();
        filters.put("orders", filter("tenant_id = 'abc'"));

        String sql = "SELECT id FROM orders WHERE id IN (SELECT order_id FROM items)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filters);
        String out = Transformations.parseToSql(conn, result);

        List<Object> ids = execFirstColumn(out);
        // items gets false → IN subquery empty → no orders returned
        assertTrue(ids.isEmpty(), "items not in the map must get false, making the IN subquery empty");
    }

    @Test
    void perTableMap_cteNameShadowingTable_filterStillApplied() throws SQLException, JsonProcessingException {
        // Security exploit: attacker names a CTE the same as the authorized table.
        // Inside the CTE body, "orders" refers to the BASE TABLE (non-recursive CTEs can't
        // see themselves). The filter must still be injected on that inner base-table reference.
        // Without the fix, tablesToWrap stays empty and ALL rows are exposed.
        Map<String, JsonNode> filters = new LinkedHashMap<>();
        filters.put("main.orders", filter("tenant_id = 'abc'"));

        String sql = "WITH orders AS (SELECT * FROM orders) SELECT id FROM orders";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filters);
        String out = Transformations.parseToSql(conn, result);

        List<Object> ids = execFirstColumn(out);
        // Must only see abc rows (1, 3), not xyz row (2) — filter must not be bypassed
        assertEquals(List.of(1, 3), ids,
                "CTE name shadowing the authorized table must not bypass the row filter");
    }

    // --- fail-open node-type gaps in the filter-injection traversal (RLS-bypass regressions) ---

    private List<Object> sorted(List<Object> in) {
        List<Object> copy = new ArrayList<>(in);
        copy.sort((a, b) -> ((Comparable) a).compareTo(b));
        return copy;
    }

    @Test
    void recursiveCte_topLevel_filterInjectedIntoAnchor() throws SQLException, JsonProcessingException {
        // Regression: a WITH RECURSIVE body serializes as RECURSIVE_CTE_NODE, a type the statement
        // dispatcher previously ignored — the anchor's base table passed through UNFILTERED.
        // The recursive term (WHERE false) adds no rows, so r is exactly the filtered anchor.
        String sql = "WITH RECURSIVE r(id) AS ("
                + "SELECT id FROM orders "
                + "UNION ALL "
                + "SELECT id FROM r WHERE false) "
                + "SELECT id FROM r";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"), "recursive CTE anchor must get a filter CTE: " + out);
        assertEquals(List.of(1, 3), sorted(execFirstColumn(out)),
                "only tenant abc rows may seed the recursion; xyz (id 2) must not leak");
    }

    @Test
    void recursiveCte_nestedInDerivedTable_filterInjected() throws SQLException, JsonProcessingException {
        String sql = "SELECT id FROM (WITH RECURSIVE r(id) AS ("
                + "SELECT id FROM orders UNION ALL SELECT id FROM r WHERE false) "
                + "SELECT id FROM r) sub";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"), "recursive CTE inside a derived table must be filtered: " + out);
        assertEquals(List.of(1, 3), sorted(execFirstColumn(out)), "xyz (id 2) must not leak");
    }

    @Test
    void nestedWithInsideDerivedTable_filterInjected() throws SQLException, JsonProcessingException {
        // Regression: renameBaseTablesInSelect did not walk a derived-table body's own WITH clause,
        // so orders inside the inner CTE leaked and the inner CTE name got mis-wrapped (broken SQL).
        String sql = "SELECT id FROM (WITH s AS (SELECT id, tenant_id FROM orders) SELECT id FROM s) sub";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"), "orders inside the nested WITH must be filtered: " + out);
        assertFalse(out.contains("___s"), "the inner CTE name must not be wrapped as a phantom base table: " + out);
        assertEquals(List.of(1, 3), sorted(execFirstColumn(out)), "xyz (id 2) must not leak");
    }

    @Test
    void unpivot_filterInjected() throws SQLException, JsonProcessingException {
        // Regression: a PIVOT/UNPIVOT FROM node was not handled, so the base table under `source`
        // was never wrapped. tenant_id is the kept (non-unpivoted) column; only abc must survive.
        String sql = "SELECT tenant_id FROM orders UNPIVOT (val FOR col IN (id, amount))";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"), "unpivoted base table must get a filter CTE: " + out);
        List<Object> tenants = execFirstColumn(out);
        assertTrue(tenants.stream().allMatch(t -> t.equals("abc")),
                "only tenant abc rows may appear after unpivot: " + tenants);
        assertEquals(4, tenants.size(), "2 abc rows × 2 unpivoted columns");
    }

    @Test
    void valuesListScalarSubquery_filterInjected() throws SQLException, JsonProcessingException {
        // Regression: an EXPRESSION_LIST (VALUES) FROM node's value expressions were never walked,
        // so a scalar subquery over a base table inside VALUES bypassed the filter.
        String sql = "SELECT x FROM (VALUES ((SELECT sum(amount) FROM orders))) v(x)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"), "scalar subquery over orders in VALUES must be filtered: " + out);
        List<Object> vals = execFirstColumn(out);
        assertEquals("400", String.valueOf(vals.get(0)),
                "filtered sum is 100+300=400 (all-tenant 600 would mean the filter was bypassed): " + vals);
    }

    @Test
    void orderByScalarSubquery_filterInjected() throws SQLException, JsonProcessingException {
        // Regression: ORDER BY subqueries live under modifiers[].orders[].expression, not a top-level
        // "order_bys" field the code used to read (always null) — so they were never filtered.
        String sql = "SELECT order_id FROM items ORDER BY (SELECT max(amount) FROM orders)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"),
                "orders referenced only in an ORDER BY subquery must still be filtered: " + out);
    }

    @Test
    void setOperationOrderBySubquery_filterInjected() throws SQLException, JsonProcessingException {
        // Regression: the set-operation branch never walked the union's own ORDER BY/LIMIT modifiers,
        // so a base table referenced only there bypassed the filter. orders appears ONLY in ORDER BY.
        String sql = "SELECT x FROM ("
                + "SELECT order_id AS x FROM items "
                + "UNION ALL "
                + "SELECT order_id AS x FROM items "
                + "ORDER BY (SELECT max(amount) FROM orders)) s";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"),
                "orders referenced only in a UNION's ORDER BY subquery must still be filtered: " + out);
    }

    @Test
    void unsupportedFromNode_failsClosed() throws SQLException, JsonProcessingException {
        // A FROM node type the traversal does not recognize (here a table function) must ABORT rather
        // than silently pass base tables through unfiltered. RESTRICT_READ_ONLY blocks table functions
        // upstream; this guards the injector's own fail-closed contract regardless of caller.
        String sql = "SELECT * FROM generate_series(1, 3)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        assertThrows(IllegalStateException.class,
                () -> Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'")),
                "an unhandled FROM node type must fail closed, not produce an unfiltered query");
    }
}
