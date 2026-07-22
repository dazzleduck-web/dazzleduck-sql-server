package io.dazzleduck.sql.commons;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.util.TestUtils;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link Transformations#pruneUnusedLeftJoins}.
 *
 * <p>See {@code LEFT_JOIN_PRUNING_SPEC.md}. When no optimization applies, the method returns the
 * <em>same instance</em> it was given — the bail-out tests assert that identity directly.
 *
 * <p>Star schema used throughout:
 * <pre>
 *   f (fact)      : f_id, a_id, b_id, f_col
 *   a (dimension) : a_id, a_name        -- unique on a_id
 *   b (dimension) : b_id, b_name        -- unique on b_id
 *   VIEW fv       : SELECT f.*, a.a_name, b.b_name
 *                   FROM f LEFT JOIN a ON f.a_id=a.a_id LEFT JOIN b ON f.b_id=b.b_id
 * </pre>
 * Row {@code (2,10,999,15)} has a {@code b_id} with no match in {@code b}, so a wrongly-applied
 * INNER semantics (or an incorrect elimination) would drop it — the LEFT-preservation guard.
 *
 * <p>Additional fixtures: {@code fs}/{@code fvs} (struct-typed fact column for nested-type
 * coverage) and {@code fd}/{@code fvd} (duplicate-bearing fact + DISTINCT view body).
 */
public class PruneUnusedLeftJoinsTest {

    private static DuckDBConnection conn;

    private static final String VIEW_BODY =
            "SELECT f.*, a.a_name, b.b_name " +
            "FROM f " +
            "LEFT JOIN a ON f.a_id = a.a_id " +
            "LEFT JOIN b ON f.b_id = b.b_id";

    /** View over a struct-typed fact: the outer query dereferences s.x (nested type). */
    private static final String STRUCT_VIEW_BODY =
            "SELECT fs.f_id, fs.s, a.a_name, b.b_name " +
            "FROM fs " +
            "LEFT JOIN a ON fs.a_id = a.a_id " +
            "LEFT JOIN b ON fs.b_id = b.b_id";

    /** DISTINCT view body: the select list is the dedup key, so it must never be pruned. */
    private static final String DISTINCT_VIEW_BODY =
            "SELECT DISTINCT fd.f_col, a.a_name " +
            "FROM fd " +
            "LEFT JOIN a ON fd.a_id = a.a_id " +
            "LEFT JOIN b ON fd.f_id = b.b_id";

    @BeforeAll
    static void setup() throws SQLException {
        conn = ConnectionPool.getConnection();
        conn.createStatement().execute("CREATE TABLE f (f_id INT, a_id INT, b_id INT, f_col INT)");
        conn.createStatement().execute("INSERT INTO f VALUES (1,10,100,5),(2,10,999,15),(3,20,100,25)");
        conn.createStatement().execute("CREATE TABLE a (a_id INT, a_name VARCHAR)");
        conn.createStatement().execute("INSERT INTO a VALUES (10,'a10'),(20,'a20')");
        conn.createStatement().execute("CREATE TABLE b (b_id INT, b_name VARCHAR)");
        conn.createStatement().execute("INSERT INTO b VALUES (100,'b100')");
        conn.createStatement().execute("CREATE VIEW fv AS " + VIEW_BODY);
        // Nested-type fixture: fact with a STRUCT column; second row's b_id has no match in b.
        conn.createStatement().execute("CREATE TABLE fs (f_id INT, a_id INT, b_id INT, s STRUCT(x INT, y VARCHAR))");
        conn.createStatement().execute("INSERT INTO fs VALUES (1,10,100,{'x':1,'y':'p'}),(2,20,999,{'x':2,'y':'q'})");
        conn.createStatement().execute("CREATE VIEW fvs AS " + STRUCT_VIEW_BODY);
        // DISTINCT fixture: two fact rows share f_col=5 but map to different a_name values,
        // so DISTINCT (f_col, a_name) yields 3 rows while DISTINCT (f_col) would yield 2.
        conn.createStatement().execute("CREATE TABLE fd (f_id INT, a_id INT, f_col INT)");
        conn.createStatement().execute("INSERT INTO fd VALUES (1,10,5),(2,20,5),(3,10,15)");
        conn.createStatement().execute("CREATE VIEW fvd AS " + DISTINCT_VIEW_BODY);
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.createStatement().execute("DROP VIEW IF EXISTS fv");
        conn.createStatement().execute("DROP VIEW IF EXISTS fvs");
        conn.createStatement().execute("DROP VIEW IF EXISTS fvd");
        conn.createStatement().execute("DROP TABLE IF EXISTS f");
        conn.createStatement().execute("DROP TABLE IF EXISTS fs");
        conn.createStatement().execute("DROP TABLE IF EXISTS fd");
        conn.createStatement().execute("DROP TABLE IF EXISTS a");
        conn.createStatement().execute("DROP TABLE IF EXISTS b");
        conn.close();
    }

    // ---- helpers ----

    private JsonNode prune(String outerSql) throws Exception {
        return prune(outerSql, VIEW_BODY);
    }

    private JsonNode prune(String outerSql, String viewBody) throws Exception {
        JsonNode outer = Transformations.parseToTree(conn, outerSql);
        JsonNode body = Transformations.parseToTree(conn, viewBody);
        return Transformations.pruneUnusedLeftJoins(outer, body);
    }

    /** Assert the method bailed out: the exact input instance comes back. */
    private void assertBailsOut(String outerSql, String viewBody) throws Exception {
        JsonNode outer = Transformations.parseToTree(conn, outerSql);
        JsonNode body = Transformations.parseToTree(conn, viewBody);
        JsonNode pruned = Transformations.pruneUnusedLeftJoins(outer, body);
        assertSame(outer, pruned, "expected a no-op (same instance returned) for: " + outerSql);
    }

    /** Count JOIN nodes anywhere in the tree. */
    private int countJoins(JsonNode node) {
        if (node == null) return 0;
        int c = 0;
        if (node.isObject()) {
            JsonNode type = node.get("type");
            if (type != null && "JOIN".equals(type.asText())) c++;
            var it = node.fields();
            while (it.hasNext()) c += countJoins(it.next().getValue());
        } else if (node.isArray()) {
            for (JsonNode child : node) c += countJoins(child);
        }
        return c;
    }

    private List<List<Object>> exec(String sql) throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery(sql);
        int cols = rs.getMetaData().getColumnCount();
        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= cols; i++) row.add(rs.getObject(i));
            rows.add(row);
        }
        return rows;
    }

    /**
     * The pruned output must return the same rows as the original query against the real view.
     * {@link TestUtils#isEqual} compares the two result sets order-insensitively (bidirectional
     * EXCEPT) and throws an AssertionError listing any differing rows.
     */
    private void assertEquivalentToView(JsonNode pruned, String originalOuterSql) throws Exception {
        String prunedSql = Transformations.parseToSql(conn, pruned);
        TestUtils.isEqual(originalOuterSql, prunedSql);
    }

    // ---- elimination cases ----

    @Test
    void onlyDimAUsed_dropsJoinB() throws Exception {
        String outer = "SELECT f_col, a_name FROM fv WHERE f_col > 10";
        JsonNode pruned = prune(outer);

        assertEquals(1, countJoins(pruned), "join to b should be eliminated, join to a kept");
        String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertFalse(sql.contains("b_name"), "b_name projection should be gone");
        assertEquivalentToView(pruned, outer);
    }

    @Test
    void bothDimsUsed_noChange_sameInstanceReturned() throws Exception {
        // Nothing to prune or eliminate — the method must return the input as-is (no inlining).
        assertBailsOut("SELECT a_name, b_name FROM fv WHERE f_col > 10", VIEW_BODY);
    }

    @Test
    void neitherDimUsed_dropsBothJoins() throws Exception {
        String outer = "SELECT f_col FROM fv WHERE f_col > 10";
        JsonNode pruned = prune(outer);

        assertEquals(0, countJoins(pruned), "both dimension joins should be eliminated");
        assertEquivalentToView(pruned, outer);
    }

    @Test
    void unmatchedFactRow_isPreserved_afterElimination() throws Exception {
        // f_id=2 has b_id=999 (no match in b). Under correct LEFT semantics it survives;
        // eliminating the (unused) LEFT JOIN b must not drop it.
        String outer = "SELECT f_id, a_name FROM fv";
        JsonNode pruned = prune(outer);

        assertEquals(1, countJoins(pruned));
        assertEquivalentToView(pruned, outer);
        List<List<Object>> rows = exec(Transformations.parseToSql(conn, pruned));
        assertEquals(3, rows.size(), "all three fact rows, including the b-unmatched one, must remain");
    }

    @Test
    void transitive_dropBThenDropA() throws Exception {
        // A view where A is referenced ONLY by B's ON clause; dropping B makes A dead too.
        String body =
                "SELECT f.*, b.b_name FROM f " +
                "LEFT JOIN a ON f.a_id = a.a_id " +
                "LEFT JOIN b ON a.a_id = b.b_id";
        JsonNode pruned = prune("SELECT f_col FROM fv2", body);
        assertEquals(0, countJoins(pruned), "drop B (unused), then A becomes unused and is dropped");
    }

    // ---- nested types ----

    @Test
    void nestedStructField_columnKept_unusedJoinDropped() throws Exception {
        // s.x parses as column_names ["s","x"] — indistinguishable from table "s", column "x".
        // The struct column s projected by the view must survive pruning, and the unused b join
        // must still be eliminated.
        String outer = "SELECT s.x, a_name FROM fvs";
        JsonNode pruned = prune(outer, STRUCT_VIEW_BODY);

        assertEquals(1, countJoins(pruned), "b is unused and must be eliminated; a is kept");
        String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertFalse(sql.contains("b_name"), "b_name projection should be gone");
        assertTrue(sql.contains("fs.s") || sql.contains("\"s\""), "struct column s must survive pruning");
        assertEquivalentToView(pruned, outer);
        List<List<Object>> rows = exec(Transformations.parseToSql(conn, pruned));
        assertEquals(2, rows.size(), "both fact rows (incl. the b-unmatched one) must remain");
    }

    // ---- regression: qualified star over the view ----

    @Test
    void qualifiedStarOverView_returnedUnchanged() throws Exception {
        // fv.* / v.* expands to ALL view columns; treating it as "no columns used" would silently
        // drop a_name/b_name from the result. Must bail out exactly like a bare star.
        assertBailsOut("SELECT fv.* FROM fv", VIEW_BODY);
        assertBailsOut("SELECT v.* FROM fv v", VIEW_BODY);
    }

    // ---- regression: DISTINCT view body ----

    @Test
    void distinctViewBody_projectionKept_unusedJoinStillDropped() throws Exception {
        // The DISTINCT key is (f_col, a_name): fd rows (5,a10),(5,a20),(15,a10) -> 3 rows.
        // Pruning a_name from the select list would shrink the key to (f_col) -> 2 rows.
        String outer = "SELECT f_col FROM fvd";
        JsonNode pruned = prune(outer, DISTINCT_VIEW_BODY);

        String sql = Transformations.parseToSql(conn, pruned);
        assertTrue(sql.toLowerCase().contains("a_name"),
                "DISTINCT select list must not be pruned (it is the dedup key)");
        assertEquals(1, countJoins(pruned), "b is unused and still eliminable under DISTINCT");
        assertEquals(3, exec(sql).size(),
                "duplicate f_col rows from distinct (f_col, a_name) pairs must survive");
        assertEquivalentToView(pruned, outer);
    }

    // ---- GROUP BY / LIMIT bodies: projection pruning is off, join elimination still applies ----

    @Test
    void groupByViewBody_unusedJoinStillDropped() throws Exception {
        // b appears only in its own ON condition: even with GROUP BY (projection pruning
        // disabled) the b join must still be eliminated. a is pinned by select + GROUP BY.
        String body =
                "SELECT f.f_col, a.a_name, count(*) AS cnt " +
                "FROM f " +
                "LEFT JOIN a ON f.a_id = a.a_id " +
                "LEFT JOIN b ON f.b_id = b.b_id " +
                "GROUP BY f.f_col, a.a_name";
        conn.createStatement().execute("CREATE OR REPLACE VIEW fvg AS " + body);
        try {
            String outer = "SELECT f_col, cnt FROM fvg";
            JsonNode pruned = prune(outer, body);

            assertEquals(1, countJoins(pruned), "b is unused and must be eliminated despite GROUP BY");
            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertTrue(sql.contains("a_name"),
                    "projection must not be pruned under GROUP BY (a_name stays even though outer ignores it)");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fvg");
        }
    }

    @Test
    void limitViewBody_unusedJoinStillDropped() throws Exception {
        // A LIMIT modifier disables projection pruning but not join elimination.
        String body =
                "SELECT f.f_col, a.a_name " +
                "FROM f " +
                "LEFT JOIN a ON f.a_id = a.a_id " +
                "LEFT JOIN b ON f.b_id = b.b_id " +
                "LIMIT 10";
        conn.createStatement().execute("CREATE OR REPLACE VIEW fvl AS " + body);
        try {
            String outer = "SELECT f_col FROM fvl";
            JsonNode pruned = prune(outer, body);

            assertEquals(1, countJoins(pruned), "b is unused and must be eliminated despite LIMIT");
            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertTrue(sql.contains("a_name"), "projection must not be pruned when modifiers exist");
            assertTrue(sql.contains("limit"), "LIMIT must survive the rewrite");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fvl");
        }
    }

    // ---- fail-safe cases (same-instance no-op) ----

    @Test
    void bareStarOverView_returnedUnchanged() throws Exception {
        assertBailsOut("SELECT * FROM fv WHERE f_col > 10", VIEW_BODY);
    }

    @Test
    void usingJoinInBody_returnedUnchanged() throws Exception {
        String body =
                "SELECT f.*, b.b_name FROM f " +
                "LEFT JOIN b USING (b_id)";
        assertBailsOut("SELECT f_col FROM fv", body);
    }

    @Test
    void multiTableOuterFrom_returnedUnchanged() throws Exception {
        // More than one base table in the outer FROM → cannot identify the view → no-op.
        assertBailsOut("SELECT fv.f_col FROM fv, a WHERE fv.a_id = a.a_id", VIEW_BODY);
    }

    // ---- CTE (WITH) in the outer query ----

    @Test
    void cteInOuterQuery_viewStillPrunedAndCteUsageCounted() throws Exception {
        // The outer FROM is the view; an auxiliary CTE is consumed by a scalar subquery in WHERE.
        // Pruning must still fire (b unused → dropped) and the CTE body must be walked for usage
        // (collectUsage descends into cte_map), so nothing referenced there is lost.
        String outer =
                "WITH threshold AS (SELECT 10 AS m) " +
                "SELECT f_col, a_name FROM fv WHERE f_col > (SELECT m FROM threshold)";
        JsonNode pruned = prune(outer);

        assertEquals(1, countJoins(pruned), "b is unused and must be eliminated; a is kept");
        String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertFalse(sql.contains("b_name"), "b_name projection should be gone");
        assertTrue(sql.contains("threshold"), "the outer CTE must survive the rewrite");
        assertEquivalentToView(pruned, outer);
    }

    @Test
    void dimColumnUsedOnlyInCte_joinKept() throws Exception {
        // a_name is referenced ONLY inside the CTE body, nowhere in the main SELECT/WHERE.
        // Because collectUsage walks cte_map, a_name counts as used and the a join must be kept;
        // b is referenced nowhere and must be dropped. Guards against pruning a join whose only
        // consumer is a CTE.
        String outer =
                "WITH names AS (SELECT a_name FROM fv) " +
                "SELECT f_col FROM fv WHERE f_col > 10";
        JsonNode pruned = prune(outer);

        String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertTrue(sql.contains("a_name"), "a_name is used in the CTE, so the a join must be kept");
        assertFalse(sql.contains("b_name"), "b_name is used nowhere, so the b join must be dropped");
        assertEquals(1, countJoins(pruned));
        assertEquivalentToView(pruned, outer);
    }

    @Test
    void selectStarFromCteWrappingView_returnedUnchanged() throws Exception {
        // The view is referenced INSIDE a CTE; the outer FROM is that CTE (`a`), not the view.
        // Two independent guards apply — `a` resolves to a WITH-declared CTE, and the outer
        // SELECT * is a bare star — so the transform must not fire.
        assertBailsOut("WITH a AS (SELECT a_name FROM fv) SELECT * FROM a", VIEW_BODY);
    }

    @Test
    void cteShadowsViewName_returnedUnchanged() throws Exception {
        // A local CTE named `fv` shadows the catalog view `fv`: the outer FROM resolves to the CTE,
        // NOT the view. Inlining the view body here would change results, so the method must bail.
        String outer =
                "WITH fv AS (SELECT 42 AS f_col, 'zz' AS a_name, 'yy' AS b_name) " +
                "SELECT f_col FROM fv";
        assertBailsOut(outer, VIEW_BODY);
    }
}
