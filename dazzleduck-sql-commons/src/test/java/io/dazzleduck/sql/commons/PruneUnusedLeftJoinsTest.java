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
import static org.junit.jupiter.api.Assertions.assertNotSame;
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

    /** Scope-aware (three-arg) prune, locating the view by name. */
    private JsonNode prune(String outerSql, String viewName, String viewBody) throws Exception {
        JsonNode outer = Transformations.parseToTree(conn, outerSql);
        JsonNode body = Transformations.parseToTree(conn, viewBody);
        return Transformations.pruneUnusedLeftJoins(outer, viewName, body);
    }

    /** Assert the three-arg (scope-aware) method bailed out: the exact input instance comes back. */
    private void assertBailsOut(String outerSql, String viewName, String viewBody) throws Exception {
        JsonNode outer = Transformations.parseToTree(conn, outerSql);
        JsonNode body = Transformations.parseToTree(conn, viewBody);
        JsonNode pruned = Transformations.pruneUnusedLeftJoins(outer, viewName, body);
        assertSame(outer, pruned, "expected a no-op (same instance returned) for: " + outerSql);
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

    /** Count non-overlapping occurrences of {@code needle} in {@code haystack}. */
    private int countOccurrences(String haystack, String needle) {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) count++;
        return count;
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

    // ---- scope-aware (three-arg) prune: view referenced inside a CTE body ----

    @Test
    void cteWrappingView_prunedWithinCteBody() throws Exception {
        // The view is referenced inside the CTE `a`, which projects only a_name; the outer
        // SELECT * is over the CTE (expands to a_name), NOT the view. b is used nowhere in the
        // view's scope, so its join must be eliminated inside the CTE body.
        String outer = "WITH a AS (SELECT a_name FROM fv) SELECT * FROM a";
        JsonNode pruned = prune(outer, "fv", VIEW_BODY);

        assertEquals(1, countJoins(pruned), "b join must be eliminated inside the CTE body; a kept");
        String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertFalse(sql.contains("b_name"), "b_name projection should be gone");
        assertEquivalentToView(pruned, outer);
        assertEquals(3, exec(Transformations.parseToSql(conn, pruned)).size(),
                "all fact rows (incl. the b-unmatched one) must survive");
    }

    @Test
    void cteWrappingView_dimColumnUsedInCteBody_joinKept() throws Exception {
        // b_name IS referenced in the CTE body (WHERE), so the b join must be kept; a_name unused
        // in the view's scope, so the a join is eliminated.
        String outer = "WITH a AS (SELECT f_col FROM fv WHERE b_name = 'b100') SELECT * FROM a";
        JsonNode pruned = prune(outer, "fv", VIEW_BODY);

        String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertTrue(sql.contains("b_name"), "b_name is used in the CTE body, so the b join must be kept");
        assertFalse(sql.contains("a_name"), "a_name is unused, so the a join must be dropped");
        assertEquals(1, countJoins(pruned));
        assertEquivalentToView(pruned, outer);
    }

    @Test
    void threeArg_topLevelViewFrom_delegatesToV1() throws Exception {
        // When the outer FROM is the view itself, the scope-aware entry point matches the v1 path.
        String outer = "SELECT f_col, a_name FROM fv WHERE f_col > 10";
        JsonNode pruned = prune(outer, "fv", VIEW_BODY);

        assertEquals(1, countJoins(pruned), "b eliminated, a kept");
        assertEquivalentToView(pruned, outer);
    }

    @Test
    void cteWrappingView_bothDimsUsedInCteBody_returnedUnchanged() throws Exception {
        // Both dimension columns flow through the CTE body → nothing to prune or eliminate → no-op.
        assertBailsOut("WITH a AS (SELECT a_name, b_name FROM fv) SELECT * FROM a", "fv", VIEW_BODY);
    }

    @Test
    void starOverViewInsideCteBody_returnedUnchanged() throws Exception {
        // The CTE body does SELECT * over the view — its columns cannot be enumerated, so the STAR
        // bail applies within the view's scope even though the reference is nested.
        assertBailsOut("WITH a AS (SELECT * FROM fv) SELECT a_name FROM a", "fv", VIEW_BODY);
    }

    @Test
    void viewNameReboundBySiblingCte_returnedUnchanged() throws Exception {
        // `fv` is declared as a CTE in the same WITH, so the reference inside CTE `a` resolves to
        // that CTE, not the catalog view. Must bail.
        String outer =
                "WITH fv AS (SELECT 1 AS a_name), a AS (SELECT a_name FROM fv) SELECT * FROM a";
        assertBailsOut(outer, "fv", VIEW_BODY);
    }

    @Test
    void viewReferencedByTwoCteBodies_eachPrunedInItsOwnScope() throws Exception {
        // Both CTEs reference the view; each is pruned against its own usage:
        // CTE a uses a_name → keeps join a, drops join b. CTE b uses f_col only → drops both.
        String outer =
                "WITH a AS (SELECT a_name FROM fv), b AS (SELECT f_col FROM fv) " +
                "SELECT * FROM a, b";
        JsonNode pruned = prune(outer, "fv", VIEW_BODY);

        String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertEquals(1, countOccurrences(sql, "left join"),
                "one LEFT JOIN total: a's inline keeps join a; b's inline drops both");
        assertFalse(sql.contains("b_name"), "b_name is unused in every scope");
        assertEquivalentToView(pruned, outer);
    }

    @Test
    void viewReferencedTopLevelAndInCte_bothPrunedIndependently() throws Exception {
        // Top-level scope uses f_col + b_name (drops join a); CTE scope uses a_name (drops join b).
        // Each reference is pruned against its own scope's usage.
        String outer =
                "WITH c AS (SELECT a_name FROM fv) " +
                "SELECT f_col, b_name FROM fv WHERE f_col > (SELECT count(*) FROM c)";
        JsonNode pruned = prune(outer, "fv", VIEW_BODY);

        assertEquals(2, countJoins(pruned),
                "top-level inline keeps join b, CTE inline keeps join a — one join each");
        String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertTrue(sql.contains("a_name"), "CTE scope needs a_name");
        assertTrue(sql.contains("b_name"), "top-level scope needs b_name");
        assertEquivalentToView(pruned, outer);
    }

    @Test
    void starScopeSkipped_otherReferenceStillPruned() throws Exception {
        // CTE a does SELECT * over the view (columns not enumerable → skipped, stays a raw view
        // reference); CTE b is still pruned to zero joins.
        String outer =
                "WITH a AS (SELECT * FROM fv), b AS (SELECT f_col FROM fv) " +
                "SELECT a.a_name, b.f_col FROM a, b";
        JsonNode pruned = prune(outer, "fv", VIEW_BODY);

        String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertEquals(0, countOccurrences(sql, "left join"),
                "b's inline drops both joins; a's star scope is skipped, not inlined");
        assertTrue(sql.contains("fv"), "the star CTE must still reference the view by name");
        assertEquivalentToView(pruned, outer);
    }

    // ---- review probes: edge cases ----

    @Test
    void nestedWithInsideCteBodyShadowsView_returnedUnchanged() throws Exception {
        // The CTE body carries its own WITH that rebinds `fv`; the inner FROM fv resolves to that
        // nested CTE, not the catalog view. Inlining the view body there would replace 'inner'
        // with real fact data. Must bail.
        String outer =
                "WITH a AS (WITH fv AS (SELECT 'inner' AS a_name) SELECT a_name FROM fv) " +
                "SELECT * FROM a";
        assertBailsOut(outer, "fv", VIEW_BODY);
    }

    @Test
    void viewInCteAndOuterFromJoin_cteBodyPruned_outerRefUntouched() throws Exception {
        // The view appears both inside CTE `a` and in the outer FROM join. Only the CTE body is
        // eligible (outer FROM is not a single base table); the outer fv reference must keep
        // pointing at the real view and results must be equivalent.
        String outer =
                "WITH a AS (SELECT a_name FROM fv) " +
                "SELECT a.a_name, fv.b_name FROM a, fv";
        JsonNode outerAst = Transformations.parseToTree(conn, outer);
        JsonNode pruned = Transformations.pruneUnusedLeftJoins(
                outerAst, "fv", Transformations.parseToTree(conn, VIEW_BODY));

        assertNotSame(outerAst, pruned, "the CTE body is eligible, so a rewrite must occur");
        String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertTrue(sql.contains("fv"), "the outer FROM must still reference the view by name");
        assertEquivalentToView(pruned, outer);
    }

    @Test
    void schemaQualifiedSameNamedView_returnedUnchanged() throws Exception {
        // s2.fv is a DIFFERENT view that happens to share the name. viewName "fv" is unqualified,
        // so a schema-qualified reference must not match — inlining the default-schema body in its
        // place would silently swap the view. Must bail.
        conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS s2");
        conn.createStatement().execute(
                "CREATE OR REPLACE VIEW s2.fv AS SELECT 'other' AS a_name, 'x' AS b_name, 0 AS f_col");
        try {
            assertBailsOut("WITH a AS (SELECT a_name FROM s2.fv) SELECT * FROM a", "fv", VIEW_BODY);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS s2.fv");
            conn.createStatement().execute("DROP SCHEMA IF EXISTS s2");
        }
    }

    @Test
    void aliasedViewRefInCteBody_pruned() throws Exception {
        // The view is referenced with an alias inside the CTE body; qualification uses the alias.
        String outer = "WITH a AS (SELECT x.a_name FROM fv x) SELECT * FROM a";
        JsonNode pruned = prune(outer, "fv", VIEW_BODY);

        assertEquals(1, countJoins(pruned), "b join eliminated; a kept");
        assertEquivalentToView(pruned, outer);
    }

    // ---- qualified viewName matching ----

    @Test
    void qualifiedViewName_matchesQualifiedRefInCteBody() throws Exception {
        // viewName "s3.fv" must match FROM s3.fv inside the CTE body and prune there.
        conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS s3");
        conn.createStatement().execute("CREATE OR REPLACE VIEW s3.fv AS " + VIEW_BODY);
        try {
            String outer = "WITH a AS (SELECT a_name FROM s3.fv) SELECT * FROM a";
            JsonNode pruned = prune(outer, "s3.fv", VIEW_BODY);

            assertEquals(1, countJoins(pruned), "b join eliminated inside the CTE body; a kept");
            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertFalse(sql.contains("b_name"), "b_name projection should be gone");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS s3.fv");
            conn.createStatement().execute("DROP SCHEMA IF EXISTS s3");
        }
    }

    @Test
    void qualifiedViewName_matchesQualifiedRefAtTopLevel() throws Exception {
        // Top-level FROM s3.fv with viewName "s3.fv" delegates to the v1 path.
        conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS s3");
        conn.createStatement().execute("CREATE OR REPLACE VIEW s3.fv AS " + VIEW_BODY);
        try {
            String outer = "SELECT f_col, a_name FROM s3.fv WHERE f_col > 10";
            JsonNode pruned = prune(outer, "s3.fv", VIEW_BODY);

            assertEquals(1, countJoins(pruned), "b eliminated, a kept");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS s3.fv");
            conn.createStatement().execute("DROP SCHEMA IF EXISTS s3");
        }
    }

    @Test
    void qualifiedViewName_doesNotMatchUnqualifiedRef() throws Exception {
        // An unqualified FROM fv resolves via the search path — invisible at the AST level — so a
        // qualified viewName must not match it. No-op.
        assertBailsOut("WITH a AS (SELECT a_name FROM fv) SELECT * FROM a", "s3.fv", VIEW_BODY);
        assertBailsOut("SELECT f_col, a_name FROM fv", "s3.fv", VIEW_BODY);
    }

    @Test
    void qualifiedRefAtTopLevel_notShadowedByCteOfSameBareName_stillPruned() throws Exception {
        // Top-level variant of the shadow case: a CTE named `fv` exists, but the top-level FROM is
        // the qualified s3.fv, which always resolves to the catalog. The v1 delegation path must
        // not let the bare-name CTE block pruning.
        conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS s3");
        conn.createStatement().execute("CREATE OR REPLACE VIEW s3.fv AS " + VIEW_BODY);
        try {
            String outer =
                    "WITH fv AS (SELECT 1 AS t) " +
                    "SELECT f_col, a_name FROM s3.fv WHERE f_col > (SELECT t FROM fv)";
            JsonNode outerAst = Transformations.parseToTree(conn, outer);
            JsonNode pruned = Transformations.pruneUnusedLeftJoins(
                    outerAst, "s3.fv", Transformations.parseToTree(conn, VIEW_BODY));

            assertNotSame(outerAst, pruned,
                    "the bare-name CTE must not block pruning of the qualified s3.fv reference");
            assertEquals(1, countJoins(pruned), "b eliminated, a kept — view inlined at top level");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS s3.fv");
            conn.createStatement().execute("DROP SCHEMA IF EXISTS s3");
        }
    }

    @Test
    void qualifiedRef_notShadowedByCteOfSameBareName_stillPruned() throws Exception {
        // A CTE named `fv` shadows only unqualified references; s3.fv always resolves to the
        // catalog. With a qualified viewName the shadow bail must not block pruning.
        conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS s3");
        conn.createStatement().execute("CREATE OR REPLACE VIEW s3.fv AS " + VIEW_BODY);
        try {
            String outer =
                    "WITH fv AS (SELECT 'cte' AS tag), a AS (SELECT a_name FROM s3.fv) " +
                    "SELECT a.a_name, fv.tag FROM a, fv";
            JsonNode pruned = prune(outer, "s3.fv", VIEW_BODY);

            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertFalse(sql.contains("b_name"), "b join must be pruned despite the fv-named CTE");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS s3.fv");
            conn.createStatement().execute("DROP SCHEMA IF EXISTS s3");
        }
    }
}
