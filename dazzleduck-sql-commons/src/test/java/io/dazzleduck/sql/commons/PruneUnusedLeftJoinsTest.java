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

    /**
     * The shape issue #483 asks for: a lookup pre-aggregated per fact row and LEFT JOINed on that
     * row's identity, instead of a correlated scalar subquery. Unique by construction — one row per
     * ({@code rid}, {@code cid}) group — and both group keys are pinned by the ON condition.
     */
    private static final String UNIQUE_SUBQUERY_VIEW_BODY =
            "SELECT t.f_id, t.customer_id, g.resolved AS rules " +
            "FROM fl t " +
            "LEFT JOIN (" +
            "    SELECT u.rid, u.cid, list(d.name) AS resolved " +
            "    FROM (SELECT t2.f_id AS rid, t2.customer_id AS cid, UNNEST(t2.matched_rules) AS mr FROM fl t2) u " +
            "    JOIN dim_rule d ON d.rule_id = u.mr " +
            "    GROUP BY u.rid, u.cid" +
            ") g ON g.rid = t.f_id AND g.cid = t.customer_id";

    /** Same, with the subquery's columns renamed positionally by a column-alias list. */
    private static final String RENAMED_SUBQUERY_VIEW_BODY =
            "SELECT t.f_id, g.v AS rules " +
            "FROM fl t " +
            "LEFT JOIN (SELECT t2.f_id, list(t2.customer_id) FROM fl t2 GROUP BY t2.f_id) g(k, v) " +
            "  ON g.k = t.f_id";

    /** DISTINCT dedups on the whole select list, so pinning all of it pins the row. */
    private static final String DISTINCT_SUBQUERY_VIEW_BODY =
            "SELECT t.f_id, g.k AS dim " +
            "FROM fl t " +
            "LEFT JOIN (SELECT DISTINCT d.rule_id AS k FROM dim_rule d) g ON g.k = t.customer_id";

    /**
     * Two pre-aggregated lookups whose bodies both use the inner alias {@code t2} — the shape the
     * real consumer has (four lookups, same inner alias in each). An inner alias is invisible
     * outside its own subquery, so reusing it must not make the second lookup look correlated.
     */
    private static final String TWO_LOOKUP_VIEW_BODY =
            "SELECT t.f_id, g1.r1 AS rules1, g2.r2 AS rules2 " +
            "FROM fl t " +
            "LEFT JOIN (SELECT t2.f_id AS rid, list(t2.customer_id) AS r1 FROM fl t2 GROUP BY t2.f_id) g1 " +
            "  ON g1.rid = t.f_id " +
            "LEFT JOIN (SELECT t2.f_id AS rid, list(t2.customer_id) AS r2 FROM fl t2 GROUP BY t2.f_id) g2 " +
            "  ON g2.rid = t.f_id";

    /**
     * A STAR in a grouped subquery. {@code a.*} is expanded in place, so its column takes the name
     * {@code f_id} and the grouping key {@code b.f_id} is renamed {@code f_id_1} on the way out —
     * which means {@code ON g.f_id} pins the star's column and leaves {@code b.f_id} unpinned. The
     * subquery therefore has several rows per fact row (9 against 3), and dropping the join would
     * collapse them. The star side is a single-column table because {@code a.*} has to bind
     * against the GROUP BY.
     */
    private static final String STAR_SUBQUERY_VIEW_BODY =
            "SELECT t.f_id AS f_id, t.customer_id AS unused_col FROM fl t " +
            "LEFT JOIN (SELECT a.*, a.f_id AS af, b.f_id FROM fk a, fl b GROUP BY a.f_id, b.f_id) g " +
            "  ON g.f_id = t.f_id AND g.af = t.f_id";

    /**
     * A duplicate among <em>non-key</em> names shifts a key's name. DuckDB renames the second
     * {@code k} to {@code k_1}, which collides with the explicit {@code a.x AS k_1}, so the key is
     * pushed to {@code k_1_1} and {@code ON g.k_1} pins {@code max(b.k)} instead — the same value
     * in every group, so one fact row matches all three (3 rows against 1).
     */
    private static final String RENAME_SHIFT_VIEW_BODY =
            "SELECT t.x FROM rs_t t " +
            "LEFT JOIN (SELECT max(a.k) AS k, max(b.k) AS k, a.x AS k_1 FROM rs_a a, rs_b b GROUP BY a.x) g " +
            "  ON g.k_1 = t.x";

    /**
     * An unaliased expression's generated name collides with an explicit alias: DuckDB names
     * {@code max(b.k)} after its text, renames the key {@code a.x AS "max(b.k)"} to
     * {@code "max(b.k)_1"}, and {@code ON g."max(b.k)"} pins the aggregate — 5 in every group.
     */
    private static final String AUTO_NAME_VIEW_BODY =
            "SELECT t.x FROM rs_t t " +
            "LEFT JOIN (SELECT max(b.k), a.x AS \"max(b.k)\" FROM rs_a a, rs_b b GROUP BY a.x) g " +
            "  ON g.\"max(b.k)\" = t.x";

    /** Three grouping keys, all pinned — including a derived one, so the keys are not all columns. */
    private static final String THREE_KEY_VIEW_BODY =
            "SELECT t.f_id, g.r AS rules FROM fl t " +
            "LEFT JOIN (SELECT t2.f_id AS a, t2.customer_id AS b, t2.f_id % 2 AS c, list(t2.f_id) AS r " +
            "           FROM fl t2 GROUP BY t2.f_id, t2.customer_id, t2.f_id % 2) g " +
            "  ON g.a = t.f_id AND g.b = t.customer_id AND g.c = t.f_id % 2";

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
        // Unique-subquery fixture (issue #483). Row 2 has an empty list, row 3 a NULL list, and
        // row 5 an id that resolves to nothing — each produces no group, so the LEFT JOIN is what
        // keeps the fact row. Row 4 repeats an id, so the group holds a duplicate.
        conn.createStatement().execute("CREATE TABLE fl (f_id INT, customer_id INT, matched_rules INT[])");
        conn.createStatement().execute(
                "INSERT INTO fl VALUES (1,7,[10,20]),(2,7,[]),(3,8,NULL),(4,9,[10,10]),(5,9,[999])");
        conn.createStatement().execute("CREATE TABLE dim_rule (rule_id INT, name VARCHAR)");
        conn.createStatement().execute("INSERT INTO dim_rule VALUES (10,'r10'),(20,'r20')");
        conn.createStatement().execute("CREATE VIEW flv AS " + UNIQUE_SUBQUERY_VIEW_BODY);
        conn.createStatement().execute("CREATE VIEW flvr AS " + RENAMED_SUBQUERY_VIEW_BODY);
        conn.createStatement().execute("CREATE VIEW flvd AS " + DISTINCT_SUBQUERY_VIEW_BODY);
        conn.createStatement().execute("CREATE VIEW flv2 AS " + TWO_LOOKUP_VIEW_BODY);
        conn.createStatement().execute("CREATE VIEW flv3 AS " + THREE_KEY_VIEW_BODY);
        conn.createStatement().execute("CREATE TABLE fk (f_id INT)");
        conn.createStatement().execute("INSERT INTO fk VALUES (1),(2),(3),(4),(5)");
        conn.createStatement().execute("CREATE VIEW flvs AS " + STAR_SUBQUERY_VIEW_BODY);
        conn.createStatement().execute("CREATE TABLE rs_t AS SELECT 5 AS x");
        conn.createStatement().execute("CREATE TABLE rs_a AS SELECT * FROM (VALUES (1,1),(2,2),(3,3)) v(k, x)");
        conn.createStatement().execute("CREATE TABLE rs_b AS SELECT 5 AS k");
        conn.createStatement().execute("CREATE VIEW rsv AS " + RENAME_SHIFT_VIEW_BODY);
        conn.createStatement().execute("CREATE VIEW rsav AS " + AUTO_NAME_VIEW_BODY);
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.createStatement().execute("DROP VIEW IF EXISTS fv");
        conn.createStatement().execute("DROP VIEW IF EXISTS fvs");
        conn.createStatement().execute("DROP VIEW IF EXISTS fvd");
        conn.createStatement().execute("DROP VIEW IF EXISTS flv");
        conn.createStatement().execute("DROP VIEW IF EXISTS flvr");
        conn.createStatement().execute("DROP VIEW IF EXISTS flvd");
        conn.createStatement().execute("DROP VIEW IF EXISTS flv2");
        conn.createStatement().execute("DROP VIEW IF EXISTS flv3");
        conn.createStatement().execute("DROP VIEW IF EXISTS flvs");
        conn.createStatement().execute("DROP VIEW IF EXISTS rsv");
        conn.createStatement().execute("DROP VIEW IF EXISTS rsav");
        conn.createStatement().execute("DROP TABLE IF EXISTS f");
        conn.createStatement().execute("DROP TABLE IF EXISTS fs");
        conn.createStatement().execute("DROP TABLE IF EXISTS fd");
        conn.createStatement().execute("DROP TABLE IF EXISTS fl");
        conn.createStatement().execute("DROP TABLE IF EXISTS dim_rule");
        conn.createStatement().execute("DROP TABLE IF EXISTS fk");
        conn.createStatement().execute("DROP TABLE IF EXISTS rs_t");
        conn.createStatement().execute("DROP TABLE IF EXISTS rs_a");
        conn.createStatement().execute("DROP TABLE IF EXISTS rs_b");
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

    /** The optimized plan for {@code sql}, as one string. */
    private String explain(String sql) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN " + sql)) {
            while (rs.next()) {
                plan.append(rs.getString(rs.getMetaData().getColumnCount())).append('\n');
            }
        }
        return plan.toString();
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
     * The pruned output must return the same rows as the original query against the real view,
     * <em>with the same multiplicities</em>.
     *
     * <p>{@link TestUtils#isEqual} alone is not enough here: it compares with a bidirectional plain
     * {@code EXCEPT}, which has set semantics, so duplicate rows are invisible to it. An unsound
     * LEFT JOIN elimination fails precisely by changing how many times each left row appears — the
     * rows it loses are duplicates of rows it keeps — so a set comparison passes on exactly the bug
     * this suite exists to catch. The rows are therefore also compared as multisets.
     */
    private void assertEquivalentToView(JsonNode pruned, String originalOuterSql) throws Exception {
        String prunedSql = Transformations.parseToSql(conn, pruned);
        TestUtils.isEqual(originalOuterSql, prunedSql); // a readable diff when the sets differ
        List<String> expected = sortedRows(originalOuterSql);
        List<String> actual = sortedRows(prunedSql);
        assertEquals(expected, actual,
                "row multiplicities differ (" + expected.size() + " expected, " + actual.size()
                        + " after pruning) for: " + prunedSql);
    }

    /** The rows of {@code sql} rendered as strings and sorted, so two results compare as multisets. */
    private List<String> sortedRows(String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        for (List<Object> row : exec(sql)) rows.add(String.valueOf(row));
        java.util.Collections.sort(rows);
        return rows;
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

    // ---- shared RLS filter-CTE with several consumers ----

    /**
     * The RLS authorizer emits ONE filter CTE per table, so a query referencing the view twice has
     * both references rewritten to the same `___fv`. Here `a_name` is used only inside the sibling
     * CTE and the outer scope needs only `b_name` — collecting consumer usage from the outer scope
     * alone prunes join `a` out from under the sibling, and the query no longer binds.
     */
    @Test
    void sharedFilterCte_columnUsedOnlyInSiblingCte_stillBinds() throws Exception {
        String outer =
                "WITH ___fv AS (SELECT * FROM fv WHERE f_col > 0), " +
                "     page AS (SELECT f_id FROM ___fv WHERE a_name = 'a10') " +
                "SELECT x.b_name FROM ___fv x JOIN page p ON p.f_id = x.f_id";
        JsonNode pruned = prune(outer, "fv", VIEW_BODY);

        assertEquivalentToView(pruned, outer);
    }

    /** The two-phase page shape: a narrow CTE picks the page, the outer dresses it. */
    @Test
    void sharedFilterCte_narrowSiblingAndWideOuter_bothCorrect() throws Exception {
        String outer =
                "WITH ___fv AS (SELECT * FROM fv WHERE f_col > 0), " +
                "     page AS (SELECT f_id FROM ___fv ORDER BY f_id LIMIT 2) " +
                "SELECT x.f_id, x.a_name, x.b_name FROM ___fv x JOIN page p ON p.f_id = x.f_id " +
                "ORDER BY x.f_id";
        JsonNode pruned = prune(outer, "fv", VIEW_BODY);

        assertEquivalentToView(pruned, outer);
    }

    /**
     * The point of the split: a narrow consumer must not inherit a wide one's joins. `page` needs
     * only f_id and the outer only b_name — so page's clone should carry zero joins while the
     * outer's carries exactly one. Pruned against the union (the pre-split behaviour) both scopes
     * would pay for join b.
     */
    @Test
    void sharedFilterCte_narrowConsumerGetsItsOwnJoinFreeClone() throws Exception {
        String outer =
                "WITH ___fv AS (SELECT * FROM fv WHERE f_col > 0), " +
                "     page AS (SELECT f_id FROM ___fv ORDER BY f_id LIMIT 2) " +
                "SELECT x.f_id, x.b_name FROM ___fv x JOIN page p ON p.f_id = x.f_id " +
                "ORDER BY x.f_id";
        JsonNode pruned = prune(outer, "fv", VIEW_BODY);

        assertEquals(0, joinsInCte(pruned, "___fv__c0"), "page's clone needs no dimension join");
        assertEquals(1, joinsInCte(pruned, "___fv__c1"), "the outer's clone needs only join b");
        assertEquivalentToView(pruned, outer);
    }

    /**
     * The shared filter-CTE's only consumer is a sibling CTE, not the enclosing statement (the
     * outer query selects from `page`, never from `___fv`), and `a_name` is read only inside that
     * sibling. The single-consumer path must read usage from the sibling — reading it from the
     * top-level scope instead never sees `a_name`, prunes join `a` out from under `page`, and
     * `page`'s `SELECT a_name` no longer binds.
     */
    @Test
    void sharedFilterCte_soleConsumerIsSiblingCte_keepsItsColumns() throws Exception {
        String outer =
                "WITH ___fv AS (SELECT * FROM fv WHERE f_col > 0), " +
                "     page AS (SELECT a_name, f_id FROM ___fv) " +
                "SELECT p.f_id FROM page p ORDER BY p.f_id";
        JsonNode pruned = prune(outer, "fv", VIEW_BODY);

        assertEquals(1, joinsInCte(pruned, "___fv"),
                "join a is read only by the sibling CTE and must survive its pruning");
        assertEquivalentToView(pruned, outer);
    }

    /**
     * Two star-filter CTEs over the same view. Splitting the first grows the cte_map, so visiting
     * the references low-index-first would leave the second CTE's precomputed index pointing at a
     * freshly-inserted clone — `w2` would never be pruned. Visiting high-index-first keeps every
     * unvisited index valid, so `w2` is still narrowed to its lone consumer's `a_name`: join `b`
     * eliminated, join `a` kept (one join in the inlined body).
     */
    @Test
    void twoFilterCtesOverSameView_secondStillPrunedAfterFirstSplits() throws Exception {
        String outer =
                "WITH w1 AS (SELECT * FROM fv WHERE f_col > 0), " +
                "     w2 AS (SELECT * FROM fv WHERE f_col > 5), " +
                "     page AS (SELECT f_id FROM w1), " +
                "     side AS (SELECT f_id, a_name FROM w2) " +
                "SELECT x.b_name, p.f_id, s.a_name " +
                "FROM w1 x JOIN page p ON p.f_id = x.f_id " +
                "          JOIN side s ON s.f_id = x.f_id " +
                "ORDER BY p.f_id";
        JsonNode pruned = prune(outer, "fv", VIEW_BODY);

        assertEquals(1, joinsInCte(pruned, "w2"),
                "w2 must be pruned to its consumer's a_name — reached only if splitting w1 did not "
                        + "shift w2 out from under its index");
        assertEquivalentToView(pruned, outer);
    }

    /** JOIN nodes inside the CTE declared under {@code key}; -1 when no such CTE exists. */
    private int joinsInCte(JsonNode pruned, String key) {
        JsonNode map = pruned.path("statements").path(0).path("node").path("cte_map").path("map");
        for (JsonNode entry : map) {
            if (key.equalsIgnoreCase(entry.path("key").asText())) {
                return countJoins(AstNodes.cteBody(entry));
            }
        }
        return -1;
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

    // ---- complex select-list entries: droppable when aliased and unused ----
    //
    // An entry is droppable when we can name it and the outer query never uses that name — which
    // now includes aliased CASE/CAST/FUNCTION/SUBQUERY entries, not only COLUMN_REFs. Two payoffs:
    // the expression stops being computed, and it stops contributing references, so an expression
    // whose lambda parameter reads as an unqualified column no longer disables join elimination
    // for the whole view. STARs and unaliased expressions are still kept unconditionally.

    /** Body with an aliased plain expression and both dimension joins. */
    private static final String EXPR_VIEW_BODY =
            "SELECT f.f_id, f.f_col, a.a_name, b.b_name, f.f_col + f.a_id AS total " +
            "FROM f " +
            "LEFT JOIN a ON f.a_id = a.a_id " +
            "LEFT JOIN b ON f.b_id = b.b_id";

    /** Body with a lambda: the parameter `r` is a single-part COLUMN_REF to the reference
     *  counter, so while this entry survives, no join in the body is eliminable. */
    private static final String LAMBDA_VIEW_BODY =
            "SELECT f.f_id, f.f_col, a.a_name, b.b_name, " +
            "list_filter([f.a_id], lambda r: r > 0) AS flt " +
            "FROM f " +
            "LEFT JOIN a ON f.a_id = a.a_id " +
            "LEFT JOIN b ON f.b_id = b.b_id";

    @Test
    void unusedAliasedExpression_isDropped() throws Exception {
        conn.createStatement().execute("CREATE OR REPLACE VIEW fve AS " + EXPR_VIEW_BODY);
        try {
            String outer = "SELECT f_col FROM fve WHERE f_col > 10";
            JsonNode pruned = prune(outer, EXPR_VIEW_BODY);

            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertFalse(sql.contains("total"), "the unused aliased expression must be dropped: " + sql);
            assertEquals(0, countJoins(pruned), "no dimension column is used");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fve");
        }
    }

    @Test
    void usedAliasedExpression_isKept() throws Exception {
        conn.createStatement().execute("CREATE OR REPLACE VIEW fve AS " + EXPR_VIEW_BODY);
        try {
            String outer = "SELECT f_col, total FROM fve";
            JsonNode pruned = prune(outer, EXPR_VIEW_BODY);

            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertTrue(sql.contains("total"), "the referenced expression must survive: " + sql);
            assertEquals(0, countJoins(pruned),
                    "the expression references only f, so both joins still go");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fve");
        }
    }

    @Test
    void unaliasedExpression_isKept() throws Exception {
        // No alias → its output name is whatever DuckDB derives from the expression text, which
        // the prune does not model. Kept regardless of projection; the unused join still goes.
        String body =
                "SELECT f.f_id, f.f_col, a.a_name, f.f_col + f.a_id " +
                "FROM f " +
                "LEFT JOIN a ON f.a_id = a.a_id " +
                "LEFT JOIN b ON f.b_id = b.b_id";
        conn.createStatement().execute("CREATE OR REPLACE VIEW fvu AS " + body);
        try {
            String outer = "SELECT f_col FROM fvu";
            JsonNode pruned = prune(outer, body);

            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertTrue(sql.contains("f_col + "),
                    "an unaliased expression is unnameable and must be kept: " + sql);
            assertEquals(0, countJoins(pruned),
                    "the kept expression references only f, and the unused a_name column is "
                    + "pruned as usual — so both joins still go");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fvu");
        }
    }

    /** The §7.0 regression case (analytics-schema): dropping the unused lambda entry removes its
     *  unqualified `r` before join elimination counts references, so elimination works again. */
    @Test
    void unusedLambdaExpression_droppedAndJoinsEliminated() throws Exception {
        conn.createStatement().execute("CREATE OR REPLACE VIEW fvl AS " + LAMBDA_VIEW_BODY);
        try {
            String outer = "SELECT f_col FROM fvl WHERE f_col > 10";
            JsonNode pruned = prune(outer, LAMBDA_VIEW_BODY);

            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertFalse(sql.contains("list_filter"), "the unused lambda entry must be dropped: " + sql);
            assertEquals(0, countJoins(pruned),
                    "with the lambda gone, its parameter no longer reads as an unqualified column, "
                    + "so both joins are eliminable");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fvl");
        }
    }

    /** The complement: a lambda the caller DID ask for still blocks elimination — that limitation
     *  is unchanged, this feature only removes entries nobody uses. */
    @Test
    void usedLambdaExpression_keptAndJoinsRetained() throws Exception {
        conn.createStatement().execute("CREATE OR REPLACE VIEW fvl AS " + LAMBDA_VIEW_BODY);
        try {
            String outer = "SELECT f_col, flt FROM fvl";
            JsonNode pruned = prune(outer, LAMBDA_VIEW_BODY);

            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertTrue(sql.contains("list_filter"), "the referenced lambda entry must survive: " + sql);
            assertEquals(2, countJoins(pruned),
                    "the surviving lambda parameter still blocks elimination of every join");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fvl");
        }
    }

    @Test
    void allEntriesUnused_selectListIsNeverEmptied() throws Exception {
        // The outer query references no view column at all, so every (droppable) entry is unused.
        // The kept-list-empty guard must leave the select list alone rather than emit `SELECT FROM`;
        // join elimination still fires independently.
        String body =
                "SELECT f.f_col + f.a_id AS total " +
                "FROM f " +
                "LEFT JOIN a ON f.a_id = a.a_id";
        conn.createStatement().execute("CREATE OR REPLACE VIEW fvx AS " + body);
        try {
            String outer = "SELECT 42 AS c FROM fvx";
            JsonNode pruned = prune(outer, body);

            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertTrue(sql.contains("total"),
                    "the last entry must be kept — a select list can never be emptied: " + sql);
            assertEquals(0, countJoins(pruned), "the unused join is still eliminated");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fvx");
        }
    }

    // ---- shapes where the select list is semantically load-bearing: pruning must not run ----

    /** GROUP BY ALL derives the grouping key FROM the select list, and serializes with EMPTY
     *  group_expressions (aggregate_handling=FORCE_AGGREGATES is the only marker) — so without its
     *  own gate, dropping an entry silently changes the grouping. Join elimination still applies. */
    @Test
    void groupByAllBody_selectListNotPruned_unusedJoinStillDropped() throws Exception {
        String body =
                "SELECT f.a_id, a.a_name, sum(f.f_col) AS s " +
                "FROM f " +
                "LEFT JOIN a ON f.a_id = a.a_id " +
                "LEFT JOIN b ON f.b_id = b.b_id " +
                "GROUP BY ALL";
        conn.createStatement().execute("CREATE OR REPLACE VIEW fvall AS " + body);
        try {
            String outer = "SELECT a_id, s FROM fvall";
            JsonNode pruned = prune(outer, body);

            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertTrue(sql.contains("a_name"),
                    "under GROUP BY ALL the select list IS the grouping key — a_name must stay: " + sql);
            assertEquals(1, countJoins(pruned), "b is unused and still eliminable under GROUP BY ALL");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fvall");
        }
    }

    /** QUALIFY is its own field (not a modifier) and may reference a select-list alias — dropping
     *  the aliased window function would unbind a valid query. With an inline window in the
     *  QUALIFY (every reference qualified), the gate is what keeps the rn entry; join elimination
     *  still applies. */
    @Test
    void qualifyBody_selectListNotPruned_unusedJoinStillDropped() throws Exception {
        String body =
                "SELECT f.f_id, f.f_col, " +
                "row_number() OVER (PARTITION BY f.a_id ORDER BY f.f_id) AS rn " +
                "FROM f " +
                "LEFT JOIN b ON f.b_id = b.b_id " +
                "QUALIFY row_number() OVER (PARTITION BY f.a_id ORDER BY f.f_id) = 1";
        conn.createStatement().execute("CREATE OR REPLACE VIEW fvq AS " + body);
        try {
            String outer = "SELECT f_col FROM fvq";
            JsonNode pruned = prune(outer, body);

            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertTrue(sql.contains("rn"),
                    "the QUALIFY gate must keep the whole select list, rn included: " + sql);
            assertEquals(0, countJoins(pruned), "b is unused and still eliminable under QUALIFY");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fvq");
        }
    }

    /** The common QUALIFY spelling references the alias unqualified ({@code QUALIFY rn = 1}), which
     *  ALSO reads as an unqualified column to the join-elimination counter — so the entire prune
     *  no-ops for that shape: select list gated, joins retained, input returned as-is. */
    @Test
    void qualifyReferencingAnAlias_wholePruneBailsOut() throws Exception {
        String body =
                "SELECT f.f_id, f.f_col, " +
                "row_number() OVER (PARTITION BY f.a_id ORDER BY f.f_id) AS rn " +
                "FROM f " +
                "LEFT JOIN b ON f.b_id = b.b_id " +
                "QUALIFY rn = 1";
        assertBailsOut("SELECT f_col FROM fvq2", body);
    }

    /** HAVING may reference a select alias, and HAVING without GROUP BY also forces implicit
     *  aggregation — either way the select list must be left alone. */
    @Test
    void havingBody_selectListNotPruned() throws Exception {
        String body =
                "SELECT sum(f.f_col) AS s, 42 AS tag " +
                "FROM f " +
                "LEFT JOIN a ON f.a_id = a.a_id " +
                "HAVING sum(f.f_col) > 0";
        conn.createStatement().execute("CREATE OR REPLACE VIEW fvh AS " + body);
        try {
            String outer = "SELECT tag FROM fvh";
            JsonNode pruned = prune(outer, body);

            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertTrue(sql.contains("sum("), "HAVING forces aggregation — s must stay: " + sql);
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fvh");
        }
    }

    /** Implicit aggregation has NO parse-level marker (STANDARD_HANDLING, empty groups, no HAVING):
     *  `SELECT 42 AS tag, sum(x) AS s` is one row, and dropping s would make it N. Aggregate-ness is
     *  a binder fact, so the guard is structural: when nothing kept references a column, every kept
     *  entry may be a plain constant, and nothing is dropped. Join elimination is unaffected. */
    @Test
    void implicitAggregationBody_aggregateNotDroppedForAConstantProjection() throws Exception {
        String body =
                "SELECT 42 AS tag, sum(f.f_col) AS s " +
                "FROM f " +
                "LEFT JOIN a ON f.a_id = a.a_id";
        conn.createStatement().execute("CREATE OR REPLACE VIEW fvagg AS " + body);
        try {
            String outer = "SELECT tag FROM fvagg";
            JsonNode pruned = prune(outer, body);

            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertTrue(sql.contains("sum("),
                    "dropping the only aggregate would turn 1 row into N — s must stay: " + sql);
            assertEquals(0, countJoins(pruned), "a is unused and still eliminable");
            assertEquals(1, exec(Transformations.parseToSql(conn, pruned)).size(),
                    "the pruned body must still aggregate to one row");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fvagg");
        }
    }

    // ---- case-insensitive identifier matching (DuckDB resolves identifiers case-insensitively) ----

    /** A consumer spelling a column in a different case binds in DuckDB, so the prune must read it
     *  as used — exact matching dropped the entry and the pruned body then failed to bind. */
    @Test
    void differentlyCasedReference_stillCountsAsUsed() throws Exception {
        conn.createStatement().execute("CREATE OR REPLACE VIEW fve AS " + EXPR_VIEW_BODY);
        try {
            String outer = "SELECT F_COL, TOTAL FROM fve";
            JsonNode pruned = prune(outer, EXPR_VIEW_BODY);

            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertTrue(sql.contains("total"), "TOTAL must match the total alias case-insensitively: " + sql);
            assertTrue(sql.contains("f_col"), "F_COL must match the f_col column case-insensitively: " + sql);
            assertEquals(0, countJoins(pruned), "no dimension column is used under any casing");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fve");
        }
    }

    /** A body referencing {@code A.a_name} against a join on table {@code a} binds in DuckDB, so the
     *  qualifier count must fold case — exact matching read the join as unused and eliminated it
     *  out from under a still-projected column. */
    @Test
    void differentlyCasedQualifierInBody_keepsItsJoin() throws Exception {
        String body =
                "SELECT f.f_id, f.f_col, A.a_name, b.b_name " +
                "FROM f " +
                "LEFT JOIN a ON f.a_id = a.a_id " +
                "LEFT JOIN b ON f.b_id = b.b_id";
        conn.createStatement().execute("CREATE OR REPLACE VIEW fvc AS " + body);
        try {
            String outer = "SELECT f_col, a_name FROM fvc";
            JsonNode pruned = prune(outer, body);

            assertEquals(1, countJoins(pruned),
                    "the a join is used through the differently-cased A.a_name and must survive; "
                    + "only b is eliminable");
            assertEquivalentToView(pruned, outer);
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fvc");
        }
    }

    /** A CTE named {@code FV} shadows view {@code fv} in DuckDB, so the shadow bail must also
     *  match case-insensitively — an exact match would inline the view over a reference that
     *  actually resolves to the CTE. */
    @Test
    void differentlyCasedCteShadow_stillBailsOut() throws Exception {
        assertBailsOut("WITH FV AS (SELECT 1 AS f_col) SELECT f_col FROM fv", "fv", VIEW_BODY);
    }

    /**
     * The one observable behaviour change, pinned as a decision: a correlated scalar subquery that
     * returns more than one row errors at runtime — and DuckDB plans it as a delim join regardless
     * of projection, so the view errors even for callers who never projected the column. Dropping
     * the unused entry makes those callers succeed. One direction only: an error becomes a result,
     * never a result a different result.
     */
    @Test
    void multiRowScalarSubquery_errorSuppressedWhenColumnUnused() throws Exception {
        conn.createStatement().execute("CREATE TABLE dup (d_id INT, d_name VARCHAR)");
        conn.createStatement().execute("INSERT INTO dup VALUES (10,'x'),(10,'y')");
        String body =
                "SELECT f.f_id, f.f_col, " +
                "(SELECT dup.d_name FROM dup WHERE dup.d_id = f.a_id) AS one_name " +
                "FROM f " +
                "LEFT JOIN a ON f.a_id = a.a_id";
        conn.createStatement().execute("CREATE OR REPLACE VIEW fverr AS " + body);
        try {
            String outer = "SELECT f_col FROM fverr";
            org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, () -> exec(outer),
                    "precondition: the un-pruned view must error even though one_name is not projected");

            JsonNode pruned = prune(outer, body);
            String sql = Transformations.parseToSql(conn, pruned).toLowerCase();
            assertFalse(sql.contains("one_name"), "the unused subquery entry must be dropped: " + sql);
            assertEquals(3, exec(Transformations.parseToSql(conn, pruned)).size(),
                    "with the subquery dropped, the same projection succeeds");
        } finally {
            conn.createStatement().execute("DROP VIEW IF EXISTS fverr");
            conn.createStatement().execute("DROP TABLE IF EXISTS dup");
        }
    }

    // ---- LEFT JOIN to a subquery that is provably unique on the join key (issue #483) ----

    @Test
    void uniqueSubquery_notProjected_isDropped() throws Exception {
        // Both grouping keys are pinned by the ON condition, so a fact row matches at most one
        // group: dropping the join cannot change the row count. The subquery's own inner JOIN goes
        // with it, which is the point — a fact-only read stops scanning dim_rule.
        JsonNode pruned = prune("SELECT f_id, customer_id FROM flv", UNIQUE_SUBQUERY_VIEW_BODY);
        assertEquals(0, countJoins(pruned), "the subquery join and its inner join must both go");
        assertEquivalentToView(pruned, "SELECT f_id, customer_id FROM flv");
    }

    @Test
    void uniqueSubquery_notProjected_planStopsScanningTheDimension() throws Exception {
        // The acceptance criterion from the issue: with no column from the lookup, the optimized
        // plan reads the fact table only. DuckDB does not drop the join itself, which is why the
        // rewrite has to.
        JsonNode pruned = prune("SELECT f_id, customer_id FROM flv", UNIQUE_SUBQUERY_VIEW_BODY);
        String prunedPlan = explain(Transformations.parseToSql(conn, pruned));
        assertFalse(prunedPlan.contains("dim_rule"), "pruned plan still scans dim_rule:\n" + prunedPlan);
        // The same query against the real view does scan it, so the assertion above has teeth.
        assertTrue(explain("SELECT f_id, customer_id FROM flv").contains("dim_rule"),
                "expected the unpruned view to scan dim_rule");
    }

    @Test
    void uniqueSubquery_projected_isKept() throws Exception {
        // The lookup is used, so the join stays and the results are unchanged.
        JsonNode pruned = prune("SELECT f_id, rules FROM flv", UNIQUE_SUBQUERY_VIEW_BODY);
        assertEquals(2, countJoins(pruned), "LEFT JOIN to the subquery plus its inner JOIN");
        assertEquivalentToView(pruned, "SELECT f_id, rules FROM flv");
    }

    @Test
    void uniqueSubquery_unmatchedFactRows_arePreserved() throws Exception {
        // Empty list, NULL list and an id that resolves to nothing each produce no group. If the
        // elimination were INNER-like, these rows would vanish.
        JsonNode pruned = prune("SELECT f_id FROM flv", UNIQUE_SUBQUERY_VIEW_BODY);
        assertEquals(0, countJoins(pruned));
        assertEquals(5, exec(Transformations.parseToSql(conn, pruned)).size(), "every fact row survives");
    }

    @Test
    void renamedSubqueryColumns_arePinnedByTheirNewNames() throws Exception {
        // g(k, v) renames positionally, so the ON condition names the key as k.
        JsonNode pruned = prune("SELECT f_id FROM flvr", RENAMED_SUBQUERY_VIEW_BODY);
        assertEquals(0, countJoins(pruned));
        assertEquivalentToView(pruned, "SELECT f_id FROM flvr");
    }

    @Test
    void distinctSubquery_wholeSelectListPinned_isDropped() throws Exception {
        JsonNode pruned = prune("SELECT f_id FROM flvd", DISTINCT_SUBQUERY_VIEW_BODY);
        assertEquals(0, countJoins(pruned));
        assertEquivalentToView(pruned, "SELECT f_id FROM flvd");
    }

    @Test
    void reversedEqualityOrder_stillPinsTheKey() throws Exception {
        // ON t.f_id = g.rid rather than g.rid = t.f_id: the subquery column sits on the right of
        // the equality, which says nothing about whether it is pinned.
        String body = "SELECT t.f_id, g.r AS rules FROM fl t " +
                "LEFT JOIN (SELECT t2.f_id AS rid, list(t2.customer_id) AS r FROM fl t2 GROUP BY t2.f_id) g " +
                "  ON t.f_id = g.rid";
        JsonNode pruned = prune("SELECT f_id FROM flv", body);
        assertEquals(0, countJoins(pruned), "the key is pinned whichever side of = it is on");
    }

    @Test
    void threeKeyGroupBy_allPinned_isDropped() throws Exception {
        // A single grouping set over three keys serializes as [[0,1,2]]; all three are pinned.
        // The body is a real view here, so it has to bind as well as parse — an earlier version of
        // this test grouped by count(*), which DuckDB rejects, so it proved nothing.
        JsonNode pruned = prune("SELECT f_id FROM flv3", THREE_KEY_VIEW_BODY);
        assertEquals(0, countJoins(pruned));
        assertEquivalentToView(pruned, "SELECT f_id FROM flv3");
    }

    @Test
    void duplicateNonKeyNames_shiftTheKeysName_isKept() throws Exception {
        // The key's own name is unique, but a duplicate elsewhere in the select list makes DuckDB
        // rename, and the rename lands on the key's name. Results, not shape: 3 rows, not 1.
        JsonNode pruned = prune("SELECT x FROM rsv", RENAME_SHIFT_VIEW_BODY);
        assertEquivalentToView(pruned, "SELECT x FROM rsv");
        assertEquals(3, exec("SELECT x FROM rsv").size(), "the fixture must multiply rows");
    }

    @Test
    void anUnaliasedExpressionsGeneratedName_shiftsTheKeysName_isKept() throws Exception {
        JsonNode pruned = prune("SELECT x FROM rsav", AUTO_NAME_VIEW_BODY);
        assertEquivalentToView(pruned, "SELECT x FROM rsav");
        assertEquals(3, exec("SELECT x FROM rsav").size(), "the fixture must multiply rows");
    }

    @Test
    void starInAGroupedSubquery_isKept() throws Exception {
        // The star makes the grouping key's output name unknowable, so the join must stay — and
        // here it genuinely multiplies: the subquery has one row per (a.f_id, b.f_id) pair while
        // only a.f_id is pinned. A real view, so the row count is checked and not just the shape.
        JsonNode pruned = prune("SELECT f_id FROM flvs", STAR_SUBQUERY_VIEW_BODY);
        assertEquals(2, countJoins(pruned), "the star hides which column g.f_id names, so the join stays");
        assertEquivalentToView(pruned, "SELECT f_id FROM flvs");
        // Spelled out: pruning it would have collapsed the multiplication.
        assertEquals(exec("SELECT f_id FROM flvs").size(),
                exec(Transformations.parseToSql(conn, pruned)).size());
        assertTrue(exec("SELECT f_id FROM flvs").size() > exec("SELECT f_id FROM fl").size(),
                "the fixture must actually multiply rows, or this test proves nothing");
    }

    // ---- kept: uniqueness not provable ----

    /**
     * Prunes with a one-off body and asserts the LEFT JOIN to the subquery survived.
     *
     * <p>Two ways it can survive. Usually the body's unused select-list entry is pruned, the body
     * is inlined, and the join has to still be there. But when nothing at all was optimizable the
     * method returns its input untouched, and then the view body — join included — is what still
     * runs; there is no inlined tree to count joins in.
     */
    private void assertSubqueryJoinKept(String viewBody) throws Exception {
        JsonNode outer = Transformations.parseToTree(conn, "SELECT f_id FROM flv");
        JsonNode body = Transformations.parseToTree(conn, viewBody);
        JsonNode pruned = Transformations.pruneUnusedLeftJoins(outer, body);
        if (pruned == outer) return; // bailed out: the body, and its join, are untouched
        assertTrue(countJoins(pruned) >= 1,
                "expected the join to be kept for body: " + viewBody);
    }

    @Test
    void groupByAll_isKept() throws Exception {
        // GROUP BY ALL serializes with no group expressions, so there is nothing to pin.
        assertSubqueryJoinKept(
                "SELECT t.f_id, g.resolved AS rules FROM fl t " +
                "LEFT JOIN (SELECT t2.f_id, list(t2.customer_id) AS resolved FROM fl t2 GROUP BY ALL) g " +
                "  ON g.f_id = t.f_id");
    }

    @Test
    void groupingSets_isKept() throws Exception {
        // Several grouping sets: one key value can appear in more than one output row.
        assertSubqueryJoinKept(
                "SELECT t.f_id, g.resolved AS rules FROM fl t " +
                "LEFT JOIN (SELECT t2.f_id, t2.customer_id, list(t2.f_id) AS resolved FROM fl t2 " +
                "           GROUP BY GROUPING SETS ((t2.f_id), (t2.customer_id))) g ON g.f_id = t.f_id");
    }

    @Test
    void rollup_isKept() throws Exception {
        assertSubqueryJoinKept(
                "SELECT t.f_id, g.resolved AS rules FROM fl t " +
                "LEFT JOIN (SELECT t2.f_id, t2.customer_id, list(t2.f_id) AS resolved FROM fl t2 " +
                "           GROUP BY ROLLUP (t2.f_id, t2.customer_id)) g ON g.f_id = t.f_id");
    }

    @Test
    void unpinnedGroupKey_isKept() throws Exception {
        // Grouped by (f_id, customer_id) but joined on f_id alone, so one fact row can match
        // several groups and dropping the join would change the row count.
        assertSubqueryJoinKept(
                "SELECT t.f_id, g.resolved AS rules FROM fl t " +
                "LEFT JOIN (SELECT t2.f_id, t2.customer_id, list(t2.f_id) AS resolved FROM fl t2 " +
                "           GROUP BY t2.f_id, t2.customer_id) g ON g.f_id = t.f_id");
    }

    @Test
    void setOperationSubquery_isKept() throws Exception {
        // A UNION at the top is not a SELECT_NODE, so no grouping can be read off it.
        assertSubqueryJoinKept(
                "SELECT t.f_id, g.k AS rules FROM fl t " +
                "LEFT JOIN (SELECT d.rule_id AS k FROM dim_rule d UNION SELECT t2.f_id FROM fl t2) g " +
                "  ON g.k = t.f_id");
    }

    @Test
    void correlatedLateralSubquery_isKept() throws Exception {
        // LATERAL is absent from the serialized AST — the ref type stays REGULAR — so what rejects
        // this is the body naming t, an alias the enclosing FROM defines.
        assertSubqueryJoinKept(
                "SELECT t.f_id, g.resolved AS rules FROM fl t " +
                "LEFT JOIN LATERAL (SELECT t2.f_id, list(t2.customer_id) AS resolved FROM fl t2 " +
                "                   WHERE t2.customer_id = t.customer_id GROUP BY t2.f_id) g " +
                "  ON g.f_id = t.f_id");
    }

    @Test
    void distinctOnSubquery_isKept() throws Exception {
        // DISTINCT ON dedups on its targets, not on the select list, so pinning the list proves
        // nothing about them.
        assertSubqueryJoinKept(
                "SELECT t.f_id, g.k AS rules FROM fl t " +
                "LEFT JOIN (SELECT DISTINCT ON (d.name) d.rule_id AS k FROM dim_rule d) g " +
                "  ON g.k = t.customer_id");
    }

    @Test
    void nonEquiJoinToUniqueSubquery_isKept() throws Exception {
        assertSubqueryJoinKept(
                "SELECT t.f_id, g.resolved AS rules FROM fl t " +
                "LEFT JOIN (SELECT t2.f_id, list(t2.customer_id) AS resolved FROM fl t2 GROUP BY t2.f_id) g " +
                "  ON g.f_id > t.f_id");
    }

    @Test
    void subqueryAliasReferencedOutsideItsOwnOn_isKept() throws Exception {
        // The body's WHERE still reads the subquery, so it is not unused however provably unique it
        // is. The body carries an unused select-list entry as well, so projection pruning fires and
        // the body really is inlined — otherwise this would pass on the bail-out path without ever
        // reaching the decision under test.
        String body = "SELECT t.f_id, t.customer_id AS unused_col FROM fl t " +
                "LEFT JOIN (SELECT t2.f_id, list(t2.customer_id) AS resolved FROM fl t2 GROUP BY t2.f_id) g " +
                "  ON g.f_id = t.f_id WHERE g.resolved IS NOT NULL";
        JsonNode outer = Transformations.parseToTree(conn, "SELECT f_id FROM flv");
        JsonNode pruned = Transformations.pruneUnusedLeftJoins(outer, Transformations.parseToTree(conn, body));
        assertNotSame(outer, pruned, "the unused select-list entry should have been pruned");
        assertEquals(1, countJoins(pruned), "the join is read by the body's WHERE, so it stays");
    }

    @Test
    void duplicateKeyOutputNames_isKept() throws Exception {
        // DuckDB exposes `SELECT a.k, b.k ... GROUP BY a.k, b.k` as k and k_1, so `ON g.k = t.k`
        // pins only the first key. Reading both as "k" would prove a uniqueness that is not there
        // and multiply the fact rows away.
        String body = "SELECT t.f_id AS f_id, t.customer_id AS unused_col FROM fl t " +
                "LEFT JOIN (SELECT a.f_id, b.f_id FROM fl a, fl b GROUP BY a.f_id, b.f_id) g " +
                "  ON g.f_id = t.f_id";
        JsonNode outer = Transformations.parseToTree(conn, "SELECT f_id FROM flv");
        JsonNode pruned = Transformations.pruneUnusedLeftJoins(outer, Transformations.parseToTree(conn, body));
        if (pruned == outer) return;
        // Two: the LEFT JOIN that must stay, plus the subquery's own `fl a, fl b` cross join.
        assertEquals(2, countJoins(pruned), "only one of the two keys is pinned, so the join stays");
    }

    @Test
    void duplicateOutputNamesUnderDistinct_isKept() throws Exception {
        // Same ambiguity on the DISTINCT path: the dedup key is both columns, exposed as f_id and
        // f_id_1, so `ON g.f_id = t.f_id` pins only the first.
        String body = "SELECT t.f_id AS f_id, t.customer_id AS unused_col FROM fl t " +
                "LEFT JOIN (SELECT DISTINCT a.f_id, b.f_id FROM fl a, fl b) g ON g.f_id = t.f_id";
        JsonNode outer = Transformations.parseToTree(conn, "SELECT f_id FROM flv");
        JsonNode pruned = Transformations.pruneUnusedLeftJoins(outer, Transformations.parseToTree(conn, body));
        if (pruned == outer) return;
        assertEquals(2, countJoins(pruned), "the unpinned second dedup column keeps the join");
    }

    @Test
    void innerAliasShadowingAnEnclosingOne_isKept() throws Exception {
        // The subquery's own alias is t, which the enclosing FROM also uses. SQL scoping means the
        // inner one wins inside the body, so this join would in fact be safe to drop — it is kept
        // because the enclosing-scope check cannot tell a shadowing alias from a correlation. A
        // documented over-conservatism: choosing a distinct inner alias prunes.
        String body = "SELECT t.f_id, g.r AS rules FROM fl t " +
                "LEFT JOIN (SELECT t.f_id AS rid, list(t.customer_id) AS r FROM fl t GROUP BY t.f_id) g " +
                "  ON g.rid = t.f_id";
        JsonNode outer = Transformations.parseToTree(conn, "SELECT f_id FROM flv");
        JsonNode pruned = Transformations.pruneUnusedLeftJoins(outer, Transformations.parseToTree(conn, body));
        if (pruned == outer) return;
        assertEquals(1, countJoins(pruned));
    }

    @Test
    void keyPinnedToItself_isKept() throws Exception {
        // g.f_id = g.f_id pins nothing: both sides are inside the subquery.
        assertSubqueryJoinKept(
                "SELECT t.f_id, g.resolved AS rules FROM fl t " +
                "LEFT JOIN (SELECT t2.f_id, list(t2.customer_id) AS resolved FROM fl t2 GROUP BY t2.f_id) g " +
                "  ON g.f_id = g.f_id");
    }

    // ---- inner aliases of a sibling subquery are not "enclosing" ----

    @Test
    void secondLookupIsEliminated_thoughItReusesTheFirstsInnerAlias() throws Exception {
        // Projecting only the first lookup must still drop the second. Both bodies name t2, which
        // is bound inside each subquery and invisible outside it, so the second is not correlated.
        JsonNode pruned = prune("SELECT f_id, rules1 FROM flv2", TWO_LOOKUP_VIEW_BODY);
        assertEquals(1, countJoins(pruned), "only the projected lookup's join should remain");
        assertEquivalentToView(pruned, "SELECT f_id, rules1 FROM flv2");
    }

    @Test
    void neitherLookupProjected_dropsBoth() throws Exception {
        JsonNode pruned = prune("SELECT f_id FROM flv2", TWO_LOOKUP_VIEW_BODY);
        assertEquals(0, countJoins(pruned));
        assertEquivalentToView(pruned, "SELECT f_id FROM flv2");
    }

    @Test
    void bothLookupsProjected_keepsBoth() throws Exception {
        // Every select-list entry is used and neither join is droppable, so there is nothing to
        // optimize and the input comes back untouched — the view, with both joins, is what runs.
        assertBailsOut("SELECT f_id, rules1, rules2 FROM flv2", TWO_LOOKUP_VIEW_BODY);
    }
}
