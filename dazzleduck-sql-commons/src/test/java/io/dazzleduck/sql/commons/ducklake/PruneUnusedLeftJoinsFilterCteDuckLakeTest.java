package io.dazzleduck.sql.commons.ducklake;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.authorization.SqlAuthorizer;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Approach-A prototype coverage: join elimination through the RLS authorizer's row-filter CTE.
 *
 * <p>The RESTRICT_READ_ONLY authorizer rewrites {@code SELECT <cols> FROM <view> WHERE <rw>} into
 * <pre>WITH ___view AS (SELECT * FROM view WHERE &lt;rls-filter&gt;) SELECT &lt;cols&gt; FROM ___view WHERE &lt;rw&gt;</pre>
 * The scope-aware {@link Transformations#pruneUnusedLeftJoins(JsonNode, String, JsonNode)} must see
 * <em>through</em> that injected {@code SELECT *} — recovering used columns from the CTE's consumers
 * plus the filter's own WHERE — and still eliminate the view's unused LEFT JOINs, for every role,
 * with an identical result set. This is what {@code classic-search-v2-join-pruning.md} (Approach A)
 * specifies for classic-search v2, where the view stays the authorization target.
 */
public class PruneUnusedLeftJoinsFilterCteDuckLakeTest {

    @TempDir
    static Path WORKSPACE;

    private static final String CATALOG = "dl_fc";
    private static DuckDBConnection conn;

    // Fact (DuckLake, hidden rowid projected as `rid`) LEFT JOIN two dimensions for their names.
    private static final String VIEW_BODY =
            "SELECT f.rowid AS rid, f.f_id, f.f_col, a.a_name, b.b_name " +
            "FROM " + CATALOG + ".fact f " +
            "LEFT JOIN " + CATALOG + ".dim_a a ON f.a_id = a.a_id " +
            "LEFT JOIN " + CATALOG + ".dim_b b ON f.b_id = b.b_id";

    // Snapshot-aware variant (classic-search v2's AT-view): the AT lives on the retained fact ref.
    private static final String VIEW_BODY_AT =
            "SELECT f.rowid AS rid, f.f_id, f.f_col, a.a_name, b.b_name " +
            "FROM " + CATALOG + ".fact f " +
            "AT (TIMESTAMP => COALESCE(TRY_CAST(getvariable('cs2_as_of') AS TIMESTAMPTZ), now())) " +
            "LEFT JOIN " + CATALOG + ".dim_a a ON f.a_id = a.a_id " +
            "LEFT JOIN " + CATALOG + ".dim_b b ON f.b_id = b.b_id";

    @BeforeAll
    static void setup() throws SQLException {
        String ws = WORKSPACE.toString();
        conn = ConnectionPool.getConnection();
        exec("INSTALL ducklake");
        exec("LOAD ducklake");
        exec("ATTACH 'ducklake:" + ws + "/metadata' AS " + CATALOG + " (DATA_PATH '" + ws + "/data')");
        exec("CREATE TABLE " + CATALOG + ".fact(f_id INT, a_id INT, b_id INT, f_col INT)");
        // rowid 0,1,2; row (2,10,999,15) has a b_id with no match in dim_b (LEFT-JOIN unmatched).
        exec("INSERT INTO " + CATALOG + ".fact VALUES (1,10,100,5),(2,10,999,15),(3,20,100,25)");
        exec("CREATE TABLE " + CATALOG + ".dim_a(a_id INT, a_name VARCHAR)");
        exec("INSERT INTO " + CATALOG + ".dim_a VALUES (10,'a10'),(20,'a20')");
        exec("CREATE TABLE " + CATALOG + ".dim_b(b_id INT, b_name VARCHAR)");
        exec("INSERT INTO " + CATALOG + ".dim_b VALUES (100,'b100')");
        exec("CREATE VIEW fv AS " + VIEW_BODY);
        exec("CREATE VIEW fv_at AS " + VIEW_BODY_AT);
    }

    @AfterAll
    static void tearDown() throws SQLException {
        exec("DROP VIEW IF EXISTS fv");
        exec("DROP VIEW IF EXISTS fv_at");
        exec("DETACH " + CATALOG);
        conn.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    // ---- helpers ----

    /** Emulate the authorizer: wrap {@code viewName} in a `SELECT * FROM view WHERE <filter>` CTE. */
    private JsonNode authorize(String outerSql, String filter) throws Exception {
        JsonNode query = Transformations.parseToTree(conn, outerSql);
        JsonNode filterNode = SqlAuthorizer.compileFilterString(filter);
        return Transformations.injectFilterCtes(query, filterNode);
    }

    private JsonNode pruneThroughFilterCte(String outerSql, String filter, String viewName,
                                           String viewBody) throws Exception {
        JsonNode authorized = authorize(outerSql, filter);
        JsonNode body = Transformations.parseToTree(conn, viewBody);
        return Transformations.pruneUnusedLeftJoins(authorized, viewName, body);
    }

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

    private List<List<Object>> execRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            List<List<Object>> rows = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getObject(i));
                rows.add(row);
            }
            return rows;
        }
    }

    /** The pruned query and the un-pruned authorized query must return the same rows. Compared as a
     *  multiset (sorted): neither query has an ORDER BY, so row order is not guaranteed and differs
     *  once the join is eliminated — only the set of rows (and its cardinality) is contractual. */
    private void assertRowsEqualAuthorized(JsonNode pruned, String outerSql, String filter)
            throws Exception {
        String prunedSql = Transformations.parseToSql(conn, pruned);
        String authorizedSql = Transformations.parseToSql(conn, authorize(outerSql, filter));
        assertEquals(sortedRepr(execRows(authorizedSql)), sortedRepr(execRows(prunedSql)),
                "pruned result set must equal the un-pruned authorized result set");
    }

    private List<String> sortedRepr(List<List<Object>> rows) {
        List<String> repr = new ArrayList<>();
        for (List<Object> row : rows) repr.add(String.valueOf(row));
        repr.sort(null);
        return repr;
    }

    // ---- tests ----

    @Test
    void throughFilterCte_onlyDimAUsed_dropsJoinB() throws Exception {
        String outer = "SELECT rid, a_name FROM fv";
        JsonNode pruned = pruneThroughFilterCte(outer, "f_col > 0", "fv", VIEW_BODY);

        assertEquals(1, countJoins(pruned),
                "unused dim_b join must be eliminated through the filter-CTE; dim_a kept");
        String prunedSql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertFalse(prunedSql.contains("b_name"), "dim_b projection should be gone: " + prunedSql);
        assertTrue(prunedSql.contains("f_col"), "filter column f_col must survive in the pruned body");
        assertRowsEqualAuthorized(pruned, outer, "f_col > 0");
    }

    @Test
    void throughFilterCte_neitherDimUsed_dropsBothJoins() throws Exception {
        // Projection + filter reference only fact columns → both dimension joins eliminable.
        String outer = "SELECT rid, f_col FROM fv";
        JsonNode pruned = pruneThroughFilterCte(outer, "f_id > 0", "fv", VIEW_BODY);

        assertEquals(0, countJoins(pruned), "both dimension joins must be eliminated");
        assertRowsEqualAuthorized(pruned, outer, "f_id > 0");
    }

    @Test
    void filterColumnNotInProjection_survivesPruning() throws Exception {
        // The reconciliation case: f_id is referenced ONLY by the RLS filter, never projected.
        // The pruned body must still project it (from the CTE WHERE), or the wrapper won't bind.
        String outer = "SELECT rid, a_name FROM fv";
        JsonNode pruned = pruneThroughFilterCte(outer, "f_id = 2", "fv", VIEW_BODY);

        assertEquals(1, countJoins(pruned));
        String prunedSql = Transformations.parseToSql(conn, pruned);
        // Binds + executes: only rowid 1 (f_id=2) matches.
        List<List<Object>> rows = execRows(prunedSql);
        assertEquals(1, rows.size(), "filter f_id=2 selects exactly one row");
        assertRowsEqualAuthorized(pruned, outer, "f_id = 2");
    }

    @Test
    void unmatchedLeftJoinRow_retainedAfterElimination() throws Exception {
        // rowid 1 (b_id=999) has no dim_b match; eliminating the LEFT JOIN must not drop it.
        String outer = "SELECT rid, a_name FROM fv";
        JsonNode pruned = pruneThroughFilterCte(outer, "f_col > 0", "fv", VIEW_BODY);

        assertEquals(1, countJoins(pruned));
        List<List<Object>> rows = execRows(Transformations.parseToSql(conn, pruned));
        assertEquals(3, rows.size(), "all three fact rows (incl. the dim_b-unmatched one) must remain");
    }

    @Test
    void atTimeTravel_preservedThroughFilterCte() throws Exception {
        String outer = "SELECT rid, a_name FROM fv_at";
        JsonNode pruned = pruneThroughFilterCte(outer, "f_col > 0", "fv_at", VIEW_BODY_AT);

        assertEquals(1, countJoins(pruned), "dim_b join eliminated even with an AT clause on the fact");
        String lower = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertFalse(lower.contains("b_name"), "dim_b projection should be gone");
        assertTrue(lower.contains("getvariable"),
                "AT(TIMESTAMP => …) must be preserved through the filter-CTE: " + lower);
        assertEquals(3, execRows(Transformations.parseToSql(conn, pruned)).size());
    }

    @Test
    void outerStarOverCte_noOp_viewNotInlined() throws Exception {
        // `SELECT *` over the CTE needs every view column → cannot enumerate → must NOT prune.
        String outer = "SELECT * FROM fv";
        JsonNode pruned = pruneThroughFilterCte(outer, "f_col > 0", "fv", VIEW_BODY);

        String prunedSql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertFalse(prunedSql.contains(CATALOG + ".fact"),
                "the view body must NOT be inlined on a star projection: " + prunedSql);
        assertTrue(prunedSql.contains("fv"), "the view reference should remain: " + prunedSql);
    }

    @Test
    void rlsFilter_stillApplied_afterPruning() throws Exception {
        // The whole point of keeping the wrapper: the row filter must still bind + restrict.
        String outer = "SELECT rid, a_name FROM fv";
        JsonNode pruned = pruneThroughFilterCte(outer, "f_id >= 2", "fv", VIEW_BODY);

        List<List<Object>> rows = execRows(Transformations.parseToSql(conn, pruned));
        assertEquals(2, rows.size(), "filter f_id>=2 keeps rowids 1,2 only");
        assertRowsEqualAuthorized(pruned, outer, "f_id >= 2");
    }
}
