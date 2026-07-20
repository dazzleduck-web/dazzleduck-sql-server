package io.dazzleduck.sql.commons.ducklake;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.util.TestUtils;
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

/**
 * DuckLake-specific coverage for {@link Transformations#pruneUnusedLeftJoins}.
 *
 * <p>The fact table is a DuckLake table, so it carries the hidden {@code rowid} meta column
 * (excluded from {@code SELECT *}, but explicitly selectable). The view projects {@code f.rowid}
 * and LEFT JOINs two dimensions. Eliminating an unused dimension join must leave {@code rowid}
 * intact and keep the query row-equivalent to the original view.
 *
 * <p>Note: a {@code rowid} filter does not prune DuckLake data files (the scanner reads all files),
 * so {@code rowid} is for identity/lookup, not selective scans — but that is orthogonal to the
 * join-elimination correctness verified here.
 */
public class PruneUnusedLeftJoinsDuckLakeTest {

    @TempDir
    static Path WORKSPACE;

    private static final String CATALOG = "dl_pj";
    private static DuckDBConnection conn;

    // View body: DuckLake fact (hidden rowid, projected as `rid`) LEFT JOIN two dimensions.
    private static final String VIEW_BODY =
            "SELECT f.rowid AS rid, f.f_id, f.f_col, a.a_name, b.b_name " +
            "FROM " + CATALOG + ".fact f " +
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
        // rowid 0,1,2 respectively; row (2,10,999,15) has a b_id with no match in dim_b.
        exec("INSERT INTO " + CATALOG + ".fact VALUES (1,10,100,5),(2,10,999,15),(3,20,100,25)");
        exec("CREATE TABLE " + CATALOG + ".dim_a(a_id INT, a_name VARCHAR)");
        exec("INSERT INTO " + CATALOG + ".dim_a VALUES (10,'a10'),(20,'a20')");
        exec("CREATE TABLE " + CATALOG + ".dim_b(b_id INT, b_name VARCHAR)");
        exec("INSERT INTO " + CATALOG + ".dim_b VALUES (100,'b100')");
        exec("CREATE VIEW fv_dl AS " + VIEW_BODY);
    }

    @AfterAll
    static void tearDown() throws SQLException {
        exec("DROP VIEW IF EXISTS fv_dl");
        exec("DETACH " + CATALOG);
        conn.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    // ---- helpers ----

    private JsonNode prune(String outerSql) throws Exception {
        JsonNode outer = Transformations.parseToTree(conn, outerSql);
        JsonNode body = Transformations.parseToTree(conn, VIEW_BODY);
        return Transformations.pruneUnusedLeftJoins(outer, body);
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

    private void assertEquivalentToView(JsonNode pruned, String originalOuterSql) throws Exception {
        TestUtils.isEqual(originalOuterSql, Transformations.parseToSql(conn, pruned));
    }

    // ---- tests ----

    @Test
    void rowidProjected_onlyDimAUsed_dropsJoinB() throws Exception {
        String outer = "SELECT rid, a_name FROM fv_dl";
        JsonNode pruned = prune(outer);

        assertEquals(1, countJoins(pruned), "unused dim_b join should be eliminated, dim_a kept");
        String prunedSql = Transformations.parseToSql(conn, pruned).toLowerCase();
        assertEquals(false, prunedSql.contains("b_name"), "dim_b projection should be gone");
        assertEquivalentToView(pruned, outer);

        // rowid still resolves through the inlined subquery: three distinct hidden ids 0,1,2.
        List<List<Object>> rows = execRows(Transformations.parseToSql(conn, pruned));
        assertEquals(3, rows.size());
    }

    @Test
    void rowidProjected_neitherDimUsed_dropsBothJoins() throws Exception {
        String outer = "SELECT rid, f_col FROM fv_dl WHERE f_col > 10";
        JsonNode pruned = prune(outer);

        assertEquals(0, countJoins(pruned), "both dimension joins should be eliminated");
        assertEquivalentToView(pruned, outer);
    }

    @Test
    void rowidUsed_keepsUnmatchedFactRow() throws Exception {
        // The fact row with rowid=1 (b_id=999) has no dim_b match; eliminating the LEFT JOIN
        // must not drop it — all three rowids must survive.
        String outer = "SELECT rid, a_name FROM fv_dl";
        JsonNode pruned = prune(outer);

        assertEquals(1, countJoins(pruned));
        assertEquivalentToView(pruned, outer);
        List<List<Object>> rows = execRows(Transformations.parseToSql(conn, pruned));
        assertEquals(3, rows.size(), "all three fact rows (incl. the dim_b-unmatched one) must remain");
    }
}
