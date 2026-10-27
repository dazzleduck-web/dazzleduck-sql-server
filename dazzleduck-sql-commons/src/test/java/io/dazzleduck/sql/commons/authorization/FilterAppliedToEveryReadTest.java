package io.dazzleduck.sql.commons.authorization;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RESTRICTED mode's row filter must apply to EVERY read of the authorized table, not only the
 * outermost SELECT's WHERE: a table read in a scalar subquery, an EXISTS, or a CTE body is the same
 * table and must return the same rows. Each query below reads only the authorized table, so it
 * passes the access check; what is tested is which rows come back.
 */
public class FilterAppliedToEveryReadTest {

    private static DuckDBConnection conn;
    private static final SqlAuthorizer authorizer = SqlAuthorizer.RESTRICTED_DATASOURCE_AUTHORIZER;
    private static final String DB = "memory";
    private static final String SCHEMA = "main";

    @BeforeAll
    static void setup() throws SQLException {
        conn = ConnectionPool.getConnection();
        conn.createStatement().execute("CREATE TABLE orders (id INT, tenant_id VARCHAR, amount INT)");
        conn.createStatement().execute("INSERT INTO orders VALUES (1,'abc',100),(2,'xyz',200),(3,'abc',300)");
        conn.createStatement().execute("CREATE TABLE secrets (id INT, tenant_id VARCHAR, amount INT)");
        conn.createStatement().execute("INSERT INTO secrets VALUES (9,'abc',999)");
        // A real schema named like DuckDB's USING KEY recurring-table qualifier.
        conn.createStatement().execute("CREATE SCHEMA recurring");
        conn.createStatement().execute("CREATE TABLE recurring.secrets AS SELECT * FROM secrets");
        conn.createStatement().execute("CREATE TABLE recurring.orders AS SELECT * FROM orders");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.createStatement().execute("DROP TABLE IF EXISTS orders");
        conn.createStatement().execute("DROP TABLE IF EXISTS secrets");
        conn.createStatement().execute("DROP SCHEMA IF EXISTS recurring CASCADE");
        conn.close();
    }

    /** Both claim shapes grant the same thing and must behave identically. */
    private static List<Map<String, String>> grants() {
        return List.of(
                Map.of(Headers.HEADER_TABLE, "orders", Headers.HEADER_FILTER, "tenant_id='abc'"),
                Map.of(Headers.HEADER_ACCESS, "[[\"table\",\"orders\",\"*\",\"tenant_id='abc'\"]]"));
    }

    private static List<SqlAuthorizer> restrictedAuthorizers() {
        return List.of(SqlAuthorizer.RESTRICTED_DATASOURCE_AUTHORIZER, SqlAuthorizer.RESTRICT_READ_ONLY_AUTHORIZER);
    }

    private static List<Object> run(String sql, Map<String, String> claims) throws Exception {
        return run(authorizer, sql, claims);
    }

    private static List<Object> run(SqlAuthorizer authorizer, String sql, Map<String, String> claims) throws Exception {
        JsonNode authorized = authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn, sql), claims);
        String rewritten = Transformations.parseToSql(conn, authorized);
        return firstColumn(rewritten);
    }

    /** First column of every row, as text, so a wider numeric type than expected cannot matter. */
    private static List<Object> firstColumn(String sql) throws SQLException {
        List<Object> rows = new ArrayList<>();
        try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
            while (rs.next()) rows.add(String.valueOf(rs.getObject(1)));
        }
        return rows;
    }

    @Test
    void theTopLevelReadIsFiltered() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("1", "3"), run("SELECT id FROM orders ORDER BY id", claims));
        }
    }

    @Test
    void aScalarSubqueryReadIsFiltered() throws Exception {
        // Unfiltered this is 600 (every tenant); filtered it is 400 (tenant abc only).
        for (var claims : grants()) {
            assertEquals(List.of("400", "400"),
                    run("SELECT (SELECT sum(amount) FROM orders) AS total FROM orders", claims));
        }
    }

    @Test
    void aSubqueryCannotLeakAnotherTenantsColumnValues() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("abc:100,abc:300", "abc:100,abc:300"),
                    run("SELECT (SELECT string_agg(tenant_id || ':' || amount, ',' ORDER BY amount) FROM orders) AS v FROM orders", claims));
        }
    }

    @Test
    void anExistsSubqueryCannotTestAnotherTenantsRows() throws Exception {
        // The EXISTS looks for a tenant 'xyz' row; filtered, it can never find one, so no rows.
        for (var claims : grants()) {
            assertEquals(List.of(),
                    run("SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM orders o2 WHERE o2.tenant_id = 'xyz')", claims));
        }
    }

    @Test
    void aCteBodyReadIsFiltered() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("400"), run("WITH x AS (SELECT * FROM orders) SELECT sum(amount) FROM x", claims));
        }
    }

    @Test
    void aJoinOfTheTableToItselfIsFilteredOnBothSides() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("2"), run("SELECT count(*) FROM orders a JOIN orders b ON a.id = b.id", claims));
        }
    }

    /**
     * The Flight server authorizes a query once for its info and once for its stream, so the second
     * pass sees a query that already carries filter CTEs. It must stay valid and filtered.
     */
    @Test
    void authorizingAnAlreadyAuthorizedQueryStaysValidAndFiltered() throws Exception {
        for (var claims : grants()) {
            for (String sql : new String[]{"SELECT id FROM orders ORDER BY id", "SELECT id FROM memory.main.orders ORDER BY id"}) {
                JsonNode once = authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn, sql), claims);
                JsonNode twice = authorizer.authorize("user", DB, SCHEMA, once, claims);
                assertEquals(List.of("1", "3"), firstColumn(Transformations.parseToSql(conn, twice)), sql);
            }
        }
    }

    /**
     * A correlated subquery is one read of the table per outer row, and each must be filtered: here
     * it asks for the largest OTHER row, which unfiltered answers 200 — tenant xyz's amount.
     */
    @Test
    void aCorrelatedSubqueryCannotSeeAnotherTenantsRows() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("300", "100"),
                    run("SELECT (SELECT max(amount) FROM orders o2 WHERE o2.id <> o.id) AS m FROM orders o ORDER BY o.id", claims));
        }
    }

    /** An IN subquery is an existence oracle over the whole table unless it too is filtered. */
    @Test
    void anInSubqueryCannotProbeAnotherTenantsValues() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("false", "false"),
                    run("SELECT 200 IN (SELECT amount FROM orders) AS probe FROM orders ORDER BY id", claims));
        }
    }

    /** Both arms of a set operation read the table, so both are filtered: 2 rows twice, not 3. */
    @Test
    void bothArmsOfAUnionAreFiltered() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("4"),
                    run("SELECT count(*) FROM (SELECT amount FROM orders UNION ALL SELECT amount FROM orders) u", claims));
        }
    }

    /**
     * The pruning copy of the filter on the outermost WHERE is only valid where that SELECT reads
     * the table itself. Here it reads a CTE projecting one column, so a copy of
     * {@code tenant_id = 'abc'} would have no {@code tenant_id} to bind to and the query would fail
     * to plan — correctness comes from the CTE, which already filtered the read.
     */
    @Test
    void aCteBodyProjectingOnlySomeColumnsStillPlans() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("400"), run("WITH x AS (SELECT amount FROM orders) SELECT sum(amount) FROM x", claims));
            assertEquals(List.of("400"),
                    run("SELECT sum(amount) FROM (SELECT amount FROM orders) u", claims));
        }
    }

    /** A CTE may shadow the filtered column with one of its own; the read beneath it is filtered. */
    @Test
    void aCteThatRedefinesTheFilteredColumnStillPlans() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("400"),
                    run("WITH c AS (SELECT * EXCLUDE (tenant_id), 'zzz' AS tenant_id FROM orders) SELECT sum(amount) FROM c", claims));
        }
    }

    /**
     * The shape the ingestion clients and the planner generate for a path grant — a table function
     * unioned into a typed empty relation — still takes its filter. Only authorization is exercised;
     * the files are not read.
     */
    @Test
    void theCanonicalPathGrantShapeStillTakesItsFilter() throws Exception {
        var claims = Map.of(Headers.HEADER_ACCESS,
                "[[\"path\",\"example/data/hive_table\",\"*\",\"key='k2'\"]]");
        JsonNode authorized = authorizer.authorize("user", DB, SCHEMA,
                Transformations.parseToTree(conn, io.dazzleduck.sql.commons.util.TestConstants.SUPPORTED_HIVE_PATH_QUERY), claims);
        assertTrue(Transformations.parseToSql(conn, authorized).contains("'k2'"));
    }

    /**
     * A filtered path grant whose filter has nowhere to go is refused. The old code returned
     * {@code null} from the filter step, which the caller dereferenced.
     */
    @Test
    void aFilteredFunctionGrantRefusesAReadItCannotFilter() {
        String sql = "SELECT 1 AS c FROM (SELECT count(*) FROM read_parquet('example/data/hive_table/*/*/*.parquet')) u";
        var claims = Map.of(Headers.HEADER_ACCESS,
                "[[\"path\",\"example/data/hive_table\",\"*\",\"key='k2'\"]]");
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn, sql), claims));
    }

    /**
     * A CTE named like the table does not shadow a QUALIFIED reference: DuckDB binds {@code main.orders}
     * and {@code memory.main.orders} to the table itself. Treating them as CTE references left the
     * read with no filter CTE, so every tenant's rows came back.
     */
    @Test
    void aCteNamedLikeTheTableDoesNotHideAQualifiedRead() throws Exception {
        for (SqlAuthorizer a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                for (String sql : new String[]{
                        "WITH orders AS (SELECT 0 AS id) SELECT id FROM main.orders ORDER BY id",
                        "WITH orders AS (SELECT 0 AS id) SELECT id FROM memory.main.orders ORDER BY id",
                        "WITH orders AS (SELECT 0 AS id) SELECT id FROM (SELECT id FROM main.orders) s ORDER BY id",
                        "WITH orders AS (SELECT 0 AS id) SELECT o.id FROM orders c JOIN main.orders o ON true ORDER BY o.id",
                        "WITH orders AS (SELECT 0 AS id) SELECT id FROM orders WHERE id IN (SELECT id FROM main.orders) "
                                + "UNION ALL SELECT id FROM main.orders ORDER BY id",
                        "SELECT id FROM (WITH orders AS (SELECT 0 AS id) SELECT id FROM main.orders) s ORDER BY id"}) {
                    List<Object> rows = run(a, sql, claims);
                    assertTrue(!rows.contains("2"), a + " / " + sql + " leaked tenant xyz: " + rows);
                }
            }
        }
    }

    /**
     * A CTE body sees only the CTEs declared BEFORE it: DuckDB binds {@code orders} in {@code a}'s body
     * to the table, not to the {@code orders} CTE declared after it, so that read must be filtered.
     */
    @Test
    void aCteDeclaredLaterDoesNotHideAReadInAnEarlierBody() throws Exception {
        for (SqlAuthorizer a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                for (String sql : new String[]{
                        "WITH a AS (SELECT id FROM orders), orders AS (SELECT 0 AS id) SELECT id FROM a ORDER BY id",
                        "WITH a AS (SELECT id FROM orders), orders AS (SELECT 0 AS id) "
                                + "SELECT id FROM a UNION ALL SELECT id FROM orders ORDER BY id"}) {
                    List<Object> rows = run(a, sql, claims);
                    assertTrue(!rows.contains("2"), a + " / " + sql + " leaked tenant xyz: " + rows);
                }
            }
        }
    }

    /**
     * The qualified read under a same-named CTE is a direct table read, so RESTRICTED mode keeps its
     * pruning copy of the filter on the outer WHERE (RESTRICT_READ_ONLY places none).
     */
    @Test
    void aQualifiedReadUnderASameNamedCteKeepsThePruningCopy() throws Exception {
        for (SqlAuthorizer a : List.of(SqlAuthorizer.RESTRICTED_DATASOURCE_AUTHORIZER)) {
            for (var claims : grants()) {
                JsonNode authorized = a.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn,
                        "WITH orders AS (SELECT 0 AS id) SELECT id FROM main.orders"), claims);
                JsonNode outer = Transformations.getFirstStatementNode(authorized);
                assertTrue(outer.path("where_clause").isObject(), a + ": " + Transformations.parseToSql(conn, authorized));
            }
        }
    }

    /** A WITH over a set operation is accepted (its names are CTEs, not tables) and its bodies filtered. */
    @Test
    void aWithOverASetOperationIsAuthorizedAndFiltered() throws Exception {
        for (SqlAuthorizer a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                assertEquals(List.of("1", "1", "3", "3"), run(a, "WITH x AS (SELECT id FROM orders) "
                        + "SELECT id FROM x UNION ALL SELECT id FROM orders ORDER BY id", claims), a.toString());
            }
        }
    }

    /** In one query, the bare name still reads the CTE and the qualified one the filtered table. */
    @Test
    void theBareNameStillReadsTheShadowingCte() throws Exception {
        for (SqlAuthorizer a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                assertEquals(List.of("0", "1", "3"), run(a, "WITH orders AS (SELECT 0 AS id) "
                        + "SELECT id FROM orders UNION ALL SELECT id FROM main.orders ORDER BY id", claims), a.toString());
                // DuckDB matches CTE names case-insensitively, so ORDERS is the CTE too.
                assertEquals(List.of("0", "1", "3"), run(a, "WITH orders AS (SELECT 0 AS id) "
                        + "SELECT id FROM ORDERS UNION ALL SELECT id FROM main.orders ORDER BY id", claims), a.toString());
            }
        }
    }

    /** A table the grant does not list cannot be reached by hiding it behind a same-named CTE either. */
    @Test
    void aCteNamedLikeAnUngrantedTableDoesNotHideAQualifiedRead() throws Exception {
        for (SqlAuthorizer a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                for (String sql : new String[]{
                        "WITH secrets AS (SELECT 0 AS amount) SELECT amount FROM main.secrets",
                        // A WITH over a set operation: its bodies are reads too.
                        "WITH x AS (SELECT amount FROM secrets) SELECT amount FROM x UNION ALL SELECT amount FROM orders",
                        "WITH x AS (SELECT amount FROM secrets) SELECT amount FROM orders UNION ALL SELECT amount FROM x",
                        "WITH orders AS (SELECT amount FROM secrets) SELECT amount FROM orders UNION ALL SELECT amount FROM orders"}) {
                    List<Object> rows;
                    try {
                        rows = run(a, sql, claims);
                    } catch (UnauthorizedException refused) {
                        continue;
                    }
                    assertTrue(!rows.contains("999"), a + " / " + sql + " read an ungranted table: " + rows);
                }
            }
        }
    }

    /** Asserts {@code sql} returns no tenant-xyz order and no row of the ungranted {@code secrets}, or is refused. */
    private static void assertNoLeak(SqlAuthorizer a, String sql, Map<String, String> claims) throws Exception {
        List<Object> rows;
        try {
            JsonNode authorized = a.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn, sql), claims);
            rows = allColumns(Transformations.parseToSql(conn, authorized));
        } catch (UnauthorizedException refused) {
            return;
        }
        for (Object row : rows) {
            String r = String.valueOf(row);
            assertTrue(!r.contains("xyz") && !r.contains("200") && !r.contains("600") && !r.contains("999"),
                    a + " / " + sql + " leaked: " + rows);
        }
    }

    /** Every column of every row, comma-joined per row. */
    private static List<Object> allColumns(String sql) throws SQLException {
        List<Object> rows = new ArrayList<>();
        try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= n; i++) b.append(i > 1 ? "," : "").append(rs.getObject(i));
                rows.add(b.toString());
            }
        }
        return rows;
    }

    /**
     * DuckDB folds only ASCII letters when it matches identifiers; Java's equalsIgnoreCase also folds
     * U+017F (long s) to {@code s}, which took {@code "orderſ"} for {@code orders}.
     */
    @Test
    void aCteWhoseNameFoldsToTheTableOnlyOutsideAsciiDoesNotHideIt() throws Exception {
        for (SqlAuthorizer a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                assertNoLeak(a, "WITH \"orderſ\" AS (SELECT 1 AS id) SELECT id, tenant_id FROM orders", claims);
                assertNoLeak(a, "WITH \"secretſ\" AS (SELECT 1 AS id) SELECT s.id, s.amount FROM orders o, secrets s", claims);
            }
        }
    }

    /** In a recursive CTE the own name is the CTE only in the recursive arm; in the anchor it is the table. */
    @Test
    void aRecursiveCteAnchorReadOfTheSameNameIsFiltered() throws Exception {
        for (SqlAuthorizer a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                assertNoLeak(a, "WITH RECURSIVE orders AS (SELECT id, tenant_id, amount FROM orders "
                        + "UNION ALL SELECT id, tenant_id, amount FROM orders WHERE false) SELECT id, tenant_id FROM orders", claims);
                assertNoLeak(a, "WITH RECURSIVE secrets AS (SELECT id, tenant_id, amount FROM secrets UNION ALL "
                        + "SELECT id, tenant_id, amount FROM secrets WHERE false) "
                        + "SELECT s.id, s.amount FROM secrets s, (SELECT 1 FROM orders LIMIT 1) o", claims);
                // The recursive arm's self-reference still binds to the CTE.
                assertEquals(List.of("3"), run(a, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
                        + "SELECT max(n) FROM r, (SELECT 1 FROM orders LIMIT 1) o", claims), a.toString());
            }
        }
    }

    /** A USING KEY recursive arm reads its recurring table as {@code recurring.<name>}: a CTE, not a schema. */
    @Test
    void aUsingKeyRecurringReferenceIsTheCte() throws Exception {
        for (SqlAuthorizer a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                assertEquals(List.of("1", "3"), run(a, "WITH RECURSIVE r USING KEY (id) AS (SELECT id, amount FROM orders "
                        + "UNION SELECT r.id, r.amount FROM recurring.r WHERE false) SELECT id FROM r ORDER BY id", claims), a.toString());
            }
        }
    }

    /**
     * Only the CTE's own USING KEY arm may read {@code recurring.<name>} as its recurring table. A
     * quoted CTE may be named {@code "recurring.secrets"}; that must not make the real
     * {@code recurring.secrets} look like a CTE.
     */
    @Test
    void aCteNamedLikeARecurringReferenceDoesNotHideTheRecurringSchema() throws Exception {
        for (SqlAuthorizer a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                assertNoLeak(a, "WITH \"recurring.secrets\" AS (SELECT 1) SELECT s.* FROM orders o, recurring.secrets s", claims);
                assertNoLeak(a, "WITH \"recurring.orders\" AS (SELECT 1) SELECT o.* FROM orders x, recurring.orders o", claims);
                assertNoLeak(a, "WITH secrets AS (SELECT 1 AS x) SELECT s.* FROM orders o, recurring.secrets s", claims);
            }
        }
    }

    /** A subquery anywhere in an expression is a read, not only in the fields a walker happens to list. */
    @Test
    void aSubqueryInAnyExpressionPositionIsFiltered() throws Exception {
        String[] shapes = {
                "SELECT * REPLACE ((SELECT string_agg(tenant_id || ':' || amount, ',') FROM %s) AS tenant_id) FROM orders",
                "SELECT lead(amount, 99, (SELECT sum(amount) FROM %s)) OVER () AS v FROM orders",
                "SELECT sum(amount) OVER (PARTITION BY (SELECT max(amount) FROM %s)) AS v FROM orders",
                "SELECT sum(amount) OVER (ORDER BY (SELECT max(amount) FROM %s)) AS v FROM orders",
                "SELECT count(*) FILTER (WHERE amount < (SELECT max(amount) FROM %s)) OVER () AS v FROM orders",
                "SELECT string_agg(id::VARCHAR, ',' ORDER BY (SELECT max(amount) FROM %s)) AS v FROM orders",
                "SELECT id, (SELECT max(amount) FROM %s) BETWEEN 0 AND 1000000 AS v, "
                        + "(SELECT max(amount) FROM %s)::VARCHAR AS w FROM orders",
                "SELECT DISTINCT ON ((SELECT max(amount) FROM %s)) id FROM orders"};
        for (SqlAuthorizer a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                for (String shape : shapes) {
                    assertNoLeak(a, shape.replace("%s", "orders"), claims);
                    assertNoLeak(a, shape.replace("%s", "secrets"), claims);
                }
            }
        }
    }

    /** A declared CTE named like a generated filter CTE in another case must not take its place. */
    @Test
    void aCteNamedLikeTheGeneratedFilterCteInAnotherCaseDoesNotReplaceIt() throws Exception {
        for (SqlAuthorizer a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                assertEquals(List.of("1", "3"), run(a, "WITH ___ORDERS AS (SELECT 7 AS id) SELECT id FROM orders ORDER BY id", claims),
                        a.toString());
                assertEquals(List.of("1", "3"), run(a, "WITH ___MEMORY_MAIN_ORDERS AS (SELECT 7 AS id) "
                        + "SELECT id FROM memory.main.orders ORDER BY id", claims), a.toString());
            }
        }
    }
}
