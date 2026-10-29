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
     * A nested CTE named like an OUTER CTE binds the outer one in its own body (DuckDB: the inner
     * {@code o} below reads the outer {@code o}, not a table). The injector used to drop the outer
     * name from that body's scope, wrap {@code o} as a base table, and the query then failed to bind
     * (no table {@code o}) where it should return the filtered rows.
     */
    @Test
    void aNestedCteReadingASameNamedOuterCteReadsTheOuterCte() throws Exception {
        String sql = "WITH o AS (SELECT id FROM orders) "
                + "SELECT id FROM (WITH o AS (SELECT id FROM o) SELECT id FROM o) ORDER BY id";
        for (SqlAuthorizer a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                assertEquals(List.of("1", "3"), run(a, sql, claims), a + " / " + claims);
            }
        }
    }

    /**
     * A real table whose name equals a generated filter-CTE name ({@code ___orders} beside
     * {@code orders}) keeps its own rows: the generated name for {@code orders} must not take
     * {@code ___orders}, or that table's filter-CTE body {@code SELECT * FROM ___orders} binds to the
     * orders filter CTE and returns orders rows.
     */
    @Test
    void aTableNamedLikeAGeneratedFilterCteKeepsItsOwnRows() throws Exception {
        conn.createStatement().execute("CREATE TABLE ___orders (id INT, tenant_id VARCHAR, amount INT)");
        conn.createStatement().execute("INSERT INTO ___orders VALUES (42,'abc',1)");
        try {
            JsonNode query = Transformations.parseToTree(conn,
                    "SELECT 'o' AS t, id FROM orders UNION ALL SELECT 'u' AS t, id FROM ___orders ORDER BY t, id");
            JsonNode filtered = Transformations.injectFilterCtes(query, Map.of(
                    "memory.main.orders", SqlAuthorizer.compileFilterString("tenant_id = 'abc'"),
                    "memory.main.___orders", SqlAuthorizer.compileFilterString("tenant_id = 'abc'")), DB, SCHEMA);
            assertEquals(List.of("o:1", "o:3", "u:42"),
                    allColumns(Transformations.parseToSql(conn, filtered)).stream()
                            .map(r -> String.valueOf(r).replace(",", ":")).toList());
        } finally {
            conn.createStatement().execute("DROP TABLE ___orders");
        }
    }

    /**
     * {@code withUpdatedDatabaseSchema} skips a top-level FROM that names one of the query's CTEs.
     * A QUALIFIED name is never that CTE, so it still gets the connection's catalog: otherwise a
     * redirect target resolves {@code main.orders} against its own default catalog.
     */
    @Test
    void aQualifiedTopLevelReadUnderASameNamedCteStillGetsTheCatalog() throws Exception {
        JsonNode updated = SqlAuthorizer.withUpdatedDatabaseSchema(Transformations.parseToTree(conn,
                "WITH orders AS (SELECT 0 AS id) SELECT id FROM main.orders"), DB, SCHEMA);
        JsonNode from = Transformations.getFirstStatementNode(updated).get("from_table");
        assertEquals(DB, from.get("catalog_name").asText());
        // The bare name IS the CTE and stays unqualified.
        JsonNode bare = Transformations.getFirstStatementNode(SqlAuthorizer.withUpdatedDatabaseSchema(
                Transformations.parseToTree(conn, "WITH orders AS (SELECT 0 AS id) SELECT id FROM orders"),
                DB, SCHEMA)).get("from_table");
        assertEquals("", bare.get("catalog_name").asText());
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

    /**
     * A table function's ARGUMENTS can hold a subquery (DuckDB evaluates it), so a read placed there
     * is a read like any other: it must be authorized and filtered, or refused. The collector looked
     * only at the first argument's constant path, so {@code secrets} below was never checked.
     */
    @Test
    void aSubqueryInATableFunctionArgumentIsAuthorized() throws Exception {
        String[] queries = {
                "SELECT count(*) AS n FROM orders, range(0, (SELECT max(amount) FROM secrets)) r",
                "SELECT r.* FROM orders, unnest((SELECT list(amount) FROM secrets)) r",
                "SELECT r.* FROM orders, unnest((SELECT list(tenant_id) FROM orders)) r",
                "SELECT count(*) AS n FROM orders, range((SELECT count(*) FROM orders o2 WHERE o2.tenant_id = 'xyz') * 600) r",
        };
        for (var a : restrictedAuthorizers()) {
            for (var claims : grants()) {
                for (String sql : queries) {
                    assertNoLeak(a, sql, claims);
                }
            }
        }
    }

    /**
     * Under a FUNCTION grant {@code range} itself is allowed, so only the argument's read can be
     * refused. Unchecked, {@code count(*)} here is secrets' {@code max(amount)}: 999.
     */
    @Test
    void aSubqueryInAGrantedFunctionsArgumentIsAuthorized() throws Exception {
        var rangeGrants = List.of(
                Map.of(Headers.HEADER_ACCESS, "[[\"function\",\"range\",\"*\",\"true\"]]"),
                Map.of(Headers.HEADER_FUNCTION, "range", Headers.HEADER_PATH, "unused/"));
        for (var claims : rangeGrants) {
            assertNoLeak(authorizer, "SELECT count(*) AS n FROM range(0, (SELECT max(amount) FROM secrets)) r", claims);
            assertNoLeak(authorizer, "SELECT count(*) AS n FROM range((SELECT max(amount) FROM secrets)) r", claims);
        }
    }

    /**
     * A path grant checks every path a table function names. A list that mixes an allowed constant
     * with a COMPUTED element used to be checked on the constant alone, and DuckDB then read the
     * computed one: any path, past the grant. Such a read is refused.
     */
    @Test
    void aPathGrantRefusesAComputedPathInAList() throws Exception {
        // The legacy unfiltered path claim returns the query as is once the paths pass, so it is
        // the shape that leaked: it allowed the list below and DuckDB read /etc/passwd.
        var claims = Map.of(Headers.HEADER_PATH, "example/data/hive_table");
        for (String sql : new String[]{
                "SELECT count(*) FROM read_parquet(['example/data/hive_table/a.parquet', '/etc/' || 'passwd'])",
                "SELECT count(*) FROM read_parquet(['example/data/hive_table/a.parquet', getvariable('p')])",
                "SELECT count(*) FROM read_parquet('/etc/' || 'passwd')"}) {
            assertThrows(UnauthorizedException.class, () ->
                    authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn, sql), claims), sql);
        }
        // An all-constant list of granted paths is still allowed.
        authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn,
                "SELECT count(*) FROM read_parquet(['example/data/hive_table/a.parquet', 'example/data/hive_table/b.parquet'])"),
                claims);
    }

    /**
     * A table function in FROM is refused under a table grant, a row generator included: a
     * catalog table macro named range / unnest / generate_series shadows the built-in (even
     * unqualified) and reads whatever its body reads, so the name cannot make it data-free. The
     * select-list form, {@code SELECT unnest(col) FROM t}, is a scalar call and still works.
     */
    @Test
    void aTableFunctionInFromIsRefusedUnderATableGrantButSelectListUnnestWorks() throws Exception {
        for (String sql : new String[]{
                "SELECT u.x FROM orders o, unnest([o.id * 10, o.id * 10 + 1]) u(x)",
                "SELECT * FROM orders, range(3)",
                "SELECT * FROM orders, range(1 + 0)"}) {
            assertThrows(UnauthorizedException.class, () -> run(sql, Map.of(Headers.HEADER_TABLE, "orders")), sql);
            for (var claims : grants()) {
                assertThrows(UnauthorizedException.class, () -> run(sql, claims), sql + " / " + claims);
            }
        }
        for (var claims : grants()) {
            assertEquals(List.of("10", "11", "30", "31"),
                    run("SELECT unnest([id * 10, id * 10 + 1]) AS x FROM orders ORDER BY x", claims),
                    claims.toString());
        }
    }

    /**
     * A subquery in an AT clause would run inside the filter CTE's body (DuckDB ignores AT on a CTE
     * reference) without the filter, reading every tenant's rows: refused.
     */
    @Test
    void aSubqueryInAnAtClauseIsRefused() {
        for (var claims : grants()) {
            assertThrows(IllegalStateException.class, () ->
                    run("SELECT * FROM orders AT (VERSION => (SELECT max(id) FROM orders))", claims));
        }
    }

    /**
     * TABLESAMPLE stays on the reference that wrote it: the filter CTE's body is shared by every
     * reference, so a sample in it sampled references the query did not sample.
     */
    @Test
    void aTablesampleStaysOnItsOwnReference() throws Exception {
        JsonNode authorized = authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn,
                "SELECT count(*) FROM orders a TABLESAMPLE 50% JOIN orders b USING (id)"), grants().get(0));
        String sql = Transformations.parseToSql(conn, authorized);
        JsonNode statement = Transformations.getFirstStatementNode(authorized);
        JsonNode body = statement.get("cte_map").get("map").get(0).get("value").get("query").get("node");
        assertTrue(body.get("from_table").get("sample").isNull(), "the shared body is not sampled: " + sql);
        JsonNode join = statement.get("from_table");
        assertTrue(!join.get("left").get("sample").isNull(), "a keeps its sample: " + sql);
        assertTrue(join.get("right").get("sample").isNull(), "b was never sampled: " + sql);
    }

    /**
     * One table read as {@code main.orders} and as {@code orders} is one table, so its reads share
     * one filter CTE -- otherwise a volatile filter, kept materialized precisely so every read sees
     * one row set, is evaluated once per spelling.
     */
    @Test
    void differentlyQualifiedReadsOfOneTableShareOneFilterCte() throws Exception {
        JsonNode authorized = authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn,
                "SELECT id FROM orders WHERE id IN (SELECT id FROM main.orders) AND id IN (SELECT id FROM memory.main.orders)"),
                grants().get(0));
        JsonNode cteMap = Transformations.getFirstStatementNode(authorized).get("cte_map").get("map");
        assertEquals(1, cteMap.size(), Transformations.parseToSql(conn, authorized));
    }

    /** A "." or ".." segment cannot walk a path out of the granted prefix. */
    @Test
    void aPathGrantRefusesDotDotSegments() {
        var claims = Map.of(Headers.HEADER_PATH, "example/data/hive_table");
        for (String sql : new String[]{
                "SELECT count(*) FROM read_parquet('example/data/hive_table/../../../etc/passwd')",
                "SELECT count(*) FROM read_parquet('example/data/hive_table/./a.parquet')"}) {
            assertThrows(UnauthorizedException.class, () ->
                    authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn, sql), claims), sql);
        }
    }

    /**
     * A path grant's filter must bind to the file's own columns. Over a CTE the user defines the
     * columns ({@code * REPLACE ('abc' AS tenant_id)} passed every row), and a positional UNION takes
     * its names from the user's left arm: both refused. UNION BY NAME, the generated shape, passes.
     */
    @Test
    void aPathGrantFilterRefusesShapesWhereTheUserDefinesTheColumns() throws Exception {
        var claims = Map.of(Headers.HEADER_ACCESS, "[[\"path\",\"example/data/hive_table\",\"*\",\"key='k2'\"]]");
        for (String sql : new String[]{
                "WITH x AS (SELECT * REPLACE ('k2' AS key) FROM read_parquet('example/data/hive_table/*/*/*.parquet')) SELECT * FROM x",
                "FROM (FROM (VALUES (NULL::VARCHAR, NULL::VARCHAR)) t(value, key) WHERE false "
                        + "UNION ALL FROM read_parquet('example/data/hive_table/*/*/*.parquet'))"}) {
            assertThrows(UnauthorizedException.class, () ->
                    authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn, sql), claims), sql);
        }
        authorizer.authorize("user", DB, SCHEMA,
                Transformations.parseToTree(conn, io.dazzleduck.sql.commons.util.TestConstants.SUPPORTED_HIVE_PATH_QUERY), claims);
    }

    /**
     * A path grant's filter must see the file's own columns in every arm that reads it, and there
     * must be one read: a REPLACE in the arm made {@code key='k2'} pass every row, and a second read
     * of the granted file (a scalar subquery) ran unfiltered.
     */
    @Test
    void aPathGrantFilterRefusesRenamedArmsAndASecondRead() throws Exception {
        var claims = Map.of(Headers.HEADER_ACCESS, "[[\"path\",\"example/data/hive_table\",\"*\",\"key='k2'\"]]");
        String file = "read_parquet('example/data/hive_table/*/*/*.parquet', hive_partitioning = true)";
        String typed = "FROM (VALUES(NULL::DATE, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR)) t(dt, p, key, value) WHERE false";
        for (String sql : new String[]{
                "FROM (" + typed + " UNION ALL BY NAME SELECT * REPLACE ('k2' AS key) FROM " + file + ")",
                "FROM (" + typed + " UNION ALL BY NAME SELECT dt, p, 'k2' AS key, value FROM " + file + ")",
                "FROM (" + typed + " UNION ALL BY NAME FROM " + file + " f(dt, p, value, key))",
                "SELECT (SELECT string_agg(DISTINCT key) FROM " + file + ") AS leak, count(*) FROM ("
                        + io.dazzleduck.sql.commons.util.TestConstants.SUPPORTED_HIVE_PATH_QUERY + ")"}) {
            assertThrows(UnauthorizedException.class, () ->
                    authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn, sql), claims), sql);
        }
        // The generated shape still passes, and a grant with no filter (true) is not restricted.
        authorizer.authorize("user", DB, SCHEMA,
                Transformations.parseToTree(conn, io.dazzleduck.sql.commons.util.TestConstants.SUPPORTED_HIVE_PATH_QUERY), claims);
        var unfiltered = Map.of(Headers.HEADER_ACCESS, "[[\"path\",\"example/data/hive_table\",\"*\",\"true\"]]");
        authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn,
                "FROM (" + typed + " UNION ALL BY NAME SELECT * REPLACE ('k2' AS key) FROM " + file + ")"), unfiltered);
    }

    /** Column aliases stay on the reference: the shared body keeps the table's own columns. */
    @Test
    void columnAliasesOnAFilteredTableStillWork() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("1", "3"),
                    run("SELECT a FROM orders AS o(a, b, c) ORDER BY a", claims), claims.toString());
            // and a second reference without aliases shares the body unchanged
            assertEquals(List.of("1", "1", "3", "3"),
                    run("SELECT a FROM orders AS o(a, b, c) UNION ALL SELECT id FROM orders ORDER BY 1", claims),
                    claims.toString());
        }
    }

    /** Every spelling of one table gets its filter: the first reference's case must not decide it. */
    @Test
    void aTableSpelledDifferentlyStillGetsItsFilter() throws Exception {
        JsonNode filtered = Transformations.injectFilterCtes(Transformations.parseToTree(conn,
                        "SELECT count(*) FROM \"ORDERS\" a JOIN orders b USING (id)"),
                Map.of("orders", SqlAuthorizer.compileFilterString("tenant_id = 'abc'")), DB, SCHEMA);
        assertEquals(List.of("2"), firstColumn(Transformations.parseToSql(conn, filtered)));
    }

    /** Two identical AT clauses are one version: one filter CTE, whatever their source positions. */
    @Test
    void identicalAtClausesShareOneFilterCte() throws Exception {
        JsonNode filtered = Transformations.injectFilterCtes(Transformations.parseToTree(conn,
                        "SELECT * FROM orders AS a AT (VERSION => 3) JOIN orders AS b AT (VERSION => 3) USING (id)"),
                Map.of("orders", SqlAuthorizer.compileFilterString("tenant_id = 'abc'")), DB, SCHEMA);
        assertEquals(1, Transformations.getFirstStatementNode(filtered).get("cte_map").get("map").size());
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
