package io.dazzleduck.sql.common.authorization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.auth.UnauthorizedException;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.Transformations.CatalogSchemaTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Date;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class SimpleAuthorizerTest {

    static Stream<Arguments> provideParametersForPaths() {
        return Stream.of(
                Arguments.of("select * from read_parquet('/x/y/z/abc.parquet')", "/x/y/z/abc.parquet", Transformations.TableType.TABLE_FUNCTION, "read_parquet"),
                Arguments.of("select * from read_json('abc.json')", "abc.json", Transformations.TableType.TABLE_FUNCTION, "read_json"),
                Arguments.of("select * from read_csv('abc.csv')", "abc.csv", Transformations.TableType.TABLE_FUNCTION, "read_csv")
        );
    }

    static Stream<Arguments> provideParametersForTables() {
        return Stream.of(
                Arguments.of("select * from a", "a", "BASE_TABLE")
        );
    }

    public static String TABLE_FUNCTION_INNER_QUERY = "FROM (FROM (VALUES(NULL::VARCHAR, NULL::MAP(VARCHAR, VARCHAR), NULL::VARCHAR[], NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR)) t(missing_str, missing_map, missing_array, dt, p, key, value) \n" +
            "WHERE false\n" +
            "UNION ALL BY NAME\n" +
            "FROM read_parquet('example/hive_table/*/*/*.parquet'))";
    static Stream<Arguments> provideParametersForTableFunction() {
        return Stream.of(
                Arguments.of(TABLE_FUNCTION_INNER_QUERY),
                Arguments.of(String.format("%s where dt = 'x'", TABLE_FUNCTION_INNER_QUERY)),
                Arguments.of(String.format("select count(*), key FROM (%s where dt = 'x')", TABLE_FUNCTION_INNER_QUERY))
        );
    }


    @ParameterizedTest()
    @ValueSource(strings = {
            "select count(*), c from (select * from t1 where a = b) group by c",
            "select * from t1 where a = b",
            "select * from t1"
    })
    public void testBaseTable(String sql) throws SQLException, JsonProcessingException {
        String filterToCombiner = "select * from t1 where x = y";
        var query = Transformations.parseToTree(sql);
        var toAddQuery = Transformations.parseToTree(filterToCombiner);
        var toAddFilter = Transformations.getWhereClauseForBaseTable(Transformations.getFirstStatementNode(toAddQuery));
        var newQuery = SimpleAuthorizer.addFilterToBaseTable(query, toAddFilter);
        var newQueryString = Transformations.parseToSql(newQuery);
        assertTrue(newQueryString.contains("x") && newQueryString.contains("y"));
    }


    @ParameterizedTest()
    @MethodSource("provideParametersForTableFunction")
    public void testTableFunction(String sql) throws SQLException, JsonProcessingException {
        String filterToCombiner = "select * from t1 where dt = 'abcdy'";
        var query = Transformations.parseToTree(sql);
        var toAddQuery = Transformations.parseToTree(filterToCombiner);
        var toAddFilter = Transformations.getWhereClauseForBaseTable(Transformations.getFirstStatementNode(toAddQuery));
        var newQuery = SimpleAuthorizer.addFilterToTableFunction(query, toAddFilter);
        var newQueryString = Transformations.parseToSql(newQuery);
        System.out.println(newQueryString);
        assertTrue(newQueryString.contains("x") && newQueryString.contains("abcdy"));
    }


    @ParameterizedTest()
    @ValueSource(strings = {
            "select * from read_parquet('abc')",
            "select * from read_delta('abc')"
    })
    public void validateForAuthorizationPositiveTest(String sql) throws SQLException, JsonProcessingException, UnauthorizedException {
        JsonNode parsedQuery = Transformations.parseToTree(sql);
        SimpleAuthorizer.validateForAuthorization(parsedQuery);
    }

    @ParameterizedTest()
    @ValueSource(strings = {
            "with x as ( select * from generate_series(10)  where x = y) select * from x",
            "select * from a ,b ",
            "select * from read_parquet('abc') where a in (select name from b)",
            "select * from read_parquet('abc') where a in (select name from b) and c = d",
            "select * from read_parquet('abc') where a in (select name from b) or c = d"
    })
    public void validateForAuthorizationFailureTest(String sql) throws SQLException, JsonProcessingException, UnauthorizedException {
        JsonNode parsedQuery = Transformations.parseToTree(sql);
        var thrown = assertThrows(
                UnauthorizedException.class,
                () -> SimpleAuthorizer.validateForAuthorization(parsedQuery),
                "Expected doThing() to throw, but it didn't");
    }

    @Test
    public void testLoad() throws SQLException, JsonProcessingException, UnauthorizedException {
        var userGroupMapping = Map.of("u1", List.of("g1", "g2"));
        var accessRows = List.of(
                new AccessRow("g1", "d1", "s1", "t1", Transformations.TableType.BASE_TABLE, List.of(), "a = 1", new Date(0), null),
                new AccessRow("g2", "d1", "s1", "t1", Transformations.TableType.BASE_TABLE, List.of(), "a = 2", new Date(0), null ));
        var s = new SimpleAuthorizer(userGroupMapping, accessRows);
        var q = Transformations.parseToTree("select * from t1  where t = x");
        var result = s.authorize("u1", "d1", "s1", q);
        var sql = Transformations.parseToSql(result);
        assertEquals("SELECT * FROM t1 WHERE ((t = x) AND ((a = 2) OR (a = 1)))", sql);
    }

    @ParameterizedTest
    @MethodSource("provideParametersForPaths")
    void testGetTableForPaths(String sql, String expectedTablePath, Transformations.TableType tableType, String functionName) throws SQLException, JsonProcessingException {


        var tree = Transformations.parseToTree(sql);
        var tableOrPaths =
        Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(tree), "c", "s");
        assertEquals(1, tableOrPaths.size());
        assertEquals(new Transformations.CatalogSchemaTable(null, null, expectedTablePath, tableType, functionName),
                tableOrPaths.get(0));
    }

    @ParameterizedTest
    @MethodSource("provideParametersForTables")
    void testGetTableForTable(String sql, String expectedPath, Transformations.TableType tableType) throws SQLException, JsonProcessingException {
        var tree = Transformations.parseToTree(sql);
        var tableOrPaths =
        Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(tree), "c", "s");
        assertEquals(1, tableOrPaths.size());
        assertEquals(new CatalogSchemaTable("c", "s", expectedPath, tableType),
                tableOrPaths.get(0));
    }

    @Test
    public void testDDL() throws SQLException, JsonProcessingException {
        var sql = "select null::struct( a string, b struct ( x string)) union all select a ";
        var tree = Transformations.parseToTree(sql);
    }
}
