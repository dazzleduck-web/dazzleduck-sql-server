package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dazzleduck.sql.common.auth.UnauthorizedException;
import io.dazzleduck.sql.common.authorization.AccessRow;
import io.dazzleduck.sql.common.authorization.SimpleAuthorizer;
import io.dazzleduck.sql.common.authorization.SqlAuthorizer;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.util.TestConstants;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.flight.FlightStreamReader;
import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class AccessControlTest {

    public static final String TEST_CATALOG = "test_catalog";
    public static final String TEST_SCHEMA = "test_schema";
    public static final String TEST_USER = "test_user";
    static String supportedTableQuery = "SELECT a, b, c from test_table where filter1 and filter2";
    static String filter = "key = 'k1'";
    public static final String TEST_GROUP = "test_group";
    public static final String TEST_TABLE = "test_table";
    static AccessRow pathAccessRow = new AccessRow("test_group", null, null, "example/hive_table/*/*/*.parquet", Transformations.TableType.TABLE_FUNCTION, List.of(), filter, new Date(System.currentTimeMillis() + Duration.ofHours(1).toMillis()), "read_parquet");
    static AccessRow tableAccessRow = new AccessRow(TEST_GROUP, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, Transformations.TableType.BASE_TABLE, List.of(), filter, new Date(System.currentTimeMillis() + Duration.ofHours(1).toMillis()), null);
    static SqlAuthorizer sqlAuthorizer = new SimpleAuthorizer(Map.of(TEST_USER, List.of(TEST_GROUP)),
            List.of(pathAccessRow, tableAccessRow));

    static String aggregateSql(String innerSql) {
        return "SELECT key, count(*), count(distinct value) FROM (%s) GROUP BY key".formatted(innerSql);
    }

    static Location TEST_LOCATION = Location.forGrpcInsecure("localhost", 38888);

    static ServerClient SERVER_CLIENT;

    @BeforeAll
    public static void setup() throws IOException, NoSuchAlgorithmException {
        var flightTestUtils = new FlightTestUtils() {
            @Override
            public String testSchema() {
                return TEST_SCHEMA;
            }

            @Override
            public String testCatalog() {
                return TEST_CATALOG;
            }

            @Override
            public String testUser() {
                return TEST_USER;
            }
        };

        var  tableName = "%s.%s.%s".formatted(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
        SERVER_CLIENT = flightTestUtils.createRestrictedServerClient(TEST_LOCATION, sqlAuthorizer);
        var warehousePath = SERVER_CLIENT.warehousePath();
        var setupSql = new String[]{
                "ATTACH '%s/%s.duckdb' AS %s".formatted(warehousePath, TEST_CATALOG, TEST_CATALOG),
                "CREATE SCHEMA %s.%s".formatted(TEST_CATALOG, TEST_SCHEMA),
                "CREATE TABLE %s (key string,  value string)".formatted(tableName),
                "INSERT INTO %s VALUES('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')".formatted(tableName)
        };
        ConnectionPool.executeBatch(setupSql);
    }

    @Test
    public void readMissingColumn() {
        ConnectionPool.printResult(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
    }

    @Test
    public void testAggregation() {
        var aggregateSql = aggregateSql(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
        ConnectionPool.printResult(aggregateSql);
    }

    @Test
    public void rowLevelFilterForPath() throws SQLException, JsonProcessingException, UnauthorizedException {
        var query = Transformations.parseToTree(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
        var authorizedQuery = sqlAuthorizer.authorize(AccessControlTest.TEST_USER, null, null, query);
        var result = Transformations.parseToSql(authorizedQuery);
        ConnectionPool.execute(result);
    }

    @Test
    public void rowLevelFilterForPathAggregation() throws SQLException, JsonProcessingException, UnauthorizedException {
        var aggregateSql = aggregateSql(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
        var query = Transformations.parseToTree(aggregateSql);
        var authorizedQuery = sqlAuthorizer.authorize(AccessControlTest.TEST_USER, null, null, query);
        var result = Transformations.parseToSql(authorizedQuery);
        ConnectionPool.execute(result);
    }

    @Test
    public void rowLevelFilterForTable() throws SQLException, JsonProcessingException, UnauthorizedException {
        var query = Transformations.parseToTree(supportedTableQuery);
        var authorizedQuery = sqlAuthorizer.authorize(TEST_USER, TEST_CATALOG, TEST_SCHEMA, query);
        Transformations.parseToSql(authorizedQuery);
    }

    @Test
    public void rowLevelFilterForTableAggregation() throws SQLException, JsonProcessingException, UnauthorizedException {
        var aggregateSql = aggregateSql(supportedTableQuery);
        var query = Transformations.parseToTree(aggregateSql);
        var authorizedQuery = sqlAuthorizer.authorize(AccessControlTest.TEST_USER, TEST_CATALOG, AccessControlTest.TEST_SCHEMA, query);
        Transformations.parseToSql(authorizedQuery);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select * from " + TEST_TABLE,
            "select * from " + TEST_SCHEMA + "." +TEST_TABLE ,
            "select * from " + TEST_CATALOG + "." + TEST_SCHEMA + "." +TEST_TABLE })
    public void testTable(String sql) throws SQLException, IOException {
        var info = SERVER_CLIENT.flightSqlClient().execute(sql);
        var stream = SERVER_CLIENT.flightSqlClient().getStream(info.getEndpoints().get(0).getTicket());
        var expected = "select * from %s.%s.%s where %s".formatted(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, filter);
        TestUtils.isEqual(expected, SERVER_CLIENT.clientAllocator(), FlightStreamReader.of(stream, SERVER_CLIENT.clientAllocator()));
    }

    @Test
    public void testPath() throws SQLException, IOException {
        var info = SERVER_CLIENT.flightSqlClient().execute(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
        var stream = SERVER_CLIENT.flightSqlClient().getStream(info.getEndpoints().get(0).getTicket());
        var expected = "select * from (%s) where %s".formatted(TestConstants.SUPPORTED_HIVE_PATH_QUERY, filter);
        TestUtils.isEqual(expected, SERVER_CLIENT.clientAllocator(), FlightStreamReader.of(stream, SERVER_CLIENT.clientAllocator()));
    }
}
