package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestConstants;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;


public class JwtClaimBasedAuthorizerTest {

    public static final String TEST_CATALOG = "test_jwt_catalog";
    public static final String TEST_SCHEMA = "test_jwt_schema";
    public static final String TEST_USER = "restricted_user";
    public static final String TEST_TABLE =  "test_jwt_table";
    public static final String UNAUTHORIZED_TABLE = "unauthorized_"+TEST_TABLE;
    public static ServerClient SERVER_CLIENT = null;
    private static Location SERVER_TEST_LOCATION;

    @BeforeAll
    public static void setup() throws IOException, NoSuchAlgorithmException {
        // Use dynamic port allocation
        SERVER_TEST_LOCATION = FlightTestUtils.findNextLocation();
        var flightTestUtils = FlightTestUtils.createForDatabaseSchema(TEST_USER, "password", TEST_CATALOG, TEST_SCHEMA);
        var  tableName = "%s.%s.%s".formatted(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
        var  unauthorizedTableName = "%s.%s.%s".formatted(TEST_CATALOG, TEST_SCHEMA, UNAUTHORIZED_TABLE);

        SERVER_CLIENT = flightTestUtils.createRestrictedServerClient(SERVER_TEST_LOCATION,  Map.of("table", TEST_TABLE,
                        "filter", "key = 'k2'",
                        "path", "example/hive_table"));
        var setupSql = getSetupSql(tableName, unauthorizedTableName);
        ConnectionPool.executeBatch(setupSql);
    }

    @AfterAll
    public static void cleanup() throws Exception {
        // Close all resources
        if (SERVER_CLIENT != null) {
            SERVER_CLIENT.close();
        }

        // Clean up database
        try {
            ConnectionPool.execute("DROP SCHEMA %s.%s CASCADE".formatted(TEST_CATALOG, TEST_SCHEMA));
        } catch (Exception e) {
            System.err.println("Error dropping schema: " + e.getMessage());
        }

        try {
            ConnectionPool.execute("DETACH %s".formatted(TEST_CATALOG));
        } catch (Exception e) {
            System.err.println("Error detaching catalog: " + e.getMessage());
        }
    }


    private static String[] getSetupSql(String tableName,
                                        String unauthorizedTableName) {
        var warehousePath = SERVER_CLIENT.warehousePath();
        return new String[]{
                "ATTACH '%s/%s.duckdb' AS %s".formatted(warehousePath, TEST_CATALOG, TEST_CATALOG),
                "CREATE SCHEMA %s.%s".formatted(TEST_CATALOG, TEST_SCHEMA),
                "CREATE TABLE %s (key string,  value string)".formatted(tableName),
                "CREATE TABLE %s (key string,  value string)".formatted(unauthorizedTableName),
                "INSERT INTO %s VALUES('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')".formatted(tableName)
        };
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select * from " + TEST_TABLE,
            "select * from " + TEST_CATALOG +"." + TEST_SCHEMA+"." + TEST_TABLE
    })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testAuthorizedTable(String testQuery) throws Exception {
        var expectedQuery = "select * from %s.%s.%s where key =  'k2'".formatted(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
        FlightTestUtils.testQuery(expectedQuery,  testQuery, SERVER_CLIENT.flightSqlClient(), SERVER_CLIENT.clientAllocator());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select * from " + UNAUTHORIZED_TABLE,
            "select * from " + TEST_CATALOG +"." + TEST_SCHEMA+"." + UNAUTHORIZED_TABLE
    })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testUnAuthorizedTable(String testQuery) throws Exception {
        assertThrows(FlightRuntimeException.class, () -> SERVER_CLIENT.flightSqlClient().execute(testQuery));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            TestConstants.SUPPORTED_HIVE_PATH_QUERY
    })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testAuthorizedPath(String testQuery) throws Exception {
        var expectedQuery = "%s where key =  'k2'".formatted(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
        FlightTestUtils.testQuery(expectedQuery, testQuery, SERVER_CLIENT.flightSqlClient(), SERVER_CLIENT.clientAllocator());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            TestConstants.SUPPORTED_HIVE_UNAUTHORIZED_PATH_QUERY
    })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testUnAuthorizedPath(String testQuery) throws Exception {
        var expectedQuery = "%s where key =  'k2'".formatted(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
        assertThrows(FlightRuntimeException.class, () -> FlightTestUtils.testQuery(expectedQuery,  testQuery, SERVER_CLIENT.flightSqlClient(), SERVER_CLIENT.clientAllocator()));
    }
}
