package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.LocalStartupConfigProvider;
import io.dazzleduck.sql.common.StartupScriptProvider;
import io.dazzleduck.sql.common.auth.UnauthorizedException;
import io.dazzleduck.sql.common.authorization.*;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.util.TestConstants;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.sql.Date;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.dazzleduck.sql.common.LocalStartupConfigProvider.SCRIPT_LOCATION_KEY;
import static org.junit.jupiter.api.Assertions.*;

public class AccessControlTest {

    String supportedTableQuery = "SELECT a, b, c from test_table where filter1 and filter2";
    String filter = "key = 'k1'";
    AccessRow pathAccessRow = new AccessRow("test_group", null, null, "example/hive_table/*/*/*.parquet", Transformations.TableType.TABLE_FUNCTION, List.of(), filter, new Date(System.currentTimeMillis() + Duration.ofHours(1).toMillis()), "read_parquet");
    AccessRow tableAccessRow = new AccessRow("test_group", "test_db", "test_schema", "test_table", Transformations.TableType.BASE_TABLE, List.of(), filter, new Date(System.currentTimeMillis() + Duration.ofHours(1).toMillis()), null);
    SqlAuthorizer sqlAuthorizer = new SimpleAuthorizer(Map.of("test_user", List.of("test_group")),
            List.of(pathAccessRow, tableAccessRow));

    private static final String USER = "admin";
    private static final String PASSWORD = "admin";
    private static final String LOCALHOST = "localhost";
    private static final String CLUSTER_HEADER_KEY = "cluster_id";
    private static final String TEST_CLUSTER = "TEST_CLUSTER";
    private static final BufferAllocator clientAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static FlightSqlClient sqlClient;
    private static final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55556);

    String aggregateSql(String inner) {
        return "SELECT key, count(*), count(distinct value) FROM (%s) GROUP BY key".formatted(inner);
    }

    @BeforeEach
    public void setUpServer() throws Exception {
        File startUpFile = File.createTempFile("/temp/startup/startUpFile", ".sql");
        startUpFile.deleteOnExit();
        String startUpFileContent = "CREATE SCHEMA IF NOT EXISTS example; CREATE TABLE IF NOT EXISTS example.hive_table (p STRING); INSERT INTO example.hive_table VALUES ('1'), ('2');";
        try (var writer = new FileWriter(startUpFile)) {
            writer.write(startUpFileContent);
        }
        String startUpFileLocation = startUpFile.getAbsolutePath();
        var startupClassConfig = "%s.%s=%s".formatted(StartupScriptProvider.STARTUP_SCRIPT_CONFIG_PREFIX, StartupScriptProvider.STARTUP_SCRIPT_CONFIG_PROVIDER_CLASS_KEY, LocalStartupConfigProvider.class.getName());
        var locationConfig = "%s.%s=%s".formatted(StartupScriptProvider.STARTUP_SCRIPT_CONFIG_PREFIX, SCRIPT_LOCATION_KEY, "\"" + startUpFileLocation.replace("\\", "/") + "\"");
        File authorizationFile = File.createTempFile("authorization", ".json");
        authorizationFile.deleteOnExit();
        String authFileContent =
                """
                    {"group": "restricted", "database": null, "schema": null, "tableOrPath": "example/hive_table/*/*/*.parquet", "tableType": "TABLE_FUNCTION", "columns": [], "filter": "p = '1'", "expiration": null, "functionName": "read_parquet"},
                    {"group": "admin", "database": null, "schema": null, "tableOrPath": "example/hive_table/*/*/*.parquet", "tableType": "TABLE_FUNCTION", "columns": [], "filter": "p = '1'", "expiration": null, "functionName": "read_parquet"}
                """;
        try (var writer = new FileWriter(authorizationFile)) {
            writer.write(authFileContent);
        }
        String authFileLocation = authorizationFile.getAbsolutePath();
        var accessConf = ConfigFactory.parseResources("access.conf");
        var classConfig = "%s.%s=%s".formatted(AuthorizationProvider.AUTHORIZATION_CONFIG_PREFIX, AuthorizationProvider.AUTHORIZATION_CONFIG_PROVIDER_CLASS_KEY, LocalAuthorizationConfigProvider.class.getName());
        var accessGroupConfig = "%s.%s=%s".formatted(AuthorizationProvider.AUTHORIZATION_CONFIG_PREFIX, LocalAuthorizationConfigProvider.AUTHORIZATION_KEY, accessConf.root().render());
        var accessRowConfig = "%s.%s=%s".formatted(AuthorizationProvider.AUTHORIZATION_CONFIG_PREFIX, LocalAuthorizationConfigProvider.AUTHORIZATION_FILE_KEY, "\"" + authFileLocation.replace("\\", "/") + "\"");
        Main.main(new String[]{"--conf", "useEncryption=false", "--conf", "flight-sql.port=55556", "--conf", classConfig, "--conf", accessGroupConfig, "--conf", accessRowConfig, "--conf", startupClassConfig, "--conf", locationConfig, "--conf", "accessMode=" + AccessMode.RESTRICTED});
        sqlClient = new FlightSqlClient(FlightClient.builder(clientAllocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER, PASSWORD, Map.of(CLUSTER_HEADER_KEY, TEST_CLUSTER)))
                .build());
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
        var authorizedQuery = sqlAuthorizer.authorize("test_user", null, null, query);
        var result = Transformations.parseToSql(authorizedQuery);
        ConnectionPool.execute(result);
    }

    @Test
    public void rowLevelFilterForPathAggregation() throws SQLException, JsonProcessingException, UnauthorizedException {
        var aggregateSql = aggregateSql(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
        var query = Transformations.parseToTree(aggregateSql);
        var authorizedQuery = sqlAuthorizer.authorize("test_user", null, null, query);
        var result = Transformations.parseToSql(authorizedQuery);
        ConnectionPool.execute(result);
    }

    @Test
    public void rowLevelFilterForTable() throws SQLException, JsonProcessingException, UnauthorizedException {
        var query = Transformations.parseToTree(supportedTableQuery);
        var authorizedQuery = sqlAuthorizer.authorize("test_user", "test_db", "test_schema", query);
        Transformations.parseToSql(authorizedQuery);
    }

    @Test
    public void rowLevelFilterForTableAggregation() throws SQLException, JsonProcessingException, UnauthorizedException {
        var aggregateSql = aggregateSql(supportedTableQuery);
        var query = Transformations.parseToTree(aggregateSql);
        var authorizedQuery = sqlAuthorizer.authorize("test_user", "test_db", "test_schema", query);
        Transformations.parseToSql(authorizedQuery);
    }

    @Test
    public void authorizationTest() throws Exception {
        try (var conn = ConnectionPool.getConnection();
             var stmt = conn.createStatement();
             var rs = stmt.executeQuery("SELECT * FROM example.hive_table WHERE p = '1'")) {
            assertTrue(rs.next());
        }
    }

    @Test
    public void authorizationTest2() throws Exception {
        sqlClient.execute("SELECT 1");
    }
}
