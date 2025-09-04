package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.auth.UnauthorizedException;
import io.dazzleduck.sql.common.authorization.*;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.util.TestConstants;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class AccessControlTest {

    String supportedTableQuery = "SELECT a, b, c from test_table where filter1 and filter2";
    String filter = "key = 'k1'";
    AccessRow pathAccessRow = new AccessRow("test_group", null, null, "example/hive_table/*/*/*.parquet", Transformations.TableType.TABLE_FUNCTION, List.of(), filter, new Date(System.currentTimeMillis() + Duration.ofHours(1).toMillis()), "read_parquet");
    AccessRow tableAccessRow = new AccessRow("test_group", "test_db", "test_schema", "test_table", Transformations.TableType.BASE_TABLE, List.of(), filter, new Date(System.currentTimeMillis() + Duration.ofHours(1).toMillis()), null);
    SqlAuthorizer sqlAuthorizer = new SimpleAuthorizer(Map.of("test_user", List.of("test_group")),
            List.of(pathAccessRow, tableAccessRow));

    String aggregateSql(String inner) {
        return "SELECT key, count(*), count(distinct value) FROM (%s) GROUP BY key".formatted(inner);
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
        File authorizationFile = File.createTempFile("authorization", ".json");
        authorizationFile.deleteOnExit();
        String authFileContent = """
                {
                  "role": "admin",
                  "database": null,
                  "schema": null,
                  "path": "example/hive_table/*/*/*.parquet",
                  "tableType": "TABLE_FUNCTION",
                  "columns": [],
                  "condition": "p = '1'",
                  "metadata": null,
                  "function": "read_parquet"
                }
                """;
        try (var writer = new FileWriter(authorizationFile)) {
            writer.write(authFileContent);
        }
        String authFileLocation = authorizationFile.getAbsolutePath();
        var accessConf = ConfigFactory.parseResources("access.conf");
        var classConfig = "%s.%s=%s".formatted(AuthorizationProvider.AUTHORIZATION_CONFIG_PREFIX, AuthorizationProvider.AUTHORIZATION_CONFIG_PROVIDER_CLASS_KEY, LocalAuthorizationConfigProvider.class.getName());
        var accessGroupConfig = "%s.%s=%s".formatted(AuthorizationProvider.AUTHORIZATION_CONFIG_PREFIX, LocalAuthorizationConfigProvider.AUTHORIZATION_KEY, accessConf.root().render());
        var accessRowConfig = "%s.%s=%s".formatted(AuthorizationProvider.AUTHORIZATION_CONFIG_PREFIX, LocalAuthorizationConfigProvider.AUTHORIZATION_FILE_KEY, authFileLocation);
        Main.main(new String[]{"--conf", classConfig, "--conf", accessGroupConfig, "--conf", accessRowConfig});
    }
}
