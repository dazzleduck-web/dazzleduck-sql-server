package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.StatementHandle;
import io.dazzleduck.sql.login.LoginRequest;
import io.dazzleduck.sql.login.LoginResponse;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import org.junit.jupiter.api.*;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;

import static io.dazzleduck.sql.common.Headers.*;
import static io.dazzleduck.sql.http.server.HttpServerAuthorizationTest.warehousePath;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HttpDuckLakePlannerTest {

    private static final int PLANNER_PORT = 8095;
    private static final String BASE_URL = "http://localhost:%s".formatted(PLANNER_PORT);

    private static final String CATALOG = "test_ducklake";
    private static final String SCHEMA = "main";
    private static final String TABLE_PARTITIONED = "tt_p";

    private String warehouse;
    private String ducklakeRoot;

    private final HttpClient client = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    /* ------------------------------------------------------------------ */
    /* Setup                                                               */
    /* ------------------------------------------------------------------ */

    @BeforeAll
    void setup() throws Exception {
        warehousePath = "/tmp/" + UUID.randomUUID();
        new File(warehousePath).mkdir();
        ducklakeRoot = Files.createTempDirectory("ducklake_").toString();
        warehouse = warehousePath.replace("\\", "/");
        startPlannerServer();
        setupDuckLakeCatalog();
        createDuckLakeTables();
    }

    private void startPlannerServer() throws Exception {
        String[] args = {
                "--conf", "dazzleduck_server.http.port=%s".formatted(PLANNER_PORT),
                "--conf", "dazzleduck_server.%s=%s".formatted(
                ConfigUtils.WAREHOUSE_CONFIG_KEY, warehouse),
                "--conf", "dazzleduck_server.access_mode=RESTRICTED"
        };

        String[] args1 = {"--conf", "dazzleduck_server.http.port=%s".formatted(PLANNER_PORT),
                "--conf", "dazzleduck_server.%s=%s".formatted(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehouse),
                "--conf", "dazzleduck_server.ingestion.max_delay_ms=500"};
        Main.main(args1);
    }

    private void setupDuckLakeCatalog() {
        ConnectionPool.executeBatch(new String[]{
                "INSTALL ducklake",
                "LOAD ducklake",
                "ATTACH 'ducklake:%s/metadata' AS %s (DATA_PATH '%s/data')"
                        .formatted(ducklakeRoot, CATALOG, ducklakeRoot)
        });
    }

    private void createDuckLakeTables() {
        ConnectionPool.executeBatch(new String[]{"CREATE SCHEMA IF NOT EXISTS %s.%s".formatted(CATALOG, SCHEMA),

                """
                CREATE TABLE %s.%s.%s (key VARCHAR,value VARCHAR,p INT)
                """.formatted(CATALOG, SCHEMA, TABLE_PARTITIONED),

                """ 
                ALTER TABLE %s.%s.%s SET PARTITIONED BY (p)
                """.formatted(CATALOG, SCHEMA, TABLE_PARTITIONED),

                """
                 INSERT INTO %s.%s.%s VALUES ('k1','v1',1),('k2','v2',2),('k3','v3',1)
                """.formatted(CATALOG, SCHEMA, TABLE_PARTITIONED)});
    }

    @Test
    void testDuckLakePlannerSplits() throws Exception {
        var jwt = login();

        var request = HttpRequest.newBuilder(URI.create(BASE_URL + "/v1/plan"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(new QueryRequest("SELECT * FROM %s WHERE p = 1".formatted(TABLE_PARTITIONED)))))
                .header(HeaderNames.AUTHORIZATION.defaultCase(), jwt.tokenType() + " " + jwt.accessToken())
                .header(HEADER_SPLIT_SIZE, "1")
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());assertEquals(200, response.statusCode(), response.body());
        StatementHandle[] plans = mapper.readValue(response.body(), StatementHandle[].class);
        assertEquals(1, plans.length);
        for (StatementHandle h : plans) {
            assertTrue(h.query().contains(TABLE_PARTITIONED), "DuckLake table must not be rewritten");
        }
    }

    private LoginResponse login() throws Exception {
        var req = HttpRequest.newBuilder(URI.create(BASE_URL + "/v1/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(new LoginRequest("admin", "admin", Map.of(HEADER_DATABASE, CATALOG, HEADER_SCHEMA, SCHEMA)))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();

        var resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        return mapper.readValue(resp.body(), LoginResponse.class);
    }

    @AfterAll
    void cleanup() throws Exception {
        ConnectionPool.execute("DETACH %s".formatted(CATALOG));
        deleteRecursively(new File(warehouse));
        deleteRecursively(new File(ducklakeRoot));
    }

    private static void deleteRecursively(File f) {
        if (f == null || !f.exists()) return;
        if (f.isDirectory()) {
            for (File c : f.listFiles()) deleteRecursively(c);
        }
        f.delete();
    }
}
