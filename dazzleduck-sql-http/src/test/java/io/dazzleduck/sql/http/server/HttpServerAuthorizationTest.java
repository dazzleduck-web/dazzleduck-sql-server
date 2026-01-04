package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.login.LoginRequest;
import io.dazzleduck.sql.login.LoginResponse;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.*;

import java.io.IOException;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Create authorization server
 * Create tables. Insert some data
 * Check if the filters are applied when getStream is called.
 */
public class HttpServerAuthorizationTest {

    static HttpClient client;
    static ObjectMapper mapper = new ObjectMapper();
    static String warehousePath;

    static final int PORT = 8092;

    @BeforeAll
    static void setup() throws Exception {
        warehousePath = "/tmp/" + UUID.randomUUID();
        new java.io.File(warehousePath).mkdir();

        String[] args = {
                "--conf", "dazzleduck_server.http.port=%s".formatted(PORT),
                "--conf", "dazzleduck_server.%s=%s".formatted(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath),
                "--conf", "dazzleduck_server.access_mode=RESTRICTED",
                "--conf", "dazzleduck_server.http.port=%s".formatted(PORT),
                "--conf", "dazzleduck_server.http.%s=jwt".formatted(ConfigUtils.AUTHENTICATION_KEY),
        };
        Main.main(args);
        client = HttpClient.newHttpClient();
        ConnectionPool.execute("CREATE TABLE auth_test(id INTEGER, name STRING, city STRING, age INTEGER)");
        ConnectionPool.execute("INSERT INTO auth_test VALUES (1, 'shivam', 'chhindwara', 21), (2, 'hariom', 'delhi', 22), (3, 'piyush', 'bhopal', 21)");
    }

    @AfterAll
    static void cleanup() throws Exception {
        ConnectionPool.execute("DROP TABLE IF EXISTS auth_test");
        if (warehousePath != null) {
            deleteDirectory(new java.io.File(warehousePath));
        }
    }

    private static void deleteDirectory(java.io.File directory) throws IOException {
        if (directory == null || !directory.exists()) {
            return;
        }

        if (directory.isDirectory()) {
            java.io.File[] files = directory.listFiles();
            if (files != null) {
                for (java.io.File file : files) {
                    deleteDirectory(file);
                }
            }
        }

        if (!directory.delete()) {
            throw new IOException("Failed to delete: " + directory.getAbsolutePath());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testQueryWithClaimsFilter() throws Exception {
        var claims = Map.of(
                "filter", "id = 1",
                "table", "auth_test",
                "path", warehousePath
        );
        var jwtResponse = login(claims);
        assertEquals(200, jwtResponse.statusCode());
        var jwt = mapper.readValue(jwtResponse.body(), LoginResponse.class);
        var inputStreamResponse = query("select * from auth_test", jwt);
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            var expectedSql = "select * from auth_test where (id = 1)";
            TestUtils.isEqual(expectedSql, allocator, reader);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testUnauthorizedMissingTableClaim() throws Exception {
        var claims = Map.of(
                "filter", "id = 1",
                "path", warehousePath
        );
        var jwtResponse = login(claims);
        assertEquals(200, jwtResponse.statusCode());
        var jwt = mapper.readValue(jwtResponse.body(), LoginResponse.class);
        var inputStreamResponse = query("select * from auth_test", jwt);
        assertEquals(500, inputStreamResponse.statusCode());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Disabled("Column-level authorization not yet implemented")
    public void testColumnLevelAuthorization() throws Exception {
        var claims = Map.of(
                // "columns", List.of("id", "name"),
                "table", "auth_test",
                "path", warehousePath
        );
        var jwtResponse = login(claims);
        assertEquals(200, jwtResponse.statusCode());
        var jwt = mapper.readValue(jwtResponse.body(), LoginResponse.class);
        var inputStreamResponse = query("select * from auth_test", jwt);
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            var expectedSql = "select id, name from auth_test";
            TestUtils.isEqual(expectedSql, allocator, reader);
        }
    }

    /**
     * ======== HELPER METHODS ========
     */
    // LOGIN (/login) with claims
    private HttpResponse<String> login(Map<String, String> claims) throws Exception {
        var loginRequest = HttpRequest.newBuilder(URI.create("http://localhost:%s/login".formatted(PORT)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(
                        mapper.writeValueAsBytes(new LoginRequest("admin", "admin", claims))
                ))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();
        return client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
    }

    // QUERY (/query) with sql & jwt
    private HttpResponse<InputStream> query(String sql, LoginResponse jwt) throws Exception {
        var body = mapper.writeValueAsBytes(new QueryRequest(sql));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/query".formatted(PORT)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), jwt.tokenType() + " " + jwt.accessToken())
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofInputStream());
    }
}