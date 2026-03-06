package io.dazzleduck.sql.http.server;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.auth.LoginRequest;
import io.dazzleduck.sql.common.auth.LoginResponse;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.http.server.model.QueryRequest;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import io.helidon.webserver.WebServer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.*;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HttpServerRedirectAuthorizationTest extends HttpServerTestBase {
    private static WebServer webServer;

    @BeforeAll
    static void setup() throws Exception {
        startMockResolveServer();
    }

    // -------------------------------------------------------------------------
    // Mock server (handles /login and /resolve)
    // -------------------------------------------------------------------------

    private static void startMockResolveServer() throws Exception {
        webServer = WebServer.builder()
                .routing(r -> r.register("/v1/resolve", new MockResolveService()))
                .port(5555)
                .build()
                .start();

        initWarehouse();
        initClient();
        initPort();
        startServer(
                "--conf", "dazzleduck_server.access_mode=%s".formatted(AccessMode.RESTRICTED),
                "--conf", "dazzleduck_server.http.%s=jwt".formatted(ConfigConstants.AUTHENTICATION_KEY)
        );

        ConnectionPool.executeBatch(new String[]{
                "CREATE TABLE t1 (age INT, class VARCHAR(50), city VARCHAR(100))",
                "CREATE TABLE t2 (age INT, class VARCHAR(50), city VARCHAR(100))",
                "CREATE TABLE t3 (age INT, class VARCHAR(50), city VARCHAR(100))",
                "INSERT INTO t1 VALUES (18, '12th', 'Delhi'), (20, 'BSc', 'Mumbai'), (22, 'BTech', 'Bangalore')",
                "INSERT INTO t2 VALUES (19, '12th', 'Kolkata'), (21, 'BCom', 'Chennai'), (23, 'MBA', 'Hyderabad')",
                "INSERT INTO t3 VALUES (17, '11th', 'Pune'), (24, 'MTech', 'Ahmedabad'), (26, 'PhD', 'Jaipur')"
        });
    }

    @AfterAll
    public static void cleanup() throws Exception {
        ConnectionPool.executeBatch(new String[]{
                "DROP TABLE IF EXISTS t1",
                "DROP TABLE IF EXISTS t2",
                "DROP TABLE IF EXISTS t3"
        });
        if (webServer != null) {
            webServer.stop();
        }
        System.clearProperty("dazzleduck_server.login_url");
        ConfigFactory.invalidateCaches();
        cleanupWarehouse();
    }

    // -------------------------------------------------------------------------
    // Test cases
    // -------------------------------------------------------------------------

    /**
     * Resolve endpoint grants access to the table with no row filter.
     * All three rows should be returned.
     */
    @Test
    void testRedirectAuthorizedTable() throws Exception {
        var jwtResponse = loginWithClaims(Map.of(Headers.HEADER_TOKEN_TYPE, "redirect", "cluster", "cc1", "org_id", "1", Headers.HEADER_REDIRECT_URL, "http://localhost:5555/v1/resolve"));
        assertEquals(200, jwtResponse.statusCode());
        var jwt = objectMapper.readValue(jwtResponse.body(), LoginResponse.class);

        var response = query("select * from t1", jwt);
        assertEquals(200, response.statusCode());
        try (var allocator = new RootAllocator(); var reader = new ArrowStreamReader(response.body(), allocator)) {
            TestUtils.isEqual("select * from t1 where city = 'Bangalore'", allocator, reader);
        }

        var response2 = query("select * from t2", jwt);
        assertEquals(200, response2.statusCode());
        try (var allocator = new RootAllocator(); var reader = new ArrowStreamReader(response2.body(), allocator)) {
            TestUtils.isEqual("select * from t2 where age > 20", allocator, reader);
        }

        var response3 = queryAsString("select * from redirect_test", jwt);
        assertEquals(403, response3.statusCode());
    }

    /**
     * Inline authorization (no {@code token_type} claim) still works correctly
     * when running alongside redirect-mode tests.
     * The inline filter {@code id = 2} is applied and only bob is returned.
     */
    @Test
    void testInlineStillWorksRegression() throws Exception {
        var jwtResponse = loginWithClaims(Map.of(Headers.HEADER_TABLE, "t2", Headers.HEADER_REDIRECT_URL, "http://localhost:5555/v1/resolve"));
        assertEquals(200, jwtResponse.statusCode());
        var jwt = objectMapper.readValue(jwtResponse.body(), LoginResponse.class);

        var response = query("select * from t2", jwt);
        assertEquals(200, response.statusCode());
        try (var allocator = new RootAllocator(); var reader = new ArrowStreamReader(response.body(), allocator)) {
            TestUtils.isEqual("select * from t2", allocator, reader);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private HttpResponse<String> loginWithClaims(Map<String, String> claims) throws Exception {
        var loginRequest = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/login")).POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin", claims)))).header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        return client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<InputStream> query(String sql, LoginResponse jwt) throws Exception {
        var body = objectMapper.writeValueAsBytes(new QueryRequest(sql));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query")).POST(HttpRequest.BodyPublishers.ofByteArray(body)).header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).header(HeaderNames.AUTHORIZATION.defaultCase(), jwt.tokenType() + " " + jwt.accessToken()).build();
        return client.send(request, HttpResponse.BodyHandlers.ofInputStream());
    }

    private HttpResponse<String> queryAsString(String sql, LoginResponse jwt) throws Exception {
        var body = objectMapper.writeValueAsBytes(new QueryRequest(sql));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query")).POST(HttpRequest.BodyPublishers.ofByteArray(body)).header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).header(HeaderNames.AUTHORIZATION.defaultCase(), jwt.tokenType() + " " + jwt.accessToken()).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
