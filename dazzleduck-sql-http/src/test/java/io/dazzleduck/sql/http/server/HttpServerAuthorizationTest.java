package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessType;
import io.dazzleduck.sql.commons.authorization.SqlAuthorizer;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.common.auth.LoginRequest;
import io.dazzleduck.sql.common.auth.LoginResponse;
import io.dazzleduck.sql.http.server.model.QueryRequest;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.*;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Create authorization server
 * Create tables. Insert some data
 * Check if the filters are applied when getStream is called.
 */
public class HttpServerAuthorizationTest extends HttpServerTestBase {

    @BeforeAll
    static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer(
                "--conf", "dazzleduck_server.access_mode=RESTRICTED",
                "--conf", "dazzleduck_server.http.%s=jwt".formatted(ConfigConstants.AUTHENTICATION_KEY));
        ConnectionPool.execute("CREATE TABLE auth_test(id INTEGER, name STRING, city STRING, age INTEGER)");
        ConnectionPool.execute("INSERT INTO auth_test VALUES (1, 'admin', 'main', 30), (2, 'user1', 'location1', 25), (3, 'user2', 'location2', 28)");
    }

    @AfterAll
    static void cleanup() throws Exception {
        ConnectionPool.execute("DROP TABLE IF EXISTS auth_test");
        cleanupWarehouse();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testQueryWithClaimsFilter() throws Exception {
        var claims = Map.of(
                Headers.HEADER_FILTER, "id = 1",
                Headers.HEADER_TABLE, "auth_test",
                Headers.HEADER_PATH, warehousePath
        );
        var jwtResponse = loginWithClaims(claims);
        assertEquals(200, jwtResponse.statusCode());
        var jwt = objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
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
                Headers.HEADER_FILTER, "id = 1",
                Headers.HEADER_PATH, warehousePath
        );
        var jwtResponse = loginWithClaims(claims);
        assertEquals(200, jwtResponse.statusCode());
        var jwt = objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
        var inputStreamResponse = query("select * from auth_test", jwt);
        assertEquals(403, inputStreamResponse.statusCode());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Disabled("Column-level authorization not yet implemented")
    public void testColumnLevelAuthorization() throws Exception {
        var claims = Map.of(
                // "columns", List.of("id", "name"),
                Headers.HEADER_TABLE, "auth_test",
                Headers.HEADER_PATH, warehousePath
        );
        var jwtResponse = loginWithClaims(claims);
        assertEquals(200, jwtResponse.statusCode());
        var jwt = objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
        var inputStreamResponse = query("select * from auth_test", jwt);
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            var expectedSql = "select id, name from auth_test";
            TestUtils.isEqual(expectedSql, allocator, reader);
        }
    }

    @Test
    public void testHasWriteAccessWithWriteAccessType() {
        var authorizer = SqlAuthorizer.JWT_AUTHORIZER;
        var claims = Map.of(
                Headers.HEADER_ACCESS_TYPE, AccessType.WRITE.name(),
                Headers.HEADER_PATH, "data/ingestion"
        );
        assertTrue(authorizer.hasWriteAccess("admin", "data/ingestion", claims));
        assertTrue(authorizer.hasWriteAccess("admin", "data/ingestion/subpath", claims));
    }

    @Test
    public void testHasWriteAccessWithoutWriteAccessType() {
        var authorizer = SqlAuthorizer.JWT_AUTHORIZER;
        var claims = Map.of(
                Headers.HEADER_PATH, "data/ingestion"
        );
        assertFalse(authorizer.hasWriteAccess("admin", "data/ingestion", claims));
    }

    @Test
    public void testHasWriteAccessWithReadAccessType() {
        var authorizer = SqlAuthorizer.JWT_AUTHORIZER;
        var claims = Map.of(
                Headers.HEADER_ACCESS_TYPE, AccessType.READ.name(),
                Headers.HEADER_PATH, "data/ingestion"
        );
        assertFalse(authorizer.hasWriteAccess("admin", "data/ingestion", claims));
    }

    @Test
    public void testHasWriteAccessWithUnauthorizedPath() {
        var authorizer = SqlAuthorizer.JWT_AUTHORIZER;
        var claims = Map.of(
                Headers.HEADER_ACCESS_TYPE, AccessType.WRITE.name(),
                Headers.HEADER_PATH, "data/ingestion"
        );
        assertFalse(authorizer.hasWriteAccess("admin", "other/path", claims));
    }

    @Test
    public void testHasWriteAccessWithoutPath() {
        var authorizer = SqlAuthorizer.JWT_AUTHORIZER;
        var claims = Map.of(
                Headers.HEADER_ACCESS_TYPE, AccessType.WRITE.name()
        );
        assertFalse(authorizer.hasWriteAccess("admin", "data/ingestion", claims));
    }

    /**
     * ======== HELPER METHODS ========
     */
    // LOGIN (/v1/login) with claims
    private HttpResponse<String> loginWithClaims(Map<String, String> claims) throws Exception {
        var loginRequest = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(
                        objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin", claims))
                ))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();
        return client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
    }

    // QUERY (/v1/query) with sql & jwt
    private HttpResponse<InputStream> query(String sql, LoginResponse jwt) throws Exception {
        var body = objectMapper.writeValueAsBytes(new QueryRequest(sql));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), jwt.tokenType() + " " + jwt.accessToken())
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofInputStream());
    }
}
