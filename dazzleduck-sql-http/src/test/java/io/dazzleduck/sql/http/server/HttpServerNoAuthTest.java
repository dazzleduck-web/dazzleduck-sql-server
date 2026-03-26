package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.http.server.model.QueryRequest;
import io.helidon.http.HeaderValues;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Tests that when http.authentication=none, all protected endpoints are accessible without a JWT token.
 */
public class HttpServerNoAuthTest extends HttpServerTestBase {

    @BeforeAll
    public static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer("--conf", "dazzleduck_server.http.%s=none".formatted(ConfigConstants.AUTHENTICATION_KEY));
        installArrowExtension();
    }

    @AfterAll
    public static void cleanup() throws Exception {
        cleanupWarehouse();
    }

    @Test
    public void testQueryPostWithoutTokenReturns200() throws IOException, InterruptedException, SQLException {
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, response.statusCode(), "Query should succeed without JWT when auth=none");
        try (var allocator = new RootAllocator();
             var reader = new ArrowStreamReader(response.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    @Test
    public void testQueryGetWithoutTokenReturns200() throws IOException, InterruptedException, SQLException {
        var query = "select * from generate_series(5) order by 1";
        var encoded = URLEncoder.encode(query, StandardCharsets.UTF_8);
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query?q=%s".formatted(encoded)))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, response.statusCode(), "GET query should succeed without JWT when auth=none");
        try (var allocator = new RootAllocator();
             var reader = new ArrowStreamReader(response.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    @Test
    public void testHealthWithoutTokenReturns200() throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/health"))
                .GET()
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Health endpoint should always be accessible");
    }

    @Test
    public void testLoginStillWorksWithNoAuth() throws IOException, InterruptedException {
        // /v1/login is never protected by JWT filter — verify it still returns a token
        var jwt = login();
        assertNotEquals(null, jwt.accessToken(), "Login should still return a JWT token even when auth=none");
    }
}
