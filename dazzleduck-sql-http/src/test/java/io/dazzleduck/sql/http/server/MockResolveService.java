package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.commons.authorization.ResolveAccessRow;
import io.dazzleduck.sql.commons.authorization.ResolveResponse;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

/**
 * Helidon mock service for the {@code GET /resolve} endpoint used in
 * redirect-mode authorization tests.
 *
 * <p>On each request the service:
 * <ol>
 *   <li>Reads the {@code Authorization: Bearer <token>} header.</li>
 *   <li>Base64-decodes the JWT payload (no signature verification needed in tests).</li>
 *   <li>Extracts the {@code cluster} and {@code org_id} claims.</li>
 *   <li>Builds a {@link ResolveResponse} that grants the bearer access to all tables
 *       inside that cluster, with an injected row-level filter
 *       {@code org_id = '<value>'} derived from the token's {@code org_id} claim.</li>
 *   <li>Returns the response as JSON with HTTP 200.</li>
 * </ol>
 *
 * <p>If the {@code Authorization} header is missing or the JWT payload cannot be decoded
 * the service responds with HTTP 401.
 *
 * <p>Usage:
 * <pre>{@code
 * MockResolveService resolveService = new MockResolveService();
 *
 * WebServer mockServer = WebServer.builder()
 *         .routing(r -> r.register("/v1/resolve", resolveService))
 *         .port(0)
 *         .build()
 *         .start();
 * }</pre>
 */
public class MockResolveService implements HttpService {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String BEARER_PREFIX = "Bearer ";

    @Override
    public void routing(HttpRules rules) {
        rules.get("/", this::handle);
    }

    private void handle(ServerRequest req, ServerResponse res) throws IOException {
        // 1. Extract the raw bearer token from the Authorization header
        var authHeader = req.headers().value(HeaderNames.AUTHORIZATION);
        if (authHeader.isEmpty() || !authHeader.get().startsWith(BEARER_PREFIX)) {
            res.status(Status.UNAUTHORIZED_401).send("Missing or invalid Authorization header");
            return;
        }
        String token = authHeader.get().substring(BEARER_PREFIX.length());

        // 2. Decode the JWT payload (middle segment) without signature verification
        JsonNode claims;
        try {
            String[] parts = token.split("\\.");
            byte[] payloadBytes = Base64.getUrlDecoder().decode(parts[1]);
            claims = MAPPER.readTree(payloadBytes);
        } catch (Exception e) {
            res.status(Status.UNAUTHORIZED_401).send("Cannot decode JWT payload");
            return;
        }

        // 3. Extract cluster and org_id claims
        String cluster = claims.has("cluster") ? claims.get("cluster").asText() : "default";
        String orgId = claims.has("org_id") ? claims.get("org_id").asText() : "";

        if (cluster == null || cluster.isEmpty() || orgId.isEmpty()) {
            res.status(Status.UNAUTHORIZED_401).send("cluster or org_id is empty");
            return;
        }

        // --- Tables (BASE_TABLE) ---
        // redirect_test: primary test table, carries org_id row-level filter
        ResolveAccessRow redirectTest = new ResolveAccessRow(
                "memory", "main", "t1", "BASE_TABLE",
                List.of(), "city = 'Bangalore'", "", "2099-12-31");

        // redirect_users: second test table, no row filter
        ResolveAccessRow redirectUsers = new ResolveAccessRow(
                "memory", "main", "t2", "BASE_TABLE",
                List.of(), "age > 20", "", "2099-12-31");

        // redirect_events: third test table, carries org_id row-level filter
        ResolveAccessRow redirectEvents = new ResolveAccessRow(
                "memory", "main", "t3", "BASE_TABLE",
                List.of(), "", "", "2025-12-31");

        // --- Functions (TABLE_FUNCTION) ---
        // read_parquet: grants access to all parquet files under the cluster's data path
        ResolveAccessRow readParquet = new ResolveAccessRow(
                "memory", "main", cluster, "TABLE_FUNCTION",
                List.of(), "", "read_parquet", "2099-12-31");

        // read_delta: same path prefix, different function
        ResolveAccessRow readDelta = new ResolveAccessRow(
                "memory", "main", cluster, "TABLE_FUNCTION",
                List.of(), "", "read_delta", "2099-12-31");

        ResolveResponse response = new ResolveResponse(
                List.of(redirectTest, redirectUsers, redirectEvents),
                List.of(readParquet, readDelta),
                "1");

        // 5. Return the response as JSON
        MAPPER.writeValue(res.outputStream(), response);
    }
}
