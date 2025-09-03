package io.dazzleduck.sql.common.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.Map;

public class HttpAuthorizationConfigProvider implements AuthorizationProvider {
    public static final String AUTHORIZATION_URL_KEY = "auth.scriptUrl";
    public static final String LOGIN_URL_KEY = "auth.loginUrl";
    public static final String AUTHORIZATION_CLAIMS_KEY = "auth.claims";
    public static final String AUTHORIZATION_USER_KEY = "auth.username";
    public static final String AUTHORIZATION_PASS_KEY = "auth.password";

    private static final HttpClient httpClient = HttpClient.newBuilder().build();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    Config config;
    public SqlAuthorizer getHttpAuthorization() throws IOException, InterruptedException {
        String requestBody = objectMapper.writeValueAsString(Map.of(
                "email", config.hasPath(AUTHORIZATION_USER_KEY) ? config.getString(AUTHORIZATION_USER_KEY) : null,
                "password", config.hasPath(AUTHORIZATION_PASS_KEY) ? config.getString(AUTHORIZATION_PASS_KEY) : null,
                "claims", config.hasPath(AUTHORIZATION_CLAIMS_KEY) ? config.getObject(AUTHORIZATION_CLAIMS_KEY).unwrapped(): null
        ));

        var loginUrl = config.hasPath(LOGIN_URL_KEY) ? config.getString(LOGIN_URL_KEY) : null;
        if (loginUrl == null) {
            throw new IllegalArgumentException("Login URL is missing");
        }

        HttpRequest loginRequest = HttpRequest.newBuilder()
                .uri(URI.create(loginUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> loginResponse = httpClient.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        if (loginResponse.statusCode() != 200) {
            throw new RuntimeException("Failed to login: " + loginResponse.body());
        }

        var token = objectMapper.readTree(loginResponse.body()).get("token").asText();
        // Decode JWT
        JsonNode payload = decodeJwtPayload(token);
        String orgId = payload.has("org_id") ? payload.get("org_id").asText() : null;
        String cluster = payload.has("cluster") ? payload.get("cluster").asText() : null;
        if (orgId == null || cluster == null) {
            throw new IllegalArgumentException("JWT missing required claims: orgId/cluster");
        }

        // Build URL
        return new NOOPAuthorizer();
    }

    private JsonNode decodeJwtPayload(String jwt) throws IOException {
        String[] parts = jwt.split("\\.");
        if (parts.length < 2) throw new IllegalArgumentException("Invalid JWT token");
        String payloadJson = new String(Base64.getUrlDecoder().decode(parts[1]));
        return objectMapper.readTree(payloadJson);
    }

    @Override
    public void loadInner(Config config) {
        this.config = config;
    }

    @Override
    public SqlAuthorizer getAuthorization() throws IOException, InterruptedException {
        return getHttpAuthorization();
    }
}
