package io.dazzleduck.sql.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Map;

public class HttpStartupConfigProvider implements StartupScriptProvider {
    public static final String SCRIPT_URL_KEY = "startup.scriptUrl";
    public static final String LOGIN_URL_KEY = "startup.loginUrl";
    public static final String STRING_CLAIMS_KEY = "startup.claims";
    public static final String STARTUP_USER_KEY = "startup.username";
    public static final String STARTUP_PASS_KEY = "startup.password";

    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(HttpStartupConfigProvider.class);

    Config config;
    public String getHttpStartupScript() throws IOException, InterruptedException {
        String requestBody = objectMapper.writeValueAsString(Map.of(
                "email", config.hasPath(STARTUP_USER_KEY) ? config.getString(STARTUP_USER_KEY) : null,
                "password", config.hasPath(STARTUP_PASS_KEY) ? config.getString(STARTUP_PASS_KEY) : null,
                "claims", config.hasPath(STRING_CLAIMS_KEY) ? config.getObject(STRING_CLAIMS_KEY).unwrapped(): null
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
        var startupUrl = config.hasPathOrNull(SCRIPT_URL_KEY) ? config.getString(SCRIPT_URL_KEY) : null;
        if (startupUrl == null) {
            throw new IllegalArgumentException("Start up url missing: \nURL: " + null);
        }
        String url = startupUrl + orgId + "/startup-script/" + cluster;
        // Fetch startup script
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            Path path = Paths.get(response.body());
            if (Files.isRegularFile(path)) {
                return Files.readString(path).trim();
            } else {
                throw new IOException("Response does not point to a valid file: " + path);
            }
        } else {
            throw new IOException("Unexpected response (" + response.statusCode() + "): " + response.body());
        }
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
    public String getStartupScript() throws IOException, InterruptedException {
        return getHttpStartupScript();
    }
}
