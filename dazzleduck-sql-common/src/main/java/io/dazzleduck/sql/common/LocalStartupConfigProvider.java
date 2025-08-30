package io.dazzleduck.sql.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class LocalStartupConfigProvider implements StartupScriptProvider {
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String SCRIPT_LOCATION_KEY = "script-location";
    Config config;
    public String getStartupScript() throws IOException {
        String startUpFile = config.hasPathOrNull(SCRIPT_LOCATION_KEY) ? config.getString(SCRIPT_LOCATION_KEY) : null;
        if (startUpFile != null) {
            Path path = Paths.get(startUpFile);
            if (Files.isRegularFile(path)) {
                return Files.readString(path).trim();
            }
        }
        return null;
    }

    public String getHttpStartupScript() throws IOException, InterruptedException {
        String requestBody = objectMapper.writeValueAsString(Map.of(
                "username", config.getString("username"),
                "password", config.getString("password"),
                "claims", config.hasPath("claims") ? config.getString("claims") : ""
        ));
        String baseUrl = "http://localhost:8080/api/orgs";
        String uri = config.hasPath("orgId") ? baseUrl + "/" + config.getString("orgId") : baseUrl;
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
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
            throw new IOException("Unexpected response: " + response.statusCode() + " - " + response.body());
        }
    }

    @Override
    public void loadInner(Config config) {
        this.config = config;
    }
}
