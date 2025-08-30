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

public class HttpStartupConfigProvider implements StartupScriptProvider {
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    Config config;
    public String getHttpStartupScript() throws IOException, InterruptedException {
        Map<String, Object> body = Map.of(
                "email", config.getString("username"),
                "password", config.getString("password"),
                "claims", config.hasPath("claims") ? objectMapper.readValue(config.getString("claims"), Map.class) : Map.of()
        );
        String requestBody = objectMapper.writeValueAsString(body);
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

    @Override
    public String getStartupScript() throws IOException {
        return "";
    }
}
