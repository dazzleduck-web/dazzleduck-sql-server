package io.dazzleduck.sql.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.ingestion.FlightSender;
import io.dazzleduck.sql.login.LoginRequest;
import io.dazzleduck.sql.login.LoginResponse;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

public final class HttpSender extends FlightSender.AbstractFlightSender {

    private final HttpClient client = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    private final String baseUrl;
    private final String username;
    private final String password;
    private final String targetPath;
    private final Duration timeout;

    private final long maxMem;
    private final long maxDisk;

    private String jwt = null;
    private Instant jwtExpiry = Instant.EPOCH;
    private static final Duration REFRESH_SKEW = Duration.ofSeconds(60);

    public HttpSender(
            String baseUrl,
            String username,
            String password,
            String targetPath,
            Duration timeout,
            long maxInMemorySize,
            long maxOnDiskSize
    ) {
        this.baseUrl = baseUrl;
        this.username = username;
        this.password = password;
        this.targetPath = targetPath;
        this.timeout = timeout;
        this.maxMem = maxInMemorySize;
        this.maxDisk = maxOnDiskSize;
    }

    @Override
    public long getMaxInMemorySize() {
        return maxMem;
    }

    @Override
    public long getMaxOnDiskSize() {
        return maxDisk;
    }

    private synchronized void login() throws Exception {
        var body = mapper.writeValueAsBytes(new LoginRequest(username, password));

        var req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/login"))
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .build();

        var resp = client.send(req, HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() != 200) {
            throw new RuntimeException("Login failed: " + resp.body());
        }

        LoginResponse lr = mapper.readValue(resp.body(), LoginResponse.class);
        this.jwt = lr.tokenType() + " " + lr.accessToken();
        this.jwtExpiry = lr.expiresIn() != null
                ? Instant.now().plusSeconds(lr.expiresIn())
                : Instant.now().plus(Duration.ofHours(5));
    }

    private synchronized String getJwt() throws Exception {
        if (jwt == null || Instant.now().isAfter(jwtExpiry.minus(REFRESH_SKEW))) {
            login();
        }
        return jwt;
    }

    @Override
    protected void doSend(SendElement element) throws InterruptedException {
        try {
            var resp = sendRequest(element, false);

            if (resp.statusCode() == 401 || resp.statusCode() == 403) {
                synchronized (this) {
                    jwt = null;
                }
                resp = sendRequest(element, true);
            }

            if (resp.statusCode() != 200) {
                throw new RuntimeException("Ingestion failed: " + resp.body());
            }

        } catch (InterruptedException e) {
            // Re-throw to allow graceful shutdown
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private HttpResponse<String> sendRequest(SendElement element, boolean isRetry) throws Exception {
        InputStream is = element.read();

        var req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/ingest?path=" + targetPath))
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> is))
                .header("Authorization", getJwt())
                .header("Content-Type", "application/vnd.apache.arrow.stream")
                .build();

        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }
}