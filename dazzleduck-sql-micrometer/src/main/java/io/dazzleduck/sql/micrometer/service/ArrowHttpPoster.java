package io.dazzleduck.sql.micrometer.service;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ArrowHttpPoster {
    private static final Logger log = LoggerFactory.getLogger(ArrowHttpPoster.class);

    private ArrowHttpPoster() {}

    public static int postBytes(HttpClient httpClient, byte[] arrowBytes, String url, Duration timeout) throws IOException, InterruptedException {
        Objects.requireNonNull(httpClient, "httpClient");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(timeout)
                .header("Content-Type", "application/vnd.apache.arrow.stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(arrowBytes))
                .build();

        HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        int status = resp.statusCode();
        if (status / 100 != 2) {
            log.warn("POST to {} returned non-2xx status {}. Body: {}", url, status, resp.body());
        } else {
            log.debug("POST to {} returned {} (body len={})", url, status, (resp.body() == null ? 0 : resp.body().length()));
        }
        return status;
    }

    // Convenience overload using default HttpClient
    public static int postBytes(byte[] arrowBytes, String url) throws IOException, InterruptedException {
        return postBytes(HttpClient.newHttpClient(), arrowBytes, url, Duration.ofSeconds(10));
    }
}
