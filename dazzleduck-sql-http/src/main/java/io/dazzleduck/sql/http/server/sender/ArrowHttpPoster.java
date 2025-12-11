package io.dazzleduck.sql.http.server.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;

public final class ArrowHttpPoster {
    private static final Logger log = LoggerFactory.getLogger(ArrowHttpPoster.class);

    private ArrowHttpPoster() {} // utility, prevent construction

    public static int postBytes(HttpClient httpClient,
                                byte[] arrowBytes,
                                String url,
                                Duration timeout)
            throws IOException, InterruptedException {

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
        }

        return status;
    }
}

