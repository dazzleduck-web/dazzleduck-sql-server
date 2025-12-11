package io.dazzleduck.sql.login;

import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ProxyLoginService implements HttpService {
    private final HttpClient client = HttpClient.newHttpClient();
    private final String target;
    private static final Logger logger = LoggerFactory.getLogger(ProxyLoginService.class);
    public ProxyLoginService(String target) {
        this.target = target;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post("/", this::forwardLogin);
    }

    private void forwardLogin(ServerRequest req, ServerResponse res) throws IOException {
        try {
            String requestJson = req.content().as(String.class);
            HttpRequest httpReq = HttpRequest.newBuilder()
                    .uri(URI.create(target))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestJson))
                    .build();

            HttpResponse<String> backendResponse = client.send(httpReq, HttpResponse.BodyHandlers.ofString());
            res.status(Status.create(backendResponse.statusCode()));
            res.send(backendResponse.body());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            res.status(Status.INTERNAL_SERVER_ERROR_500).send("Login forwarding failed: " + e.getMessage());
        }
    }
}
