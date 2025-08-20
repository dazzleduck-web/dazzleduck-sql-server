package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import io.dazzleduck.sql.common.auth.Validator;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import io.jsonwebtoken.Jwts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Calendar;
import java.util.Map;

public class LoginService implements HttpService {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    private static final Logger logger = LoggerFactory.getLogger(LoginService.class);
    private final Config config;
    private final SecretKey secretKey;
    private final Validator authenticator;
    private final Duration expiration;

    public LoginService(Config config, SecretKey secretKey) {
        this.config = config;
        this.authenticator = Validator.load(config);
        this.secretKey = secretKey;
        this.expiration = config.getDuration("jwt.token.expiration");
    }
    @Override
    public void routing(HttpRules rules) {
        rules.post("/", this::handleLogin);
    }

    private void handleLogin(ServerRequest serverRequest, ServerResponse serverResponse) throws IOException {
        var inputStream = serverRequest.content().inputStream();
        var loginRequest = MAPPER.readValue(inputStream, LoginObject.class);
        try {
            String requestBody = MAPPER.writeValueAsString(Map.of(
                    "email", loginRequest.username(),
                    "password", loginRequest.password()
            ));
            Long orgId = Long.valueOf(loginRequest.claims().get("orgId"));
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8080/api/login/org/" + orgId))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                serverResponse.status(Status.UNAUTHORIZED_401).send("Login failed");
                return;
            }
            serverResponse.send(response.body());
        } catch (Exception e ){
            serverResponse.status(Status.UNAUTHORIZED_401);
            serverResponse.send();
        }
    }
}
