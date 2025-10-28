package io.dazzleduck.sql.login;

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
import java.time.Duration;
import java.util.Calendar;

public class LoginService implements HttpService {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger logger = LoggerFactory.getLogger(LoginService.class);
    private final Config config;
    private final SecretKey secretKey;
    private final Validator validator;
    private final Duration jwtExpiration;

    public LoginService(Config validatorConfig, SecretKey secretKey, Duration jwtExpiration) {
        this.config = validatorConfig;
        this.validator = Validator.load(validatorConfig);
        this.secretKey = secretKey;
        this.jwtExpiration = jwtExpiration;
    }
    @Override
    public void routing(HttpRules rules) {
        rules.post("/", this::handleLogin);
    }

    private void handleLogin(ServerRequest serverRequest, ServerResponse serverResponse) throws IOException {
        var inputStream = serverRequest.content().inputStream();
        var loginRequest = MAPPER.readValue(inputStream, LoginObject.class);
        try {
            validator.validate(loginRequest.username(), loginRequest.password());
            Calendar expiration = Calendar.getInstance();
            expiration.add(Calendar.MINUTE,
                    (int)this.jwtExpiration.toMinutes());
            String jwt = Jwts.builder()
                    .subject(loginRequest.username())
                    .expiration(expiration.getTime())
                    .claims(loginRequest.claims())
                    .signWith(secretKey).compact();
            serverResponse.send(jwt.getBytes());
        } catch (Exception e ){
            serverResponse.status(Status.UNAUTHORIZED_401);
            serverResponse.send();
        }
    }
}