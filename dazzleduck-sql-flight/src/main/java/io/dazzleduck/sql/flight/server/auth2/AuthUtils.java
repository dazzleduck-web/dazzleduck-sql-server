package io.dazzleduck.sql.flight.server.auth2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import io.dazzleduck.sql.common.auth.Validator;
import io.jsonwebtoken.security.Keys;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import javax.crypto.SecretKey;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class AuthUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    private static String generateBasicAuthHeader(String username, String password) {
        byte[] up = Base64.getEncoder().encode((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        return Auth2Constants.BASIC_PREFIX +
                new String(up);
    }

    public static CallHeaderAuthenticator getAuthenticator(Config config) throws NoSuchAlgorithmException {
        BasicCallHeaderAuthenticator.CredentialValidator validator = createCredentialValidator(config);
        CallHeaderAuthenticator authenticator = new BasicCallHeaderAuthenticator(validator);
        String secretKeyStr = config.hasPath("secretKey")
                ? config.getString("secretKey")
                : "change_me";
        SecretKey secretKey = Keys.hmacShaKeyFor(secretKeyStr.getBytes(StandardCharsets.UTF_8));
        return new GeneratedJWTTokenAuthenticator(authenticator, secretKey, config);
    }

    public static CallHeaderAuthenticator getAuthenticator() throws NoSuchAlgorithmException {
        CallHeaderAuthenticator authenticator = new BasicCallHeaderAuthenticator(NO_AUTH_CREDENTIAL_VALIDATOR);
        var secretKey = Validator.generateRandoSecretKey();
        return new GeneratedJWTTokenAuthenticator(authenticator, secretKey);
    }

    public static FlightClientMiddleware.Factory createClientMiddlewareFactory(String username,
                                                                               String password,
                                                                               Map<String, String> headers) {
        return new FlightClientMiddleware.Factory() {
            private volatile String bearer = null;

            @Override
            public FlightClientMiddleware onCallStarted(CallInfo info) {
                return new FlightClientMiddleware() {
                    @Override
                    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                        if (bearer == null) {
                            outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
                                    AuthUtils.generateBasicAuthHeader(username, password));
                        } else {
                            outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
                                    bearer);
                        }
                        headers.forEach(outgoingHeaders::insert);
                    }

                    @Override
                    public void onHeadersReceived(CallHeaders incomingHeaders) {
                        bearer = incomingHeaders.get(Auth2Constants.AUTHORIZATION_HEADER);
                    }

                    @Override
                    public void onCallCompleted(CallStatus status) {

                    }
                };
            }
        };
    }

    public static BasicCallHeaderAuthenticator.CredentialValidator createCredentialValidator(Config config) {
        return config.getBoolean("httpLogin") ?
                createHttpCredentialValidator()
                : createCredentialValidator();
    }

    private static BasicCallHeaderAuthenticator.CredentialValidator createHttpCredentialValidator() {
        return new BasicCallHeaderAuthenticator.CredentialValidator() {
            @Override
            public CallHeaderAuthenticator.AuthResult validate(String username, String password) throws Exception {
                int index = username.lastIndexOf("@");
                String actualUsername = username;
                String org = "0";

                if (index != -1) {
                    actualUsername = username.substring(0, index);
                    String orgPart = username.substring(index + 1);
                    if (!orgPart.isEmpty()) {
                        org = orgPart;
                    }
                }
                Long orgId;
                try {
                    orgId = Long.valueOf(org);
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid orgId: " + org, e);
                }
                String requestBody = objectMapper.writeValueAsString(Map.of(
                        "username", actualUsername,
                        "password", password,
                        "orgId", orgId
                ));

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8090/login"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() != 200) {
                    throw new RuntimeException("Failed to fetch token: " + response.body());
                }

                return new CallHeaderAuthenticator.AuthResult() {
                    @Override
                    public String getPeerIdentity() {
                        return username;
                    }

                    @Override
                    public void appendToOutgoingHeaders(CallHeaders headers) {
                        headers.insert(Auth2Constants.AUTHORIZATION_HEADER, "Bearer " + response.body());
                    }
                };
            }
        };
    }

    private static BasicCallHeaderAuthenticator.CredentialValidator createCredentialValidator() {
        return new BasicCallHeaderAuthenticator.CredentialValidator() {
            @Override
            public CallHeaderAuthenticator.AuthResult validate(String username, String password) throws Exception {
                int index = username.lastIndexOf("@");
                String actualUsername;
                String org = "0";
                if (index != -1) {
                    actualUsername = username.substring(0, index);
                    String orgPart = username.substring(index + 1);
                    if (!orgPart.isEmpty()) {
                        org = orgPart;
                    }
                } else {
                    actualUsername = username;
                }
                Long orgId;
                try {
                    orgId = Long.valueOf(org);
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid orgId: " + org, e);
                }
                String requestBody = objectMapper.writeValueAsString(Map.of(
                        "email", actualUsername,
                        "password", password
                ));

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8080/api/login/org/" + orgId))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                String jwt = response.body();

                if (response.statusCode() == 200 && jwt != null) {
                    return new CallHeaderAuthenticator.AuthResult() {
                        @Override
                        public String getPeerIdentity() {
                            return actualUsername;
                        }
                        @Override
                        public void appendToOutgoingHeaders(CallHeaders headers) {
                            headers.insert(Auth2Constants.AUTHORIZATION_HEADER, "Bearer " + jwt);
                        }
                    };
                } else {
                    throw new RuntimeException("Authentication failure: " + response.statusCode() + " - " + response.body());
                }
            }
        };
    }

    private static final BasicCallHeaderAuthenticator.CredentialValidator NO_AUTH_CREDENTIAL_VALIDATOR = new BasicCallHeaderAuthenticator.CredentialValidator() {
        @Override
        public CallHeaderAuthenticator.AuthResult validate(String username, String password) throws Exception {
            if(!password.isEmpty()) {
                return new CallHeaderAuthenticator.AuthResult() {
                    @Override
                    public String getPeerIdentity() {
                        return username;
                    }
                };
            } else {
                throw new RuntimeException("Authentication failure");
            }
        }
    };

}
