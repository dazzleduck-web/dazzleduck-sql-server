package io.dazzleduck.sql.flight.server.auth2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import io.dazzleduck.sql.common.auth.Validator;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

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
        var secretKey = Validator.generateRandoSecretKey();
        return new GeneratedJWTTokenAuthenticator( authenticator, secretKey, config);
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
        List<? extends ConfigObject> users = config.getObjectList("users");
        Map<String, String> passwords = new HashMap<>();
        users.forEach(o -> {
            String name = o.toConfig().getString("username");
            String password = o.toConfig().getString("password");
            passwords.put(name, password);
        });
        return config.getBoolean("httpLogin") ?
                createHttpCredentialValidator(passwords, 123L, List.of("cc1", "cc2", "cc3"))
                : createCredentialValidator(passwords);
    }

    private static BasicCallHeaderAuthenticator.CredentialValidator createHttpCredentialValidator(Map<String, String> userPassword, Long orgId, List<String> clusterId) {
        return new BasicCallHeaderAuthenticator.CredentialValidator() {
            @Override
            public CallHeaderAuthenticator.AuthResult validate(String username, String password) throws Exception {
                String requestBody = objectMapper.writeValueAsString(Map.of(
                        "username", username,
                        "password", password,
                        "orgId", orgId,
                        "clusterId", clusterId
                ));

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8080/login"))
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

    private static BasicCallHeaderAuthenticator.CredentialValidator createCredentialValidator(Map<String, String> userPassword) {
        Map<String, byte[]> userHashMap = new HashMap<>();
        userPassword.forEach((u, p) -> userHashMap.put(u, Validator.hash(p)));
        return new BasicCallHeaderAuthenticator.CredentialValidator() {
            @Override
            public CallHeaderAuthenticator.AuthResult validate(String username, String password) throws Exception {
                var storePassword = userHashMap.get(username);
                if (storePassword != null &&
                        !password.isEmpty() &&
                        Validator.passwordMatch(storePassword, Validator.hash(password))) {
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
