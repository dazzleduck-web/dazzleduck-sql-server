package io.dazzleduck.sql.flight.server.auth2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import io.dazzleduck.sql.common.auth.Validator;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;

public class AuthUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    private static String generateBasicAuthHeader(String username, String password) {
        byte[] up = Base64.getEncoder().encode((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        return Auth2Constants.BASIC_PREFIX +
                new String(up);
    }

    public static AdvanceJWTTokenAuthenticator getAuthenticator(Config config) {
        var validator = createCredentialValidator(config);
        var authenticator = new AdvanceBasicCallHeaderAuthenticator(validator);
        var base64Key = config.getString(ConfigUtils.SECRET_KEY_KEY);
        var secretKey = Validator.fromBase64String(base64Key);
        return new AdvanceJWTTokenAuthenticator(authenticator, secretKey, config);
    }

    public static AdvanceJWTTokenAuthenticator getTestAuthenticator() throws NoSuchAlgorithmException {
        var authenticator = new AdvanceBasicCallHeaderAuthenticator(NO_AUTH_CREDENTIAL_VALIDATOR);
        var secretKey = Validator.generateRandoSecretKey();
        return new AdvanceJWTTokenAuthenticator(authenticator, secretKey);
    }

    public static AdvanceJWTTokenAuthenticator getTestAuthenticator(Config config) throws NoSuchAlgorithmException {
        var authenticator = new AdvanceBasicCallHeaderAuthenticator(NO_AUTH_CREDENTIAL_VALIDATOR);
        var secretKey = Validator.generateRandoSecretKey();
        return new AdvanceJWTTokenAuthenticator(authenticator, secretKey, config);
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

    public static AdvanceBasicCallHeaderAuthenticator.AdvanceCredentialValidator createCredentialValidator(Config config) {
        return config.hasPath("login.url") ?
                new HttpCredentialValidator(config)
                : new ConfBasedCredentialValidator(config);
    }

    private static final AdvanceBasicCallHeaderAuthenticator.AdvanceCredentialValidator NO_AUTH_CREDENTIAL_VALIDATOR = (username, password, callHeaders) -> {
        if(!password.isEmpty()) {
            return (CallHeaderAuthenticator.AuthResult) () -> username;
        } else {
            throw new RuntimeException("Authentication failure");
        }
    };
}
