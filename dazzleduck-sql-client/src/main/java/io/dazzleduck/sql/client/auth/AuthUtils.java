package io.dazzleduck.sql.client.auth;


import org.apache.arrow.flight.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public final class AuthUtils {

    private static final String AUTHORIZATION = "authorization";

    private AuthUtils() {
    }

    public static FlightClientMiddleware.Factory createClientMiddlewareFactory(
            String username,
            String password,
            Map<String, String> headers
    ) {
        return new FlightClientMiddleware.Factory() {

            private volatile String bearer;

            @Override
            public FlightClientMiddleware onCallStarted(CallInfo info) {
                return new FlightClientMiddleware() {

                    @Override
                    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                        if (bearer == null) {
                            outgoingHeaders.insert(
                                    AUTHORIZATION,
                                    basicAuth(username, password)
                            );
                        } else {
                            outgoingHeaders.insert(AUTHORIZATION, bearer);
                        }
                        headers.forEach(outgoingHeaders::insert);
                    }

                    @Override
                    public void onHeadersReceived(CallHeaders incomingHeaders) {
                        String auth = incomingHeaders.get(AUTHORIZATION);
                        if (auth != null && auth.startsWith("Bearer ")) {
                            bearer = auth;
                        }
                    }

                    @Override
                    public void onCallCompleted(CallStatus status) {
                        // no-op
                    }
                };
            }
        };
    }

    public static String basicAuth(String username, String password) {
        String token = username + ":" + password;
        return "Basic " + Base64.getEncoder()
                .encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
}

