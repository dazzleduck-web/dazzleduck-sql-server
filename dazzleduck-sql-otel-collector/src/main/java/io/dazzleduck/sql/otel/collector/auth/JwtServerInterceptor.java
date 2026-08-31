package io.dazzleduck.sql.otel.collector.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.auth.JwtClaimsExtractor;
import io.dazzleduck.sql.common.auth.LoginResponse;
import io.dazzleduck.sql.commons.auth.Validator;
import io.grpc.*;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import io.dazzleduck.sql.common.SslUtils;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class JwtServerInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(JwtServerInterceptor.class);

    /** gRPC context key populated from the {@value Headers#CLAIM_INGESTION_QUEUE} JWT claim. */
    public static final Context.Key<String> QUEUE_CONTEXT_KEY =
            Context.key(Headers.CLAIM_INGESTION_QUEUE);

    /**
     * gRPC context key carrying the caller's verified JWT claims minus the registered token
     * claims ({@code exp}, {@code iat}, {@code nbf}, {@code iss}, {@code aud}, {@code jti}),
     * values stringified. Consumed by signal services for queues with {@code extract_claims} on.
     */
    public static final Context.Key<Map<String, String>> CLAIMS_CONTEXT_KEY =
            Context.key("verified-claims");

    /** Token-plumbing claims that never reach the claims column. */
    private static final Set<String> REGISTERED_CLAIMS = Set.of(
            Claims.EXPIRATION, Claims.ISSUED_AT, Claims.NOT_BEFORE,
            Claims.ISSUER, Claims.AUDIENCE, Claims.ID);
    private static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String BASIC_PREFIX = "Basic ";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = SslUtils.httpClient();

    private final SecretKey secretKey;
    private final JwtParser jwtParser;
    private final boolean verifySignature;
    private final Map<String, byte[]> userHashMap;
    private final Duration jwtExpiration;
    private final String loginUrl;

    public JwtServerInterceptor(SecretKey secretKey, Map<String, byte[]> userHashMap,
                                Duration jwtExpiration, String loginUrl, boolean verifySignature) {
        this.secretKey = secretKey;
        this.verifySignature = verifySignature;
        this.jwtParser = verifySignature
                ? Jwts.parser().verifyWith(secretKey).build()
                : Jwts.parser().unsecured().build();
        this.userHashMap = userHashMap;
        this.jwtExpiration = jwtExpiration;
        this.loginUrl = loginUrl;
        if (!verifySignature) {
            log.warn("JWT signature verification is disabled — tokens are not cryptographically validated");
        }
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String authHeader = headers.get(AUTHORIZATION_KEY);
        if (authHeader == null) {
            call.close(Status.UNAUTHENTICATED.withDescription("Missing Authorization header"), new Metadata());
            return new ServerCall.Listener<>() {};
        }

        if (authHeader.startsWith(BASIC_PREFIX)) {
            return handleBasicAuth(authHeader.substring(BASIC_PREFIX.length()), call, headers, next);
        } else if (authHeader.startsWith(BEARER_PREFIX)) {
            return handleBearer(authHeader.substring(BEARER_PREFIX.length()), call, headers, next);
        } else {
            call.close(Status.UNAUTHENTICATED.withDescription("Unsupported Authorization scheme"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
    }

    private <ReqT, RespT> ServerCall.Listener<ReqT> handleBasicAuth(
            String encoded, ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        try {
            String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
            int colonPos = decoded.indexOf(':');
            if (colonPos == -1) {
                call.close(Status.UNAUTHENTICATED.withDescription("Invalid Basic auth format"), new Metadata());
                return new ServerCall.Listener<>() {};
            }
            String username = decoded.substring(0, colonPos);
            String password = decoded.substring(colonPos + 1);

            String token = loginUrl != null
                    ? delegateLogin(username, password)
                    : validateLocallyAndGenerateToken(username, password);

            String finalToken = token;
            ServerCall<ReqT, RespT> wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                @Override
                public void sendHeaders(Metadata responseHeaders) {
                    responseHeaders.put(AUTHORIZATION_KEY, finalToken);
                    super.sendHeaders(responseHeaders);
                }
            };
            // Extract queue claim from the newly issued token and propagate to context
            // so services can route to the correct ingestion queue on this same call.
            String bearerValue = token.startsWith(BEARER_PREFIX) ? token.substring(BEARER_PREFIX.length()) : token;
            try {
                var claims = JwtClaimsExtractor.parseJwtClaims(bearerValue, jwtParser, verifySignature);
                return startCallWithClaimContexts(claims, wrappedCall, headers, next);
            } catch (Exception ignored) {}
            return next.startCall(wrappedCall, headers);
        } catch (Exception e) {
            log.debug("Basic auth failed: {}", e.getMessage());
            call.close(Status.UNAUTHENTICATED.withDescription("Authentication failed"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
    }

    private String delegateLogin(String username, String password) throws Exception {
        String requestBody = MAPPER.writeValueAsString(Map.of(
                "username", username,
                "password", password
        ));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(loginUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
        HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("Login delegation failed: " + response.statusCode());
        }
        LoginResponse result = MAPPER.readValue(response.body(), LoginResponse.class);
        return result.tokenType() + " " + result.accessToken();
    }

    private String validateLocallyAndGenerateToken(String username, String password) {
        byte[] storedHash = userHashMap.get(username);
        if (storedHash == null || !Validator.passwordMatch(storedHash, Validator.hash(password))) {
            throw new RuntimeException("Invalid credentials");
        }
        return BEARER_PREFIX + generateToken(username);
    }

    private <ReqT, RespT> ServerCall.Listener<ReqT> handleBearer(
            String token, ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        try {
            var claims = JwtClaimsExtractor.parseJwtClaims(token, jwtParser, verifySignature);
            return startCallWithClaimContexts(claims, call, headers, next);
        } catch (Exception e) {
            log.debug("JWT validation failed: {}", e.getMessage());
            call.close(Status.UNAUTHENTICATED.withDescription("Invalid or expired JWT token"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
    }

    private <ReqT, RespT> ServerCall.Listener<ReqT> startCallWithClaimContexts(
            Claims claims,
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        Context ctx = Context.current().withValue(CLAIMS_CONTEXT_KEY, filterClaims(claims));
        String queueId = claims.get(Headers.CLAIM_INGESTION_QUEUE, String.class);
        if (queueId != null) {
            ctx = ctx.withValue(QUEUE_CONTEXT_KEY, queueId);
        }
        return Contexts.interceptCall(ctx, call, headers, next);
    }

    /** Drops the registered token claims and stringifies the rest. */
    static Map<String, String> filterClaims(Claims claims) {
        Map<String, String> filtered = new LinkedHashMap<>();
        claims.forEach((key, value) -> {
            if (value != null && !REGISTERED_CLAIMS.contains(key)) {
                filtered.put(key, String.valueOf(value));
            }
        });
        return filtered;
    }

    private String generateToken(String subject) {
        Calendar expiration = Calendar.getInstance();
        expiration.add(Calendar.MINUTE, (int) jwtExpiration.toMinutes());
        return Jwts.builder()
                .subject(subject)
                .expiration(expiration.getTime())
                .signWith(secretKey)
                .compact();
    }
}
