package io.dazzleduck.sql.http.server;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.commons.authorization.SqlAuthorizer;
import io.dazzleduck.sql.commons.authorization.SubjectAndVerifiedClaims;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.http.UnauthorizedException;
import io.helidon.webserver.http.Filter;
import io.helidon.webserver.http.FilterChain;
import io.helidon.webserver.http.RoutingRequest;
import io.helidon.webserver.http.RoutingResponse;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;

import javax.crypto.SecretKey;
import java.util.*;

public class JwtAuthenticationFilter implements Filter {
    public static final String SUBJECT_KEY = "subject";
    public static final String INGESTION_PATH = "/v1/ingest";
    private static final int BEARER_LENGTH = "Bearer ".length();
    private final Config config;
    private final SecretKey secretKey;
    private final JwtParser jwtParser;
    private final List<String> paths;
    private final List<String> claimHeader;
    private final Set<String> validateHeaders;
    private final SqlAuthorizer sqlAuthorizer;

    public JwtAuthenticationFilter(List<String> paths, Config config, SecretKey secretKey, SqlAuthorizer sqlAuthorizer) {
        this.config = config;
        this.secretKey = secretKey;
        this.jwtParser = Jwts.parser()
                .verifyWith(secretKey)
                .build();
        this.paths = paths;
        this.claimHeader = config.getStringList(ConfigConstants.JWT_TOKEN_CLAIMS_GENERATE_HEADERS_KEY);
        this.validateHeaders = new HashSet<>(config.getStringList(ConfigConstants.JWT_TOKEN_CLAIMS_VALIDATE_HEADERS_KEY));
        this.sqlAuthorizer = sqlAuthorizer;
    }

    public SubjectAndVerifiedClaims authenticate(String token) {
        try {
            var jwt = jwtParser.parseSignedClaims(token);
            var payload = jwt.getPayload();
            var subject = payload.getSubject();
            var expiration = payload.getExpiration();

            var allClaimsFromJWT = new HashMap<String, String>();
            for (String key : claimHeader) {
                var claimFromJwt = payload.get(key, String.class);
                allClaimsFromJWT.put(key, claimFromJwt);
            }
            var tokenType = payload.get(Headers.HEADER_TOKEN_TYPE, String.class);
            allClaimsFromJWT.put(Headers.HEADER_TOKEN_TYPE, tokenType != null ? tokenType : Headers.HEADER_TOKEN_INLINE);
            var redirectUrl = payload.get(Headers.HEADER_REDIRECT_URL, String.class);
            if (redirectUrl != null) {
                allClaimsFromJWT.put(Headers.HEADER_REDIRECT_URL, redirectUrl);
            }
            allClaimsFromJWT.put(Headers.HEADER_BEARER_TOKEN, token);
            if (expiration.after(new Date())) {
                return new SubjectAndVerifiedClaims(subject, Collections.unmodifiableMap(allClaimsFromJWT));
            }
            throw new UnauthorizedException("jwt expired for subject :" + subject);
        } catch (Exception e) {
            throw new UnauthorizedException("invalid jwt");
        }
    }

    @Override
    public void filter(FilterChain chain, RoutingRequest req, RoutingResponse res) {
        boolean authenticate = false;
        for (var path : paths) {
            if (req.path().path().startsWith(path)) {
                authenticate = true;
                break;
            }
        }
        if (!authenticate) {
            chain.proceed();
            return;
        }

        if ("OPTIONS".equalsIgnoreCase(req.prologue().method().name())) {
            chain.proceed();
            return;
        }

        var token = req.headers().value(HeaderNames.AUTHORIZATION);
        if (token.isEmpty()) {
            res.status(Status.UNAUTHORIZED_401);
            res.send();
        } else {
            try {
                var subjectAndClaims = authenticate(removeBearer(token.get()));

                // Check write access for ingestion requests
                if (req.path().path().startsWith(INGESTION_PATH)) {
                    var path = req.query().get(Headers.QUERY_PARAMETER_INGESTION_QUEUE);
                    if (path == null || path.isEmpty()) {
                        res.status(Status.BAD_REQUEST_400);
                        res.send("ingestion_queue parameter is required".getBytes());
                        return;
                    }
                    if (!sqlAuthorizer.hasWriteAccess(subjectAndClaims.subject(), path, subjectAndClaims.verifiedClaims())) {
                        res.status(Status.FORBIDDEN_403);
                        res.send(("No write access to ingestion_queue: " + path).getBytes());
                        return;
                    }
                }

                req.context().register(SUBJECT_KEY, subjectAndClaims);
                chain.proceed();
            } catch (UnauthorizedException unauthorizedException) {
                String errorMsg = unauthorizedException.getMessage() != null
                    ? unauthorizedException.getMessage()
                    : "Unauthorized";
                res.status(Status.UNAUTHORIZED_401);
                res.send(errorMsg.getBytes());
            }
        }
    }

    private static String removeBearer(String input) {
        if (input == null || input.length() <= BEARER_LENGTH || !input.startsWith("Bearer ")) {
            throw new UnauthorizedException("Invalid Authorization header format: must start with 'Bearer '");
        }
        return input.substring(BEARER_LENGTH);
    }
}
