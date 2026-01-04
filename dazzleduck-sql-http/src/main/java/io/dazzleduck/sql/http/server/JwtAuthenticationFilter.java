package io.dazzleduck.sql.http.server;

import com.typesafe.config.Config;
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
    private static final int BEARER_LENGTH = "Bearer ".length();
    private final Config config;
    private final SecretKey secretKey;
    private final JwtParser jwtParser;
    private final List<String> paths;
    private final List<String> claimHeader;
    private final Set<String> validateHeaders;

    public JwtAuthenticationFilter(List<String> paths, Config config, SecretKey secretKey) {
        this.config = config;
        this.secretKey = secretKey;
        this.jwtParser = Jwts.parser()     // (1)
                .verifyWith(secretKey)      //     or a constant key used to verify all signed JWTs
                .build();
        this.paths = paths;
        this.claimHeader = config.getStringList("jwt_token.claims.generate.headers");
        this.validateHeaders = new HashSet<>(config.getStringList("jwt_token.claims.validate.headers"));
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
                var subject = authenticate(removeBearer(token.get()));
                req.context().register(SUBJECT_KEY, subject);
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
