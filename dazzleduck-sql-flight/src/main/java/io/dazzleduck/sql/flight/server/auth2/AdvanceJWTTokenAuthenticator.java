package io.dazzleduck.sql.flight.server.auth2;

import com.google.common.base.Strings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.common.Headers;
import io.grpc.Metadata;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeExceptionFactory;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.grpc.MetadataAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.time.Duration;
import java.util.*;

public class AdvanceJWTTokenAuthenticator implements CallHeaderAuthenticator {

    private static final Logger logger = LoggerFactory.getLogger(AdvanceBasicCallHeaderAuthenticator.class);
    private final SecretKey key;
    private final JwtParser jwtParser;
    private final Duration timeMinutes;
    private final CallHeaderAuthenticator initialAuthenticator;
    private final List<String> claimHeader;
    private final boolean generateToken;
    private final Set<String> validateHeaders;


    public AdvanceJWTTokenAuthenticator(CallHeaderAuthenticator initialAuthenticator, SecretKey key) {
        this(initialAuthenticator, key,  defaulConfig());
    }


    public AdvanceJWTTokenAuthenticator(CallHeaderAuthenticator initialAuthenticator, SecretKey key, Config config) {
        this.key = key;
        this.jwtParser = Jwts.parser()     // (1)
                .verifyWith(key)//     or a constant key used to verify all signed JWTs
                .build();
        this.initialAuthenticator = initialAuthenticator;
        this.timeMinutes = config.getDuration(ConfigConstants.JWT_TOKEN_EXPIRATION_KEY);
        this.claimHeader = config.getStringList(ConfigConstants.JWT_TOKEN_CLAIMS_GENERATE_HEADERS_KEY);
        this.validateHeaders = new HashSet<>(config.getStringList(ConfigConstants.JWT_TOKEN_CLAIMS_VALIDATE_HEADERS_KEY));
        this.generateToken = config.getBoolean(ConfigConstants.JWT_TOKEN_GENERATION_KEY);
    }

    private static Config defaulConfig() {
        String defaultConfig = """
                jwt_token.expiration = 1h
                jwt_token.claims.generate.headers = []
                jwt_token.claims.validate.headers = []
                jwt_token.generation = true
                """;
        return ConfigFactory.parseString(defaultConfig);
    }


    @Override
    public AuthResultWithClaims authenticate(CallHeaders incomingHeaders) {
        // Check if headers contain a bearer token and if so, validate the token.
        final String bearerToken =
                AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BEARER_PREFIX);
        if (bearerToken != null) {
            return validateBearer(bearerToken, incomingHeaders);
        }

        // Delegate to the basic auth handler to do the validation.
        final CallHeaderAuthenticator.AuthResult result =
                initialAuthenticator.authenticate(incomingHeaders);
        return getAuthResultWithBearerToken(result, incomingHeaders);
    }


    protected AuthResultWithClaims getAuthResultWithBearerToken(AuthResult authResult, CallHeaders incomingHeaders) {

        // We generate a dummy header and call appendToOutgoingHeaders with it.
        // We then inspect the dummy header and parse the bearer token if present in the header
        // and generate a new bearer token if a bearer token is not present in the header.
        final CallHeaders dummyHeaders = new MetadataAdapter(new Metadata());
        authResult.appendToOutgoingHeaders(dummyHeaders);
        String bearerToken = AuthUtilities.getValueFromAuthHeader(dummyHeaders, Auth2Constants.BEARER_PREFIX);

        if (!Strings.isNullOrEmpty(bearerToken)) {
            return validateBearer(bearerToken, incomingHeaders);
        } else if (!generateToken) {
            logger.error("Authentication failed delegate token is missing");
            throw CallStatus.INTERNAL.toRuntimeException();
        }

        // Else, generate JWT manually (for Flight-based login)
        Calendar expiration = Calendar.getInstance();
        expiration.add(Calendar.MINUTE, (int) timeMinutes.toMinutes());

        var builder = Jwts.builder()
                .subject(authResult.getPeerIdentity())
                .expiration(expiration.getTime())
                .signWith(key);

        var verifiedClaims =  new HashMap<String, String >();
        for (String key : claimHeader) {
            var value = incomingHeaders.get(key);
            builder.claim(key, value);
            verifiedClaims.put(key, value);
        }
        String jwt = builder
                .compact();

        return new AuthResultWithClaims(authResult.getPeerIdentity(), jwt, verifiedClaims);
    }


    protected AuthResultWithClaims validateBearer(String bearerToken, CallHeaders incomingHeader) {
        try {
            var jwt = jwtParser.parseSignedClaims(bearerToken);
            var payload = jwt.getPayload();
            var subject = payload.getSubject();
            var expiration = payload.getExpiration();
            if (expiration.before(new Date())) {
                throw FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.UNAUTHENTICATED.code(), null, "Expired", null));
            }

            for(var validateKey : validateHeaders) {
                var incomingHeaderValue = incomingHeader.get(validateKey);
                var claimFromJwt = payload.get(validateKey, String.class);
                if (!claimFromJwt.equals(incomingHeaderValue)) {
                    throw FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.UNAUTHENTICATED.code(), null, "jwt and headers do not match", null));
                }
            }

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
            allClaimsFromJWT.put(Headers.HEADER_BEARER_TOKEN, bearerToken);
            return new AuthResultWithClaims(subject, bearerToken, allClaimsFromJWT);

        } catch (Exception e) {
            throw CallStatus.UNAUTHENTICATED.toRuntimeException();
        }
    }
}
