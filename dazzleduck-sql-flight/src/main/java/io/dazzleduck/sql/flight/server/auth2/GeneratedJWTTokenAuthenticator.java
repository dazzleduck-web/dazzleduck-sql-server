package io.dazzleduck.sql.flight.server.auth2;

import com.google.common.base.Strings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Metadata;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeExceptionFactory;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.BearerTokenAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.grpc.MetadataAdapter;

import javax.crypto.SecretKey;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;


public class GeneratedJWTTokenAuthenticator extends BearerTokenAuthenticator {
    private final SecretKey key;
    private final Duration timeMinutes;
    private final JwtParser jwtParser;

    public GeneratedJWTTokenAuthenticator(CallHeaderAuthenticator initialAuthenticator, SecretKey key, Config config) {
        super(initialAuthenticator);
        this.key = key;
        this.jwtParser = Jwts.parser()     // (1)
                .verifyWith(key)//     or a constant key used to verify all signed JWTs
                .build();
        this.timeMinutes = config.getDuration("jwt.token.expiration");
    }

    public GeneratedJWTTokenAuthenticator(CallHeaderAuthenticator initialAuthenticator, SecretKey key) {
        super(initialAuthenticator);
        this.key = key;
        this.jwtParser = Jwts.parser()     // (1)
                .verifyWith(key) //     or a constant key used to verify all signed JWTs
                .build();
        this.timeMinutes = Duration.ofMinutes(60);
    }

    @Override
    protected AuthResult getAuthResultWithBearerToken(AuthResult authResult) {

        // We generate a dummy header and call appendToOutgoingHeaders with it.
        // We then inspect the dummy header and parse the bearer token if present in the header
        // and generate a new bearer token if a bearer token is not present in the header.
        final CallHeaders dummyHeaders = new MetadataAdapter(new Metadata());
        authResult.appendToOutgoingHeaders(dummyHeaders);
        String bearerToken = AuthUtilities.getValueFromAuthHeader(dummyHeaders, Auth2Constants.BEARER_PREFIX);

        if (!Strings.isNullOrEmpty(bearerToken)) {
            Token.token = bearerToken;
            return authResult; // Already has JWT from AuthUtils
        }

        // Else, generate JWT manually (for Flight-based login)
        Calendar expiration = Calendar.getInstance();
        expiration.add(Calendar.MINUTE, (int) timeMinutes.toMinutes());

        String jwt = Jwts.builder()
                .subject(authResult.getPeerIdentity())
                .expiration(expiration.getTime())
                .claim("orgId", "1211") // this will be generated dynamically
                .claim("clusterId", List.of("cc1", "cc2", "cc3")) // this will be generated dynamically
                .signWith(key)
                .compact();

        Token.token = jwt;

        return new AuthResult() {
            @Override
            public String getPeerIdentity() {
                return authResult.getPeerIdentity();
            }

            @Override
            public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
                authResult.appendToOutgoingHeaders(outgoingHeaders);
                outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + jwt);
            }
        };
    }

    @Override
    protected AuthResult validateBearer(String bearerToken) {
        try {
            var jwt = jwtParser.parseSignedClaims(bearerToken);
            var payload = jwt.getPayload();
            var subject = payload.getSubject();
            var expiration = payload.getExpiration();
            if (expiration.before(new Date())) {
                throw FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.UNAUTHENTICATED.code(), null, "Expired", null));
            }
            return new AuthResult() {
                @Override
                public String getPeerIdentity() {
                    return subject;
                }

                @Override
                public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
                    outgoingHeaders.insert(
                            Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + bearerToken);
                }
            };
        } catch (Exception e) {
            throw CallStatus.UNAUTHENTICATED.toRuntimeException();
        }
    }
}
