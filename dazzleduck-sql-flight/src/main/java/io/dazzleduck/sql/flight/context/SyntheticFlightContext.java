package io.dazzleduck.sql.flight.context;

import io.dazzleduck.sql.commons.authorization.SubjectAndVerifiedClaims;
import io.dazzleduck.sql.flight.server.auth2.AdvanceServerCallHeaderAuthMiddleware;
import io.dazzleduck.sql.flight.server.auth2.AuthResultWithClaims;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.Auth2Constants;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SyntheticFlightContext implements FlightProducer.CallContext {
    private final CallHeaders callHeaders;
    private final String peerIdentity;
    private final SubjectAndVerifiedClaims subjectAndVerifiedClaims;
    private final Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> middlewareMap;

    private final AuthResultWithClaims authResultWithClaims;

    public SyntheticFlightContext(Map<String, List<String>> headers,
                                  @Nullable SubjectAndVerifiedClaims subjectAndVerifiedClaims) {
        this(headers, subjectAndVerifiedClaims, Map.of());
    }

    public SyntheticFlightContext(Map<String, List<String>> headers,
                                  @Nullable SubjectAndVerifiedClaims subjectAndVerifiedClaims,
                                  Map<String, List<String>> parameters) {
        this.callHeaders = new FlightCallHeaders();
        this.subjectAndVerifiedClaims = subjectAndVerifiedClaims;
        headers.forEach((k, vs) -> vs.forEach(v -> callHeaders.insert(k, v)));
        parameters.forEach((k, vs) -> vs.forEach(v -> callHeaders.insert(k, v)));
        ServerHeaderMiddleware serverHeaderMiddleware = new ServerHeaderMiddleware.Factory()
                .onCallStarted(null, callHeaders, null);
        middlewareMap = Map.of(FlightConstants.HEADER_KEY, serverHeaderMiddleware);
        if (subjectAndVerifiedClaims == null) {
            peerIdentity = getPeerIdentityFromAuthHeader(headers);
        } else {
            peerIdentity = subjectAndVerifiedClaims.subject();
        }
        if (subjectAndVerifiedClaims != null) {
            authResultWithClaims = new AuthResultWithClaims(peerIdentity, "", this.subjectAndVerifiedClaims.verifiedClaims());
        } else {
            authResultWithClaims = new AuthResultWithClaims(peerIdentity, null, Map.of());
        }
    }

    private static String getPeerIdentityFromAuthHeader(Map<String, List<String>> headers) {
        var authEncodedList =
                headers.get(Auth2Constants.AUTHORIZATION_HEADER);
        String _peerIdentity = null;
        if (authEncodedList != null && !authEncodedList.isEmpty() && authEncodedList.get(0).startsWith(Auth2Constants.BASIC_PREFIX)) {
            var authEncoded = authEncodedList.get(0)
                    .substring(Auth2Constants.BASIC_PREFIX.length() + 1);
            // The value has the format Base64(<username>:<password>)
            final String authDecoded =
                    new String(Base64.getDecoder().decode(authEncoded), StandardCharsets.UTF_8);
            final int colonPos = authDecoded.indexOf(':');
            if (colonPos != -1) {
                _peerIdentity = authDecoded.substring(0, colonPos);
            }
        }
        return _peerIdentity;
    }

    @Override
    public String peerIdentity() {
        return peerIdentity;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public <T extends FlightServerMiddleware> T getMiddleware(FlightServerMiddleware.Key<T> key) {
        //noinspection unchecked
        if (key.equals(AdvanceServerCallHeaderAuthMiddleware.KEY)) {
            return (T) new AdvanceServerCallHeaderAuthMiddleware(authResultWithClaims);
        } else {
            return (T) middlewareMap.get(key);
        }
    }

    @Override
    public Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> getMiddleware() {
        return Map.of();
    }
}