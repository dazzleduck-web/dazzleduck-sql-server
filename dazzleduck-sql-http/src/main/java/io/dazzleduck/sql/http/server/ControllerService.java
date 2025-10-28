package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.commons.authorization.SubjectAndVerifiedClaims;
import io.dazzleduck.sql.flight.context.SyntheticFlightContext;
import io.helidon.webserver.http.ServerRequest;
import org.apache.arrow.flight.FlightProducer;

public interface ControllerService {
    static FlightProducer.CallContext createContext(ServerRequest request) {
        var subjectAndVerifiedClaims = request.context().get(JwtAuthenticationFilter.SUBJECT_KEY, SubjectAndVerifiedClaims.class);
        return new SyntheticFlightContext(request.headers().toMap(),
                subjectAndVerifiedClaims.orElse(null), request.query().toMap());
    }
}
