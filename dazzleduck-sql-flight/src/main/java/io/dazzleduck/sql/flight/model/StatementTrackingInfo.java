package io.dazzleduck.sql.flight.model;

import io.dazzleduck.sql.flight.server.StatementHandle;
import org.apache.arrow.flight.FlightProducer;

public record StatementTrackingInfo(
        String user,
        String statementId,
        String query,
        String keyValue
) {
    public static StatementTrackingInfo from(FlightProducer.CallContext ctx, StatementHandle handle) {
        String user = ctx.peerIdentity();
        String statementId = String.valueOf(handle.queryId());
        String query = handle.query();
        return new StatementTrackingInfo(
                user,
                statementId,
                query,
                user + "-" + statementId
        );
    }

    public static StatementTrackingInfo fromPrepared(FlightProducer.CallContext ctx, String handleString, String query) {
        String user = ctx.peerIdentity();
        String statementId = handleString;
        return new StatementTrackingInfo(
                user,
                statementId,
                query,
                user + "-" + statementId
        );
    }
}

