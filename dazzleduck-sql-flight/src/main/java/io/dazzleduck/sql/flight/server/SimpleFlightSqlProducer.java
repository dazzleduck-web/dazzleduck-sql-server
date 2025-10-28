package io.dazzleduck.sql.flight.server;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;

public interface SimpleFlightSqlProducer {
    FlightInfo getFlightInfoStatementFromQuery(final String query, final FlightProducer.CallContext context, final FlightDescriptor descriptor);

    void getStreamStatement(
            StatementHandle statementHandle,
            final FlightProducer.CallContext context,
            final FlightProducer.ServerStreamListener listener);
}
