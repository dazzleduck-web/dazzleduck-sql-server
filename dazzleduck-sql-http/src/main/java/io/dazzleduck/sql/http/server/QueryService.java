package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.server.SimpleBulkIngestConsumer;
import io.dazzleduck.sql.flight.server.StatementHandle;
import io.helidon.http.Status;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql;


public class QueryService extends AbstractQueryBasedService {

    private final FlightProducer flightProducer;
    private final String producerId;

    public QueryService(SimpleBulkIngestConsumer flightProducer, AccessMode accessMode) {
        super(accessMode);
        this.flightProducer = flightProducer;
        this.producerId = flightProducer.getProducerId();
    }


    protected void handleInternal(ServerRequest request,
                                  ServerResponse response,
                                  QueryRequest query) {
        var context = ControllerService.createContext(request);
        try {
            var id = query.id() == null ? StatementHandle.nextStatementId() : query.id();
            var statementHandle = StatementHandle.newStatementHandle(id, query.query(), producerId, -1);
            var ticket = createTicket(statementHandle);
            var listener = new OutputStreamServerStreamListener(response);
            flightProducer.getStream(context, ticket, listener);
            listener.waitForEnd();
        } catch (Exception e) {
            logger.atError().setCause(e).log("Error sending query result");
            response.send(e.getMessage());
            response.status(Status.INTERNAL_SERVER_ERROR_500);
        }
    }

    private Ticket createTicket(StatementHandle statementHandle) throws JsonProcessingException {
        var builder = FlightSql.TicketStatementQuery.newBuilder();
        builder.setStatementHandle(ByteString.copyFrom(MAPPER.writeValueAsBytes(statementHandle)));
        var request = builder.build();
        return new Ticket(Any.pack(request).toByteArray());
    }
}
