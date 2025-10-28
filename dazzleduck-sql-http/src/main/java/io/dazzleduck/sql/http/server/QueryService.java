package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.server.StatementHandle;
import io.helidon.http.Status;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;

import static com.google.protobuf.Any.pack;

public class QueryService extends AbstractQueryBasedService {

    private final FlightProducer flightProducer;
    private final String secretKey;

    public QueryService(FlightProducer flightProducer, AccessMode accessMode, String secretKey) {
        super(accessMode);
        this.flightProducer = flightProducer;
        this.secretKey = secretKey;
    }


    protected void handleInternal(ServerRequest request,
                                  ServerResponse response,
                                  String query) {
        var context = ControllerService.createContext(request);
        try {
            var ticket = createTicket(query);
            var listener = new OutputStreamServerStreamListener(response);
            flightProducer.getStream(context, ticket, listener);
            listener.waitForEnd();
        } catch (Exception e) {
            response.send(e.getMessage().getBytes());
            response.status(Status.INTERNAL_SERVER_ERROR_500);
        }
    }

    private Ticket createTicket(String query) throws JsonProcessingException {
        var builder = FlightSql.TicketStatementQuery.newBuilder();
        var handle = new StatementHandle(query, -1, "http_producer", -1);
        var statementHandle  = handle.signed(secretKey);
        builder.setStatementHandle(ByteString.copyFrom(MAPPER.writeValueAsBytes(statementHandle)));
        var request = builder.build();
        return new Ticket(pack(request).toByteArray());
    }
}
