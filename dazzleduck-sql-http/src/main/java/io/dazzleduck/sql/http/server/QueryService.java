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
    private final HttpConfig httpConfig;

    public QueryService(SimpleBulkIngestConsumer flightProducer, AccessMode accessMode) {
        this(flightProducer, accessMode, HttpConfig.defaultConfig());
    }

    public QueryService(SimpleBulkIngestConsumer flightProducer, AccessMode accessMode, HttpConfig httpConfig) {
        super(accessMode);
        this.flightProducer = flightProducer;
        this.producerId = flightProducer.getProducerId();
        this.httpConfig = httpConfig;
    }


    protected void handleInternal(ServerRequest request,
                                  ServerResponse response,
                                  QueryRequest query) {
        var context = ControllerService.createContext(request);
        OutputStreamServerStreamListener listener = null;
        try {
            var id = query.id() == null ? StatementHandle.nextStatementId() : query.id();
            var statementHandle = StatementHandle.newStatementHandle(id, query.query(), producerId, -1);
            var ticket = createTicket(statementHandle);
            listener = new OutputStreamServerStreamListener(response);
            flightProducer.getStream(context, ticket, listener);
            listener.waitForEnd(httpConfig.getQueryTimeoutMs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt status
            logger.atError().setCause(e).log("Query execution interrupted");
            String errorMsg = "Query execution interrupted";
            response.status(Status.INTERNAL_SERVER_ERROR_500);
            response.send(errorMsg);
        } catch (Exception e) {
            logger.atError().setCause(e).log("Error sending query result");
            String errorMsg = e.getMessage() != null ? e.getMessage() : "Internal server error";
            response.status(Status.INTERNAL_SERVER_ERROR_500);
            response.send(errorMsg);
        } finally {
            // Ensure listener is properly completed even on exception
            if (listener != null && !listener.isCompleted()) {
                listener.forceComplete();
            }
        }
    }

    private Ticket createTicket(StatementHandle statementHandle) throws JsonProcessingException {
        var builder = FlightSql.TicketStatementQuery.newBuilder();
        builder.setStatementHandle(ByteString.copyFrom(MAPPER.writeValueAsBytes(statementHandle)));
        var request = builder.build();
        return new Ticket(Any.pack(request).toByteArray());
    }
}
