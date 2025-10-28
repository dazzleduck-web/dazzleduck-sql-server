package io.dazzleduck.sql.http.server;

import com.google.protobuf.Any;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.server.StatementHandle;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;

import java.io.IOException;
import java.util.ArrayList;

public class PlaningService extends AbstractQueryBasedService implements ParameterUtils {
    BufferAllocator allocator;
    String location;
    private final FlightProducer flightProducer;

    public PlaningService(FlightProducer flightProducer, String location, BufferAllocator allocator, AccessMode accessMode) {
        super(accessMode);
        this.allocator = allocator;
        this.location = location;
        this.flightProducer = flightProducer;
    }
    @Override
    protected void handleInternal(ServerRequest request, ServerResponse response, String query) {
        try {
            var command = FlightSql.CommandStatementQuery.newBuilder().setQuery(query).build();
            var info = flightProducer.getFlightInfo(ControllerService.createContext(request),
                    FlightDescriptor.command(Any.pack(command).toByteArray()));
            var result = new ArrayList<StatementHandle>();
            for ( var endpoint : info.getEndpoints()) {
                var any = FlightSqlUtils.parseOrThrow(endpoint.getTicket().getBytes());
                var statementQuery = FlightSqlUtils.unpackOrThrow(any, FlightSql.TicketStatementQuery.class);
                var statementHandle = MAPPER.readValue(statementQuery.getStatementHandle().toByteArray(), StatementHandle.class);
                result.add(statementHandle);
            }
            var outputStream = response.outputStream();
            MAPPER.writeValue(outputStream, result);
            outputStream.close();
        } catch (IOException e) {
            throw new InternalErrorException(500, e.getMessage());
        }
    }
}

