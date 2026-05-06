package io.dazzleduck.sql.flight.namedquery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.dazzleduck.sql.flight.server.DirectOutputStreamListener;
import io.dazzleduck.sql.flight.server.HttpFlightAdaptor;
import io.dazzleduck.sql.flight.server.JsonOutputStreamListener;
import io.dazzleduck.sql.flight.server.StatementHandle;
import io.dazzleduck.sql.flight.server.TsvOutputStreamListener;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.io.OutputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface NamedQueryServiceAdaptor {
    
    ObjectMapper MAPPER = new ObjectMapper();

    String LIST_ITEMS_QUERY = "SELECT id, name, description, parameter_descriptions AS parameterDescriptions, validators AS validatorDescriptions, preferred_display, query_group FROM %s WHERE id > %d ORDER BY id LIMIT %d";
    String GET_NAMED_QUERY = "SELECT id, name, template, description, parameter_descriptions AS parameterDescriptions, validators AS validatorDescriptions, preferred_display, query_group FROM %s WHERE name = '%s'";

    String getTemplateTable(FlightProducer.CallContext context);

    HttpFlightAdaptor getHttpFlightAdaptor();

    void getStreamNamedQuery(String name, Map<String, String> parameters,
                             FlightProducer.CallContext context,
                             FlightProducer.ServerStreamListener listener);

    default CompletableFuture<Void> getStreamNamedQueryDirect(
            String name, Map<String, String> parameters,
            FlightProducer.CallContext context,
            Supplier<OutputStream> outputStreamSupplier,
            CompressionUtil.CodecType compressionCodec) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        var listener = new DirectOutputStreamListener(outputStreamSupplier, future, compressionCodec);
        getStreamNamedQuery(name, parameters, context, listener);
        return future;
    }

    default CompletableFuture<Void> listItemsDirect(long offset, int limit,
                                                   FlightProducer.CallContext context,
                                                   Supplier<OutputStream> outputStreamSupplier) {
        return streamJson(LIST_ITEMS_QUERY.formatted(getTemplateTable(context), offset, limit), context, outputStreamSupplier, true);
    }

    default CompletableFuture<Void> getNamedQueryDirect(String name,
                                                       FlightProducer.CallContext context,
                                                       Supplier<OutputStream> outputStreamSupplier) {
        String safeName = name.replace("'", "''");
        return streamJson(GET_NAMED_QUERY.formatted(getTemplateTable(context), safeName), context, outputStreamSupplier, false)
            .exceptionallyCompose(ex -> {
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                if (cause instanceof NoSuchElementException) {
                    return CompletableFuture.failedFuture(new TemplateNotFoundException("Named query not found: " + name));
                }
                return CompletableFuture.failedFuture(ex);
            });
    }

    default CompletableFuture<Void> streamJson(String sql, FlightProducer.CallContext context,
                                               Supplier<OutputStream> outputStreamSupplier,
                                               boolean includeArrayBrackets) {
        try {
            var id = StatementHandle.nextStatementId();
            var statementHandle = StatementHandle.newStatementHandle(
                    id, sql, getHttpFlightAdaptor().getProducerId(), -1);
            var ticket = FlightSql.TicketStatementQuery.newBuilder()
                    .setStatementHandle(ByteString.copyFrom(MAPPER.writeValueAsBytes(statementHandle)))
                    .build();

            CompletableFuture<Void> future = new CompletableFuture<>();
            JsonOutputStreamListener listener = new JsonOutputStreamListener(outputStreamSupplier, future, includeArrayBrackets);
            getHttpFlightAdaptor().getStreamStatement(ticket, context, listener);
            return future;
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    default CompletableFuture<Void> streamTsvNamedQuery(String name, Map<String, String> parameters,
                                                         FlightProducer.CallContext context,
                                                         Supplier<OutputStream> outputStreamSupplier) {
        return TsvOutputStreamListener.pipeArrowToTsv(
                pipe -> getStreamNamedQueryDirect(name, parameters, context, pipe,
                        CompressionUtil.CodecType.NO_COMPRESSION),
                outputStreamSupplier);
    }

    /** Signals that a named query template could not be found in the DB. */
    class TemplateNotFoundException extends Exception {
        public TemplateNotFoundException(String message) {
            super(message);
        }
    }
}
