package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import io.dazzleduck.sql.flight.server.HttpFlightAdaptor;
import io.dazzleduck.sql.flight.server.StatementHandle;
import io.dazzleduck.sql.common.ContentTypes;
import io.dazzleduck.sql.http.server.model.HttpConfig;
import io.dazzleduck.sql.http.server.model.QueryRequest;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;


public class QueryService extends AbstractQueryBasedService {

    private final HttpFlightAdaptor httpFlightAdaptor;
    private final String producerId;
    private final HttpConfig httpConfig;

    public QueryService(HttpFlightAdaptor httpFlightAdaptor) {
        this(httpFlightAdaptor, HttpConfig.defaultConfig());
    }

    public QueryService(HttpFlightAdaptor httpFlightAdaptor, HttpConfig httpConfig) {
        this.httpFlightAdaptor = httpFlightAdaptor;
        this.producerId = httpFlightAdaptor.getProducerId();
        this.httpConfig = httpConfig;
    }


    protected void handleInternal(ServerRequest request,
                                  ServerResponse response,
                                  QueryRequest query) {
        var context = ControllerService.createContext(request);
        try {
            var id = query.id() == null ? StatementHandle.nextStatementId() : query.id();
            var statementHandle = StatementHandle.newStatementHandle(id, query.query(), producerId, -1);
            var ticket = createTicket(statementHandle);

            var acceptHeader = request.headers().value(HeaderNames.ACCEPT);
            String accept = acceptHeader.orElse("");
            boolean wantsTsv = accept.contains(ContentTypes.TEXT_TSV);
            boolean wantsJsonl = accept.contains(ContentTypes.APPLICATION_JSONL)
                    || accept.contains(ContentTypes.APPLICATION_X_NDJSON);

            CompletableFuture<Void> future;
            if (wantsTsv) {
                logger.debug("TSV output requested for query: {}", query.query());
                response.header("Content-Type", ContentTypes.TEXT_TSV_UTF8);
                future = httpFlightAdaptor.streamTsv(ticket, context, () -> response.outputStream());
            } else if (wantsJsonl) {
                logger.debug("JSONL output requested for query: {}", query.query());
                response.header("Content-Type", ContentTypes.APPLICATION_JSONL_UTF8);
                future = httpFlightAdaptor.streamJsonl(ticket, context, () -> response.outputStream());
            } else {
                // Get Arrow compression codec from header (defaults to ZSTD)
                CompressionUtil.CodecType compressionCodec = ParameterUtils.getArrowCompression(request);
                logger.debug("Using Arrow compression codec: {}", compressionCodec);
                logger.debug("Calling getStreamStatementDirect for query: {}", query.query());
                response.header("Content-Type", ContentTypes.APPLICATION_ARROW);
                future = httpFlightAdaptor.getStreamStatementDirect(ticket, context, () -> response.outputStream(), compressionCodec);
            }

            logger.debug("Waiting for future.get() with timeout {}ms", httpConfig.getQueryTimeoutMs());
            future.get(httpConfig.getQueryTimeoutMs(), TimeUnit.MILLISECONDS);
            logger.debug("future.get() completed successfully");

        } catch (IllegalArgumentException e) {
            logger.error("Invalid Arrow compression header value", e);
            if (!response.isSent()) {
                response.status(Status.BAD_REQUEST_400);
                response.send(e.getMessage());
            }
        } catch (TimeoutException e) {
            logger.error("Query execution timeout after {}ms", httpConfig.getQueryTimeoutMs());
            if (!response.isSent()) {
                response.status(Status.GATEWAY_TIMEOUT_504);
                response.send("Query execution timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Query execution interrupted", e);
            if (!response.isSent()) {
                response.status(Status.INTERNAL_SERVER_ERROR_500);
                response.send("Query execution interrupted");
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            logger.error("Error executing query", cause);
            if (!response.isSent()) {
                ControllerService.sendFlightError(response, cause);
            }
        } catch (Exception e) {
            logger.error("Error sending query result", e);
            if (!response.isSent()) {
                ControllerService.sendFlightError(response, e);
            }
        }
    }

    private FlightSql.TicketStatementQuery createTicket(StatementHandle statementHandle) throws JsonProcessingException {
        var builder = FlightSql.TicketStatementQuery.newBuilder();
        builder.setStatementHandle(ByteString.copyFrom(MAPPER.writeValueAsBytes(statementHandle)));
        return builder.build();
    }

}
