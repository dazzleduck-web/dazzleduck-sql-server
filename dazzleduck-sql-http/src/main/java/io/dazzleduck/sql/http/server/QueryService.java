package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import io.dazzleduck.sql.flight.server.HttpFlightAdaptor;
import io.dazzleduck.sql.flight.server.StatementHandle;
import io.dazzleduck.sql.http.server.model.HttpConfig;
import io.dazzleduck.sql.http.server.model.QueryRequest;
import io.helidon.http.Status;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


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

            // Get Arrow compression codec from header (defaults to ZSTD)
            CompressionUtil.CodecType compressionCodec = ParameterUtils.getArrowCompression(request);
            logger.atDebug().log("Using Arrow compression codec: {}", compressionCodec);

            logger.atDebug().log("Calling getStreamStatementDirect for query: {}", query.query());
            var future = httpFlightAdaptor.getStreamStatementDirect(ticket, context, () -> response.outputStream(), compressionCodec);

            logger.atDebug().log("Waiting for future.get() with timeout {}ms", httpConfig.getQueryTimeoutMs());
            future.get(httpConfig.getQueryTimeoutMs(), TimeUnit.MILLISECONDS);
            logger.atDebug().log("future.get() completed successfully");

        } catch (IllegalArgumentException e) {
            logger.atError().setCause(e).log("Invalid Arrow compression header value");
            if (!response.isSent()) {
                response.status(Status.BAD_REQUEST_400);
                response.send(e.getMessage());
            }
        } catch (TimeoutException e) {
            logger.atError().log("Query execution timeout after {}ms", httpConfig.getQueryTimeoutMs());
            if (!response.isSent()) {
                response.status(Status.GATEWAY_TIMEOUT_504);
                response.send("Query execution timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.atError().setCause(e).log("Query execution interrupted");
            if (!response.isSent()) {
                response.status(Status.INTERNAL_SERVER_ERROR_500);
                response.send("Query execution interrupted");
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            logger.atError().setCause(cause).log("Error executing query");
            if (!response.isSent()) {
                handleError(response, cause);
            }
        } catch (Exception e) {
            logger.atError().setCause(e).log("Error sending query result");
            if (!response.isSent()) {
                handleError(response, e);
            }
        }
    }

    private void handleError(ServerResponse response, Throwable cause) {
        String errorMsg = cause.getMessage() != null ? cause.getMessage() : "Internal server error";
        Status httpStatus = Status.INTERNAL_SERVER_ERROR_500;

        if (cause instanceof FlightRuntimeException flightEx) {
            FlightStatusCode statusCode = flightEx.status().code();
            errorMsg = flightEx.status().description() != null
                    ? flightEx.status().description()
                    : errorMsg;
            httpStatus = mapFlightStatusToHttp(statusCode);
        }

        response.status(httpStatus);
        response.send(errorMsg);
    }

    private Status mapFlightStatusToHttp(FlightStatusCode statusCode) {
        return switch (statusCode) {
            case OK -> Status.OK_200;
            case INVALID_ARGUMENT -> Status.BAD_REQUEST_400;
            case UNAUTHENTICATED -> Status.UNAUTHORIZED_401;
            case UNAUTHORIZED -> Status.FORBIDDEN_403;
            case NOT_FOUND -> Status.NOT_FOUND_404;
            case TIMED_OUT -> Status.GATEWAY_TIMEOUT_504;
            case ALREADY_EXISTS -> Status.CONFLICT_409;
            case UNIMPLEMENTED -> Status.NOT_IMPLEMENTED_501;
            case UNAVAILABLE -> Status.SERVICE_UNAVAILABLE_503;
            default -> Status.INTERNAL_SERVER_ERROR_500;
        };
    }

    private FlightSql.TicketStatementQuery createTicket(StatementHandle statementHandle) throws JsonProcessingException {
        var builder = FlightSql.TicketStatementQuery.newBuilder();
        builder.setStatementHandle(ByteString.copyFrom(MAPPER.writeValueAsBytes(statementHandle)));
        return builder.build();
    }
}
