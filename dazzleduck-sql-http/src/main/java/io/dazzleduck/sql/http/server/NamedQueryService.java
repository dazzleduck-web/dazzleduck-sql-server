package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.ParameterValidationException;
import io.dazzleduck.sql.flight.namedquery.NamedQueryRequest;
import io.dazzleduck.sql.flight.namedquery.NamedQueryServiceAdaptor;
import io.dazzleduck.sql.flight.server.TsvOutputStreamListener;
import io.dazzleduck.sql.http.server.model.ContentTypes;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * HTTP service that executes named (templated) SQL queries.
 */
public class NamedQueryService implements HttpService, ControllerService {

    private static final Logger logger = LoggerFactory.getLogger(NamedQueryService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_PAGE_LIMIT = 20;
    private static final int MAX_PAGE_LIMIT = 200;

    private final NamedQueryServiceAdaptor adaptor;
    private final long timeoutMillis;

    public NamedQueryService(NamedQueryServiceAdaptor adaptor, long timeoutMillis) {
        this.adaptor = adaptor;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get("/", this::handleList)
             .get("/{name}", this::handleGetByName)
             .post("/", this::handlePost);
    }

    /** GET / — returns a paginated list of named queries. */
    private void handleList(ServerRequest request, ServerResponse response) {
        long offset = ParameterUtils.getParameterValue("offset", request, 0L, Long.class);
        int limit   = ParameterUtils.getParameterValue("limit",  request, DEFAULT_PAGE_LIMIT, Integer.class);
        if (offset < 0) offset = 0;
        if (limit  < 1) limit  = DEFAULT_PAGE_LIMIT;
        if (limit  > MAX_PAGE_LIMIT) limit = MAX_PAGE_LIMIT;

        var callContext = ControllerService.createContext(request);
        response.headers().set(HeaderNames.CONTENT_TYPE, ContentTypes.APPLICATION_JSON);
        var future = adaptor.listItemsDirect(offset, limit, callContext, () -> response.outputStream());

        await(future, response, (cause, res) -> {
            logger.error("Error listing named queries", cause);
            ControllerService.sendFlightError(res, cause);
        });
    }

    /** GET /{name} — returns the full named query object. */
    private void handleGetByName(ServerRequest request, ServerResponse response) {
        var segments = request.path().segments();
        String name = segments.get(segments.size() - 1).value();

        var callContext = ControllerService.createContext(request);
        response.headers().set(HeaderNames.CONTENT_TYPE, ContentTypes.APPLICATION_JSON);
        var future = adaptor.getNamedQueryDirect(name, callContext, () -> response.outputStream());
        await(future, response, (cause, res) -> {
            if (cause instanceof NamedQueryServiceAdaptor.TemplateNotFoundException) {
                logger.warn("Named query not found: {}", name);
                res.status(Status.NOT_FOUND_404).send(cause.getMessage());
            } else {
                logger.error("Error fetching named query '{}'", name, cause);
                ControllerService.sendFlightError(res, cause);
            }
        });
    }

    private void handlePost(ServerRequest request, ServerResponse response) {
        NamedQueryRequest namedQuery;
        try {
            namedQuery = MAPPER.readValue(request.content().inputStream(), NamedQueryRequest.class);
        } catch (Exception e) {
            logger.error("Failed to parse named query request body", e);
            response.status(Status.BAD_REQUEST_400).send("Invalid request body");
            return;
        }

        if (namedQuery.name() == null || namedQuery.name().isBlank()) {
            response.status(Status.BAD_REQUEST_400).send("Missing required field: name");
            return;
        }

        var callContext = ControllerService.createContext(request);
        var acceptHeader = request.headers().value(HeaderNames.ACCEPT);
        boolean wantsTsv = acceptHeader.isPresent() && acceptHeader.get().contains(ContentTypes.TEXT_TSV);

        CompletableFuture<Void> future;
        if (wantsTsv) {
            response.headers().set(HeaderNames.CONTENT_TYPE, ContentTypes.TEXT_TSV_UTF8);
            future = new CompletableFuture<>();
            var listener = new TsvOutputStreamListener(() -> response.outputStream(), future);
            adaptor.getStreamNamedQuery(namedQuery.name(), namedQuery.parameters(), callContext, listener);
        } else {
            response.headers().set(HeaderNames.CONTENT_TYPE, ContentTypes.APPLICATION_ARROW);
            var compressionCodec = ParameterUtils.getArrowCompression(request);
            future = adaptor.getStreamNamedQueryDirect(namedQuery.name(), namedQuery.parameters(),
                    callContext, () -> response.outputStream(), compressionCodec);
        }

        await(future, response, (cause, res) -> {
            if (cause instanceof ParameterValidationException) {
                logger.warn("Parameter validation failed for named query: {}", cause.getMessage());
                res.status(Status.BAD_REQUEST_400).send(cause.getMessage());
            } else if (cause instanceof NamedQueryServiceAdaptor.TemplateNotFoundException) {
                logger.warn("Named query not found: {}", cause.getMessage());
                res.status(Status.NOT_FOUND_404).send(cause.getMessage());
            } else {
                logger.error("Error executing named query", cause);
                ControllerService.sendFlightError(res, cause);
            }
        });
    }

    /**
     * Awaits a query future and dispatches exceptions uniformly across all handlers.
     * The {@code onExecutionError} callback handles only the unwrapped cause from
     * {@link ExecutionException} — the per-handler varying part.
     */
    private void await(CompletableFuture<Void> future, ServerResponse response,
                       BiConsumer<Throwable, ServerResponse> onExecutionError) {
        try {
            future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            logger.error("Named query timed out after {}ms", timeoutMillis);
            if (!response.isSent()) response.status(Status.GATEWAY_TIMEOUT_504).send("Query execution timeout");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Named query execution interrupted", e);
            if (!response.isSent()) response.status(Status.INTERNAL_SERVER_ERROR_500).send("Query execution interrupted");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (!response.isSent()) onExecutionError.accept(cause, response);
        } catch (Exception e) {
            logger.error("Named query execution error", e);
            if (!response.isSent()) ControllerService.sendFlightError(response, e);
        }
    }
}
