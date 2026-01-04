package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.authorization.SqlAuthorizer;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractQueryBasedService implements HttpService, ControllerService {

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    protected final SqlAuthorizer sqlAuthorizer;

    public AbstractQueryBasedService(AccessMode accessMode) {
        if(AccessMode.RESTRICTED == accessMode) {
            this.sqlAuthorizer = SqlAuthorizer.JWT_AUTHORIZER;
        } else {
            this.sqlAuthorizer = SqlAuthorizer.NOOP_AUTHORIZER;
        }
    }
    protected static final Logger logger = LoggerFactory.getLogger(AbstractQueryBasedService.class);

    protected void handle(ServerRequest request,
                        ServerResponse response, QueryRequest requestObject) {
        try {
            handleInternal(request, response, requestObject);
        } catch (HttpException e) {
            if (e instanceof InternalErrorException) {
                logger.atError().setCause(e).log("Error");
            }
            String errorMsg = e.getMessage() != null ? e.getMessage() : "Internal server error";
            var msg = errorMsg.getBytes();
            response.status(e.errorCode);
            response.send(msg);
        }
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get("/", this::handleGet)
                .post("/", this::handlePost)
                .head("/", this::handleHead);
    }

    private void handleHead(ServerRequest serverRequest, ServerResponse serverResponse) {
        serverResponse.headers()
                .set(HeaderNames.CONTENT_ENCODING, HeaderValues.TRANSFER_ENCODING_CHUNKED.values());
        serverResponse.status(Status.OK_200);
        serverResponse.send();
    }

    private void handleGet(ServerRequest request,
                           ServerResponse response) {
        var query = request.requestedUri().query();
        var q = request.requestedUri().query().get("q");

        // Validate required query parameter
        if (q == null || q.isEmpty()) {
            response.status(Status.BAD_REQUEST_400);
            response.send("Missing required parameter: q");
            return;
        }

        QueryRequest queryRequest;
        if (query.contains("id")) {
            try {
                long id = Long.parseLong(query.get("id"));
                queryRequest = new QueryRequest(q, id);
            } catch (NumberFormatException e) {
                response.status(Status.BAD_REQUEST_400);
                response.send("Invalid id parameter: must be a valid number");
                return;
            }
        } else {
            queryRequest = new QueryRequest(q);
        }
        handle(request, response, queryRequest);
    }

    public void handlePost(ServerRequest request,
                           ServerResponse response) {
        try {
            var requestObject = MAPPER.readValue(request.content().inputStream(), QueryRequest.class);
            handle(request, response, requestObject);
        } catch (IOException e) {
            logger.atError().setCause(e).log("Failed to parse request body");
            String errorMsg = e.getMessage() != null ? e.getMessage() : "Invalid request body";
            response.status(Status.BAD_REQUEST_400);
            response.send(errorMsg);
        }
    }

    protected abstract void handleInternal(ServerRequest request,
                                           ServerResponse response, QueryRequest requestObject) ;
}
