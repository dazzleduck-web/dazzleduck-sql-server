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

public abstract class AbstractQueryBasedService implements HttpService {

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
                        ServerResponse response, String query) {
        try {
            handleInternal(request, response, query);
        } catch (HttpException e) {
            if (e instanceof InternalErrorException) {
                logger.atError().setCause(e).log("Error");
            }
            var msg = e.getMessage().getBytes();
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
        var query = request.requestedUri().query().get("q");
        handle(request, response, query);
    }

    public void handlePost(ServerRequest request,
                           ServerResponse response) throws IOException {
        var queryObject = MAPPER.readValue(request.content().inputStream(), QueryObject.class);
        handle(request, response, queryObject.query());
    }

    protected abstract void handleInternal(ServerRequest request,
                                           ServerResponse response, String query) ;
}
