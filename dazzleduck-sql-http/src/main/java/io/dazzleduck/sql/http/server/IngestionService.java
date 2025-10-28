package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.flight.ingestion.IngestionParameters;
import io.dazzleduck.sql.flight.server.SimpleBulkIngestConsumer;
import io.helidon.common.uri.UriQuery;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static io.dazzleduck.sql.common.Headers.*;

public class IngestionService implements HttpService, ParameterUtils, ControllerService {

    private final String warehousePath;

    private final SimpleBulkIngestConsumer bulkIngestConsumer;

    private final BufferAllocator bufferAllocator;

    public IngestionService(SimpleBulkIngestConsumer bulkIngestConsumer,
                            String warehousePath, BufferAllocator allocator)  {
        this.warehousePath = warehousePath;
        this.bulkIngestConsumer = bulkIngestConsumer;
        this.bufferAllocator = allocator;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post("/", this::handlePost);
    }

    protected boolean handleMismatchContentType(ServerRequest serverRequest, ServerResponse serverResponse){
        var contentType = serverRequest.headers().value(HeaderNames.CONTENT_TYPE);
        if (contentType.isEmpty() || !contentType.get().equals(ContentTypes.APPLICATION_ARROW)) {
            serverResponse.status(Status.UNSUPPORTED_MEDIA_TYPE_415);
            serverResponse.send();
            return true;
        }
        return false;
    }

    protected IngestionParameters parseIngestionParameters(ServerRequest serverRequest) {
        UriQuery query = serverRequest.query();
        var path = query.get("path");
        final String completePath = warehousePath + "/" + path;
        String format = ParameterUtils.getParameterValue(HEADER_DATA_FORMAT, serverRequest, "parquet", String.class);
        var partitionString = ParameterUtils.getParameterValue(HEADER_DATA_PARTITION, serverRequest, null, String.class);
        var tranformationString = ParameterUtils.getParameterValue(HEADER_DATA_TRANSFORMATION, serverRequest, null, String.class);
        var producerId = ParameterUtils.getParameterValue(HEADER_PRODUCER_ID, serverRequest, null, String.class);
        var producerBatchId = ParameterUtils.getParameterValue(HEADER_PRODUCER_BATCH_ID, serverRequest, -1L, Long.class);
        var sortOrderString = ParameterUtils.getParameterValue(HEADER_SORT_ORDER, serverRequest, null, String.class);
        return new IngestionParameters(completePath, format, getArray(partitionString),
                getArray(tranformationString), getArray(sortOrderString), producerId, producerBatchId, Map.of());
    }

    private String[] getArray(String stringValue) {
        return stringValue == null? new String[0]: stringValue.split(",");
    }

    private void handlePost(ServerRequest serverRequest, ServerResponse serverResponse) throws ExecutionException, InterruptedException, IOException {
        if (handleMismatchContentType(serverRequest, serverResponse)) {
            return;
        }
        var context = ControllerService.createContext(serverRequest);
        var ingestionParameters = parseIngestionParameters(serverRequest);
        var runnable = bulkIngestConsumer.acceptPutStatementBulkIngest(context, ingestionParameters,
                createStream(serverRequest.content().inputStream()), new FlightClient.PutListener() {
                    @Override
                    public void getResult() {

                    }

                    @Override
                    public void onNext(PutResult val) {

                    }

                    @Override
                    public void onError(Throwable t) {

                    }

                    @Override
                    public void onCompleted() {
                        //serverResponse.status(Status.OK_200);
                    }
                });
        runnable.run();
        serverResponse.status(Status.OK_200);
        serverResponse.send();
    }

    private ArrowReader createStream(InputStream inputStream){
        var readableByteChannel = Channels.newChannel(inputStream);
        return new ArrowStreamReader(readableByteChannel, bufferAllocator);
    }
}
