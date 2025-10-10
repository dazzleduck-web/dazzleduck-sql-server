package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.authorization.SqlAuthorizer;
import io.dazzleduck.sql.commons.ingestion.*;
import io.helidon.common.uri.UriQuery;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static io.dazzleduck.sql.common.Headers.*;

public class IngestionService implements HttpService, ParameterUtils {
    private final String warehousePath;
    private final Config config;

    private final PostIngestionTaskFactory postIngestionTaskFactory;

    private static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(1);

    private static final long DEFAULT_MAX_BUCKET_SIZE = 16 * 1024 * 1024;

    private final ConcurrentHashMap<String, BulkIngestQueue<String, IngestionResult>> ingestionQueueMap =
            new ConcurrentHashMap<>();
    private final SqlAuthorizer sqlAuthorizer;
    private final Path tempDir;

    public IngestionService(String warehousePath, Config config, AccessMode accessMode, Path tempDir)  {
        this.warehousePath = warehousePath;
        this.config = config;
        this.sqlAuthorizer = accessMode == AccessMode.COMPLETE? SqlAuthorizer.NOOP_AUTHORIZER : SqlAuthorizer.JWT_AUTHORIZER;
        this.tempDir = tempDir;
        this.postIngestionTaskFactory = connectionResult -> (PostIngestionTask) () -> {/*do nothing*/};
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post("/", this::handlePost);
    }

    private void handlePost(ServerRequest serverRequest, ServerResponse serverResponse) throws ExecutionException, InterruptedException, IOException {
        var contentType = serverRequest.headers().value(HeaderNames.CONTENT_TYPE);
        if (contentType.isEmpty() || !contentType.get().equals(ContentTypes.APPLICATION_ARROW)) {
            serverResponse.status(Status.UNSUPPORTED_MEDIA_TYPE_415);
            serverResponse.send();
            return;
        }
        UriQuery query = serverRequest.query();
        var path = query.get("path");
        final String completePath = warehousePath + "/" + path;

        String format = ParameterUtils.getParameterValue(HEADER_DATA_FORMAT, serverRequest, "parquet", String.class);
        var partitions = ParameterUtils.getParameterValue(HEADER_DATA_PARTITION, serverRequest, null, String.class);
        var tranformationString = ParameterUtils.getParameterValue(HEADER_DATA_TRANSFORMATION, serverRequest, null, String.class);
        var producerId = ParameterUtils.getParameterValue(HEADER_PRODUCER_ID, serverRequest, null, String.class);
        var producerBatchId = ParameterUtils.getParameterValue(HEADER_PRODUCER_BATCH_ID, serverRequest, -1L, Long.class);
        var sortOrder = ParameterUtils.getParameterValue(HEADER_SORT_ORDER, serverRequest, null, String.class);
        int totalSize = 1024;


        // Read request body and write it to temp file
        String tempFile;
        try (InputStream in = serverRequest.content().inputStream()){
            tempFile = BulkIngestQueue.writeAndValidateTempFile(tempDir, in);
        }
        var batch = new Batch<>(
                splitParam(sortOrder),
                splitParam(tranformationString),
                splitParam(partitions),
                tempFile,
                producerId,
                producerBatchId,
                totalSize,
                format,
                Instant.now()
        );
        var ingestionQueue = ingestionQueueMap.computeIfAbsent(completePath, p -> {
            return new ParquetIngestionQueue(p, p, DEFAULT_MAX_BUCKET_SIZE, DEFAULT_MAX_DELAY,
                    postIngestionTaskFactory,
                    Executors.newSingleThreadScheduledExecutor(), Clock.systemDefaultZone());
        });
        var result = ingestionQueue.addToQueue(batch);
        var futureResult = result.get(); // blocks until batch processed
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.valueToTree(futureResult);
        jsonNode.put("completionTime", Instant.now().toString());
        serverResponse.status(Status.OK_200).header(HeaderNames.CONTENT_TYPE, ContentTypes.APPLICATION_JSON).send(jsonNode.toString());
    }

    private static String[] splitParam(String value) {
        return (value == null || value.isBlank()) ? new String[0] : value.split(",");
    }
}
