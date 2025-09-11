package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.helidon.common.uri.UriQuery;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static io.dazzleduck.sql.common.Headers.*;

public class IngestionService implements HttpService, ParameterUtils{
    private final String warehousePath;
    private final BufferAllocator allocator;

    public IngestionService(String warehousePath, BufferAllocator allocator) {
        this.warehousePath = warehousePath;
        this.allocator = allocator;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post("/", this::handlePost);
    }

    private void handlePost(ServerRequest serverRequest, ServerResponse serverResponse) throws SQLException {
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
        List<String> partitionList = partitions == null ? List.of() : Arrays.asList(partitions.split(","));
        var reader = new ArrowStreamReader(serverRequest.content().inputStream(), allocator);
        ConnectionPool.bulkIngestToFile(reader, allocator, completePath, partitionList, format, tranformationString);
        serverResponse.status(Status.OK_200);
        serverResponse.send();
    }
}
