package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.planner.SplitPlanner;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import io.helidon.http.ServerRequestHeaders;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.memory.BufferAllocator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Optional;

public class PlaningService extends AbstractQueryBasedService {
    BufferAllocator allocator;
    String location;

    public PlaningService(String location, BufferAllocator allocator) {
        this.allocator = allocator;
        this.location = location;
    }
    @Override
    protected void handleInternal(ServerRequest request, ServerResponse response, String query) {
        try (var connection = ConnectionPool.getConnection()) {
            var tree = Transformations.parseToTree(connection, query);
            if (tree.get("error").asBoolean()) {
               response.status(500);
               try(var outputStream = response.outputStream()) {
                   outputStream.write(tree.get("error_message").asText().getBytes());
               }
               return;
            }
            long splitSize = getSplitSize(request.headers());
            var splits = SplitPlanner.getSplitTreeAndSize(tree, splitSize);
            var result = new ArrayList<Split>();
            for (var treeAndSize : splits) {
                var sql = Transformations.parseToSql(treeAndSize.tree());
                result.add(new Split(location, sql, treeAndSize.size()));
            }
            response.headers().set(HeaderValues.CONTENT_TYPE_JSON);
            var outputStream = response.outputStream();
            MAPPER.writeValue(outputStream, result);
            outputStream.close();
        } catch (SQLException sqlException) {
            throw new BadRequestException(400, sqlException.getMessage());
        } catch (IOException e) {
            throw new InternalErrorException(500, e.getMessage());
        }
    }

    private long getSplitSize(ServerRequestHeaders headers) {
        return headers.value(HeaderNames.create(Headers.HEADER_SPLIT_SIZE))
                .flatMap(v -> {
                    try {
                        return Optional.of(Long.parseLong(v));
                    } catch (Exception e) {
                        return Optional.of(Headers.DEFAULT_SPLIT_SIZE);
                    }
                }).orElse(Headers.DEFAULT_SPLIT_SIZE);
    }
}
