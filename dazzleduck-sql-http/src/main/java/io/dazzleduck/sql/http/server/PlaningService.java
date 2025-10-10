package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.planner.SplitPlanner;
import io.helidon.http.HeaderValues;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.memory.BufferAllocator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

public class PlaningService extends AbstractQueryBasedService implements ParameterUtils {
    BufferAllocator allocator;
    String location;

    public PlaningService(String location, BufferAllocator allocator, AccessMode accessMode) {
        super(accessMode);
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
            long splitSize = ParameterUtils.getParameterValue(Headers.HEADER_SPLIT_SIZE, request, Headers.DEFAULT_SPLIT_SIZE, Long.class);
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
}
