package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBResultSet;

import java.nio.channels.Channels;
import java.sql.SQLException;

public class QueryService extends AbstractQueryBasedService {

    private final BufferAllocator allocator;

    public QueryService(BufferAllocator allocator, AccessMode accessMode) {
        super(accessMode);
        this.allocator = allocator;
    }

    protected void handleInternal(ServerRequest request,
                                ServerResponse response, String query) {
        var fetchSizeHeader = request.headers().value(HeaderNames.create(Headers.HEADER_FETCH_SIZE));
        int fetchSize = fetchSizeHeader.map(Integer::parseInt).orElse(Headers.DEFAULT_ARROW_FETCH_SIZE);
        try (var connection = ConnectionPool.getConnection();
             var statement =  connection.createStatement()) {
            var hssResultSet = statement.execute(query);
            var os = response.outputStream();
            if (hssResultSet) {
                try (DuckDBResultSet resultSet = (DuckDBResultSet) statement.getResultSet();
                     ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(allocator, fetchSize)) {
                    var vsr = reader.getVectorSchemaRoot();
                    try (ArrowStreamWriter writer = new ArrowStreamWriter(vsr, null, Channels.newChannel(os))) {
                        var respHeaders = response.headers();
                        respHeaders.set(HeaderNames.CONTENT_TYPE, ContentTypes.APPLICATION_ARROW);
                        response.status(Status.OK_200);
                        writer.start();
                        while (reader.loadNextBatch()) {
                            writer.writeBatch();
                        }
                        writer.end();
                    }
                }
            } else {
                response.status(Status.OK_200);
                os.close();
            }
        } catch (SQLException e) {
            throw new BadRequestException(400, e.getMessage());
        } catch (Exception e) {
            throw new InternalErrorException(500, e.getMessage());
        }
    }
}
