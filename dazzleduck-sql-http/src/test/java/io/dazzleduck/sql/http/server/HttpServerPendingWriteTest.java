package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.http.server.model.ContentTypes;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for verifying that PendingWriteExceededException returns correct HTTP 429 status code.
 */
public class HttpServerPendingWriteTest extends HttpServerTestBase {

    @BeforeAll
    public static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        // Start server with max_pending_write set to 1 byte - any ingestion will exceed this
        startServer("--conf", "dazzleduck_server.ingestion.max_pending_write=1");
        installArrowExtension();
    }

    @AfterAll
    public static void cleanup() throws Exception {
        cleanupWarehouse();
    }

    @Test
    public void testIngestionRejectedWhenPendingWriteExceeded() throws IOException, InterruptedException, SQLException {
        var path = "pending_write_test";
        Files.createDirectories(Path.of(ingestionPath, path));

        String query = "select * from generate_series(10)";
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, query, 1000)) {

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            var streamWrite = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null,
                    Channels.newChannel(byteArrayOutputStream));
            while (reader.loadNextBatch()) {
                streamWrite.writeBatch();
            }
            streamWrite.end();

            var request = authenticatedRequestBuilder(URI.create(baseUrl + "/v1/ingest?ingestion_queue=%s".formatted(path)))
                    .POST(HttpRequest.BodyPublishers.ofInputStream(() ->
                            new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
                    .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                    .build();

            var res = client.send(request, HttpResponse.BodyHandlers.ofString());

            // Should return 429 Too Many Requests
            assertEquals(429, res.statusCode(), "Expected 429 Too Many Requests when pending write limit exceeded");
            assertTrue(res.body().contains("Pending write limit exceeded"),
                    "Response body should contain error message about pending write limit");
        }
    }
}
