package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.http.server.model.QueryRequest;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A large Arrow result must reach the client intact. Uncompressed, a wide result is written as
 * large batches, fast, which is where its chunked HTTP/1.1 framing used to come apart: the client
 * found body bytes where a chunk size belonged and dropped the connection.
 */
public class HttpServerLargeArrowTest extends HttpServerTestBase {

    private static final int ROWS = 50_000;
    // 25 VARCHAR columns of ~205 bytes: ~256 MB uncompressed.
    private static final String WIDE_QUERY = "SELECT " + IntStream.range(0, 25)
            .mapToObj(c -> "repeat('x', 200) || i::VARCHAR AS c" + c)
            .collect(Collectors.joining(", ")) + " FROM range(" + ROWS + ") t(i)";

    @BeforeAll
    static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer();
    }

    @AfterAll
    static void cleanup() throws Exception {
        cleanupWarehouse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "zstd"})
    public void wideResultArrivesIntactOverHttp1(String compression) throws Exception {
        var http1 = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        var token = getJWTToken();
        for (int attempt = 0; attempt < 5; attempt++) {
            var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query"))
                    .header("Authorization", "Bearer " + token)
                    .header(Headers.HEADER_ARROW_COMPRESSION, compression)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(
                            objectMapper.writeValueAsBytes(new QueryRequest(WIDE_QUERY))))
                    .build();
            var response = http1.send(request, HttpResponse.BodyHandlers.ofInputStream());
            assertEquals(200, response.statusCode());
            assertEquals(ROWS, countRows(response.body()), "attempt " + attempt);
        }
    }

    private static long countRows(InputStream body) throws Exception {
        try (body;
             var allocator = new RootAllocator();
             var reader = new ArrowStreamReader(body, allocator)) {
            long rows = 0;
            while (reader.loadNextBatch()) {
                rows += reader.getVectorSchemaRoot().getRowCount();
            }
            return rows;
        }
    }
}
