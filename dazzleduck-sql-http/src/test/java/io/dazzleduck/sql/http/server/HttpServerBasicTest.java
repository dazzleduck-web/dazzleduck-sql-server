package io.dazzleduck.sql.http.server;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.http.server.model.ContentTypes;
import io.dazzleduck.sql.http.server.model.QueryRequest;
import io.helidon.http.HeaderValues;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.dazzleduck.sql.common.Headers.*;
import static org.junit.jupiter.api.Assertions.*;

public class HttpServerBasicTest extends HttpServerTestBase {

    @BeforeAll
    public static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer();
        installArrowExtension();
    }

    @AfterAll
    public static void cleanup() throws Exception {
        cleanupWarehouse();
    }

    @Test
    public void testQueryWithPost() throws IOException, InterruptedException, SQLException {
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    @Test
    public void testQueryWithGet() throws IOException, InterruptedException, SQLException {
        var query = "select * from generate_series(10) order by 1";
        var urlEncode = URLEncoder.encode(query, StandardCharsets.UTF_8);
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query?q=%s".formatted(urlEncode)))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    @Test
    public void testSetWithGet() throws IOException, InterruptedException, SQLException {
        var query = "SET enable_progress_bar = true";
        var urlEncode = URLEncoder.encode(query, StandardCharsets.UTF_8);
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query?q=%s".formatted(urlEncode)))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, inputStreamResponse.statusCode());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            while (reader.loadNextBatch()) {
                System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
            }
        }
    }

    @Test
    public void testWithDuckDB() {
        String viewSql = "select * from read_arrow(concat('%s/v1/query?q=',url_encode('select 1')))".formatted(baseUrl);
        ConnectionPool.execute(viewSql);
    }

    @Test
    public void testLogin() throws IOException, InterruptedException {
        getJWTToken();
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "arrow"})
    public void testIngestionPostNoPartition(String format) throws IOException, InterruptedException, SQLException {
        var path = "abc";
        Files.createDirectories(Path.of(ingestionPath, "%s.%s".formatted(path, format)));
        String query = "select * from generate_series(10)";
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, query, 1000);
             var byteArrayOutputStream = new ByteArrayOutputStream();
             var streamWrite = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, byteArrayOutputStream)) {
            streamWrite.start();
            while (reader.loadNextBatch()) {
                streamWrite.writeBatch();
            }
            streamWrite.end();
            var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/ingest?ingestion_queue=%s.%s".formatted(path, format)))
                    .POST(HttpRequest.BodyPublishers.ofInputStream(() ->
                            new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
                    .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                    .header(HEADER_DATA_FORMAT, format)
                    .build();

            var res = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, res.statusCode());
            var testSql = String.format("select count(*) from read_%s('%s/%s.%s/*.%s')", format, ingestionPath, path, format, format);
            var lines = ConnectionPool.collectFirst(testSql, Long.class);
            assertEquals(11, lines);
        }
    }

    @Test
    public void testIngestionPost() throws IOException, InterruptedException, SQLException {
        String query = "select generate_series, generate_series a from generate_series(10)";
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, query, 1000);
             var byteArrayOutputStream = new ByteArrayOutputStream();
             var streamWrite = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, byteArrayOutputStream)) {
            streamWrite.start();
            while (reader.loadNextBatch()) {
                streamWrite.writeBatch();
            }
            streamWrite.end();
            var table = "table_single";

            Files.createDirectories(Path.of(ingestionPath, table));
            var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/ingest?ingestion_queue=%s".formatted(table)))
                    .POST(HttpRequest.BodyPublishers.ofInputStream(() ->
                            new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
                    .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                    .header(HEADER_DATA_PARTITION, urlEncode("a"))
                    .header(HEADER_DATA_PROJECT, urlEncode("*, (a + 1) as b"))
                    .header(HEADER_SORT_ORDER, urlEncode("b desc"))
                    .build();
            var res = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, res.statusCode());
            var testSql = "select generate_series, a, b from read_parquet('%s/%s/*/*.parquet')".formatted(ingestionPath, table);
            var expected = "select generate_series, generate_series a, (a+1) as b from generate_series(10) order by b desc";
            System.out.println(testSql);
            TestUtils.isEqual(expected, testSql);
        }
    }

    @Test
    public void testIngestionPostFromFile() throws SQLException, IOException, InterruptedException {
        var path = "file1";
        Files.createDirectories(Path.of(ingestionPath, path));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/ingest?ingestion_queue=%s".formatted(path)))
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> {
                    try {
                        return new FileInputStream("example/arrow_ipc/file1.arrow");
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                })).header("Content-Type", ContentTypes.APPLICATION_ARROW).build();
        var res = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        var testSql = String.format("select count(*) from read_parquet('%s/%s/*.parquet')", ingestionPath, path);
        var lines = ConnectionPool.collectFirst(testSql, Long.class);
        assertEquals(11, lines);
    }

    @Test
    public void writeIPC() throws IOException, SQLException {
        String filename = "/tmp/" + UUID.randomUUID() + ".arrow";
        String query = "select * from generate_series(10)";
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection connection = ConnectionPool.getConnection()) {
            try (var reader = ConnectionPool.getReader(connection, allocator, query, 1000);
                 var outputStream = new FileOutputStream(filename, false);
                 var streamWrite = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, outputStream)) {
                streamWrite.start();
                while (reader.loadNextBatch()) {
                    streamWrite.writeBatch();
                }
                streamWrite.end();
            }
            try (var reader = new ArrowStreamReader(new FileInputStream(filename), allocator)) {
                TestUtils.isEqual(query, allocator, reader);
            }
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testIngestionPostConcurrent() throws IOException, SQLException {
        final int totalRequests = 100;
        final int parallelism = 100;
        String query = "select generate_series, generate_series a from generate_series(10)";
        var ingestionPath = getIngestionPath();
        var path = "table";
        Files.createDirectories(Path.of( ingestionPath, path));
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, query, 1000);
             var byteArrayOutputStream = new ByteArrayOutputStream();
             var streamWrite = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, byteArrayOutputStream)) {
            streamWrite.start();
            while (reader.loadNextBatch()) {
                streamWrite.writeBatch();
            }
            streamWrite.end();
            var executor = Executors.newFixedThreadPool(parallelism);
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int i = 0; i < totalRequests; i++) {
                int final1 = i;
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/ingest?ingestion_queue=%s".formatted(path)))
                                .POST(HttpRequest.BodyPublishers.ofInputStream(() ->
                                        new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
                                .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                                .header(HEADER_DATA_PARTITION, urlEncode("a"))
                                .header(HEADER_DATA_PROJECT, urlEncode("*, a + " + final1 + " as b"))
                                .header(HEADER_SORT_ORDER, urlEncode("b desc"))
                                .build();

                        HttpResponse<String> res = client.send(request, HttpResponse.BodyHandlers.ofString());
                        assertEquals(200, res.statusCode());
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);
                futures.add(future);
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            executor.shutdown();
        }
    }

    private String getIngestionPath() {
        return "ingestion";
    }

    @Test
    public void testError() throws IOException, InterruptedException {
        var query = "select fr";
        var urlEncode = URLEncoder.encode(query, StandardCharsets.UTF_8);
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query?q=%s".formatted(urlEncode)))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(400, inputStreamResponse.statusCode());
        byte[] bytes;
        try (var b = inputStreamResponse.body()) {
            bytes = b.readAllBytes();
        }
        assertTrue(new String(bytes).contains("Error"));
    }

    @Test
    public void testInvalidCompressionHeader() throws Exception {
        var query = "SELECT 1";
        var urlEncode = URLEncoder.encode(query, StandardCharsets.UTF_8);
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query?q=%s".formatted(urlEncode)))
                .GET()
                .header(HEADER_ARROW_COMPRESSION, "invalid_codec")
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, response.statusCode());
        assertTrue(response.body().contains("Invalid Arrow compression codec specified"));
        assertTrue(response.body().contains("invalid_codec"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"zstd", "ZSTD", "zstandard", "ZSTANDARD", "ZsTd", "none", "NONE", "NoNe"})
    public void testValidCompressionHeaders(String compressionValue) throws Exception {
        var query = "SELECT 1";
        var urlEncode = URLEncoder.encode(query, StandardCharsets.UTF_8);
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query?q=%s".formatted(urlEncode)))
                .GET()
                .header(HEADER_ARROW_COMPRESSION, compressionValue)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, response.statusCode());
        // Verify we can read the Arrow stream
        try (var allocator = new RootAllocator();
             var inputStream = response.body();
             var reader = new ArrowStreamReader(inputStream, allocator)) {
            assertTrue(reader.loadNextBatch());
        }
    }
}
