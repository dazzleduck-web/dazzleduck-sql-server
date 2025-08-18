package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HttpServerTest {
    static HttpClient client;
    static ObjectMapper objectMapper = new ObjectMapper();

    private static String warehousePath;

    @BeforeAll
    public static void setup() throws NoSuchAlgorithmException {
        warehousePath = "/tmp/" + UUID.randomUUID();
        new File(warehousePath).mkdir();
        String[] args1 = {"--conf", "port=8080",   "--conf", "warehousePath=" + warehousePath };
        Main.main(args1);
        client = HttpClient.newHttpClient();
        String[] args = {"--conf", "port=8081", "--conf", "auth=jwt", "--conf", "warehousePath=" + warehousePath };
        Main.main(args);
        String[] sqls = {"INSTALL arrow FROM community", "LOAD arrow"};
        ConnectionPool.executeBatch(sqls);
    }

    @Test
    public void testQueryWithPost() throws IOException, InterruptedException, SQLException {
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryObject(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8080/query"))
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
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8080/query?q=" + urlEncode))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    @Test
    public void testQueryWithJwtExpectUnauthorized() throws IOException, InterruptedException {
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryObject(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8081/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(401, inputStreamResponse.statusCode());
    }

    @Test
    public void testQueryWithJwtExpect() throws IOException, InterruptedException, SQLException {
        var loginRequest = HttpRequest.newBuilder(URI.create("http://localhost:8081/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginObject("admin", "admin"))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var jwtResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, jwtResponse.statusCode());
        var jwt = jwtResponse.body();
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryObject(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8081/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), "Bearer " + jwt)
                .build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    @Test
    public void testWithDuckDB() {
        String viewSql = "select * from read_arrow(concat('http://localhost:8080/query?q=',url_encode('select 1')))";

        ConnectionPool.execute(viewSql);
    }

    @Test
    public void testWithDuckDBAuthorized() throws IOException, InterruptedException {
        var loginRequest = HttpRequest.newBuilder(URI.create("http://localhost:8081/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginObject("admin", "admin"))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var jwtResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        var httpAuthSql = "CREATE SECRET http_auth (\n" +
                "    TYPE http,\n" +
                "    EXTRA_HTTP_HEADERS MAP {\n" +
                "        'Authorization': 'Bearer " + jwtResponse.body() + "'\n" +
                "    }\n" +
                ")";

        String viewSql = "select * from read_arrow(concat('http://localhost:8081/query?q=',url_encode('select 1')))";
        String[] sqls = {"INSTALL arrow FROM community", "LOAD arrow"};
        ConnectionPool.executeBatch(sqls);
        ConnectionPool.execute(httpAuthSql);
        ConnectionPool.execute(viewSql);
    }

    @Test
    public void testLogin() throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8080/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginObject("admin", "admin", 123L))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, inputStreamResponse.statusCode());
        System.out.println(inputStreamResponse.body());
    }

    @Test
    public void testPlanning() throws IOException, InterruptedException {
        String query = "select count(*) from read_parquet('example/hive_table/*/*/*.parquet', hive_types = {'dt': DATE, 'p': VARCHAR})";
        var body = objectMapper.writeValueAsBytes(new QueryObject(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8080/plan"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        var res = objectMapper.readValue(inputStreamResponse.body(), Split[].class);
        assertEquals(1, res.length);
    }

    @Test
    public void testIngestion() throws IOException, InterruptedException, SQLException {
        String query = "select * from generate_series(10)";
        try(BufferAllocator allocator = new RootAllocator();
            DuckDBConnection connection = ConnectionPool.getConnection();
            var reader = ConnectionPool.getReader( connection, allocator, query, 1000 );
            var byteArrayOutputStream = new ByteArrayOutputStream();
            var streamWrite = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, byteArrayOutputStream)) {
            streamWrite.start();
            while (reader.loadNextBatch()) {
                streamWrite.writeBatch();
            }
            streamWrite.end();
            var request = HttpRequest.newBuilder(URI.create("http://localhost:8080/ingest?path=abc.parquet"))
                    .POST(HttpRequest.BodyPublishers.ofInputStream(() ->
                            new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
                    .header("Content-Type", ContentTypes.APPLICATION_ARROW).build();
            var res = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, res.statusCode());
            var testSql = String.format("select count(*) from read_parquet('%s/abc.parquet')", warehousePath);
            var lines = ConnectionPool.collectFirst(testSql, Long.class);
            assertEquals(11, lines);
        }
    }

    @Test
    public void testIngestionFromFile() throws SQLException, IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8080/ingest?path=file1.parquet"))
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> {
                    try {
                        return new FileInputStream("example/arrow_ipc/file1.arrow");
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                })).header("Content-Type", ContentTypes.APPLICATION_ARROW).build();
        var res = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        var testSql = String.format("select count(*) from read_parquet('%s/file1.parquet')", warehousePath);
        var lines = ConnectionPool.collectFirst(testSql, Long.class);
        assertEquals(11, lines);
    }

    @Test
    public void writeIPC() throws IOException, InterruptedException, SQLException {
        String filename = "/tmp/" + UUID.randomUUID() + ".arrow";
        String query = "select * from generate_series(10)";
        try(BufferAllocator allocator = new RootAllocator();
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
            try (var reader = new ArrowStreamReader(new FileInputStream(filename), allocator);
                 final ArrowArrayStream arrow_array_stream = ArrowArrayStream.allocateNew(allocator)) {
                Data.exportArrayStream(allocator, reader, arrow_array_stream);
                connection.registerArrowStream("test", arrow_array_stream);
                ConnectionPool.printResult(connection, allocator, "select * from test");
            }
        }
    }
}