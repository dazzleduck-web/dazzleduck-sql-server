package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestConstants;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.flight.server.StatementHandle;
import io.dazzleduck.sql.login.LoginRequest;
import io.dazzleduck.sql.login.LoginResponse;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

import static io.dazzleduck.sql.common.Headers.*;
import static org.junit.jupiter.api.Assertions.*;

public class HttpServerTest {
    static HttpClient client;
    static ObjectMapper objectMapper = new ObjectMapper();


    private static String warehousePath;

    public static final int TEST_PORT1 = 8090;
    public static final int TEST_PORT2 = 8091;

    private static final String LONG_RUNNING_QUERY = "with t as " +
            "(select len(split(concat('abcdefghijklmnopqrstuvwxyz:', generate_series), ':')) as len  from generate_series(1, 1000000000) )" +
            " select count(*) from t where len = 10";

    @BeforeAll
    public static void setup() throws Exception {
        warehousePath = "/tmp/" + UUID.randomUUID();
        new File(warehousePath).mkdir();
        String[] args1 = {"--conf", "dazzleduck_server.http.port=%s".formatted(TEST_PORT1),
                "--conf", "dazzleduck_server.%s=%s".formatted(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath)};
        Main.main(args1);
        client = HttpClient.newHttpClient();
        String[] args = {"--conf", "dazzleduck_server.http.port=%s".formatted(TEST_PORT2), "--conf", "dazzleduck_server.http.%s=jwt".formatted(ConfigUtils.AUTHENTICATION_KEY), "--conf", "dazzleduck_server.%s=%s".formatted(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath) };
        Main.main(args);
        String[] sqls = {"INSTALL arrow FROM community", "LOAD arrow"};
        ConnectionPool.executeBatch(sqls);
    }

    @Test
    public void testQueryWithPost() throws IOException, InterruptedException, SQLException {
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/query".formatted(TEST_PORT1)))
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
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/query?q=%s".formatted(TEST_PORT1, urlEncode)))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    @Test
    public void testSetWithGet() throws IOException, InterruptedException {
        var query = "SET enable_progress_bar = true;";
        var urlEncode = URLEncoder.encode(query, StandardCharsets.UTF_8);
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/query?q=%s".formatted(TEST_PORT1, urlEncode)))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, inputStreamResponse.statusCode());
    }


    @Test
    public void testQueryWithJwtExpectUnauthorized() throws IOException, InterruptedException {
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/query".formatted(TEST_PORT2)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(401, inputStreamResponse.statusCode());
    }

    @Test
    public void testQueryWithJwtExpect() throws IOException, InterruptedException, SQLException {
        var loginRequest = HttpRequest.newBuilder(URI.create("http://localhost:%s/login".formatted(TEST_PORT2)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin"))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var jwtResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, jwtResponse.statusCode());
        var jwt = objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/query".formatted(TEST_PORT2)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), jwt.tokenType() + " " + jwt.accessToken())
                .build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    @Test
    public void testWithDuckDB() {
        String viewSql = "select * from read_arrow(concat('http://localhost:%s/query?q=',url_encode('select 1')))".formatted(TEST_PORT1);
        ConnectionPool.execute(viewSql);
    }

    @Test
    public void testWithDuckDBAuthorized() throws IOException, InterruptedException {
        var loginRequest = HttpRequest.newBuilder(URI.create("http://localhost:%s/login".formatted(TEST_PORT2)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin"))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var jwtResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        var jwt = objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
        var httpAuthSql = "CREATE SECRET http_auth (\n" +
                "    TYPE http,\n" +
                "    EXTRA_HTTP_HEADERS MAP {\n" +
                "        'Authorization': '"+ jwt.tokenType() + " " + jwt.accessToken() +"'\n"+
                "    }\n" +
                ")";

        String viewSql = "select * from read_arrow(concat('http://localhost:%s/query?q=',url_encode('select 1')))".formatted(TEST_PORT2);
        String[] sqls = {"INSTALL arrow FROM community", "LOAD arrow"};
        ConnectionPool.executeBatch(sqls);
        ConnectionPool.execute(httpAuthSql);
        ConnectionPool.execute(viewSql);
    }

    @Test
    public void testLogin() throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/login".formatted(TEST_PORT1)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin", Map.of("org", "123")))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, inputStreamResponse.statusCode());
        System.out.println(inputStreamResponse.body());
    }

    @ParameterizedTest
    @ValueSource(strings = { TestConstants.SUPPORTED_HIVE_PATH_QUERY, TestConstants.SUPPORTED_AGGREGATED_HIVE_PATH_QUERY})
    public void testPlanning(String query) throws IOException, InterruptedException {
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/plan".formatted(TEST_PORT1)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        var res = objectMapper.readValue(inputStreamResponse.body(), StatementHandle[].class);
        assertEquals(1, res.length);
    }

    @Test
    public void testPrintPlaning() throws SQLException {
        var query = "%s where p='1'".formatted(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
        var request = "http://localhost:%s/plan?%s=1&q=".formatted(TEST_PORT1, HEADER_SPLIT_SIZE);
        var toExecute = "SELECT splitSize FROM read_json(concat('%s', url_encode('%s')))".formatted(request, query.replaceAll("'", "''"));
        ConnectionPool.printResult(toExecute);
        assertEquals(254, ConnectionPool.collectFirst(toExecute, Long.class));
    }


    @ParameterizedTest
    @ValueSource(strings = { TestConstants.SUPPORTED_HIVE_PATH_QUERY, TestConstants.SUPPORTED_AGGREGATED_HIVE_PATH_QUERY})
    public void testPlanningWithSmallPartition(String query) throws IOException, InterruptedException {
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/plan".formatted(TEST_PORT1)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HEADER_SPLIT_SIZE, "1")
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        var res = objectMapper.readValue(inputStreamResponse.body(), StatementHandle[].class);
        assertEquals(3, res.length);
    }

    @Test
    public void testPlanningWithFilter() throws IOException, InterruptedException {
        var filter = "WHERE dt = '2025-01-01'";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(TestConstants.SUPPORTED_HIVE_PATH_QUERY + filter));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/plan".formatted(TEST_PORT1)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HEADER_SPLIT_SIZE, "1")
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        var res = objectMapper.readValue(inputStreamResponse.body(), StatementHandle[].class);
        assertEquals(2, res.length);
    }

    @Test
    public void testPlanningWithError() throws IOException, InterruptedException {
        var errorFilter = "WHEREdt = '2025-01-01'";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(TestConstants.SUPPORTED_HIVE_PATH_QUERY + errorFilter));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/plan".formatted(TEST_PORT1)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HEADER_SPLIT_SIZE, "1")
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(500, inputStreamResponse.statusCode());
        assertNotNull(inputStreamResponse.body());
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "arrow"})
    public void testIngestionPostNoPartition(String format) throws IOException, InterruptedException, SQLException {
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
            var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/ingest?path=abc.%s".formatted(TEST_PORT1, format)))
                    .POST(HttpRequest.BodyPublishers.ofInputStream(() ->
                            new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
                    .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                    .header(HEADER_DATA_FORMAT, format)
                    .build();

            var res = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, res.statusCode());
            var testSql = String.format("select count(*) from read_%s('%s/abc.%s')", format, warehousePath, format);
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
            var table = "table-single";
            var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/ingest?path=%s".formatted(TEST_PORT1, table)))
                    .POST(HttpRequest.BodyPublishers.ofInputStream(() ->
                            new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
                    .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                    .header(HEADER_DATA_PARTITION, "a")
                    .header(HEADER_DATA_TRANSFORMATION, "(a + 1) as b")
                    .header(HEADER_SORT_ORDER, "b desc")
                    .build();
            var res = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, res.statusCode());
            var testSql = "select generate_series, a, b from read_parquet('%s/%s/*/*.parquet')".formatted(warehousePath, table);
            var expected = "select generate_series, generate_series a, (a+1) as b from generate_series(10) order by b desc";
            TestUtils.isEqual(expected, testSql);
        }
    }


    @Test
    public void testIngestionPostFromFile() throws SQLException, IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/ingest?path=file1.parquet".formatted(TEST_PORT1)))
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
    public void writeIPC() throws IOException, SQLException {
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
            try (var reader = new ArrowStreamReader(new FileInputStream(filename), allocator)) {
                TestUtils.isEqual(query, allocator, reader);
            }
        }
    }

    @Test
    public void testIngestionPostConcurrent() throws IOException, SQLException {
        final int totalRequests = 100;
        final int parallelism = 100;
        String query = "select generate_series, generate_series a from generate_series(10)";
        // Prepare Arrow payload once
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
            // Executor for parallel requests
            var executor = Executors.newFixedThreadPool(parallelism);
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int i = 0; i < totalRequests; i++) {
                int final1 = i;
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/ingest?path=table".formatted(TEST_PORT1)))
                                .POST(HttpRequest.BodyPublishers.ofInputStream(() ->
                                        new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
                                .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                                .header(HEADER_DATA_PARTITION, "a")
                                .header(HEADER_DATA_TRANSFORMATION, "a + " + final1 + " as b")
                                .header(HEADER_SORT_ORDER, "b desc")
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

    @Test
    public void testCancelWithGet() throws Exception {
        var jwt = login();
        String auth = jwt.tokenType() + " " + jwt.accessToken();

        String q = URLEncoder.encode(LONG_RUNNING_QUERY, StandardCharsets.UTF_8);
        var query = HttpRequest.newBuilder(URI.create("http://localhost:%s/query?q=%s&id=%s".formatted(TEST_PORT2, q, 11L)))
                .GET().header("Accept", HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth).build();

        var queryFuture = client.sendAsync(query, HttpResponse.BodyHandlers.ofString());
        var cancelFuture = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(500);
                var body = objectMapper.writeValueAsBytes(new QueryRequest(LONG_RUNNING_QUERY, 11L));
                var cancel = HttpRequest.newBuilder(URI.create("http://localhost:%s/cancel".formatted(TEST_PORT2)))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                        .header("Accept", HeaderValues.ACCEPT_JSON.values())
                        .header("Content-Type", "application/json")
                        .header(HeaderNames.AUTHORIZATION.defaultCase(), auth).build();
                return client.send(cancel, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        var cancelResp = cancelFuture.get(5, TimeUnit.SECONDS);
        assertTrue(Set.of(200, 202, 409).contains(cancelResp.statusCode()));

        var queryResp = queryFuture.get(15, TimeUnit.SECONDS);
        var body = queryResp.body().toLowerCase();
        assertTrue(body.contains("cancel") || body.contains("interrupted") || queryResp.statusCode() != 200);
    }


    @Test
    public void testCancelWithPost() throws Exception {
        var jwt = login();
        String auth = jwt.tokenType() + " " + jwt.accessToken();

        var body = objectMapper.writeValueAsBytes(new QueryRequest(LONG_RUNNING_QUERY, 12L));
        var query = HttpRequest.newBuilder(URI.create("http://localhost:%s/query".formatted(TEST_PORT2)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Accept", HeaderValues.ACCEPT_JSON.values())
                .header("Content-Type", "application/json")
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth).build();

        var queryFuture = client.sendAsync(query, HttpResponse.BodyHandlers.ofString());
        var cancelFuture = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(500);
                var cancelBody = objectMapper.writeValueAsBytes(new QueryRequest(LONG_RUNNING_QUERY, 12L));
                var cancel = HttpRequest.newBuilder(URI.create("http://localhost:%s/cancel".formatted(TEST_PORT2)))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(cancelBody))
                        .header("Accept", HeaderValues.ACCEPT_JSON.values())
                        .header("Content-Type", "application/json")
                        .header(HeaderNames.AUTHORIZATION.defaultCase(), auth).build();
                return client.send(cancel, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        var cancelResp = cancelFuture.get(5, TimeUnit.SECONDS);
        assertTrue(Set.of(200, 202, 409).contains(cancelResp.statusCode()));

        var queryResp = queryFuture.get(15, TimeUnit.SECONDS);
        var respBody = queryResp.body().toLowerCase();
        assertTrue(respBody.contains("cancel") || respBody.contains("interrupted") || queryResp.statusCode() != 200);
    }

    @Test
    @Disabled
    /**
     * Disabled because as of now there is no way to slow the down the processing of the query
     * By the time cancel is requested the query is already completed
     */
    public void testCancelWithPlanning() throws Exception {
        var jwt = login();
        String auth = jwt.tokenType() + " " + jwt.accessToken();
        var planBody = objectMapper.writeValueAsBytes(new QueryRequest(TestConstants.SUPPORTED_HIVE_PATH_QUERY));
        var planReq = HttpRequest.newBuilder(URI.create("http://localhost:%s/plan".formatted(TEST_PORT2)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(planBody))
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .header("Content-Type", "application/json")
                .build();
        var planResp = client.send(planReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, planResp.statusCode());
        var handles = objectMapper.readValue(planResp.body(), StatementHandle[].class);
        assertNotNull(handles);
        assertTrue(handles.length > 0);

        String plannedQuery = handles[0].query();
        long id = handles[0].queryId();

        String qEnc = URLEncoder.encode(plannedQuery, StandardCharsets.UTF_8);
        var queryReq = HttpRequest.newBuilder(URI.create("http://localhost:%s/query?q=%s&id=%s".formatted(TEST_PORT2, qEnc, id)))
                .GET()
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .header("Accept", HeaderValues.ACCEPT_JSON.values())
                .build();
        client.sendAsync(queryReq, HttpResponse.BodyHandlers.ofString());
        var cancelFuture = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(2000);
                var cancelBody = objectMapper.writeValueAsBytes(new QueryRequest(plannedQuery, id));
                var cancelReq = HttpRequest.newBuilder(URI.create("http://localhost:%s/cancel".formatted(TEST_PORT2)))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(cancelBody))
                        .header("Content-Type", "application/json")
                        .header("Accept", HeaderValues.ACCEPT_JSON.values())
                        .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                        .build();
                return client.send(cancelReq, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        var cancelResp = cancelFuture.get(5, TimeUnit.SECONDS);
        assertTrue(Set.of(200, 202, 409).contains(cancelResp.statusCode()), "unexpected cancel status");
    }

    private LoginResponse login() throws IOException, InterruptedException {
        var loginRequest = HttpRequest.newBuilder(URI.create("http://localhost:%s/login".formatted(TEST_PORT2)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin"))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();
        var jwtResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, jwtResponse.statusCode());
        return objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
    }
}
