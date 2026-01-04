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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.dazzleduck.sql.common.Headers.*;
import static org.junit.jupiter.api.Assertions.*;

public class HttpServerTest {
    static HttpClient client;
    static ObjectMapper objectMapper = new ObjectMapper();

    private static String warehousePath;

    public static final int TEST_PORT1 = 8090;
    public static final int TEST_PORT2 = 8091;
    public static final int TEST_PLANNING_PORT = 8092;

    private static final String LONG_RUNNING_QUERY = "with t as " +
            "(select len(split(concat('abcdefghijklmnopqrstuvwxyz:', generate_series), ':')) as len  from generate_series(1, 1000000000) )" +
            " select count(*) from t where len = 10";

    @BeforeAll
    public static void setup() throws Exception {
        warehousePath = "/tmp/" + UUID.randomUUID();
        new File(warehousePath).mkdir();
        String[] args1 = {"--conf", "dazzleduck_server.http.port=%s".formatted(TEST_PORT1),
                "--conf", "dazzleduck_server.%s=%s".formatted(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath),
                "--conf", "dazzleduck_server.ingestion.max_delay_ms=500"};
        Main.main(args1);
        client = HttpClient.newHttpClient();
        String[] args2 = {"--conf", "dazzleduck_server.http.port=%s".formatted(TEST_PORT2),
                "--conf", "dazzleduck_server.http.%s=jwt".formatted(ConfigUtils.AUTHENTICATION_KEY),
                "--conf", "dazzleduck_server.%s=%s".formatted(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath),
                "--conf", "dazzleduck_server.ingestion.max_delay_ms=500"};
        Main.main(args2);

        String[] args3 = {"--conf", "dazzleduck_server.http.port=%s".formatted(TEST_PLANNING_PORT),
                "--conf", "dazzleduck_server.%s=%s".formatted(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath),
                "--conf", "dazzleduck_server.ingestion.max_delay_ms=500",
                "--conf", "dazzleduck_server.access_mode=RESTRICTED"};
        Main.main(args3);
        String[] sqls = {"INSTALL arrow FROM community", "LOAD arrow"};
        ConnectionPool.executeBatch(sqls);
    }

    @AfterAll
    public static void cleanup() throws Exception {
        // Clean up warehouse directory
        if (warehousePath != null) {
            deleteDirectory(new File(warehousePath));
        }
    }

    private static void deleteDirectory(File directory) throws IOException {
        if (directory == null || !directory.exists()) {
            return;
        }

        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }

        if (!directory.delete()) {
            throw new IOException("Failed to delete: " + directory.getAbsolutePath());
        }
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
        ConnectionPool.execute("DROP SECRET http_auth");
    }

    @Test
    public void testLogin() throws IOException, InterruptedException {
        getJWTToken(TEST_PORT1);
    }

    public String getJWTToken(int port) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/login".formatted(port)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin", Map.of("org", "123")))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, inputStreamResponse.statusCode());
        var object = objectMapper.readValue(inputStreamResponse.body(), LoginResponse.class);
        return object.accessToken();
    }

    @ParameterizedTest
    @ValueSource(strings = { TestConstants.SUPPORTED_HIVE_PATH_QUERY, TestConstants.SUPPORTED_AGGREGATED_HIVE_PATH_QUERY})
    public void testPlanning(String query) throws IOException, InterruptedException {
        var loginResponse = login(TEST_PLANNING_PORT, new LoginRequest("admin", "admin",
                Map.of(HEADER_PATH, TestConstants.SUPPORTED_HIVE_PATH, HEADER_FUNCTION, "read_parquet")));
        String auth = loginResponse.tokenType() + " " + loginResponse.accessToken();
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/plan".formatted(TEST_PLANNING_PORT)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(inputStreamResponse.body());
        System.out.println(inputStreamResponse);
        var res = objectMapper.readValue(inputStreamResponse.body(), StatementHandle[].class);
        assertEquals(1, res.length);
    }

    @Test
    public void testPrintPlaning() throws SQLException, IOException, InterruptedException {
        var loginResponse = login(TEST_PLANNING_PORT, new LoginRequest("admin", "admin",
                Map.of(HEADER_PATH, TestConstants.SUPPORTED_HIVE_PATH, HEADER_FUNCTION, "read_parquet")));
        var secretQuery = """
                CREATE SECRET jwt_secret (
                    TYPE http,
                    BEARER_TOKEN '%s'
                )""".formatted(loginResponse.accessToken());
        try (Connection connection  = ConnectionPool.getConnection();
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ConnectionPool.execute(connection, secretQuery);
            var query = "%s where p='1'".formatted(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
            var request = "http://localhost:%s/plan?%s=1&q=".formatted(TEST_PLANNING_PORT, HEADER_SPLIT_SIZE);
            var toExecute = "SELECT splitSize FROM read_json(concat('%s', url_encode('%s')))".formatted(request, query.replaceAll("'", "''"));
            ConnectionPool.printResult(connection, allocator, toExecute);
            assertEquals(254, ConnectionPool.collectFirst(toExecute, Long.class));
            ConnectionPool.execute(connection, "DROP SECRET jwt_secret");
        }
    }


    @ParameterizedTest
    @ValueSource(strings = { TestConstants.SUPPORTED_HIVE_PATH_QUERY, TestConstants.SUPPORTED_AGGREGATED_HIVE_PATH_QUERY})
    public void testPlanningWithSmallPartition(String query) throws IOException, InterruptedException {
        var loginResponse = login(TEST_PLANNING_PORT, new LoginRequest("admin", "admin",
                Map.of(HEADER_PATH, TestConstants.SUPPORTED_HIVE_PATH, HEADER_FUNCTION, "read_parquet")));
        String auth = loginResponse.tokenType() + " " + loginResponse.accessToken();
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/plan".formatted(TEST_PLANNING_PORT)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HEADER_SPLIT_SIZE, "1")
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        var res = objectMapper.readValue(inputStreamResponse.body(), StatementHandle[].class);
        assertEquals(3, res.length);
    }

    @Test
    public void testPlanningWithFilter() throws IOException, InterruptedException {
        var loginResponse = login(TEST_PLANNING_PORT, new LoginRequest("admin", "admin",
                Map.of(HEADER_PATH, TestConstants.SUPPORTED_HIVE_PATH, HEADER_FUNCTION, "read_parquet")));
        String auth = loginResponse.tokenType() + " " + loginResponse.accessToken();
        var filter = "WHERE dt = '2025-01-01'";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(TestConstants.SUPPORTED_HIVE_PATH_QUERY + filter));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/plan".formatted(TEST_PLANNING_PORT)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HEADER_SPLIT_SIZE, "1")
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        var res = objectMapper.readValue(inputStreamResponse.body(), StatementHandle[].class);
        assertEquals(2, res.length);
    }

    @Test
    public void testPlanningWithError() throws IOException, InterruptedException {
        var loginResponse = login(TEST_PLANNING_PORT, new LoginRequest("admin", "admin",
                Map.of(HEADER_PATH, TestConstants.SUPPORTED_HIVE_PATH, HEADER_FUNCTION, "read_parquet")));
        String auth = loginResponse.tokenType() + " " + loginResponse.accessToken();
        var errorFilter = "WHEREdt = '2025-01-01'";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(TestConstants.SUPPORTED_HIVE_PATH_QUERY + errorFilter));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/plan".formatted(TEST_PLANNING_PORT)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HEADER_SPLIT_SIZE, "1")
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(500, inputStreamResponse.statusCode());
        assertNotNull(inputStreamResponse.body());
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "arrow"})
    public void testIngestionPostNoPartition(String format) throws IOException, InterruptedException, SQLException {
        var path = "abc";
        Files.createDirectories(Path.of(warehousePath, "%s.%s".formatted(path, format)));
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
            var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/ingest?path=%s.%s".formatted(TEST_PORT1, path, format)))
                    .POST(HttpRequest.BodyPublishers.ofInputStream(() ->
                            new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
                    .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                    .header(HEADER_DATA_FORMAT, format)
                    .build();

            var res = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, res.statusCode());
            var testSql = String.format("select count(*) from read_%s('%s/%s.%s/*.%s')", format, warehousePath, path, format, format);
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

            Files.createDirectories(Path.of(warehousePath, table));
            var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/ingest?path=%s".formatted(TEST_PORT1, table)))
                    .POST(HttpRequest.BodyPublishers.ofInputStream(() ->
                            new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
                    .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                    .header(HEADER_DATA_PARTITION, urlEncode("a"))
                    .header(HEADER_DATA_TRANSFORMATION, urlEncode("(a + 1) as b"))
                    .header(HEADER_SORT_ORDER, urlEncode("b desc"))
                    .build();
            var res = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, res.statusCode());
            var testSql = "select generate_series, a, b from read_parquet('%s/%s/*/*.parquet')".formatted(warehousePath, table);
            var expected = "select generate_series, generate_series a, (a+1) as b from generate_series(10) order by b desc";
            System.out.println(testSql);
            TestUtils.isEqual(expected, testSql);
        }
    }


    @Test
    public void testIngestionPostFromFile() throws SQLException, IOException, InterruptedException {
        var path = "file1";
        Files.createDirectories(Path.of(warehousePath, path));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/ingest?path=%s".formatted(TEST_PORT1, path)))
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> {
                    try {
                        return new FileInputStream("example/arrow_ipc/file1.arrow");
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                })).header("Content-Type", ContentTypes.APPLICATION_ARROW).build();
        var res = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        var testSql = String.format("select count(*) from read_parquet('%s/%s/*.parquet')", warehousePath, path);
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
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testIngestionPostConcurrent() throws IOException, SQLException {
        final int totalRequests = 100;
        final int parallelism = 100;
        String query = "select generate_series, generate_series a from generate_series(10)";
        var path = "table";
        Files.createDirectories(Path.of(warehousePath, path));
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
                        var request = HttpRequest.newBuilder(URI.create("http://localhost:%s/ingest?path=%s".formatted(TEST_PORT1, path)))
                                .POST(HttpRequest.BodyPublishers.ofInputStream(() ->
                                        new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
                                .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                                .header(HEADER_DATA_PARTITION, urlEncode("a"))
                                .header(HEADER_DATA_TRANSFORMATION, urlEncode("a + " + final1 + " as b"))
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

    private String urlEncode(String s){
        return URLEncoder.encode(s, Charset.defaultCharset());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCancelWithGet() throws Exception {
        var jwt = login(TEST_PORT2);
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
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCancelWithPost() throws Exception {
        var jwt = login(TEST_PORT2);
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
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Disabled("No way to slow down query processing - cancel happens too fast")
    public void testCancelWithPlanning() throws Exception {
        var jwt = login(TEST_PLANNING_PORT);
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
    @Test
    public void testIngestionWithJwt() throws Exception {
        // 1. Login to get JWT (your helper method)
        var jwt = login(TEST_PORT2);
        String auth = jwt.tokenType() + " " + jwt.accessToken();

        // 2. Build Arrow payload
        String query = "select * from generate_series(9)";
        byte[] arrowBytes;
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, query, 1000);
             var baos = new ByteArrayOutputStream();
             var writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, baos)) {

            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
            }
            writer.end();
            arrowBytes = baos.toByteArray();
        }


        var path = "secure";
        Files.createDirectories(Path.of(warehousePath, path));
        var authorizedReq = HttpRequest.newBuilder(
                        URI.create("http://localhost:%s/ingest?path=%s".formatted(TEST_PORT2, path)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(arrowBytes))
                .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .build();

        var authorizedResp = client.send(authorizedReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, authorizedResp.statusCode(),
                "Valid JWT token should allow ingestion");

        // 5. Verify file exists by reading parquet
        long count = ConnectionPool.collectFirst(
                "select count(*) from read_parquet('%s/%s/*.parquet')".formatted(warehousePath, path),
                Long.class);

        assertEquals(10, count);
    }


    private LoginResponse login(int port) throws IOException, InterruptedException {
        return login(port, new LoginRequest("admin", "admin"));
    }

    private LoginResponse login(int port, LoginRequest loginRequestPayload) throws IOException, InterruptedException {
        var loginRequest = HttpRequest.newBuilder(URI.create("http://localhost:%s/login".formatted(port)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(loginRequestPayload)))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();
        var jwtResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, jwtResponse.statusCode());
        return objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
    }
}
