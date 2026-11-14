package io.dazzleduck.sql.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.flight.stream.ArrowStreamReaderWrapper;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end load test for DuckLakePostIngestionTask backed by a real PostgreSQL catalog.
 *
 * Sends 10,000 rows (5,000 via HTTP POST, 5,000 via Flight executeIngest) in batches of 10
 * rows each, and verifies all rows land in the DuckLake table.
 *
 * HTTP uses raw java.net.http.HttpClient with JWT auth.
 * Flight uses FlightSqlClient.executeIngest() via ArrowStreamReaderWrapper.
 * Arrow data is generated from DuckDB generate_series queries.
 */
@org.junit.jupiter.api.Tag("slow")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DuckLakePostgresLoadTest {

    private static final String QUEUE_ID     = "logs_queue";
    private static final String CATALOG      = "pg_lake";
    private static final String SCHEMA       = "main";
    private static final String TABLE        = "logs";
    private static final int    ROWS_PER_BATCH  = 10;
    private static final int    TOTAL_ROWS      = 4_000;
    private static final int    HTTP_BATCHES    = TOTAL_ROWS / 2 / ROWS_PER_BATCH;   // 200
    private static final int    FLIGHT_BATCHES  = TOTAL_ROWS / 2 / ROWS_PER_BATCH;   // 200
    private static final int    CONCURRENCY     = 10; // parallel senders per protocol
    private static final int    READ_COUNT      = (HTTP_BATCHES + FLIGHT_BATCHES) / 5; // 80 reads per reader thread

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PostgreSQLContainer postgres;
    private Runtime runtime;
    private BufferAllocator allocator;
    private int httpPort;
    private int flightPort;
    private Path tempDir;
    private HttpClient httpClient;

    @BeforeAll
    void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-pg-load-test");

        postgres = new PostgreSQLContainer("postgres:15-alpine")
                .withDatabaseName("ducklake")
                .withUsername("duck")
                .withPassword("duck");
        postgres.start();

        allocator  = new RootAllocator(Long.MAX_VALUE);
        httpClient = HttpClient.newHttpClient();

        // Install extensions and set up the Postgres-backed DuckLake catalog.
        // This must happen before Runtime.start() so DuckLakeIngestionHandler can
        // query catalog metadata (paths, partition columns) during producer startup.
        ConnectionPool.executeBatch(new String[]{
                "INSTALL ducklake",
                "LOAD ducklake",
                "INSTALL arrow FROM community",
                "LOAD arrow"
        });

        // DuckLake's PostgreSQL backend uses libpq key=value connection string format.
        // METADATA_PATH ':memory:' prevents DuckLake from creating a local DuckDB file
        // named after the libpq connection string in the test's working directory.
        String pgUrl = "host=%s port=%d dbname=ducklake user=duck password=duck".formatted(
                postgres.getHost(), postgres.getFirstMappedPort());
        Path dataPath = tempDir.resolve("data");
        Files.createDirectories(dataPath.resolve(SCHEMA).resolve(TABLE));

        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s', METADATA_PATH ':memory:')".formatted(pgUrl, CATALOG, dataPath),
                    ("CREATE TABLE %s.%s.%s (" +
                            "ts TIMESTAMP, level VARCHAR, service VARCHAR, host VARCHAR, " +
                            "message VARCHAR, trace_id VARCHAR, duration_ms BIGINT, status_code INT)")
                            .formatted(CATALOG, SCHEMA, TABLE)
            });
        }

        httpPort   = findAvailablePort();
        flightPort = findAvailablePort();
        String warehousePath = Files.createTempDirectory(tempDir, "warehouse").toString();

        Config config = ConfigFactory.load().getConfig("dazzleduck_server")
                .withValue("networking_modes",
                        ConfigValueFactory.fromIterable(List.of("http", "flight-sql")))
                .withValue("http.port",    ConfigValueFactory.fromAnyRef(httpPort))
                .withValue("flight_sql.port", ConfigValueFactory.fromAnyRef(flightPort))
                .withValue("flight_sql.host", ConfigValueFactory.fromAnyRef("localhost"))
                .withValue("flight_sql.use_encryption", ConfigValueFactory.fromAnyRef(false))
                .withValue("flight_sql.data_processor_locations", ConfigValueFactory.fromIterable(
                        List.of(ConfigValueFactory.fromMap(
                                Map.of("host", "localhost", "port", flightPort, "use_encryption", false)
                        ).unwrapped())))
                .withValue("warehouse", ConfigValueFactory.fromAnyRef(warehousePath))
                .withValue("http.auth", ConfigValueFactory.fromAnyRef("jwt"))
                .withValue("ingestion.max_delay_ms", ConfigValueFactory.fromAnyRef(250))
                .withValue("ingestion_task_factory_provider.class",
                        ConfigValueFactory.fromAnyRef(
                                "io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionTaskFactoryProvider"))
                .withValue("ingestion_task_factory_provider.ingestion_queue_table_mapping",
                        ConfigValueFactory.fromIterable(List.of(
                                ConfigValueFactory.fromMap(Map.of(
                                        "ingestion_queue", QUEUE_ID,
                                        "catalog", CATALOG,
                                        "schema", SCHEMA,
                                        "table", TABLE
                                )).unwrapped())));

        runtime = Runtime.start(config);
        waitForServer("localhost", httpPort,   10_000);
        waitForServer("localhost", flightPort, 10_000);
    }

    @AfterAll
    void teardown() throws Exception {
        if (runtime != null) runtime.close();
        if (allocator != null) allocator.close();
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        } catch (Exception ignored) {}
        if (postgres != null && postgres.isRunning()) postgres.stop();
    }

    // -------------------------------------------------------------------------

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @Order(0)
    void httpAndFlightIngestionLandsInDuckLake() throws Exception {
        // Pre-generate Arrow IPC byte arrays once; avoids holding DuckDB connections
        // while the server is simultaneously writing Parquet + post-ingestion tasks.
        List<byte[]> batches = preGenerateBatches(HTTP_BATCHES + FLIGHT_BATCHES);

        var httpSent          = new LongAdder();
        var flightSent        = new LongAdder();
        var httpWriteLatencies   = new ConcurrentLinkedQueue<Long>();
        var flightWriteLatencies = new ConcurrentLinkedQueue<Long>();
        var httpReadLatencies    = new ConcurrentLinkedQueue<Long>();
        var flightReadLatencies  = new ConcurrentLinkedQueue<Long>();
        var start = Instant.now();

        String jwt = acquireJwt();
        String ingestUrl = "http://localhost:%d/v1/ingest?%s=%s"
                .formatted(httpPort, Headers.QUERY_PARAMETER_INGESTION_QUEUE, QUEUE_ID);

        ExecutorService httpPool   = Executors.newFixedThreadPool(CONCURRENCY);
        ExecutorService flightPool = Executors.newFixedThreadPool(CONCURRENCY);

        // One shared FlightSqlClient — the underlying gRPC channel is safe for concurrent streams.
        Location location = Location.forGrpcInsecure("localhost", flightPort);
        FlightSqlClient flightClient = new FlightSqlClient(
                FlightClient.builder(allocator, location)
                        .intercept(AuthUtils.createClientMiddlewareFactory("admin", "admin", Map.of()))
                        .build());

        var tableDefOptions = FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder().build();
        var ingestOptions = new FlightSqlClient.ExecuteIngestOptions(
                "", tableDefOptions, false, "", "",
                Map.of(Headers.QUERY_PARAMETER_INGESTION_QUEUE, QUEUE_ID));

        // HTTP: 500 batches × 10 rows = 5,000 rows, CONCURRENCY in-flight at a time
        var httpFutures = new ArrayList<CompletableFuture<Void>>(HTTP_BATCHES);
        for (int i = 0; i < HTTP_BATCHES; i++) {
            final int idx = i;
            httpFutures.add(CompletableFuture.runAsync(() -> {
                try {
                    long t0 = System.nanoTime();
                    postArrowBatch(ingestUrl, jwt, batches.get(idx));
                    httpWriteLatencies.add((System.nanoTime() - t0) / 1_000_000);
                    httpSent.add(ROWS_PER_BATCH);
                } catch (Exception e) {
                    throw new RuntimeException("HTTP batch " + idx + " failed", e);
                }
            }, httpPool));
        }

        // Flight: 500 batches × 10 rows = 5,000 rows, CONCURRENCY in-flight at a time
        var flightFutures = new ArrayList<CompletableFuture<Void>>(FLIGHT_BATCHES);
        for (int i = 0; i < FLIGHT_BATCHES; i++) {
            final int idx = HTTP_BATCHES + i;
            flightFutures.add(CompletableFuture.runAsync(() -> {
                try (var reader = arrowReaderFromBytes(batches.get(idx), allocator)) {
                    long t0 = System.nanoTime();
                    flightClient.executeIngest(new ArrowStreamReaderWrapper(reader, allocator), ingestOptions);
                    flightWriteLatencies.add((System.nanoTime() - t0) / 1_000_000);
                    flightSent.add(ROWS_PER_BATCH);
                } catch (Exception e) {
                    throw new RuntimeException("Flight batch " + idx + " failed", e);
                }
            }, flightPool));
        }

        // Readers stop as soon as ingestion is complete — prevents 200×500ms sleep from
        // dominating total test time (writers finish in ~13s; readers would otherwise run ~100s).
        AtomicBoolean ingestionDone = new AtomicBoolean(false);

        // HTTP COUNT reader: queries SELECT COUNT(*) via HTTP while writers are running
        String countQuery = URLEncoder.encode(
                "SELECT COUNT(*) AS cnt FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE),
                StandardCharsets.UTF_8);
        String queryUrl = "http://localhost:%d/v1/query?q=%s".formatted(httpPort, countQuery);
        var httpReaderCounts = new LongAdder();
        var httpReaderFuture = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < READ_COUNT && !ingestionDone.get(); i++) {
                try {
                    long t0 = System.nanoTime();
                    long cnt = httpCountQuery(queryUrl, jwt);
                    httpReadLatencies.add((System.nanoTime() - t0) / 1_000_000);
                    if (cnt > 0) httpReaderCounts.add(cnt);
                    Thread.sleep(500);
                } catch (Exception e) {
                    // transient errors during concurrent load are acceptable
                }
            }
        });

        // Flight JDBC COUNT reader: queries SELECT COUNT(*) via Flight SQL JDBC while writers run
        String jdbcUrl = "jdbc:arrow-flight-sql://localhost:%d?database=memory&useEncryption=0&user=admin&password=admin"
                .formatted(flightPort);
        var flightReaderCounts = new LongAdder();
        var flightReaderFuture = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < READ_COUNT && !ingestionDone.get(); i++) {
                try (java.sql.Connection jdbcConn = DriverManager.getConnection(jdbcUrl);
                     java.sql.Statement stmt = jdbcConn.createStatement()) {
                    long t0 = System.nanoTime();
                    try (java.sql.ResultSet rs = stmt.executeQuery(
                            "SELECT COUNT(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE))) {
                        flightReadLatencies.add((System.nanoTime() - t0) / 1_000_000);
                        if (rs.next()) {
                            long cnt = rs.getLong(1);
                            if (cnt > 0) flightReaderCounts.add(cnt);
                        }
                    }
                    Thread.sleep(500);
                } catch (Exception e) {
                    // transient errors during concurrent load are acceptable
                }
            }
        });

        CompletableFuture.allOf(httpFutures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
        CompletableFuture.allOf(flightFutures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
        ingestionDone.set(true);
        CompletableFuture.allOf(httpReaderFuture, flightReaderFuture).get(10, TimeUnit.SECONDS);
        flightClient.close();
        httpPool.shutdown();
        flightPool.shutdown();

        // Poll until all async DuckLakePostIngestionTask calls have committed.
        long finalCount = pollForCount(TOTAL_ROWS, Duration.ofSeconds(60));
        Duration duration = Duration.between(start, Instant.now());

        printResults(httpSent.sum(), flightSent.sum(), finalCount,
                httpReaderCounts.sum(), flightReaderCounts.sum(), duration,
                httpWriteLatencies, flightWriteLatencies, httpReadLatencies, flightReadLatencies);
        assertEquals(TOTAL_ROWS, finalCount,
                "Expected %d rows in DuckLake but found %d".formatted(TOTAL_ROWS, finalCount));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Pre-generate {@code count} Arrow IPC byte arrays, each with {@value ROWS_PER_BATCH} rows. */
    private List<byte[]> preGenerateBatches(int count) throws Exception {
        var result = new ArrayList<byte[]>(count);
        for (int b = 0; b < count; b++) {
            int offset = b * ROWS_PER_BATCH;
            String sql = logBatchSql(offset, ROWS_PER_BATCH);
            try (DuckDBConnection conn = ConnectionPool.getConnection();
                 var reader = ConnectionPool.getReader(conn, allocator, sql, ROWS_PER_BATCH);
                 ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ArrowStreamWriter writer = new ArrowStreamWriter(
                         reader.getVectorSchemaRoot(), null, Channels.newChannel(baos))) {
                writer.start();
                while (reader.loadNextBatch()) {
                    writer.writeBatch();
                }
                writer.end();
                result.add(baos.toByteArray());
            }
        }
        return result;
    }

    /** SQL that produces {@code rows} log rows starting at {@code offset}. */
    private static String logBatchSql(int offset, int rows) {
        int end = offset + rows - 1;
        return """
                SELECT
                    now()::TIMESTAMP AS ts,
                    CASE (i %% 4) WHEN 0 THEN 'INFO' WHEN 1 THEN 'DEBUG' WHEN 2 THEN 'WARN' ELSE 'ERROR' END AS level,
                    CASE (i %% 4) WHEN 0 THEN 'api-gateway' WHEN 1 THEN 'user-service'
                                  WHEN 2 THEN 'order-service' ELSE 'payment-service' END AS service,
                    'host-' || lpad(((i %% 8) + 1)::VARCHAR, 3, '0') AS host,
                    'request-' || i AS message,
                    printf('%%016x%%016x', i, i * 31) AS trace_id,
                    (i * 7 %% 1000)::BIGINT AS duration_ms,
                    CASE (i %% 5) WHEN 0 THEN 500 ELSE 200 END AS status_code
                FROM generate_series(%d, %d) t(i)
                """.formatted(offset, end);
    }

    /** Wrap raw Arrow IPC bytes in a DuckDB-backed ArrowReader for Flight executeIngest. */
    private org.apache.arrow.vector.ipc.ArrowReader arrowReaderFromBytes(byte[] bytes, BufferAllocator alloc) throws IOException {
        return new org.apache.arrow.vector.ipc.ArrowStreamReader(
                new java.io.ByteArrayInputStream(bytes), alloc);
    }

    /** Execute a COUNT query via HTTP and return the count value. */
    private long httpCountQuery(String url, String jwt) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create(url))
                .header("Authorization", "Bearer " + jwt)
                .GET()
                .timeout(Duration.ofSeconds(10))
                .build();
        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream body = response.body();
             ArrowStreamReader reader = new ArrowStreamReader(body, allocator)) {
            long count = 0L;
            if (reader.loadNextBatch()) {
                count = ((BigIntVector) reader.getVectorSchemaRoot().getVector(0)).get(0);
            }
            // Read the response to its end before closing it. Closing with the end-of-stream marker
            // unread makes the JDK client cancel the HTTP/2 stream (RST_STREAM); when that crosses the
            // server's END_STREAM, Helidon 4.5.x answers with GO_AWAY for the whole connection and
            // aborts the ingest requests sharing it (https://github.com/helidon-io/helidon/issues/12771).
            while (reader.loadNextBatch()) {
                // the remaining batches, up to the end-of-stream marker
            }
            body.transferTo(OutputStream.nullOutputStream());
            return count;
        }
    }

    /** POST Arrow IPC bytes to the ingest endpoint. Retries once on 429. */
    private void postArrowBatch(String url, String jwt, byte[] arrowBytes) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create(url))
                .header("Authorization", "Bearer " + jwt)
                .header("Content-Type", "application/vnd.apache.arrow.stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(arrowBytes))
                .timeout(Duration.ofSeconds(30))
                .build();
        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 429) {
            Thread.sleep(500);
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        }
        if (response.statusCode() != 200) {
            throw new RuntimeException("Ingest POST failed: HTTP " + response.statusCode() + " — " + response.body());
        }
    }

    /** Obtain a JWT from /v1/login. */
    private String acquireJwt() throws IOException, InterruptedException {
        String loginJson = """
                {"username":"admin","password":"admin"}""";
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + httpPort + "/v1/login"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(loginJson))
                .build();
        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("Login failed: " + response.body());
        }
        JsonNode node = MAPPER.readTree(response.body());
        return node.get("accessToken").asText();
    }

    /** Poll until the DuckLake row count reaches {@code target} or timeout elapses. */
    private long pollForCount(long target, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            long count = queryCount();
            if (count >= target) return count;
            Thread.sleep(500);
        }
        return queryCount();
    }

    private long queryCount() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            Long v = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE), Long.class);
            return v != null ? v : 0L;
        }
    }

    private static void printResults(long httpRows, long flightRows, long dbCount,
                                     long httpReadSum, long flightReadSum, Duration duration,
                                     ConcurrentLinkedQueue<Long> httpWriteLat,
                                     ConcurrentLinkedQueue<Long> flightWriteLat,
                                     ConcurrentLinkedQueue<Long> httpReadLat,
                                     ConcurrentLinkedQueue<Long> flightReadLat) {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("DuckLake + Postgres Load Test Results");
        System.out.println("=".repeat(70));
        System.out.printf("HTTP rows sent:            %,d%n", httpRows);
        System.out.printf("Flight rows sent:          %,d%n", flightRows);
        System.out.printf("Total rows in DB:          %,d%n", dbCount);
        System.out.printf("Total duration:            %,d ms%n", duration.toMillis());
        System.out.printf("Write throughput:          %.0f rows/sec%n",
                (httpRows + flightRows) / Math.max(1.0, duration.toMillis() / 1000.0));
        System.out.println("-".repeat(70));
        System.out.println("INGESTION latency (ms per batch, excludes server-side flush delay):");
        printLatency("  HTTP  ingest", httpWriteLat);
        printLatency("  Flight ingest", flightWriteLat);
        System.out.println("-".repeat(70));
        System.out.println("QUERY latency (ms per COUNT(*), excludes 500 ms inter-query sleep):");
        printLatency("  HTTP  query ", httpReadLat);
        printLatency("  Flight query", flightReadLat);
        System.out.println("-".repeat(70));
        System.out.printf("HTTP COUNT queries:        %,d  (cumulative row-count sum: %,d)%n", READ_COUNT, httpReadSum);
        System.out.printf("Flight JDBC COUNT queries: %,d  (cumulative row-count sum: %,d)%n", READ_COUNT, flightReadSum);
        System.out.println("=".repeat(70));
    }

    private static void printLatency(String label, ConcurrentLinkedQueue<Long> samples) {
        if (samples.isEmpty()) { System.out.printf("%s: no samples%n", label); return; }
        long[] a = samples.stream().mapToLong(Long::longValue).sorted().toArray();
        System.out.printf("%s: avg=%,d  p50=%,d  p95=%,d  p99=%,d  max=%,d  (n=%,d)%n",
                label,
                (long) samples.stream().mapToLong(Long::longValue).average().orElse(0),
                a[a.length / 2],
                a[(int) (a.length * 0.95)],
                a[(int) (a.length * 0.99)],
                a[a.length - 1],
                a.length);
    }

    private static int findAvailablePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    private static void waitForServer(String host, int port, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket(host, port)) {
                return;
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
        throw new RuntimeException("Server did not start on port " + port + " within " + timeoutMs + " ms");
    }
}
