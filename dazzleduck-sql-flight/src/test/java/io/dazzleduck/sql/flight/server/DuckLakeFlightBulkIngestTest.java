package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionHandler;
import io.dazzleduck.sql.commons.ingestion.QueueIdToTableMapping;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.flight.stream.ArrowStreamReaderWrapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Flight-level integration tests for bulk ingestion backed by DuckLakeIngestionHandler.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DuckLakeFlightBulkIngestTest {

    private static final String USER = "admin";
    private static final String PASSWORD = "password";
    private static final String CATALOG_NAME = "memory";
    private static final String SCHEMA_NAME = "main";

    private static final String DUCKLAKE_CATALOG = "bulk_ingest_lake";

    /** The queue that IS configured in the DuckLakeIngestionHandler. */
    private static final String KNOWN_QUEUE = "known_queue";

    /** A queue that has NO mapping — getTargetPath() will return null. */
    private static final String UNKNOWN_QUEUE = "unknown_queue";

    private BufferAllocator allocator;
    private FlightServer server;
    private FlightSqlClient client;
    private Path tempDir;

    @BeforeAll
    void setup() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        tempDir = Files.createTempDirectory("ducklake-bulk-ingest-test");

        ConnectionPool.executeBatch(new String[]{
                "INSTALL arrow FROM community",
                "LOAD arrow",
                "INSTALL ducklake",
                "LOAD ducklake"
        });

        Path dataPath = tempDir.resolve("data");
        Files.createDirectories(dataPath.resolve("main").resolve("events"));
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), DUCKLAKE_CATALOG, dataPath),
                    "CREATE TABLE %s.main.events (id BIGINT)".formatted(DUCKLAKE_CATALOG)
            });
        }

        var mapping = new QueueIdToTableMapping(KNOWN_QUEUE, DUCKLAKE_CATALOG, "main", "events", Map.of(), null);
        var ingestionHandler = new DuckLakeIngestionHandler(Map.of(KNOWN_QUEUE, mapping));

        Location serverLocation = FlightTestUtils.findNextLocation();
        String producerId = UUID.randomUUID().toString();
        var producer = new DuckDBFlightSqlProducer(
                serverLocation,
                producerId,
                "test-secret",
                allocator,
                tempDir.toString(),
                AccessMode.COMPLETE,
                DuckDBFlightSqlProducer.newTempDir(),
                ingestionHandler,
                Executors.newSingleThreadScheduledExecutor(),
                Duration.ofMinutes(2),
                Clock.systemDefaultZone(),
                new MicroMeterFlightRecorder(new SimpleMeterRegistry(), producerId),
                DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG
        );

        server = FlightServer.builder(allocator, serverLocation, producer)
                .headerAuthenticator(AuthUtils.getTestAuthenticator())
                .build()
                .start();

        client = new FlightSqlClient(FlightClient.builder(allocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER, PASSWORD,
                        Map.of(Headers.HEADER_DATABASE, CATALOG_NAME, Headers.HEADER_SCHEMA, SCHEMA_NAME)))
                .build());
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
        if (server != null) server.close();
        if (allocator != null) allocator.close();
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + DUCKLAKE_CATALOG);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ingestToKnownQueue_shouldLandDataInDuckLake() throws Exception {
        String query = "SELECT i AS id FROM range(1, 6) t(i)";

        try (DuckDBConnection conn = ConnectionPool.getConnection();
             ArrowReader reader = ConnectionPool.getReader(conn, allocator, query, 1000)) {

            var streamReader = new ArrowStreamReaderWrapper(reader, allocator);
            var options = new FlightSqlClient.ExecuteIngestOptions(
                    "",
                    FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder().build(),
                    false, CATALOG_NAME, SCHEMA_NAME,
                    Map.of(Headers.QUERY_PARAMETER_INGESTION_QUEUE, KNOWN_QUEUE));

            client.executeIngest(streamReader, options);
        }

        try (Connection conn = ConnectionPool.getConnection()) {
            Long count = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.main.events".formatted(DUCKLAKE_CATALOG), Long.class);
            assertEquals(5L, count, "Expected 5 rows in DuckLake after ingestion");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ingestToUnknownQueue_shouldReturnInvalidArgument() throws Exception {
        try (DuckDBConnection conn = ConnectionPool.getConnection();
             ArrowReader reader = ConnectionPool.getReader(conn, allocator, "SELECT 1 AS id", 1000)) {

            var streamReader = new ArrowStreamReaderWrapper(reader, allocator);
            var options = new FlightSqlClient.ExecuteIngestOptions(
                    "",
                    FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder().build(),
                    false, CATALOG_NAME, SCHEMA_NAME,
                    Map.of(Headers.QUERY_PARAMETER_INGESTION_QUEUE, UNKNOWN_QUEUE));

            FlightRuntimeException ex = assertThrows(FlightRuntimeException.class,
                    () -> client.executeIngest(streamReader, options));

            assertEquals(FlightStatusCode.INVALID_ARGUMENT, ex.status().code(),
                    "Expected INVALID_ARGUMENT for unknown queue, got: " + ex.status().code());
            assertTrue(ex.getMessage().contains(UNKNOWN_QUEUE) && ex.getMessage().contains("not found"),
                    "Error message should mention the queue name and 'not found'. Actual: " + ex.getMessage());
        }
    }
}
