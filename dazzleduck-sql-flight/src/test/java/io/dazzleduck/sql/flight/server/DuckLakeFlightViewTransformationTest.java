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
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end test for VIEW-based ingestion transformations over Arrow Flight SQL.
 * <p>
 * The queue mapping declares {@code view} + {@code input_table} instead of an inline
 * transformation. The full chain under test:
 * <ol>
 *   <li>{@code executeIngest} (Flight bulk ingest) → {@code acceptPutStatementBulkIngest}</li>
 *   <li>→ {@code getOrCreateIngestionQueue} → {@link DuckLakeIngestionHandler} resolves the
 *       view definition and rewrites {@code input_table} to the {@code __this} placeholder</li>
 *   <li>→ {@code ParquetIngestionQueue} applies the derived transformation on write</li>
 * </ol>
 * A second phase replaces the view with {@code CREATE OR REPLACE VIEW} and verifies the next
 * Flight ingest applies the NEW view body — no config change, no server restart.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DuckLakeFlightViewTransformationTest {

    private static final String USER = "admin";
    private static final String PASSWORD = "password";
    private static final String CATALOG_NAME = "memory";
    private static final String SCHEMA_NAME = "main";

    private static final String DUCKLAKE_CATALOG = "view_ingest_lake";
    private static final String QUEUE = "view_queue";
    private static final String FQ_VIEW = DUCKLAKE_CATALOG + ".main.v_events";
    private static final String FQ_INPUT_TABLE = DUCKLAKE_CATALOG + ".main.events";

    private BufferAllocator allocator;
    private FlightServer server;
    private FlightSqlClient client;
    private Path tempDir;
    private DuckDBFlightSqlProducer producer;

    @BeforeAll
    void setup() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        tempDir = Files.createTempDirectory("ducklake-view-transform-test");

        ConnectionPool.executeBatch(new String[]{
                "INSTALL arrow FROM community",
                "LOAD arrow",
                "INSTALL ducklake",
                "LOAD ducklake"
        });

        Path dataPath = tempDir.resolve("data");
        // The ingestion output directory is never auto-created — pre-create the
        // per-table path DuckLake resolves for COPY targets.
        Files.createDirectories(dataPath.resolve("main").resolve("events"));
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), DUCKLAKE_CATALOG, dataPath),
                    "CREATE TABLE %s (id BIGINT, level VARCHAR)".formatted(FQ_INPUT_TABLE),
                    // The view body over input_table becomes the ingestion transformation:
                    // SELECT id * 100 AS id, upper(level) AS level FROM __this
                    "CREATE VIEW %s AS SELECT id * 100 AS id, upper(level) AS level FROM %s"
                            .formatted(FQ_VIEW, FQ_INPUT_TABLE)
            });
        }

        // view + input_table instead of an inline transformation (last two constructor args).
        var mapping = new QueueIdToTableMapping(QUEUE, DUCKLAKE_CATALOG, "main", "events",
                Map.of(), null, FQ_VIEW, FQ_INPUT_TABLE);
        // Zero refresh interval: every ingest request re-checks staleness, so a view change is
        // picked up on the very next call (schema_version gates the actual re-resolution).
        var ingestionHandler = new DuckLakeIngestionHandler(
                Map.of(QUEUE, mapping), Duration.ZERO, Clock.systemDefaultZone());

        Location serverLocation = FlightTestUtils.findNextLocation();
        String producerId = UUID.randomUUID().toString();
        var ingestionConfig = new IngestionConfig(
                1,                       // minBucketSize: flush every batch immediately
                1024 * 1024 * 1024L,     // maxBucketSize
                2048,                    // maxBatches
                256 * 1024 * 1024L,      // maxPendingWrite
                Duration.ofSeconds(2),   // maxDelay
                Duration.ZERO            // configRefreshDelay: refresh queue state per request
        );
        producer = new DuckDBFlightSqlProducer(
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
                ingestionConfig);

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
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + DUCKLAKE_CATALOG);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void flightIngest_appliesViewTransformation_andPicksUpViewReplacement() throws Exception {
        // ---- Phase 1: ingest ids 1..5, level 'info' — view multiplies by 100 and uppercases
        ingest("SELECT i AS id, 'info' AS level FROM range(1, 6) t(i)");

        try (Connection conn = ConnectionPool.getConnection()) {
            Long transformed = ConnectionPool.collectFirst(conn,
                    ("SELECT COUNT(*) FROM %s.main.events " +
                     "WHERE id IN (100, 200, 300, 400, 500) AND level = 'INFO'").formatted(DUCKLAKE_CATALOG),
                    Long.class);
            assertEquals(5L, transformed,
                    "Phase 1: view transformation (id * 100, upper(level)) must be applied on Flight ingest");
            Long total = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.main.events".formatted(DUCKLAKE_CATALOG), Long.class);
            assertEquals(5L, total, "Phase 1: exactly the 5 transformed rows should exist");
        }

        // ---- Phase 2: replace the view — no config change, no restart
        ConnectionPool.execute(
                "CREATE OR REPLACE VIEW %s AS SELECT id * 1000 AS id, lower(level) AS level FROM %s"
                        .formatted(FQ_VIEW, FQ_INPUT_TABLE));

        // Ingest ids 6..10, level 'INFO' — the NEW view multiplies by 1000 and lowercases
        ingest("SELECT i AS id, 'INFO' AS level FROM range(6, 11) t(i)");

        try (Connection conn = ConnectionPool.getConnection()) {
            Long newTransformed = ConnectionPool.collectFirst(conn,
                    ("SELECT COUNT(*) FROM %s.main.events " +
                     "WHERE id IN (6000, 7000, 8000, 9000, 10000) AND level = 'info'").formatted(DUCKLAKE_CATALOG),
                    Long.class);
            assertEquals(5L, newTransformed,
                    "Phase 2: replaced view (id * 1000, lower(level)) must be applied on the next Flight ingest");
            Long total = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.main.events".formatted(DUCKLAKE_CATALOG), Long.class);
            assertEquals(10L, total, "Phase 2: 5 old + 5 new rows");
        }
    }

    private void ingest(String sourceQuery) throws Exception {
        try (DuckDBConnection conn = ConnectionPool.getConnection();
             ArrowReader reader = ConnectionPool.getReader(conn, allocator, sourceQuery, 1000)) {
            var streamReader = new ArrowStreamReaderWrapper(reader, allocator);
            var options = new FlightSqlClient.ExecuteIngestOptions(
                    "",
                    FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder().build(),
                    false, CATALOG_NAME, SCHEMA_NAME,
                    Map.of(Headers.QUERY_PARAMETER_INGESTION_QUEUE, QUEUE));
            client.executeIngest(streamReader, options);
        }
    }
}
