package io.dazzleduck.sql.logback;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.runtime.SharedTestServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for LogForwardingAppender configured via logback.xml.
 * Tests that logs are correctly forwarded to the server and physically partitioned on disk.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LogForwardingIntegrationTest {

    @TempDir
    static Path warehouse;

    private SharedTestServer server;

    private static final int HTTP_PORT = 18081;
    private static final String CATALOG_NAME = "test_ducklake";
    private static final String TABLE_NAME = "test_logs";
    private static final String TABLE_NAME_PARTITIONED = "test_logs_partitioned";
    private static final String SCHEMA_NAME = "main";
    private static final String INGESTION_QUEUE_ID = "test-logs";
    private static final String INGESTION_QUEUE_ID_PARTITIONED = "test-logs-partitioned";
    private static final String DUCKLAKE_DATA_DIR = "ducklake_data";

    @BeforeAll
    void setup() throws Exception {
        server = new SharedTestServer();
        String tableColumns = "(\n" +
                "    sequence_number BIGINT,\n" +
                "    timestamp TIMESTAMP,\n" +
                "    level VARCHAR,\n" +
                "    logger VARCHAR,\n" +
                "    thread VARCHAR,\n" +
                "    message VARCHAR,\n" +
                "    mdc MAP(VARCHAR, VARCHAR),\n" +
                "    throwable VARCHAR,\n" +
                "    marker VARCHAR[],\n" +
                "    key_value_pairs MAP(VARCHAR, VARCHAR),\n" +
                "    caller_class VARCHAR,\n" +
                "    caller_method VARCHAR,\n" +
                "    caller_file VARCHAR,\n" +
                "    caller_line INTEGER,\n" +
                "    application_host VARCHAR,\n" +
                "    date DATE\n" +
                ")";
        String startupScript = "INSTALL arrow FROM community;\n" +
                "LOAD arrow;\n" +
                "\n" +
                "LOAD ducklake;\n" +
                "ATTACH 'ducklake:" + warehouse + "/" + CATALOG_NAME + "' AS " + CATALOG_NAME +
                " (DATA_PATH '" + warehouse + "/" + DUCKLAKE_DATA_DIR + "');\n" +
                "USE " + CATALOG_NAME + ";\n" +
                "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " " + tableColumns + ";\n" +
                "CREATE TABLE IF NOT EXISTS " + TABLE_NAME_PARTITIONED + " " + tableColumns + ";\n" +
                "ALTER TABLE " + CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME_PARTITIONED +
                " SET PARTITIONED BY (date);\n";

        server.startWithWarehouse(
                HTTP_PORT,
                0,
                "http.auth=none",
                "warehouse=" + warehouse.toAbsolutePath(),
                // DuckLake ingestion
                "ingestion_task_factory_provider.class=io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionTaskFactoryProvider",
                // Mapping
                "ingestion_task_factory_provider.ingestion_queue_table_mapping.0.table=" + TABLE_NAME,
                "ingestion_task_factory_provider.ingestion_queue_table_mapping.0.schema=" + SCHEMA_NAME,
                "ingestion_task_factory_provider.ingestion_queue_table_mapping.0.catalog=" + CATALOG_NAME,
                "ingestion_task_factory_provider.ingestion_queue_table_mapping.0.ingestion_queue=" + INGESTION_QUEUE_ID,
                "ingestion_task_factory_provider.ingestion_queue_table_mapping.0.transformation=SELECT *, 'test-host' AS application_host, CAST(timestamp AS DATE) AS date FROM __this",
                // Second mapping — table-driven partitioning (ALTER TABLE SET PARTITIONED BY (date))
                "ingestion_task_factory_provider.ingestion_queue_table_mapping.1.table=" + TABLE_NAME_PARTITIONED,
                "ingestion_task_factory_provider.ingestion_queue_table_mapping.1.schema=" + SCHEMA_NAME,
                "ingestion_task_factory_provider.ingestion_queue_table_mapping.1.catalog=" + CATALOG_NAME,
                "ingestion_task_factory_provider.ingestion_queue_table_mapping.1.ingestion_queue=" + INGESTION_QUEUE_ID_PARTITIONED,
                "ingestion_task_factory_provider.ingestion_queue_table_mapping.1.transformation=SELECT *, 'test-host' AS application_host, CAST(timestamp AS DATE) AS date FROM __this",
                // Startup script
                "startup_script_provider.class=io.dazzleduck.sql.flight.ConfigBasedStartupScriptProvider",
                "startup_script_provider.content=" + startupScript
        );
        Files.createDirectories(Path.of(server.getWarehousePath(), DUCKLAKE_DATA_DIR));
        Files.createDirectories(Path.of(server.getWarehousePath(), DUCKLAKE_DATA_DIR, SCHEMA_NAME));
        Files.createDirectories(Path.of(server.getWarehousePath(), DUCKLAKE_DATA_DIR, SCHEMA_NAME, TABLE_NAME));

        // Reset any previous state and configure logback with our test configuration
        LogForwardingAppender.resetGlobalState();
        configureLogback();
    }

    private void configureLogback() throws JoranException {
        configureLogback("/logback-integration-test.xml");
    }

    private void configureLogback(String resource) throws JoranException {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.reset();

        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);

        try (InputStream configStream = getClass().getResourceAsStream(resource)) {
            configurator.doConfigure(configStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load logback configuration: " + resource, e);
        }
    }

    @Test
    @Order(1)
    void testLogsForwardedWithProjectExpressions() throws Exception {
        // Get a logger and generate some log messages
        Logger testLogger = LoggerFactory.getLogger("test.integration.logger");

        testLogger.info("Test message 1");
        testLogger.warn("Test warning message");
        testLogger.error("Test error message");

        // Stop all appenders to flush pending data (context.stop() calls stop() on each appender,
        // which drains the producer queue).
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        ctx.stop();

        // Poll until server-side ingestion has landed all 3 rows (replaces fixed sleeps).
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50)).untilAsserted(() ->
                TestUtils.isEqual(
                        "SELECT 3 as count",
                        "SELECT count(*) as count FROM " + CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME +
                                " WHERE logger = 'test.integration.logger'"));

        // Verify application_host was added by project expression
        TestUtils.isEqual(
                "SELECT 'test-host' as application_host",
                "SELECT DISTINCT application_host FROM " + CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME +
                        " WHERE logger = 'test.integration.logger'"
        );

        // Verify date column was added by project expression
        Long dateCount = ConnectionPool.collectFirst(
                "SELECT count(*) FROM " + CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME +
                        " WHERE logger = 'test.integration.logger' AND date IS NOT NULL",
                Long.class
        );
        assertTrue(dateCount >= 3, "Expected at least 3 rows with non-null date, got: " + dateCount);

        // Verify log levels are captured correctly
        TestUtils.isEqual(
                "SELECT 'INFO' as level UNION ALL SELECT 'WARN' as level UNION ALL SELECT 'ERROR' as level ORDER BY level",
                "SELECT DISTINCT level FROM " + CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME +
                        " WHERE logger = 'test.integration.logger' ORDER BY level"
        );
    }

    @Test
    @Order(2)
    void testLogsPartitionedByDate() throws Exception {
        // The appender is configured with <partitionBy>date</partitionBy>.
        // The server-side transformation adds a DATE column: CAST(timestamp AS DATE) AS date.
        // DuckDB COPY writes Hive-style directories: date=YYYY-MM-DD/ under the table data path.
        try (Stream<Path> paths = Files.walk(warehouse.resolve(DUCKLAKE_DATA_DIR))) {
            boolean hasDatePartition = paths
                    .filter(Files::isDirectory)
                    .anyMatch(p -> p.getFileName().toString().startsWith("date="));
            assertTrue(hasDatePartition,
                    "Expected Hive-style date=YYYY-MM-DD partition directories under " + DUCKLAKE_DATA_DIR);
        }
    }

    @Test
    @Order(3)
    void testTableDrivenPartitionBy() throws Exception {
        // Use LogForwarder directly (no Logback context) to avoid context lifecycle issues
        // after ctx.stop() in test 1. The config intentionally omits partitionBy — this
        // exercises DuckLakeIngestionHandler's fallback to ducklake_partition_column metadata.
        LogForwarderConfig config = LogForwarderConfig.builder()
                .baseUrl("http://localhost:" + HTTP_PORT)
                .username("admin")
                .password("admin")
                .ingestionQueue(INGESTION_QUEUE_ID_PARTITIONED)
                .minBatchSize(1)                        // send on first entry
                .maxSendInterval(Duration.ofSeconds(1))
                .retryCount(0)
                .build();

        try (LogForwarder forwarder = new LogForwarder(config)) {
            forwarder.addLogEntry(new LogEntry(1, Instant.now(), "INFO",
                    "test.table.partition.logger", "main",
                    "Table-driven partition test 1",
                    Map.of(), null, List.of(), Map.of(), null));
            forwarder.addLogEntry(new LogEntry(2, Instant.now(), "WARN",
                    "test.table.partition.logger", "main",
                    "Table-driven partition test 2",
                    Map.of(), null, List.of(), Map.of(), null));
        }   // close() drains pending client-side sends (httpProducer.close)

        // DuckLakeIngestionHandler.getPartitionBy() returned ["date"] from ducklake_partition_column
        // (set by ALTER TABLE SET PARTITIONED BY (date) in startup script).
        // ParquetIngestionQueue used it in COPY ... (PARTITION_BY(date)), producing date= paths.
        String partitionedFileCountSql =
                "SELECT COUNT(*) " +
                "FROM __ducklake_metadata_" + CATALOG_NAME + ".ducklake_data_file df " +
                "JOIN __ducklake_metadata_" + CATALOG_NAME + ".ducklake_table t ON df.table_id = t.table_id " +
                "WHERE t.table_name = '" + TABLE_NAME_PARTITIONED + "' " +
                "  AND t.end_snapshot IS NULL " +
                "  AND df.end_snapshot IS NULL " +
                "  AND df.path LIKE '%date=%'";

        // Poll until server-side ingest produces the table-driven date= partition files.
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100)).until(() -> {
            Long c = ConnectionPool.collectFirst(partitionedFileCountSql, Long.class);
            return c != null && c > 0;
        });
        Long partitionedFileCount = ConnectionPool.collectFirst(partitionedFileCountSql, Long.class);
        assertTrue(partitionedFileCount > 0,
                "Expected table-driven date= partition paths in ducklake_data_file for " + TABLE_NAME_PARTITIONED);
    }

    @AfterAll
    void cleanup() throws Exception {
        // Reset logback appender state
        LogForwardingAppender.resetGlobalState();

        if (server != null) server.close();

        try {
            ConnectionPool.execute("DETACH " + CATALOG_NAME);
        } catch (Exception ignored) {
        }

        if (Files.exists(warehouse)) {
            Files.walk(warehouse).sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {
                        }
                    });
        }
    }
}
