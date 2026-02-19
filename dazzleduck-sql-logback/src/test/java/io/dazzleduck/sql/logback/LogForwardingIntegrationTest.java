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
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for LogForwardingAppender configured via logback.xml.
 * Tests that logs are correctly forwarded to the server with project expressions applied.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LogForwardingIntegrationTest {

    @TempDir
    static Path warehouse;

    private SharedTestServer server;

    private static final int HTTP_PORT = 18081;
    private static final String CATALOG_NAME = "test_ducklake";
    private static final String TABLE_NAME = "test_logs";
    private static final String SCHEMA_NAME = "main";
    private static final String INGESTION_QUEUE_ID = "test-logs";
    private static final String DUCKLAKE_DATA_DIR = "ducklake_data";

    @BeforeAll
    void setup() throws Exception {
        server = new SharedTestServer();
        String startupScript = "INSTALL arrow;\n" +
                "LOAD arrow;\n" +
                "\n" +
                "LOAD ducklake;\n" +
                "ATTACH 'ducklake:" + warehouse + "/" + CATALOG_NAME + "' AS " + CATALOG_NAME +
                " (DATA_PATH '" + warehouse + "/" + DUCKLAKE_DATA_DIR + "');\n" +
                "USE " + CATALOG_NAME + ";\n" +
                "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (\n" +
                "    s_no BIGINT,\n" +
                "    timestamp TIMESTAMP,\n" +
                "    level VARCHAR,\n" +
                "    logger VARCHAR,\n" +
                "    thread VARCHAR,\n" +
                "    message VARCHAR,\n" +
                "    mdc MAP(VARCHAR, VARCHAR),\n" +
                "    application_host VARCHAR,\n" +
                "    date DATE\n" +
                ");\n";

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
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.reset();

        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);

        try (InputStream configStream = getClass().getResourceAsStream("/logback-integration-test.xml")) {
            configurator.doConfigure(configStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load logback configuration", e);
        }
    }

    @Test
    void testLogsForwardedWithProjectExpressions() throws Exception {
        // Get a logger and generate some log messages
        Logger testLogger = LoggerFactory.getLogger("test.integration.logger");

        testLogger.info("Test message 1");
        testLogger.warn("Test warning message");
        testLogger.error("Test error message");

        // Wait for logs to be added to producer queue
        Thread.sleep(1000);

        // Stop all appenders to flush pending data (context.stop() calls stop() on each appender)
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        ctx.stop();

        // Wait for server-side ingestion to complete
        Thread.sleep(2000);

        // Verify logs were received with correct project expressions
        TestUtils.isEqual(
                "SELECT 3 as count",
                "SELECT count(*) as count FROM " + CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME +
                        " WHERE logger = 'test.integration.logger'"
        );

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
