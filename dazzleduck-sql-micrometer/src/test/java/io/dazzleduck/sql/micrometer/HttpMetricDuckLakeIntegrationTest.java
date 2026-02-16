package io.dazzleduck.sql.micrometer;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.micrometer.metrics.MetricsRegistryFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.*;
import io.dazzleduck.sql.runtime.SharedTestServer;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HttpMetricDuckLakeIntegrationTest {

    @TempDir
    static Path warehouse;

    private SharedTestServer server;

    private static final int HTTP_PORT = 8081;

    private static final String CATALOG_NAME = "test_ducklake";
    private static final String TABLE_NAME = "test_metrics";
    private static final String SCHEMA_NAME = "main";

    private static final String INGESTION_QUEUE_ID = "metrics";
    private static final String DUCKLAKE_DATA_DIR = "ducklake_data";

    @BeforeAll
    void setup() throws Exception {
        server = new SharedTestServer();
        String STARTUP_SCRIPT = """
                INSTALL arrow;
                LOAD arrow;

                LOAD ducklake;
                ATTACH 'ducklake:%s/%s' AS %s (DATA_PATH '%s/%s');
                USE %s;
                CREATE TABLE IF NOT EXISTS %s (
                    timestamp TIMESTAMP,
                    name VARCHAR,
                    type VARCHAR,
                    tags MAP(VARCHAR, VARCHAR),
                    value DOUBLE,
                    min DOUBLE,
                    max DOUBLE,
                    mean DOUBLE,
                    application_host VARCHAR,
                    date DATE
                );
                """.formatted(warehouse, CATALOG_NAME, CATALOG_NAME, warehouse, DUCKLAKE_DATA_DIR, CATALOG_NAME, TABLE_NAME);

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
                "startup_script_provider.content=" + STARTUP_SCRIPT
        );
        Files.createDirectories(Path.of(server.getWarehousePath(), DUCKLAKE_DATA_DIR));
        Files.createDirectories(Path.of(server.getWarehousePath(), DUCKLAKE_DATA_DIR, SCHEMA_NAME));
        Files.createDirectories(Path.of(server.getWarehousePath(), DUCKLAKE_DATA_DIR, SCHEMA_NAME, TABLE_NAME));
    }

    @Test
    void testMetricsCanPostAndPersist() throws Exception {

        MeterRegistry registry = MetricsRegistryFactory.create();

        try {
            Counter counter = Counter.builder("records.processed").description("Number of records processed").register(registry);
            Timer timer = Timer.builder("record.processing.time").description("Time spent processing records").register(registry);
            for (int i = 0; i < 10; i++) {
                timer.record(100, TimeUnit.MILLISECONDS);
                counter.increment();
            }
                Thread.sleep(100);

        } finally {
            registry.close();
        }

        // Wait for server-side ingestion processing to complete
        Thread.sleep(500);

        // Note: This test may fail due to projection configuration changes
        // The projection feature is not being applied correctly in some cases
        TestUtils.isEqual("""
                        select 'records.processed' as name,
                               'counter'           as type,
                               10.0                as value,
                               'localhost'         as application_host
                        """,
                """
                        select name, type, value, application_host
                        from %s.%s.%s
                        where name = 'records.processed'
                        """.formatted(CATALOG_NAME, SCHEMA_NAME, TABLE_NAME)
        );
    }

    @AfterAll
    void cleanup() throws Exception {
        if (server != null) server.close();
        ConnectionPool.execute("DETACH " + CATALOG_NAME);
        String warehousePath = ConfigConstants.getWarehousePath(ConfigFactory.load().getConfig(ConfigConstants.CONFIG_PATH));
        Path path = Path.of(warehousePath);
        if (Files.exists(path)) {
            Files.walk(path).sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {
                        }
                    });
        }

        Files.createDirectories(path);
    }

    private Path waitForMetricFile(String warehousePath) throws Exception {

        Path dir = Path.of(warehousePath);
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            try (var stream = Files.walk(dir)) {
                var file = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.toString().endsWith(".parquet"))
                        .findFirst();
                if (file.isPresent()) {
                    return file.get();
                }
            }
            Thread.sleep(200);
        }

        throw new IllegalStateException("No metric file found in " + dir);
    }
}
