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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import io.dazzleduck.sql.runtime.SharedTestServer;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                "ingestion_task_factory_provider.ingestion_queue_table_mapping.0.transformation=SELECT *, 'localhost' AS application_host, CAST(timestamp AS DATE) AS date FROM __this",
                // Startup script
                "startup_script_provider.class=io.dazzleduck.sql.flight.ConfigBasedStartupScriptProvider",
                "startup_script_provider.content=" + STARTUP_SCRIPT
        );
        Files.createDirectories(Path.of(server.getWarehousePath(), DUCKLAKE_DATA_DIR));
        Files.createDirectories(Path.of(server.getWarehousePath(), DUCKLAKE_DATA_DIR, SCHEMA_NAME));
        Files.createDirectories(Path.of(server.getWarehousePath(), DUCKLAKE_DATA_DIR, SCHEMA_NAME, TABLE_NAME));
    }

    @Test
    @org.junit.jupiter.api.parallel.Execution(ExecutionMode.CONCURRENT)
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

    // ──────────────────────────────────────────────────────────────────────────
    // System metrics tests (CPU, JVM memory)
    // Verifies CPU and JVM memory metrics are published and disk metrics are NOT.
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Verifies that CPU and JVM memory metrics are published, but disk metrics are not.
     *
     * SystemMetricsPublisher binds:
     *   - ProcessorMetrics:
     *     system.cpu.count  – number of available processors (always > 0)
     *     system.cpu.usage  – system-wide CPU load [0.0, 1.0]  (may be -1 on some JVMs)
     *     process.cpu.usage – JVM process CPU load [0.0, 1.0]  (may be -1 on some JVMs)
     *
     *   - JvmMemoryMetrics with tag area=heap|nonheap:
     *     jvm.memory.used      – bytes currently used
     *     jvm.memory.committed – bytes committed by the OS
     *     jvm.memory.max       – maximum bytes available (-1 if unlimited)
     *
     *   - DiskSpaceMetrics (NOT published):
     *     disk.free, disk.total, disk.usable are NOT included in published metrics
     */
    @Test
    @Execution(ExecutionMode.CONCURRENT)
    void testSystemMetricsPublished() throws Exception {
        MeterRegistry registry = MetricsRegistryFactory.create();
        try {
            SystemMetricsPublisher.bind(registry, warehouse.toString());
            Thread.sleep(100);
        } finally {
            registry.close();
        }
        Thread.sleep(200);

        String table = fullTableRef();

        // CPU Metrics
        // system.cpu.count must exist and its value must equal Runtime.availableProcessors()
        Long rowCount = ConnectionPool.collectFirst("SELECT count(*) FROM %s WHERE name = 'system.cpu.count' AND type = 'gauge'".formatted(table), Long.class);
        assertTrue(rowCount > 0, "system.cpu.count gauge row must be present in the metrics table");

        Double cpuCount = ConnectionPool.collectFirst("SELECT max(value) FROM %s WHERE name = 'system.cpu.count' AND type = 'gauge'".formatted(table), Double.class);
        assertEquals((double) java.lang.Runtime.getRuntime().availableProcessors(), cpuCount, "system.cpu.count must equal Runtime.availableProcessors()");

        // system.cpu.usage must exist (value may legitimately be -1.0 on some JVMs when unavailable)
        Long cpuUsageRows = ConnectionPool.collectFirst("SELECT count(*) FROM %s WHERE name = 'system.cpu.usage' AND type = 'gauge'".formatted(table), Long.class);
        assertTrue(cpuUsageRows > 0, "system.cpu.usage gauge row must be present in the metrics table");

        // JVM Memory Metrics
        // jvm.memory.used with area=heap must be present and positive
        Long heapRows = ConnectionPool.collectFirst("SELECT count(*) FROM %s WHERE name = 'jvm.memory.used' AND type = 'gauge' AND tags['area'] = 'heap'".formatted(table), Long.class);
        assertTrue(heapRows > 0, "jvm.memory.used (area=heap) gauge rows must be present");

        Double heapUsed = ConnectionPool.collectFirst("SELECT max(value) FROM %s WHERE name = 'jvm.memory.used' AND type = 'gauge' AND tags['area'] = 'heap'".formatted(table), Double.class);
        assertTrue(heapUsed > 0, "jvm.memory.used (area=heap) must be > 0 bytes");

        // jvm.memory.used with area=nonheap must also be present and positive
        Long nonHeapRows = ConnectionPool.collectFirst("SELECT count(*) FROM %s WHERE name = 'jvm.memory.used' AND type = 'gauge' AND tags['area'] = 'nonheap'".formatted(table), Long.class);
        assertTrue(nonHeapRows > 0, "jvm.memory.used (area=nonheap) gauge rows must be present");

        // Disk metrics must NOT be published (verify they are absent)
        Long diskFreeRows = ConnectionPool.collectFirst("SELECT count(*) FROM %s WHERE name = 'disk.free' AND type = 'gauge'".formatted(table), Long.class);
        assertEquals(0L, diskFreeRows, "disk.free should not be published");

        Long diskTotalRows = ConnectionPool.collectFirst("SELECT count(*) FROM %s WHERE name = 'disk.total' AND type = 'gauge'".formatted(table), Long.class);
        assertEquals(0L, diskTotalRows, "disk.total should not be published");

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

    private String fullTableRef() {
        return "%s.%s.%s".formatted(CATALOG_NAME, SCHEMA_NAME, TABLE_NAME);
    }
}
