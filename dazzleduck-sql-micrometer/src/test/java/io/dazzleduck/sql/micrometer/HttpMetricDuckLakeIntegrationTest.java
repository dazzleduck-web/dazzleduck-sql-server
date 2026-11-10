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
                INSTALL arrow FROM community;
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

        // Startup diagnostics
        String meta = "__ducklake_metadata_" + CATALOG_NAME;
        try {
            String dbs = ConnectionPool.collectFirst("SELECT string_agg(database_name, ',') FROM duckdb_databases()", String.class);
            System.err.println("[SETUP DIAG] databases: " + dbs);
            String path = ConnectionPool.collectFirst("SELECT CASE WHEN s.path_is_relative THEN concat(rtrim(m.\"value\", '/'), '/', rtrim(s.path, '/'), '/', rtrim(t.path, '/')) ELSE concat(rtrim(s.path, '/'), '/', rtrim(t.path, '/')) END AS path FROM %s.ducklake_schema s JOIN %s.ducklake_table t ON (s.schema_id = t.schema_id) CROSS JOIN %s.ducklake_metadata m WHERE m.key = 'data_path' AND s.schema_name = '%s' AND t.table_name = '%s' AND s.end_snapshot IS NULL AND t.end_snapshot IS NULL".formatted(meta, meta, meta, SCHEMA_NAME, TABLE_NAME), String.class);
            System.err.println("[SETUP DIAG] TABLE_PATH_QUERY at startup: " + path);
        } catch (Exception ex) {
            System.err.println("[SETUP DIAG] startup path query failed: " + ex.getMessage());
        }
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

        // Poll until the ingested row appears, up to 15 seconds
        String expected = """
                select 'records.processed' as name,
                       'counter'           as type,
                       10.0                as value,
                       'localhost'         as application_host
                """;
        // Summed: the registry publishes every step, so a step boundary during the test splits the
        // count across rows (the increments in one, and an empty 0.0 step published on close in
        // another). Asserting a single row made the test fail whenever a boundary fell inside it.
        String actual = """
                select name, type, sum(value) as value, application_host
                from %s.%s.%s
                where name = 'records.processed'
                group by name, type, application_host
                """.formatted(CATALOG_NAME, SCHEMA_NAME, TABLE_NAME);

        long deadline = System.currentTimeMillis() + 15_000;
        while (true) {
            try {
                TestUtils.isEqual(expected, actual);
                break;
            } catch (AssertionError e) {
                if (System.currentTimeMillis() >= deadline) {
                    String meta = "__ducklake_metadata_" + CATALOG_NAME;
                    try {
                        Long rowCount = ConnectionPool.collectFirst(
                                "SELECT count(*) FROM %s.%s.%s".formatted(CATALOG_NAME, SCHEMA_NAME, TABLE_NAME), Long.class);
                        System.err.println("[DIAG] Row count = " + rowCount);
                    } catch (Exception ex) { System.err.println("[DIAG] count rows failed: " + ex.getMessage()); }
                    try {
                        String path = ConnectionPool.collectFirst(
                                "SELECT * FROM %s.ducklake_schema".formatted(meta), String.class);
                        System.err.println("[DIAG] ducklake_schema first row: " + path);
                    } catch (Exception ex) { System.err.println("[DIAG] ducklake_schema failed: " + ex.getMessage()); }
                    try {
                        String dbs = ConnectionPool.collectFirst("SELECT database_name FROM duckdb_databases() WHERE database_name LIKE '%ducklake%'", String.class);
                        System.err.println("[DIAG] ducklake databases: " + dbs);
                        String pathResult = ConnectionPool.collectFirst(
                                "SELECT CASE WHEN s.path_is_relative THEN concat(rtrim(m.\"value\", '/'), '/', rtrim(s.path, '/'), '/', rtrim(t.path, '/')) ELSE concat(rtrim(s.path, '/'), '/', rtrim(t.path, '/')) END AS path FROM %s.ducklake_schema s JOIN %s.ducklake_table t ON (s.schema_id = t.schema_id) CROSS JOIN %s.ducklake_metadata m WHERE m.key = 'data_path' AND s.schema_name = '%s' AND t.table_name = '%s' AND s.end_snapshot IS NULL AND t.end_snapshot IS NULL".formatted(meta, meta, meta, SCHEMA_NAME, TABLE_NAME), String.class);
                        System.err.println("[DIAG] TABLE_PATH_QUERY result: " + pathResult);
                    } catch (Exception ex) { System.err.println("[DIAG] path query failed: " + ex.getMessage()); }
                    // Check what the ingestion handler would return right now
                    try {
                        String path = ConnectionPool.collectFirst("SELECT CASE WHEN s.path_is_relative THEN concat(rtrim(m.\"value\", '/'), '/', rtrim(s.path, '/'), '/', rtrim(t.path, '/')) ELSE concat(rtrim(s.path, '/'), '/', rtrim(t.path, '/')) END AS path FROM %s.ducklake_schema s JOIN %s.ducklake_table t ON (s.schema_id = t.schema_id) CROSS JOIN %s.ducklake_metadata m WHERE m.key = 'data_path' AND s.schema_name = 'main' AND t.table_name = 'test_metrics' AND s.end_snapshot IS NULL AND t.end_snapshot IS NULL".formatted(meta, meta, meta), String.class);
                        System.err.println("[DIAG] At end of test, getPathFromCatalog would return: " + path);
                    } catch (Exception ex) { System.err.println("[DIAG] path2 failed: " + ex.getMessage()); }
                    try (var stream = Files.walk(warehouse)) {
                        stream.filter(Files::isRegularFile).forEach(p -> System.err.println("[DIAG] file: " + p));
                    } catch (Exception ex) { System.err.println("[DIAG] walk failed: " + ex.getMessage()); }
                    throw e;
                }
                Thread.sleep(200);
            }
        }
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
