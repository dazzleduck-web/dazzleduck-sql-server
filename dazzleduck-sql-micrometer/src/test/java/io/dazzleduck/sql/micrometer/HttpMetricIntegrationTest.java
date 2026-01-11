package io.dazzleduck.sql.micrometer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.micrometer.metrics.MetricsRegistryFactory;
import io.dazzleduck.sql.runtime.Runtime;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled
public class HttpMetricIntegrationTest {
    private static HttpClient client;

    @TempDir
    private static Path warehousePath;

    @TempDir
    private static Path tempDir;

    private Runtime runtime;

    private static String detectedDatabase;
    private static String detectedMetadataDatabase;
    private static String ducklakeCatalogPath;
    private static String ducklakeDataPath;
    private static Long tableId;

    private static final int TEST_PORT = 8096;
    private static final String BASE_URL = "http://localhost:" + TEST_PORT;
    private static final String DUCKLAKE_CATALOG = "test_ducklake";
    private static final String DUCKLAKE_METADATA = "__ducklake_metadata_" + DUCKLAKE_CATALOG;
    private static final String TEST_TABLE = "test_metrics";
    private static final String INGEST_PATH = "metrics";

    @BeforeAll
    void startServers() throws Exception {
        client = HttpClient.newHttpClient();

        ducklakeCatalogPath = tempDir.resolve(DUCKLAKE_CATALOG).toString();
        ducklakeDataPath = tempDir.resolve("ducklake_data").toString();
        // Create ingestion directory
        Files.createDirectories(warehousePath.resolve(INGEST_PATH));
        // Initialize DuckLake
        setupDuckLake();
        // Wait for server to start
        Thread.sleep(2000);

        Config config = overriddenConfig().getConfig(ConfigUtils.CONFIG_PATH);
        runtime = Runtime.start(config);
        waitForHttpServer();
    }

    private static void setupDuckLake() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "INSTALL arrow",
                    "LOAD arrow"
            });
            // Install and load DuckLake
            String[] setupQueries = {
                    "LOAD ducklake",
                    String.format("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')", ducklakeCatalogPath, DUCKLAKE_CATALOG, ducklakeDataPath),
                    "USE " + DUCKLAKE_CATALOG,
                    // Create test table with the schema matching our test data
                    String.format("""
                                CREATE TABLE IF NOT EXISTS %s (
                                    name VARCHAR,
                                    type VARCHAR,
                                    value DOUBLE,
                                    application_id VARCHAR,
                                    application_name VARCHAR,
                                    application_host VARCHAR
                                )
                            """, TEST_TABLE)
            };
            ConnectionPool.executeBatchInTxn(conn, setupQueries);

            // Detect database names
            List<String> dbs = new ArrayList<>();
            ConnectionPool.collectFirstColumn(conn, "SHOW DATABASES", String.class).forEach(dbs::add);
            // Detect metadata database
            detectedMetadataDatabase = dbs.stream().filter(d -> d != null && d.startsWith("__ducklake_metadata_")).findFirst().orElseThrow(() -> new IllegalStateException("Metadata DB not found. DBs: " + dbs));
            // Detect attached database
            detectedDatabase = dbs.stream().filter(d -> d != null && !"memory".equalsIgnoreCase(d) && !d.startsWith("__ducklake_metadata_")).findFirst().orElseThrow(() -> new IllegalStateException("Attached DB not found. DBs: " + dbs));
            // Get table ID from metadata
            String getTableIdSql = String.format("SELECT table_id FROM %s.ducklake_table WHERE table_name = '%s'", detectedMetadataDatabase, TEST_TABLE);
            tableId = ConnectionPool.collectFirst(conn, getTableIdSql, Long.class);
            assertNotNull(tableId, "Table ID should not be null");
        }
    }

    private Config overriddenConfig() {
        String override = """
                dazzleduck_server {
                  http {
                    port = 8081
                    host = "0.0.0.0"
                    authentication = "none"
                  }
                
                  warehouse = "%s"
                  temp_write_location = "%s"
                
                  post_ingestion_task_factory_provider {
                    class = "io.dazzleduck.sql.commons.ingestion.DuckLakePostIngestionTaskFactoryProvider"
                    catalog_name = "%s"
                
                    path_to_table_mapping = [
                      {
                        table_name = "%s"
                        schema_name = "main"
                        base_path = "%s"
                      }
                    ]
                  }
                }
                """.formatted(
                warehousePath.toString().replace("\\", "/"),
                warehousePath.resolve("_tmp").toString().replace("\\", "/"),
                DUCKLAKE_CATALOG,
                TEST_TABLE,
                INGEST_PATH
        );

        return ConfigFactory.parseString(override).withFallback(ConfigFactory.load()).resolve();
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

        Path metricDir = Path.of(warehousePath.toString(), INGEST_PATH);
        Path metricFile = waitForMetricFile(metricDir.toString());

        TestUtils.isEqual("""
                        select 'records.processed' as name,
                               'counter'           as type,
                               10.0                as value,
                               'ap101'             as application_id,
                               'MyApplication'     as application_name,
                               'localhost'         as application_host
                        """,
                """
                        select name, type, value, application_id, application_name, application_host
                        from read_parquet('%s')
                        where name = 'records.processed'
                        """.formatted(metricFile.toAbsolutePath())
        );
    }

    @AfterAll
    void cleanup() throws Exception {
        if (runtime != null) {
            runtime.close();
        }
        ConnectionPool.execute("DETACH " + DUCKLAKE_CATALOG);
        String warehousePath = ConfigUtils.getWarehousePath(ConfigFactory.load().getConfig(ConfigUtils.CONFIG_PATH));
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

    private void waitForHttpServer() throws Exception {
        int port = 8081;
        long deadline = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < deadline) {
            try (var socket = new java.net.Socket("localhost", port)) {
                return;
            } catch (IOException e) {
                Thread.sleep(100);
            }
        }
        throw new IllegalStateException("HTTP server did not start");
    }

    private Path waitForMetricFile(String warehousePath) throws Exception {

        Path dir = Path.of(warehousePath);
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            try (var stream = Files.list(dir)) {
                var file = stream.filter(Files::isRegularFile).findFirst();
                if (file.isPresent()) {
                    return file.get();
                }
            }
            Thread.sleep(200);
        }

        throw new IllegalStateException("No metric file found in " + dir);
    }
}
