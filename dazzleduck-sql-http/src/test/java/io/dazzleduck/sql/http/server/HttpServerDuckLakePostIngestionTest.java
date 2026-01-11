package io.dazzleduck.sql.http.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static io.dazzleduck.sql.http.server.ContentTypes.APPLICATION_ARROW;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for DuckLake post-ingestion task.
 * Tests the complete flow: ingest -> post-ingestion task -> verify file data in DuckLake.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class HttpServerDuckLakePostIngestionTest {
    private static HttpClient client;

    @TempDir
    private static Path warehousePath;

    @TempDir
    private static Path tempDir;

    private static String detectedDatabase;
    private static String detectedMetadataDatabase;
    private static String ducklakeCatalogPath;
    private static String ducklakeDataPath;
    private static Long logsTableId;
    private static Long metricsTableId;

    private static final int TEST_PORT = 8096;
    private static final String BASE_URL = "http://localhost:" + TEST_PORT;
    private static final String DUCKLAKE_CATALOG = "test_ducklake";
    private static final String TEST_TABLE_LOGS = "test_logs";
    private static final String TEST_TABLE_METRICS = "test_metrics";
    private static final String INGEST_PATH_LOGS = "logs";
    private static final String INGEST_PATH_METRICS = "metrics";

    @BeforeAll
    public static void setup() throws Exception {
        client = HttpClient.newHttpClient();

        ducklakeCatalogPath = tempDir.resolve(DUCKLAKE_CATALOG).toString();
        ducklakeDataPath = tempDir.resolve("ducklake_data").toString();

        // Create ingestion directories
        Files.createDirectories(warehousePath.resolve(INGEST_PATH_LOGS));
        Files.createDirectories(warehousePath.resolve(INGEST_PATH_METRICS));

        // Initialize DuckLake
        setupDuckLake();

        // Construct metadata database name
        String metadataDatabase = "__ducklake_metadata_" + DUCKLAKE_CATALOG;

        String configStr = """
                dazzleduck_server {
                  http {
                    port = %d
                    host = "0.0.0.0"
                    authentication = "none"
                  }
                  warehouse = "%s"
                  temp_write_location = "%s"
                
                  ingestion {
                    max_delay_ms = 500
                  }
                
                  post_ingestion_task_factory_provider {
                    class = "io.dazzleduck.sql.commons.ingestion.DuckLakePostIngestionTaskFactoryProvider"
                    catalog_name = "%s"
                
                    path_to_table_mapping = [
                      {
                        table_name = "%s"
                        schema_name = "main"
                        base_path = "%s"
                      },
                      {
                        table_name = "%s"
                        schema_name = "main"
                        base_path = "%s"
                      }
                    ]
                  }
                }
                """.formatted(
                TEST_PORT,
                warehousePath.toString().replace("\\", "/"),
                warehousePath.resolve("_tmp").toString().replace("\\", "/"),
                DUCKLAKE_CATALOG,
                TEST_TABLE_LOGS,
                INGEST_PATH_LOGS,
                TEST_TABLE_METRICS,
                INGEST_PATH_METRICS
        );

        Config config = ConfigFactory.parseString(configStr).withFallback(ConfigFactory.load()).resolve();
        Main.start(config.getConfig(ConfigUtils.CONFIG_PATH));
        Thread.sleep(500);
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
                    String.format("CREATE TABLE IF NOT EXISTS %s (b VARCHAR, a VARCHAR)", TEST_TABLE_LOGS),
                    String.format("CREATE TABLE IF NOT EXISTS %s (metric_name VARCHAR, value VARCHAR)", TEST_TABLE_METRICS)
            };
            ConnectionPool.executeBatchInTxn(conn, setupQueries);

            // Detect database names
            List<String> dbs = new ArrayList<>();
            ConnectionPool.collectFirstColumn(conn, "SHOW DATABASES", String.class).forEach(dbs::add);
            // Detect metadata database
            detectedMetadataDatabase = dbs.stream()
                    .filter(d -> d != null && d.startsWith("__ducklake_metadata_"))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Metadata DB not found. DBs: " + dbs));

            // Verify constructed metadata database name matches detected
            String constructedMetadataDb = "__ducklake_metadata_" + DUCKLAKE_CATALOG;
            assertEquals(constructedMetadataDb, detectedMetadataDatabase,
                    "Constructed metadata DB name should match detected name");

            // Detect attached database
            detectedDatabase = dbs.stream()
                    .filter(d -> d != null &&
                            !"memory".equalsIgnoreCase(d) &&
                            !d.startsWith("__ducklake_metadata_"))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Attached DB not found. DBs: " + dbs));

            // Get table IDs from metadata
            String getLogsTableIdSql = String.format(
                    "SELECT table_id FROM %s.ducklake_table WHERE table_name = '%s'",
                    detectedMetadataDatabase, TEST_TABLE_LOGS
            );
            logsTableId = ConnectionPool.collectFirst(conn, getLogsTableIdSql, Long.class);
            assertNotNull(logsTableId, "Logs table ID should not be null");

            String getMetricsTableIdSql = String.format(
                    "SELECT table_id FROM %s.ducklake_table WHERE table_name = '%s'",
                    detectedMetadataDatabase, TEST_TABLE_METRICS
            );
            metricsTableId = ConnectionPool.collectFirst(conn, getMetricsTableIdSql, Long.class);
            assertNotNull(metricsTableId, "Metrics table ID should not be null");
        }
    }

    @AfterAll
    public static void cleanup() throws Exception {
        ConnectionPool.execute("DETACH " + DUCKLAKE_CATALOG);
    }

    @Test
    @Order(1)
    public void testDuckLakeSetup() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            // Verify DuckLake catalog is attached
            String checkCatalogSql = "SELECT database_name FROM duckdb_databases() WHERE database_name = '%s'".formatted(DUCKLAKE_CATALOG);
            var catalogs = ConnectionPool.collectFirstColumn(conn, checkCatalogSql, String.class).iterator();
            assertTrue(catalogs.hasNext(), "DuckLake catalog should be attached");
            String catalogName = catalogs.next();
            assertEquals(DUCKLAKE_CATALOG, catalogName, "Catalog name should match");
            assertFalse(catalogs.hasNext(), "Only one DuckLake catalog entry expected");
            // Verify metadata database name is constructed correctly
            String expectedMetadataDb = "__ducklake_metadata_" + DUCKLAKE_CATALOG;
            assertEquals(expectedMetadataDb, detectedMetadataDatabase, "Metadata database name should follow naming convention");
        }
    }

    @Test
    @Order(2)
    public void testIngestionLogsTable() throws Exception {
        String query = "SELECT * FROM (VALUES ('apple', 'fruit'), ('banana', 'fruit'), ('carrot', 'vegetable')) AS t(b, a)";
        byte[] arrowData = generateArrowData(query);

        var request = HttpRequest.newBuilder(URI.create(BASE_URL + "/v1/ingest?path=" + INGEST_PATH_LOGS))
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(arrowData)))
                .header("Content-Type", APPLICATION_ARROW)
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Logs ingestion should succeed");
    }

    @Test
    @Order(3)
    public void testIngestionMetricsTable() throws Exception {
        String query = "SELECT * FROM (VALUES ('cpu_usage', '75.5'), ('memory_usage', '82.3'), ('disk_usage', '45.0')) AS t(metric_name, value)";
        byte[] arrowData = generateArrowData(query);

        var request = HttpRequest.newBuilder(URI.create(BASE_URL + "/v1/ingest?path=" + INGEST_PATH_METRICS))
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(arrowData)))
                .header("Content-Type", APPLICATION_ARROW)
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Metrics ingestion should succeed");
    }

    @Test
    @Order(4)
    public void testLogsDataInDuckLake() throws Exception {
        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator()) {
            ConnectionPool.execute(conn, "USE " + DUCKLAKE_CATALOG);

            // Verify row count
            Long count = ConnectionPool.collectFirst(conn, "SELECT COUNT(*) FROM " + TEST_TABLE_LOGS, Long.class);
            assertEquals(3L, count, "Logs table should contain 3 rows");

            // Verify data correctness
            String expectedSql = "SELECT * FROM (VALUES ('apple', 'fruit'), ('banana', 'fruit'), ('carrot', 'vegetable')) AS t(b, a) ORDER BY b";
            String actualSql = "SELECT b, a FROM %s ORDER BY b".formatted(TEST_TABLE_LOGS);
            TestUtils.isEqual(conn, allocator, expectedSql, actualSql);
        }
    }

    @Test
    @Order(5)
    public void testMetricsDataInDuckLake() throws Exception {
        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator()) {
            ConnectionPool.execute(conn, "USE " + DUCKLAKE_CATALOG);

            // Verify row count
            Long count = ConnectionPool.collectFirst(conn, "SELECT COUNT(*) FROM " + TEST_TABLE_METRICS, Long.class);
            assertEquals(3L, count, "Metrics table should contain 3 rows");

            // Verify data correctness
            String expectedSql = "SELECT * FROM (VALUES ('cpu_usage', '75.5'), ('memory_usage', '82.3'), ('disk_usage', '45.0')) AS t(metric_name, value) ORDER BY metric_name";
            String actualSql = "SELECT metric_name, value FROM %s ORDER BY metric_name".formatted(TEST_TABLE_METRICS);
            TestUtils.isEqual(conn, allocator, expectedSql, actualSql);
        }
    }

    @Test
    @Order(6)
    public void testMultipleIngestionsSameTable() throws Exception {
        // Ingest second batch to logs table
        String query = "SELECT * FROM (VALUES ('orange', 'fruit'), ('potato', 'vegetable')) AS t(b, a)";
        byte[] arrowData = generateArrowData(query);

        var request = HttpRequest.newBuilder(URI.create(BASE_URL + "/v1/ingest?path=" + INGEST_PATH_LOGS))
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(arrowData)))
                .header("Content-Type", APPLICATION_ARROW)
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Second logs ingestion should succeed");

        // Verify total count
        String querySql = String.format("SELECT COUNT(*) FROM %s.%s", DUCKLAKE_CATALOG, TEST_TABLE_LOGS);
        Long rowCount = ConnectionPool.collectFirst(querySql, Long.class);
        assertEquals(5L, rowCount, "Should have 5 rows after second ingestion");
    }

    @Test
    @Order(7)
    public void testDuckLakeFileMetadata() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "USE " + DUCKLAKE_CATALOG);

            // Check logs table metadata
            String logsMetadataSql = String.format("SELECT COUNT(*) FROM %s.ducklake_data_file WHERE table_id = %d", detectedMetadataDatabase, logsTableId);
            Long logsFileCount = ConnectionPool.collectFirst(conn, logsMetadataSql, Long.class);
            assertTrue(logsFileCount >= 1, "Logs table should have at least one file in metadata");

            // Check metrics table metadata
            String metricsMetadataSql = String.format("SELECT COUNT(*) FROM %s.ducklake_data_file WHERE table_id = %d", detectedMetadataDatabase, metricsTableId);
            Long metricsFileCount = ConnectionPool.collectFirst(conn, metricsMetadataSql, Long.class);
            assertTrue(metricsFileCount >= 1, "Metrics table should have at least one file in metadata");
        }
    }

    /**
     * Helper method to generate Arrow IPC data from a query
     */
    private static byte[] generateArrowData(String query) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, query, 1000);
             var outputStream = new ByteArrayOutputStream();
             var writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, outputStream)) {

            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
            }
            writer.end();
            return outputStream.toByteArray();
        }
    }
}