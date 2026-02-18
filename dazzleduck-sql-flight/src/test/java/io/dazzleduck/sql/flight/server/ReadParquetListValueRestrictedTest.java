package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for read_parquet in RESTRICTED mode with path-based authorization.
 * Validates that path-based authorization works correctly when querying parquet files.
 */
public class ReadParquetListValueRestrictedTest {

    private static final String TEST_USER = "admin";
    private static final String TEST_CATALOG = "memory";
    private static final String TEST_SCHEMA = "main";
    private static String testDataPath;
    private static String unauthorizedPath;
    private static String partition0File;
    private static String partition1File;
    private static String unauthorizedFile;
    private static ServerClient serverClient;
    private static Location serverLocation;

    @BeforeAll
    public static void setup() throws Exception {
        // Create test data directory
        testDataPath = Files.createTempDirectory("read_parquet_test_").toString();
        unauthorizedPath = Files.createTempDirectory("unauthorized_").toString();

        // Create partition directories
        Path partition0Dir = Path.of(testDataPath, "partition=0");
        Path partition1Dir = Path.of(testDataPath, "partition=1");

        Files.createDirectories(partition0Dir);
        Files.createDirectories(partition1Dir);

        // Create test parquet files
        partition0File = partition0Dir.resolve("data.parquet").toString();
        partition1File = partition1Dir.resolve("data.parquet").toString();
        unauthorizedFile = Path.of(unauthorizedPath, "data.parquet").toString();

        String[] setupSql = {
            "COPY (SELECT 'k00' as key, 'v00' as value, 0 as partition UNION ALL SELECT 'k01', 'v01', 0) TO '%s' (FORMAT parquet)".formatted(partition0File),
            "COPY (SELECT 'k10' as key, 'v10' as value, 1 as partition UNION ALL SELECT 'k11', 'v11', 1) TO '%s' (FORMAT parquet)".formatted(partition1File),
            "COPY (SELECT 'k99' as key, 'v99' as value, 9 as partition) TO '%s' (FORMAT parquet)".formatted(unauthorizedFile)
        };
        ConnectionPool.executeBatch(setupSql);

        // Setup restricted server with path claim for testDataPath
        serverLocation = FlightTestUtils.findNextLocation();
        var flightTestUtils = FlightTestUtils.createForDatabaseSchema(TEST_USER, "password", TEST_CATALOG, TEST_SCHEMA);
        serverClient = flightTestUtils.createRestrictedServerClient(serverLocation, Map.of(
                Headers.HEADER_PATH, testDataPath
        ));
    }

    @AfterAll
    public static void cleanup() throws Exception {
        if (serverClient != null) {
            serverClient.close();
        }

        // Clean up test data directories
        if (testDataPath != null) {
            deleteDirectory(new File(testDataPath));
        }
        if (unauthorizedPath != null) {
            deleteDirectory(new File(unauthorizedPath));
        }
    }

    private static void deleteDirectory(File directory) throws IOException {
        if (directory == null || !directory.exists()) {
            return;
        }
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        if (!directory.delete()) {
            throw new IOException("Failed to delete: " + directory.getAbsolutePath());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testReadParquetSingleFileAuthorized() throws Exception {
        // Query single authorized file
        String query = "SELECT * FROM read_parquet('%s')".formatted(partition0File);
        String expectedQuery = query;

        FlightTestUtils.testQuery(expectedQuery, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testReadParquetSingleFileWithFilter() throws Exception {
        // Query single file with WHERE clause
        String query = "SELECT * FROM read_parquet('%s') WHERE key = 'k00'".formatted(partition0File);
        String expectedQuery = query;

        FlightTestUtils.testQuery(expectedQuery, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testReadParquetGlobPatternAuthorized() throws Exception {
        // Query using glob pattern within authorized path
        String query = "SELECT * FROM read_parquet('%s/partition=*/data.parquet')".formatted(testDataPath);
        String expectedQuery = query;

        FlightTestUtils.testQuery(expectedQuery, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testReadParquetUnauthorizedPath() throws Exception {
        // Query with unauthorized file path should throw exception
        String query = "SELECT * FROM read_parquet('%s')".formatted(unauthorizedFile);

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testReadParquetUnauthorizedGlobPattern() throws Exception {
        // Query with unauthorized glob pattern should throw exception
        String query = "SELECT * FROM read_parquet('%s/*.parquet')".formatted(unauthorizedPath);

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testReadParquetListValueAuthorized() throws Exception {
        // Query using list_value with authorized paths
        String query = "SELECT * FROM read_parquet(list_value('%s', '%s'))".formatted(partition0File, partition1File);
        String expectedQuery = query;

        FlightTestUtils.testQuery(expectedQuery, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testReadParquetListValueWithFilter() throws Exception {
        // Query using list_value with WHERE clause
        String query = "SELECT * FROM read_parquet(list_value('%s', '%s')) WHERE key = 'k00'".formatted(partition0File, partition1File);
        String expectedQuery = query;

        FlightTestUtils.testQuery(expectedQuery, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testReadParquetListValueUnauthorized() throws Exception {
        // Query with list_value containing unauthorized path should throw exception
        String query = "SELECT * FROM read_parquet(list_value('%s', '%s'))".formatted(partition0File, unauthorizedFile);

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testReadParquetListValueAllUnauthorized() throws Exception {
        // Query with list_value containing only unauthorized paths should throw exception
        String query = "SELECT * FROM read_parquet(list_value('%s'))".formatted(unauthorizedFile);

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }
}
