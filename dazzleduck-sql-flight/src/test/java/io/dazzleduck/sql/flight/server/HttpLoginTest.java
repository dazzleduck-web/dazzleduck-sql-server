package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HttpLoginTest {
    private static final String USER = "admin";
    private static final String PASSWORD = "admin"; // for httpLogin
    private static final String CLUSTER_HEADER_KEY = "cluster_id";
    private static final String TEST_CLUSTER = "TEST_CLUSTER";
    private static final long ALLOCATOR_LIMIT = 1024 * 1024 * 1024; // 1GB

    private static BufferAllocator clientAllocator;
    private static int httpPort;
    private static int flightPort;
    private static FlightSqlClient sqlClient;
    private static Location serverLocation;
    private static String warehousePath;

    private static ServerClient serverClient;

    @BeforeAll
    public static void setup() throws Exception {
        // Create allocator with reasonable limit
        clientAllocator = new RootAllocator(ALLOCATOR_LIMIT);

        // Create warehouse directory
        warehousePath = Files.createTempDirectory("duckdb_warehouse_" + HttpLoginTest.class.getSimpleName()).toString();

        // Use dynamic port allocation
        httpPort = FlightTestUtils.findFreePortInRange(8000, 9000);
        flightPort = FlightTestUtils.findFreePortInRange(50000, 60000);

        // Start the HTTP login service
        io.dazzleduck.sql.login.Main.main(new String[]{
                "--conf", "dazzleduck_login_service.http.port: " + httpPort,
        });
        Thread.sleep(100);

        var confOverload = new String[]{"--conf", "dazzleduck_server.flight_sql.port=" + flightPort,
                "--conf", "dazzleduck_server.login_url=\"http://localhost:%s/login\"".formatted(httpPort),
                "--conf", "dazzleduck_server.flight_sql.use_encryption=false",
                "--conf", "dazzleduck_server.jwt_token.generation=false",
                "--conf", "dazzleduck_server.jwt_token.claims.generate.headers=[%s]".formatted(CLUSTER_HEADER_KEY),
                "--conf", "dazzleduck_server.jwt_token.claims.validate.headers=[%s]".formatted(CLUSTER_HEADER_KEY)};
        serverClient = FlightTestUtils.setUpFlightServerAndClient(confOverload, USER, PASSWORD, Map.of(CLUSTER_HEADER_KEY, TEST_CLUSTER));
        sqlClient = serverClient.flightSqlClient();
        serverLocation = serverClient.flightServer().getLocation();
    }


    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testLoginWithHttp() {
        var result = sqlClient.execute("select 1");
        assertNotNull(result);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testUnauthorizedWithHttp() throws Exception {
        // Send a message with correct header. This will result in success
        // Send another request with incorrect header. This will throw exception

        try (var sqlClientWithoutHeader = new FlightSqlClient(FlightClient.builder(clientAllocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER, PASSWORD, Map.of()))
                .build())) {
            var callHeaders = new FlightCallHeaders();
            callHeaders.insert(CLUSTER_HEADER_KEY, TEST_CLUSTER);
            var headerCallOptions = new HeaderCallOption(callHeaders);
            var result = sqlClientWithoutHeader.execute("select 1", headerCallOptions);
            assertNotNull(result);
            var badCallHeaders = new FlightCallHeaders();
            badCallHeaders.insert(CLUSTER_HEADER_KEY, "bad_cluster");
            assertThrows(FlightRuntimeException.class, () -> sqlClientWithoutHeader.execute("select 1", new HeaderCallOption(badCallHeaders)));
        }
    }


    @AfterAll
    public static void cleanup() throws Exception {
        // Close server client
        if (serverClient != null) {
            try {
                serverClient.close();
            } catch (Exception e) {
                System.err.println("Error closing serverClient: " + e.getMessage());
            }
        }

        // Close client allocator
        if (clientAllocator != null) {
            try {
                clientAllocator.close();
            } catch (Exception e) {
                System.err.println("Error closing clientAllocator: " + e.getMessage());
            }
        }

        // Clean up warehouse directory
        if (warehousePath != null) {
            try {
                deleteDirectory(new java.io.File(warehousePath));
            } catch (Exception e) {
                System.err.println("Error deleting warehousePath: " + e.getMessage());
            }
        }
    }

    private static void deleteDirectory(java.io.File directory) throws java.io.IOException {
        if (directory == null || !directory.exists()) {
            return;
        }

        if (directory.isDirectory()) {
            java.io.File[] files = directory.listFiles();
            if (files != null) {
                for (java.io.File file : files) {
                    deleteDirectory(file);
                }
            }
        }

        if (!directory.delete()) {
            throw new java.io.IOException("Failed to delete: " + directory.getAbsolutePath());
        }
    }
}
