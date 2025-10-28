package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HttpLoginTest {
    private static final String USER = "admin";
    private static final String PASSWORD = "admin"; // for httpLogin
    private static final String CLUSTER_HEADER_KEY = "cluster_id";
    private static final String TEST_CLUSTER = "TEST_CLUSTER";
    private static final BufferAllocator clientAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final int HTTP_PORT = 8080;
    private static FlightSqlClient sqlClient;
    private static Location serverLocation;

    private static ServerClient serverClient;

    @BeforeAll
    public static void setup() throws Exception {
        String warehousePath = Files.createTempDirectory("duckdb_warehouse_" + HttpLoginTest.class.getSimpleName()).toString();
        // Fix path escaping for Windows
        String escapedWarehousePath = warehousePath.replace("\\", "\\\\");
        // Start the HTTP login service
        io.dazzleduck.sql.login.Main.main(new String[]{
                "--conf", "dazzleduck-login-service.http.port: " + HTTP_PORT,
        });
        var confOverload = new String[]{"--conf", "flight-sql.port=55569",
                "--conf", "login.url=\"http://localhost:8080/login\"",
                "--conf", "useEncryption=false",
                "--conf", "jwt.token.generation=false",
                "--conf", "jwt.token.claims.generate.headers=[%s]".formatted(CLUSTER_HEADER_KEY),
                "--conf", "jwt.token.claims.validate.headers=[%s]".formatted(CLUSTER_HEADER_KEY)};
        serverClient = FlightTestUtils.setUpFlightServerAndClient(confOverload, USER, PASSWORD, Map.of(CLUSTER_HEADER_KEY, TEST_CLUSTER));
        sqlClient = serverClient.flightSqlClient();
        serverLocation = serverClient.flightServer().getLocation();
    }


    @Test
    public void testLoginWithHttp() {
        var result = sqlClient.execute("select 1");
        assertNotNull(result);
    }

    @Test
    public void testUnauthorizedWithHttp() {
        // Send a message with correct header. This will result in success
        // Send another request with incorrect header. This will throw exception

        var sqlClientWithoutHeader = new FlightSqlClient(FlightClient.builder(clientAllocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER, PASSWORD, Map.of()))
                .build());
        var callHeaders = new FlightCallHeaders();
        callHeaders.insert(CLUSTER_HEADER_KEY, TEST_CLUSTER);
        var headerCallOptions = new HeaderCallOption(callHeaders);
        var result = sqlClientWithoutHeader.execute("select 1", headerCallOptions);
        assertNotNull(result);
        var badCallHeaders = new FlightCallHeaders();
        badCallHeaders.insert(CLUSTER_HEADER_KEY, "bad_cluster");
        assertThrows(FlightRuntimeException.class, () -> sqlClientWithoutHeader.execute("select 1", new HeaderCallOption(badCallHeaders)));
    }


    @AfterAll
    public static void cleanup() {
        serverClient.close();
    }
}
