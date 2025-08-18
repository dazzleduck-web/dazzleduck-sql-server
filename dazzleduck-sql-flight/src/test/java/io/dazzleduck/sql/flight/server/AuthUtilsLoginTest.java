package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.http.server.Main;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AuthUtilsLoginTest {
    private static final String USER = "tanejagagan@gmail.com|49";
    private static final String PASSWORD = "secret123"; // for httpLogin
    private static final String LOCALHOST = "localhost";
    private static final BufferAllocator clientAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final int HTTP_PORT = 8090;
    private static FlightSqlClient sqlClient;
    private static String warehousePath;

    @BeforeAll
    public static void setup() throws Exception {
        warehousePath = Files.createTempDirectory("duckdb_warehouse_" + AuthUtilsLoginTest.class.getSimpleName()).toString();
        // Fix path escaping for Windows
        String escapedWarehousePath = warehousePath.replace("\\", "\\\\");
        // Start the HTTP login service
        Main.main(new String[]{
                "--conf", "port: " + HTTP_PORT,
                "--conf", "warehousePath: \"" + escapedWarehousePath + "\""
        });

        setUpFlightServerAndClient();
    }

    private static void setUpFlightServerAndClient() throws Exception {
        io.dazzleduck.sql.flight.server.Main.main(new String[]{"--conf", "port=55556", "--conf", "httpLogin=true", "--conf", "useEncryption=false"});
        final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55556);
        sqlClient = new FlightSqlClient(FlightClient.builder(clientAllocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER, PASSWORD, Map.of()))
                .build());
    }

    @Test
    public void testLoginWithHttp() {
        var result = sqlClient.execute("select 1");
        assertNotNull(result);
    }


    @AfterAll
    public static void cleanup() throws Exception {
        if (sqlClient != null) sqlClient.close();
        clientAllocator.close();
    }
}
