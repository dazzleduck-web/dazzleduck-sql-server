package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.http.server.Main;
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

public class AuthUtilsLoginTest {
    private static final String USER = "tanejagagan@gmail.com";
    private static final String PASSWORD = "secret123"; // for httpLogin
    private static final String LOCALHOST = "localhost";

    private static final String ORG_HEADER_KEY = "orgId";
    private static final String TEST_ORG = "49";
    private static final BufferAllocator clientAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final int HTTP_PORT = 8090;
    private static FlightSqlClient sqlClient;

    private static final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55556);

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
        io.dazzleduck.sql.flight.server.Main.main(new String[]{"--conf", "port=55556", "--conf", "httpLogin=true", "--conf", "useEncryption=false", "--conf", "jwt.token.claims.headers=[%s]".formatted(ORG_HEADER_KEY)});

        sqlClient = new FlightSqlClient(FlightClient.builder(clientAllocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER, PASSWORD, Map.of(ORG_HEADER_KEY, TEST_ORG)))
                .build());
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
        callHeaders.insert(ORG_HEADER_KEY, TEST_ORG);
        var headerCallOptions = new HeaderCallOption(callHeaders);
        var result = sqlClientWithoutHeader.execute("select 1", headerCallOptions);
        assertNotNull(result);
        var badCallHeaders = new FlightCallHeaders();
        badCallHeaders.insert(ORG_HEADER_KEY, "bad_org");
        assertThrows(FlightRuntimeException.class, () -> sqlClientWithoutHeader.execute("select 1", new HeaderCallOption(badCallHeaders)));
    }


    @AfterAll
    public static void cleanup() throws Exception {
        if (sqlClient != null) sqlClient.close();
        clientAllocator.close();
    }
}
