package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.server.auth2.Token;
import org.apache.arrow.flight.FlightServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DuckDBAirportExtensionTest {

    private static final int port = 55556;
    static String url = String.format("jdbc:arrow-flight-sql://localhost:%s/?database=memory&useEncryption=0&user=admin&password=admin", port);
    private static FlightServer flightServer;

    @BeforeAll
    public static void beforeAll() throws IOException, NoSuchAlgorithmException {
        String[] args = {"--conf", "port=" + port, "--conf", "useEncryption=false"};
        flightServer = Main.createServer(args);
        Thread severThread = new Thread(() -> {
            try {
                flightServer.start();
                System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
                flightServer.awaitTermination();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        severThread.start();
    }

    @AfterAll
    public static void afterAll() throws InterruptedException {
        flightServer.close();
    }

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url);
    }

    @Test
    public void duckDBAirportExtensionTest() throws SQLException {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("LOAD airport");
            String createSecretSql = String.format("""
                CREATE SECRET airport_testing (
                    TYPE AIRPORT,
                    AUTH_TOKEN '%s',
                    SCOPE 'grpc+tcp://localhost:55556'
                )
            """, Token.token);
            stmt.execute(createSecretSql);
            String attachSql = """
                        ATTACH 'hello' (
                            TYPE AIRPORT,
                            location 'grpc+tcp://localhost:55556'
                        )
                    """;
            stmt.execute(attachSql);
            stmt.execute("USE hello.static");
        }
    }
}
