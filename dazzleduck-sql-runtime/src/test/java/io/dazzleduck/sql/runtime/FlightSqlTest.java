package io.dazzleduck.sql.runtime;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Disabled
public class FlightSqlTest {
    private static final String USER = "admin";
    private static final String PASSWORD = "admin";
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightSqlTest.class);
    static FixedHostPortGenericContainer<?> flightSqlContainer = new FixedHostPortGenericContainer<>("dazzleduck")
            .withFixedExposedPort(59307, 59307)
            .withCommand("--conf", "useEncryption=false")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .waitingFor(Wait.forLogMessage("Flight Server is up: Listening on .*", 1));
    private static String jdbcUrl;

    @BeforeAll
    static void setUp() throws InterruptedException {
        flightSqlContainer.start();
        Thread.sleep(3000);
        Integer port = 59307;
        String host = flightSqlContainer.getHost();
        jdbcUrl = String.format(
                "jdbc:arrow-flight-sql://%s:%d?database=memory&useEncryption=0&user=%s&password=%s",
                host, port, USER, PASSWORD
        );
    }

    @Test
    void testSelectOne1() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            rs.next();
            int result = rs.getInt(1);
            assertEquals(1, result);
        }
    }
}
