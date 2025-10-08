package io.dazzleduck.sql.logger;

import io.dazzleduck.sql.logger.server.SimpleFlightLogServer;
import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.*;

import java.util.concurrent.TimeUnit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ArrowLoggerIntegrationTest {

    private SimpleFlightLogServer server;

    @BeforeAll
    void startServer() throws Exception {
        // Start Flight server on localhost:32010
        server = new SimpleFlightLogServer();
        // Run server in a separate thread
        new Thread(() -> {
            try {
                server.main(new String[]{});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // Give server some time to start
        TimeUnit.SECONDS.sleep(1);
    }

    @AfterAll
    void stopServer() throws Exception {
        // For simplicity, just exit JVM after test or close server if you modify it to support close
    }

    @Test
    void testLoggerSendsLogs() throws Exception {
        // Create logger using default AsyncArrowFlightSender
        ArrowSimpleLogger logger = new ArrowSimpleLogger("integration-test");

        // Write 15 log entries (triggers at least one batch flush)
        for (int i = 0; i < 15; i++) {
            logger.info("Test log entry {}", i);
        }

        // Flush any remaining logs
        logger.flush();

        // Give sender time to send logs to server
        TimeUnit.SECONDS.sleep(2);


    }
}
