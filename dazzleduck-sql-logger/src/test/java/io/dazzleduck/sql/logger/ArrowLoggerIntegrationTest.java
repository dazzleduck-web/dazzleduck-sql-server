package io.dazzleduck.sql.logger;

import io.dazzleduck.sql.logger.server.SimpleFlightLogServer;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ArrowLoggerIntegrationTest {

    private SimpleFlightLogServer server;

    @BeforeAll
    void startServer() throws Exception {
        server = new SimpleFlightLogServer();
        Thread t = new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        });

        t.setDaemon(true);
        t.start();
        TimeUnit.SECONDS.sleep(1);
    }

    @AfterAll
    void stopServer() throws Exception {
        server.stop();
    }

    @Test
    void testLoggerSendsLogs() throws Exception {
        ArrowSimpleLogger logger = new ArrowSimpleLogger("integration-test");

        int totalLogs = 15;
        for (int i = 0; i < totalLogs; i++) {
            logger.info("Test log entry {}", i);
        }

        logger.flush();
        TimeUnit.SECONDS.sleep(2);
        List<String> logs = server.getReceivedLogs();
        Assertions.assertFalse(logs.isEmpty(), "Server should receive logs from logger");
        Assertions.assertEquals(totalLogs, logs.size(), "Server must receive all log entries");
        Assertions.assertTrue(logs.get(0).contains("Test log entry 0"));
        Assertions.assertTrue(logs.get(14).contains("Test log entry 14"));
    }
}
