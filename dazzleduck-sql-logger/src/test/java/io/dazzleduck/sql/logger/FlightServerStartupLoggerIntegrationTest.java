package io.dazzleduck.sql.logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.util.TestUtils;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FlightServerStartupLoggerIntegrationTest {

    private Thread serverThread;

    @BeforeAll
    void startServers() throws Exception {
        Config config = ConfigFactory.load().getConfig(ConfigUtils.CONFIG_PATH);
        serverThread = new Thread(() -> {
            try {
                io.dazzleduck.sql.runtime.Main.start(config);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
        Thread.sleep(3000);
    }

    @Test
    void testLoggerCanPostLogs() throws Exception {
        ArrowSimpleLogger logger = new ArrowSimpleLogger("integration-test");

        for (int i = 0; i < 10; i++) {
            logger.info("Test {}", i);
        }

        logger.flush();
        Thread.sleep(2000);
        String warehousePath = ConfigUtils.getWarehousePath(
                ConfigFactory.load().getConfig(ConfigUtils.CONFIG_PATH)
        );

        Path logFile = findFirstLogFile(warehousePath);
        TestUtils.isEqual(
                """
                select 'INFO'              as level,
                       'integration-test'  as logger,
                       'main'              as thread,
                       'Test 0'            as message,
                       'ap101'             as applicationId,
                       'MyApplication'     as applicationName,
                       'localhost'         as host
                """, "select level, logger, thread, message, applicationId, applicationName, host\n" +
                        "from read_parquet('%s') limit 1".formatted(logFile.toAbsolutePath())
        );
    }

    @AfterAll
    void cleanupAndStop() throws Exception {
        cleanupWarehouse();
    }

    private void cleanupWarehouse() throws IOException {
        String warehousePath = ConfigUtils.getWarehousePath(
                ConfigFactory.load().getConfig(ConfigUtils.CONFIG_PATH)
        );

        Path path = Path.of(warehousePath);
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {}
                    });
        }
        Files.createDirectories(path);
    }
    private Path findFirstLogFile(String warehousePath) throws IOException {
        Path logsDir = Path.of(warehousePath, "logs");
        try (var stream = Files.list(logsDir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "No log file found in " + logsDir));
        }
    }

    @Test
    void testLoggerCanPostLogs1() throws Exception {
        ArrowSimpleLogger logger = new ArrowSimpleLogger("integration-test");

        for (int i = 0; i < 18; i++) {
            logger.info("Test {}", i);
        }

        logger.flush();
        Thread.sleep(2000);}
}
