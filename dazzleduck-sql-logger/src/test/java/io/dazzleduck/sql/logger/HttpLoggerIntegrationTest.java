package io.dazzleduck.sql.logger;

import io.dazzleduck.sql.commons.util.TestUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HttpLoggerIntegrationTest {

    private static final int PORT = 8081;
    static String warehousePath;

    @BeforeAll
    void startServer() throws Exception {
        warehousePath = "/tmp/log" ;
        new java.io.File(warehousePath).mkdirs();

        io.dazzleduck.sql.runtime.Main.main(new String[]{
                "--conf", "dazzleduck_server.networking_modes=[http]",
                "--conf", "dazzleduck_server.http.port=" + PORT,
                "--conf", "dazzleduck_server.http.auth=jwt",
                "--conf", "dazzleduck_server.warehouse=" + warehousePath,
                "--conf", "dazzleduck_server.ingestion.max_delay_ms=200"
        });
    }

    @Test
    void testLoggerCanPostLogs() throws Exception {
        Logger logger = LoggerFactory.getLogger(HttpLoggerIntegrationTest.class.getName());

        try {
            for (int i = 0; i < 10; i++) {
                logger.info("Test {}", i);
            }
        } finally {
            ArrowSimpleLoggerFactory factory = (ArrowSimpleLoggerFactory) LoggerFactory.getILoggerFactory();
            factory.closeAll();
        }

        // Wait for server-side ingestion

            Path logFile = findFirstLogFile(warehousePath,HttpLoggerIntegrationTest.class.getName());

            TestUtils.isEqual(
                    """
                    select 'INFO'              as level,
                           'io.dazzleduck.sql.logger.HttpLoggerIntegrationTest'  as logger,
                           'main'              as thread,
                           'Test 0'            as message,
                           'ap101'             as applicationId,
                           'MyApplication'     as applicationName,
                           'localhost'         as host
                    """, "select level, logger, thread, message, applicationId, applicationName, host\n" +
                            "from read_parquet('%s') where message = 'Test 0'".formatted(logFile.toAbsolutePath())
            );
    }

    @Test
    void testMultipleLogLevels() throws Exception {
       // ArrowSimpleLogger logger = new ArrowSimpleLogger("multi-level-test");
        Logger logger = LoggerFactory.getLogger("multi-level-test");
        try {
            logger.trace("Trace message");
            logger.debug("Debug message");
            logger.info("Info message");
        } finally {
             ArrowSimpleLoggerFactory factory = (ArrowSimpleLoggerFactory) LoggerFactory.getILoggerFactory();
            factory.closeAll();
        }

        // Verify all levels are persisted correctly
        Path logFile = findFirstLogFile(warehousePath,"multi-level-test");
        TestUtils.isEqual("select 3 as count", "select count(*) as count from read_parquet('%s') where logger = 'multi-level-test'".formatted(logFile.toAbsolutePath())
        );
    }

    @Test
    void testExceptionLogging() throws Exception {
       // ArrowSimpleLogger logger = new ArrowSimpleLogger("exception-test");
        Logger logger = LoggerFactory.getLogger("exception-test");
        try {
            Exception ex = new RuntimeException("Test exception");
            logger.error("Error occurred", ex);
        } finally {
            ArrowSimpleLoggerFactory factory =
                    (ArrowSimpleLoggerFactory) LoggerFactory.getILoggerFactory();
            factory.closeAll();
        }

        Path logFile = findFirstLogFile(warehousePath,"exception-test");
        // Verify stack trace is included in message
        TestUtils.isEqual(
                "select true as has_stacktrace",
                "select message like '%RuntimeException%' and message like '%Test exception%' as has_stacktrace " +
                        "from read_parquet('%s') where logger = 'exception-test'".formatted(logFile.toAbsolutePath())
        );
    }

    @Test
    void testEmptyAndNullMessages() throws Exception {
       // ArrowSimpleLogger logger = new ArrowSimpleLogger("empty-test");
        Logger logger = LoggerFactory.getLogger("empty-test");
        try {
            logger.info("");
            logger.info(null);
        } finally {
            ArrowSimpleLoggerFactory factory = (ArrowSimpleLoggerFactory) LoggerFactory.getILoggerFactory();
            factory.closeAll();
        }
    }
    @Test
    void testParameterizedMessages() throws Exception {
       // ArrowSimpleLogger logger = new ArrowSimpleLogger("param-test");
        Logger logger = LoggerFactory.getLogger("param-test");
        try {
            logger.info("User {} logged in from {}", "john.doe", "192.168.1.1");
        } finally {
            ArrowSimpleLoggerFactory factory = (ArrowSimpleLoggerFactory) LoggerFactory.getILoggerFactory();
            factory.closeAll();
        }

        Path logFile = findFirstLogFile(warehousePath,"param-test");
        TestUtils.isEqual(
                "select 'User john.doe logged in from 192.168.1.1' as message",
                """
                 select trim(message) as message from read_parquet('%s') where logger = 'param-test'
                """.formatted(logFile.toAbsolutePath())
        );
    }
    @Test
    void testConcurrentLogging() throws Exception {
       // ArrowSimpleLogger logger = new ArrowSimpleLogger("concurrent-test");
        Logger logger = LoggerFactory.getLogger("concurrent-test");
        try {
            Thread[] threads = new Thread[5];
            for (int t = 0; t < threads.length; t++) {
                final int threadNum = t;
                threads[t] = new Thread(() -> {
                    for (int i = 0; i < 20; i++) {
                        logger.info("Thread {} message {}", threadNum, i);
                    }
                });
                threads[t].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }
        } finally {
            ArrowSimpleLoggerFactory factory = (ArrowSimpleLoggerFactory) LoggerFactory.getILoggerFactory();
            factory.closeAll();
        }

        Path logFile = findFirstLogFile(warehousePath,"concurrent-test");
        TestUtils.isEqual(
                "select 100 as count",
                "select count(*) as count from read_parquet('%s') where logger = 'concurrent-test'"
                        .formatted(logFile.toAbsolutePath())
        );
    }

    private Path findFirstLogFile(String warehousePath, String loggerName) throws Exception {
        try (var stream = Files.list(Path.of(warehousePath))) {
            for (Path file : stream.filter(p -> p.toString().endsWith(".parquet")).toList()) {
                try {
                    TestUtils.isEqual(
                            "select 1 as ok",
                            "select 1 as ok from read_parquet('%s') where logger = '%s' limit 1"
                                    .formatted(file.toAbsolutePath(), loggerName)
                    );
                    return file; // this file contains the logger
                } catch (AssertionError ignored) {
                    // not the right file, keep scanning
                }
            }
        }
        throw new IllegalStateException(
                "No parquet file found for logger=" + loggerName + " in " + warehousePath);
    }

}