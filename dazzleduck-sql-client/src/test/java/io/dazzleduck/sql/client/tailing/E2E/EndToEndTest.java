package io.dazzleduck.sql.client.tailing.E2E;

import io.dazzleduck.sql.client.HttpSender;
import io.dazzleduck.sql.client.tailing.JsonToArrowConverter;
import io.dazzleduck.sql.client.tailing.LogTailToArrowProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;

/**
 * End-to-end test for log tail to Arrow processing pipeline
 *
 * Test flow:
 * 1. Start mock HTTP server
 * 2. Create temporary log directory
 * 3. Start log generator (creates files and writes logs)
 * 4. Start log processor (monitors directory, converts to Arrow, sends to server)
 * 5. Monitor and report statistics
 * 6. Clean up
 */
public class EndToEndTest {

    private static final Logger logger = LoggerFactory.getLogger(EndToEndTest.class);

    // Test configuration
    private static final int SERVER_PORT = 8099;
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "admin";

    private static final String APPLICATION_ID = "test-app-001";
    private static final String APPLICATION_NAME = "E2ETestApp";
    private static final String APPLICATION_HOST = "localhost";

    private static final long MAX_FILE_SIZE = 5 * 1024; // 5 KB (small for testing)
    private static final int LOG_ENTRIES_PER_BATCH = 10;
    private static final long LOG_GENERATION_INTERVAL_MS = 2000; // Generate logs every 2 seconds
    private static final long PROCESSOR_POLL_INTERVAL_MS = 10000; // Process every 10 seconds
    private static final long TEST_DURATION_MS = 60000; // Run for 60 seconds

    public static void main(String[] args) throws Exception {
        logger.info("═══════════════════════════════════════════════════════");
        logger.info("   DazzleDuck End-to-End Test Starting");
        logger.info("═══════════════════════════════════════════════════════");

        Path testLogDir = null;
        LogFileGenerator logGenerator = null;
        LogTailToArrowProcessor processor = null;
        Thread logGeneratorThread = null;
        Path warehouseDir = null;

        try {
            // Step 1: Create temporary log directory
            testLogDir = Files.createTempDirectory("dazzleduck-e2e-test-");
            logger.info("✓ Created test log directory: {}", testLogDir);
            warehouseDir = Files.createTempDirectory("dd-e2e-warehouse");

            String warehousePath = warehouseDir.toAbsolutePath().toString().replace("\\", "\\\\");
            io.dazzleduck.sql.runtime.Main.main(new String[]{
                    "--conf", "dazzleduck_server.http.port=" + SERVER_PORT,
                    "--conf", "dazzleduck_server.http.auth=jwt",
                    "--conf", "dazzleduck_server.warehouse=\"" + warehousePath + "\"",
                    "--conf", "dazzleduck_server.ingestion.max_delay_ms=500"
            });
            logger.info("✓ HTTP server started on port {}", SERVER_PORT);

            // Step 3: Initialize log generator
            logGenerator = new LogFileGenerator(testLogDir, MAX_FILE_SIZE);
            final LogFileGenerator finalLogGenerator = logGenerator;

            logGeneratorThread = new Thread(() -> {
                try {
                    finalLogGenerator.generateLogs(LOG_ENTRIES_PER_BATCH, LOG_GENERATION_INTERVAL_MS);
                } catch (InterruptedException e) {
                    logger.info("Log generator stopped");
                } catch (IOException e) {
                    logger.error("Error in log generator", e);
                }
            }, "log-generator");

            logGeneratorThread.setDaemon(true);
            logGeneratorThread.start();
            logger.info("✓ Log generator started ({}ms interval, {} entries per batch)",
                    LOG_GENERATION_INTERVAL_MS, LOG_ENTRIES_PER_BATCH);

            // Wait a bit for first log file to be created
            Thread.sleep(1000);

            // Step 4: Initialize and start log processor
            JsonToArrowConverter converter = new JsonToArrowConverter(
                    APPLICATION_ID,
                    APPLICATION_NAME,
                    APPLICATION_HOST
            );

            HttpSender httpSender = new HttpSender(
                    converter.getSchema(),
                    "http://localhost:" + SERVER_PORT,
                    USERNAME,
                    PASSWORD,
                    "test_logs",
                    Duration.ofSeconds(5),
                    1024, // 1 KB min batch
                    Duration.ofSeconds(10),
                    10 * 1024 * 1024, // 10 MB in memory
                    100 * 1024 * 1024 // 100 MB on disk
            );

            processor = new LogTailToArrowProcessor(
                    testLogDir.toString(),
                    "*.log",
                    converter,
                    httpSender,
                    PROCESSOR_POLL_INTERVAL_MS
            );

            processor.start();
            logger.info("✓ Log processor started ({}ms poll interval)", PROCESSOR_POLL_INTERVAL_MS);

            logger.info("═══════════════════════════════════════════════════════");
            logger.info("   Test Running - Duration: {}s", TEST_DURATION_MS / 1000);
            logger.info("═══════════════════════════════════════════════════════");

            // Step 5: Monitor progress
            long startTime = System.currentTimeMillis();
            int lastIngestCount = 0;
            int lastRecordCount = 0;

            while (System.currentTimeMillis() - startTime < TEST_DURATION_MS) {
                Thread.sleep(10000); // Report every 10 seconds

                int filesGenerated = finalLogGenerator.getCurrentFileNumber();
                int linesGenerated = finalLogGenerator.getTotalLinesGenerated();

                logger.info("📊 Status Update:");
                logger.info("   - Log Files Created: {}", filesGenerated);
                logger.info("   - Log Lines Generated: {}", linesGenerated);
            }

            // Step 6: Final statistics
            logger.info("═══════════════════════════════════════════════════════");
            logger.info("   Test Complete - Final Statistics");
            logger.info("═══════════════════════════════════════════════════════");
            logger.info("📈 Generation Stats:");
            logger.info("   - Total Log Files Created: {}", finalLogGenerator.getCurrentFileNumber());
            logger.info("   - Total Log Lines Generated: {}", finalLogGenerator.getTotalLinesGenerated());
            logger.info("");
            logger.info("📈 Server Stats:");
            logger.info("");

            // Validation
            int linesGenerated = finalLogGenerator.getTotalLinesGenerated();

        } catch (Exception e) {
            logger.error("Test failed with exception", e);
            throw e;
        } finally {
            processor.close();
            System.exit(0);
            // Cleanup
            logger.info("Cleaning up...");

            if (logGeneratorThread != null) {
                logGeneratorThread.interrupt();
            }

            if (processor != null) {
                processor.close();
                logger.info("✓ Processor closed");
            }

            if (testLogDir != null && Files.exists(testLogDir)) {
                Files.walk(testLogDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                logger.warn("Failed to delete {}", path, e);
                            }
                        });
                logger.info("✓ Test directory cleaned up");
            }

            logger.info("═══════════════════════════════════════════════════════");
            logger.info("   End-to-End Test Finished");
            logger.info("═══════════════════════════════════════════════════════");
        }
    }
}