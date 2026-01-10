package io.dazzleduck.sql.logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Arrow SLF4J Logger
 */
public class ArrowLoggerIntegrationTest {

    private Logger logger;

    @BeforeEach
    void setUp() {
        logger = LoggerFactory.getLogger(ArrowLoggerIntegrationTest.class);
        MDC.clear();
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void testBasicLogging() {
        logger.trace("Trace message");
        logger.debug("Debug message");
        logger.info("Info message");
        logger.warn("Warning message");
        logger.error("Error message");
    }

    @Test
    void testParameterizedLogging() {
        String user = "john_doe";
        int count = 42;

        logger.info("User {} performed {} actions", user, count);
        logger.error("Failed to process user {} with count {}", user, count);
    }

    @Test
    void testLoggingWithException() {
        Exception ex = new RuntimeException("Test exception");
        logger.error("An error occurred", ex);
    }

    @Test
    void testMDCLogging() {
        MDC.put("request_id", "req-12345");
        MDC.put("user_id", "user-67890");
        MDC.put("session_id", "sess-abcde");

        logger.info("Processing request with MDC context");

        // Verify MDC values are still present
        assertEquals("req-12345", MDC.get("request_id"));
        assertEquals("user-67890", MDC.get("user_id"));
        assertEquals("sess-abcde", MDC.get("session_id"));

        MDC.remove("session_id");
        assertNull(MDC.get("session_id"));
    }

    @Test
    void testMarkerLogging() {
        Marker importantMarker = MarkerFactory.getMarker("IMPORTANT");
        Marker securityMarker = MarkerFactory.getMarker("SECURITY");

        logger.info(importantMarker, "This is an important message");
        logger.warn(securityMarker, "Security event detected");
    }

    @Test
    void testNestedMDC() {
        MDC.put("operation", "batch_process");

        for (int i = 0; i < 5; i++) {
            MDC.put("batch_item", String.valueOf(i));
            logger.info("Processing batch item");
            MDC.remove("batch_item");
        }

        assertEquals("batch_process", MDC.get("operation"));
    }

    @Test
    void testMultithreadedLogging() throws InterruptedException {
        int threadCount = 10;
        int messagesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    MDC.put("thread_id", "thread-" + threadId);

                    for (int i = 0; i < messagesPerThread; i++) {
                        logger.info("Message {} from thread {}", i, threadId);
                    }
                } finally {
                    MDC.clear();
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Logging should complete within timeout");
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void testLogLevelFiltering() {
        // Assuming INFO level is configured
        assertTrue(logger.isInfoEnabled());
        assertTrue(logger.isWarnEnabled());
        assertTrue(logger.isErrorEnabled());

        // Trace and Debug might be disabled depending on configuration
        logger.trace("This trace may be filtered");
        logger.debug("This debug may be filtered");
    }

    @Test
    void testMDCThreadIsolation() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        // Thread 1
        executor.submit(() -> {
            try {
                MDC.put("thread_name", "thread-1");
                MDC.put("value", "value-1");
                Thread.sleep(100);

                assertEquals("thread-1", MDC.get("thread_name"));
                assertEquals("value-1", MDC.get("value"));

                logger.info("Thread 1 logging");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                MDC.clear();
                latch.countDown();
            }
        });

        // Thread 2
        executor.submit(() -> {
            try {
                MDC.put("thread_name", "thread-2");
                MDC.put("value", "value-2");
                Thread.sleep(100);

                assertEquals("thread-2", MDC.get("thread_name"));
                assertEquals("value-2", MDC.get("value"));

                logger.info("Thread 2 logging");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                MDC.clear();
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        executor.shutdown();
    }

    @Test
    void testLargeMessage() {
        StringBuilder largeMessage = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeMessage.append("Line ").append(i).append(": This is a test message. ");
        }

        logger.info("Large message: {}", largeMessage.toString());
    }

    @Test
    void testSpecialCharactersInMDC() {
        MDC.put("json_field", "{\"key\": \"value\"}");
        MDC.put("special_chars", "Line1\nLine2\tTabbed");
        MDC.put("unicode", "Hello 世界 🌍");

        logger.info("Testing special characters in MDC");

        assertEquals("{\"key\": \"value\"}", MDC.get("json_field"));
    }

    @Test
    void testLoggerFactoryReuse() {
        Logger logger1 = LoggerFactory.getLogger("test.logger");
        Logger logger2 = LoggerFactory.getLogger("test.logger");

        // Should return the same instance
        assertSame(logger1, logger2);

        logger1.info("Test message 1");
        logger2.info("Test message 2");
    }
}