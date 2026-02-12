package io.dazzleduck.sql.logger;

import io.dazzleduck.sql.client.ArrowProducer;
import io.dazzleduck.sql.common.types.JavaRow;
import org.junit.jupiter.api.*;
import org.slf4j.event.Level;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ArrowSimpleLoggerTest {

    private ArrowSimpleLogger logger;
    private MockArrowProducer producer;

    @BeforeEach
    void setup() {
        producer = new MockArrowProducer();
        logger = new ArrowSimpleLogger("test-logger", producer, Level.INFO, null);
    }

    @Test
    void testLoggerInitialization() {
        assertNotNull(logger);
    }

    @Test
    void testInfoLogIsForwarded() throws Exception {
        logger.info("hello {}", "world");

        assertTrue(producer.awaitRows(1));
        assertEquals(1, producer.getRowCount());
    }

    @Test
    void testMultipleLogsAreForwarded() throws Exception {
        for (int i = 0; i < 5; i++) {
            logger.info("log {}", i);
        }

        assertTrue(producer.awaitRows(5));
        assertEquals(5, producer.getRowCount());
    }

    @Test
    void testLogWithLoggingEvent() throws Exception {
        var event = org.mockito.Mockito.mock(org.slf4j.event.LoggingEvent.class);
        org.mockito.Mockito.when(event.getLevel())
                .thenReturn(org.slf4j.event.Level.INFO);
        org.mockito.Mockito.when(event.getMessage())
                .thenReturn("event-message");
        org.mockito.Mockito.when(event.getMarkers())
                .thenReturn(java.util.Collections.emptyList());

        logger.log(event);

        assertTrue(producer.awaitRows(1));
        assertEquals(1, producer.getRowCount());
    }

    @Test
    void testDebugLogNotForwardedWhenLevelIsInfo() throws Exception {
        logger.debug("debug message");

        // Wait a bit and verify no logs were sent
        Thread.sleep(100);
        assertEquals(0, producer.getRowCount());
    }

    @Test
    void testLogLevelChecks() {
        // Logger is configured with INFO level
        assertFalse(logger.isTraceEnabled());
        assertFalse(logger.isDebugEnabled());
        assertTrue(logger.isInfoEnabled());
        assertTrue(logger.isWarnEnabled());
        assertTrue(logger.isErrorEnabled());
    }

    @Test
    void testNullProducerDoesNotThrow() {
        ArrowSimpleLogger nullProducerLogger = new ArrowSimpleLogger("null-producer", null, Level.INFO, null);

        // Should not throw
        assertDoesNotThrow(() -> nullProducerLogger.info("test message"));
    }

    /**
     * Simple mock that captures rows without using Arrow/Netty buffers.
     */
    static class MockArrowProducer implements ArrowProducer {

        private final List<JavaRow> rows = new CopyOnWriteArrayList<>();

        @Override
        public void addRow(JavaRow row) {
            rows.add(row);
        }

        @Override
        public void enqueue(byte[] input) {
            // Not used in tests
        }

        @Override
        public long getMaxInMemorySize() {
            return 1024 * 1024;
        }

        @Override
        public long getMaxOnDiskSize() {
            return 10 * 1024 * 1024;
        }

        @Override
        public void close() {
            // No resources to close
        }

        int getRowCount() {
            return rows.size();
        }

        boolean awaitRows(int expected) throws InterruptedException {
            // Wait up to 500ms for rows to arrive
            long deadline = System.currentTimeMillis() + 500;
            while (rows.size() < expected && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            return rows.size() >= expected;
        }
    }
}
