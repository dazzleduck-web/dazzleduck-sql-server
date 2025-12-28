package io.dazzleduck.sql.logger;

import io.dazzleduck.sql.common.ingestion.FlightSender;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.jupiter.api.*;
import org.slf4j.event.SubstituteLoggingEvent;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ArrowSimpleLoggerTest {

    private ArrowSimpleLogger logger;
    private TestFlightSender sender;

    @BeforeEach
    void setup() {
        sender = new TestFlightSender();
        logger = new ArrowSimpleLogger("test-logger", sender);
    }

    @AfterEach
    void tearDown() {
        logger.close();
    }

    @Test
    void testLoggerInitialization() {
        assertNotNull(logger);
    }

    @Test
    void testInfoLogIsForwarded() throws Exception {
        logger.info("hello {}", "world");

        assertTrue(sender.awaitRows(1));
        assertEquals(1, sender.rowsReceived.get());
    }

    @Test
    void testMultipleLogsAreForwardedOnClose() throws Exception {
        for (int i = 0; i < 5; i++) {
            logger.info("log {}", i);
        }
        logger.close(); // forces bucket flush
        assertEquals(1, sender.rowsReceived.get());
    }

    @Test
    void testLogWithLoggingEvent() throws Exception {
        var event = new SubstituteLoggingEvent();
        event.setLevel(org.slf4j.event.Level.INFO);
        event.setMessage("event-message");
        event.setLoggerName("test-logger");
        logger.log(event);

        assertTrue(sender.awaitRows(1));
        assertEquals(1, sender.rowsReceived.get());
    }

    @Test
    void testCloseFlushesPendingRows() throws Exception {
        logger.info("before close");

        logger.close();

        assertTrue(sender.rowsReceived.get() >= 1);
    }

    @Test
    void testFormatReplacement() throws Exception {
        var m = ArrowSimpleLogger.class
                .getDeclaredMethod("format", String.class, Object[].class);
        m.setAccessible(true);

        String out = (String) m.invoke(
                logger,
                "A {} B {}",
                new Object[]{"X", 1}
        );

        assertEquals("A X B 1", out);
    }

    static class TestFlightSender extends FlightSender.AbstractFlightSender {

        final AtomicInteger rowsReceived = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(10);

        TestFlightSender() {
            super(
                    1024 * 1024,
                    Duration.ofSeconds(1),

                    new Schema(java.util.List.of(
                            new Field(
                                    "dummy",
                                    FieldType.nullable(new ArrowType.Utf8()),
                                    null
                            )
                    )), Clock.systemUTC()
            );
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
        protected void doSend(SendElement element) {
            rowsReceived.incrementAndGet();
            latch.countDown();
            try {
                element.read().close();
            } catch (Exception ignored) {
            }
        }

        boolean awaitRows(int expected) throws InterruptedException {
            latch.await(2, TimeUnit.SECONDS);
            return rowsReceived.get() >= expected;
        }
    }
}
