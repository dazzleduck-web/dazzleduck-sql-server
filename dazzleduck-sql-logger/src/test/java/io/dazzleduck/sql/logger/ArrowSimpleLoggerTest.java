package io.dazzleduck.sql.logger;

import io.dazzleduck.sql.client.FlightProducer;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ArrowSimpleLoggerTest {

    private ArrowSimpleLogger logger;
    private TestFlightProducer sender;

    @BeforeEach
    void setup() {
        sender = new TestFlightProducer();
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
        var event = org.mockito.Mockito.mock(org.slf4j.event.LoggingEvent.class);
        org.mockito.Mockito.when(event.getLevel())
                .thenReturn(org.slf4j.event.Level.INFO);
        org.mockito.Mockito.when(event.getMessage())
                .thenReturn("event-message");

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

    static class TestFlightProducer extends FlightProducer.AbstractFlightProducer {

        final AtomicInteger rowsReceived = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(10);

        TestFlightProducer() {
            super(
                    2048,
                    1024 * 1024,
                    Duration.ofSeconds(1),

                    new Schema(java.util.List.of(
                            new Field(
                                    "dummy",
                                    FieldType.nullable(new ArrowType.Utf8()),
                                    null
                            )
                    )), Clock.systemUTC(),
                    3,
                    1000,
                    java.util.List.of(),
                    java.util.List.of()
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
        protected void doSend(ProducerElement element) {
            rowsReceived.incrementAndGet();
            latch.countDown();
            // Consume the element to simulate actual usage
            try (org.apache.arrow.memory.BufferAllocator childAllocator =
                    bufferAllocator.newChildAllocator("test-send", 0, Long.MAX_VALUE);
                 java.io.InputStream in = element.read();
                 org.apache.arrow.vector.ipc.ArrowStreamReader reader =
                        new org.apache.arrow.vector.ipc.ArrowStreamReader(in, childAllocator)) {
                while (reader.loadNextBatch()) {
                    // Just iterate through batches
                }
            } catch (Exception ignored) {
            }
        }

        boolean awaitRows(int expected) throws InterruptedException {
            latch.await(2, TimeUnit.SECONDS);
            return rowsReceived.get() >= expected;
        }
    }
}
