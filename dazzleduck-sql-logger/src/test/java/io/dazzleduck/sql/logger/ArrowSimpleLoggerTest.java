package io.dazzleduck.sql.logger;

import org.junit.jupiter.api.*;
import org.mockito.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ArrowSimpleLoggerTest {

    @Mock
    private AsyncArrowFlightSender mockSender;

    private ArrowSimpleLogger logger;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);

        // Mock enqueue to always return true
        when(mockSender.enqueue(any())).thenReturn(true);

        logger = new ArrowSimpleLogger("test-logger", mockSender);
    }

    @AfterEach
    void teardown() {
        logger.close();
    }

    @Test
    void testLoggerInitialization_ReadsConfig() {
        assertNotNull(logger);
        assertNotNull(logger.applicationId);
        assertNotNull(logger.applicationName);
        assertNotNull(logger.host);
        assertNotNull(logger.destinationUrl);
    }

    @Test
    void testSingleLogEntry_EnqueueCalled() {
        logger.info("Hello {}", "World");
        assertEquals(1, logger.batchCounter.get());
        verify(mockSender, never()).enqueue(any()); // batch not full yet
    }

    @Test
    void testBatchFlushWhenMaxBatchSizeReached() {
        for (int i = 0; i < 10; i++) {
            logger.info("Log {}", i);
        }
        // After 10 logs, batch should flush
        verify(mockSender, atLeastOnce()).enqueue(any());
        assertEquals(0, logger.batchCounter.get());
    }

    @Test
    void testFlushMethod_SendsRemainingLogs() {
        logger.info("Log1");
        logger.info("Log2");
        assertEquals(2, logger.batchCounter.get());
        logger.flush();
        verify(mockSender, atLeastOnce()).enqueue(any());
        assertEquals(0, logger.batchCounter.get());
    }

    @Test
    void testClose_ShutsDownSchedulerAndFlushes() {
        logger.info("Log before close");
        logger.close();
        verify(mockSender, atLeastOnce()).enqueue(any());
        assertTrue(logger.scheduler.isShutdown());
    }

    @Test
    void testLogWithLoggingEvent() {
        org.slf4j.event.LoggingEvent event = mock(org.slf4j.event.LoggingEvent.class);
        when(event.getLevel()).thenReturn(org.slf4j.event.Level.INFO);
        when(event.getMessage()).thenReturn("Event log");
        logger.log(event);
        assertEquals(1, logger.batchCounter.get());
    }

    @Test
    void testFormat_ReplacesPlaceholders() throws Exception {
        var method = ArrowSimpleLogger.class.getDeclaredMethod("format", String.class, Object[].class);
        method.setAccessible(true);
        String formatted = (String) method.invoke(logger, "Hello {} {}", new Object[]{"World", 123});
        assertEquals("Hello World 123", formatted);
    }

    @Test
    void testWriteArrowAsync_HandlesExceptionGracefully() {
        // simulate enqueue throwing exception
        when(mockSender.enqueue(any())).thenThrow(new RuntimeException("Test enqueue fail"));

        // Write MAX_BATCH_SIZE logs to trigger flush
        for (int i = 0; i < 10; i++) {
            logger.info("Log {}", i);
        }

        // No exception should propagate
        assertEquals(0, logger.batchCounter.get());
    }
}
