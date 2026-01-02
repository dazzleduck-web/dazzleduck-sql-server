package io.dazzleduck.sql.logger;

import org.junit.jupiter.api.*;
import org.mockito.Mockito;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;

import static org.junit.jupiter.api.Assertions.*;
@Disabled("ArrowSimpleLogger tests temporarily disabled")
class ArrowSimpleLoggerTest {

    private ArrowSimpleLogger logger;

    @BeforeEach
    void setup() {
        logger = new ArrowSimpleLogger("test-logger");
    }

    @AfterEach
    void tearDown() {
        assertDoesNotThrow(() -> logger.close());
    }

    @Test
    void testLoggerInitialization() {
        assertNotNull(logger);
        assertEquals("test-logger", logger.getName());
    }

    @Test
    void testInfoLogDoesNotThrow() {
        assertDoesNotThrow(() ->
                logger.info("hello {}", "world")
        );
    }

    @Test
    void testMultipleLogsAndCloseDoesNotThrow() {
        for (int i = 0; i < 5; i++) {
            logger.info("log {}", i);
        }

        assertDoesNotThrow(() -> logger.close());
    }

//    @Test
//    void testLogWithLoggingEventDoesNotThrow() {
//        LoggingEvent event = Mockito.mock(LoggingEvent.class);
//        Mockito.when(event.getLevel()).thenReturn(Level.INFO);
//        Mockito.when(event.getMessage()).thenReturn("event-message");
//
//        assertDoesNotThrow(() -> logger.log(event));
//    }

    @Test
    void testCloseFlushesPendingRowsSafely() {
        logger.info("before close");

        assertDoesNotThrow(() -> logger.close());
    }

    @Test
    void testFormatReplacementViaReflection() throws Exception {
        var method = ArrowSimpleLogger.class
                .getDeclaredMethod("format", String.class, Object[].class);
        method.setAccessible(true);

        String result = (String) method.invoke(
                null,
                "A {} B {}",
                new Object[]{"X", 1}
        );

        assertEquals("A X B 1", result);
    }
}
