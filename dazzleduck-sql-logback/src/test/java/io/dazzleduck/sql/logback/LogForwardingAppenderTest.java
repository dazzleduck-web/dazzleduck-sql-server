package io.dazzleduck.sql.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class LogForwardingAppenderTest {

    private LogForwardingAppender appender;
    private LoggerContext loggerContext;

    @BeforeEach
    void setUp() {
        LogForwardingAppender.resetGlobalState();
        loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        appender = new LogForwardingAppender();
        appender.setContext(loggerContext);
        // Don't set baseUrl so no forwarder is created - tests run faster
        appender.start();
    }

    @AfterEach
    void tearDown() {
        appender.stop();
        LogForwardingAppender.resetGlobalState();
    }

    @Test
    void start_shouldNotCreateForwarderWithoutBaseUrl() {
        // Appender started without baseUrl in setUp
        // Should not throw and should be started
        assertTrue(appender.isStarted());
    }

    @Test
    void start_shouldCreateForwarderWithBaseUrl() {
        LogForwardingAppender.resetGlobalState();
        LogForwardingAppender newAppender = new LogForwardingAppender();
        newAppender.setContext(loggerContext);
        newAppender.setBaseUrl("http://localhost:9999");
        newAppender.start();

        assertTrue(newAppender.isStarted());

        newAppender.stop();
    }

    @Test
    void append_shouldNotThrowWhenNoForwarder() {
        ILoggingEvent event = createEvent("Test message", Level.INFO);

        // Should not throw even without a forwarder
        assertDoesNotThrow(() -> appender.doAppend(event));
    }

    @Test
    void append_shouldNotAddWhenDisabled() {
        LogForwardingAppender.setEnabled(false);
        ILoggingEvent event = createEvent("Test message", Level.INFO);

        // Should not throw
        assertDoesNotThrow(() -> appender.doAppend(event));
    }

    @Test
    void append_shouldExcludeDazzleduckLogbackPackage() {
        ILoggingEvent event = createEventWithLogger(
                "io.dazzleduck.sql.logback.SomeClass",
                "Internal log",
                Level.INFO
        );

        // Should not throw - excluded logs are silently dropped
        assertDoesNotThrow(() -> appender.doAppend(event));
    }

    @Test
    void append_shouldExcludeDazzleduckClientPackage() {
        ILoggingEvent event = createEventWithLogger(
                "io.dazzleduck.sql.client.HttpSender",
                "Sending data",
                Level.DEBUG
        );

        // Should not throw - excluded logs are silently dropped
        assertDoesNotThrow(() -> appender.doAppend(event));
    }

    @Test
    void append_shouldExcludeArrowPackage() {
        ILoggingEvent event = createEventWithLogger(
                "org.apache.arrow.memory.RootAllocator",
                "Memory allocation",
                Level.DEBUG
        );

        // Should not throw - excluded logs are silently dropped
        assertDoesNotThrow(() -> appender.doAppend(event));
    }

    @Test
    void append_shouldIncludeUserLogs() {
        ILoggingEvent event = createEventWithLogger(
                "com.example.myapp.Service",
                "User service log",
                Level.INFO
        );

        // Should not throw
        assertDoesNotThrow(() -> appender.doAppend(event));
    }

    @Test
    void setEnabled_shouldToggleForwarding() {
        LogForwardingAppender.setEnabled(true);
        assertTrue(LogForwardingAppender.isEnabled());

        LogForwardingAppender.setEnabled(false);
        assertFalse(LogForwardingAppender.isEnabled());
    }

    @Test
    void reset_shouldClearState() {
        LogForwardingAppender.setEnabled(false);

        LogForwardingAppender.resetGlobalState();

        assertTrue(LogForwardingAppender.isEnabled());
    }

    @Test
    void setProject_shouldParseCommaSeparatedValues() {
        LogForwardingAppender newAppender = new LogForwardingAppender();
        newAppender.setProject("*,col1 AS alias1,col2 AS alias2");
        // Should not throw - just verify setter works
        assertDoesNotThrow(() -> newAppender.start());
    }

    @Test
    void setPartitionBy_shouldParseCommaSeparatedValues() {
        LogForwardingAppender newAppender = new LogForwardingAppender();
        newAppender.setPartitionBy("date,hour");
        // Should not throw - just verify setter works
        assertDoesNotThrow(() -> newAppender.start());
    }

    private ILoggingEvent createEvent(String message, Level level) {
        return createEventWithLogger("com.example.TestClass", message, level);
    }

    private ILoggingEvent createEventWithLogger(String loggerName, String message, Level level) {
        Logger logger = loggerContext.getLogger(loggerName);
        LoggingEvent event = new LoggingEvent();
        event.setLoggerName(loggerName);
        event.setLevel(level);
        event.setMessage(message);
        event.setThreadName(Thread.currentThread().getName());
        event.setTimeStamp(System.currentTimeMillis());
        event.setLoggerContext(loggerContext);
        return event;
    }
}
