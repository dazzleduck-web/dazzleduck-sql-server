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
        LogForwardingAppender.reset();
        loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        appender = new LogForwardingAppender();
        appender.setContext(loggerContext);
        appender.start();
    }

    @AfterEach
    void tearDown() {
        appender.stop();
        LogForwardingAppender.reset();
    }

    @Test
    void append_shouldAddLogToBuffer() {
        ILoggingEvent event = createEvent("Test message", Level.INFO);

        appender.doAppend(event);

        LogBuffer buffer = LogForwardingAppender.getBuffer();
        assertEquals(1, buffer.getSize());
    }

    @Test
    void append_shouldNotAddWhenDisabled() {
        LogForwardingAppender.setEnabled(false);
        ILoggingEvent event = createEvent("Test message", Level.INFO);

        appender.doAppend(event);

        LogBuffer buffer = LogForwardingAppender.getBuffer();
        assertEquals(0, buffer.getSize());
    }

    @Test
    void append_shouldExcludeDazzleduckLogbackPackage() {
        ILoggingEvent event = createEventWithLogger(
                "io.dazzleduck.sql.logback.SomeClass",
                "Internal log",
                Level.INFO
        );

        appender.doAppend(event);

        LogBuffer buffer = LogForwardingAppender.getBuffer();
        assertEquals(0, buffer.getSize());
    }

    @Test
    void append_shouldExcludeDazzleduckClientPackage() {
        ILoggingEvent event = createEventWithLogger(
                "io.dazzleduck.sql.client.HttpSender",
                "Sending data",
                Level.DEBUG
        );

        appender.doAppend(event);

        LogBuffer buffer = LogForwardingAppender.getBuffer();
        assertEquals(0, buffer.getSize());
    }

    @Test
    void append_shouldExcludeArrowPackage() {
        ILoggingEvent event = createEventWithLogger(
                "org.apache.arrow.memory.RootAllocator",
                "Memory allocation",
                Level.DEBUG
        );

        appender.doAppend(event);

        LogBuffer buffer = LogForwardingAppender.getBuffer();
        assertEquals(0, buffer.getSize());
    }

    @Test
    void append_shouldIncludeUserLogs() {
        ILoggingEvent event = createEventWithLogger(
                "com.example.myapp.Service",
                "User service log",
                Level.INFO
        );

        appender.doAppend(event);

        LogBuffer buffer = LogForwardingAppender.getBuffer();
        assertEquals(1, buffer.getSize());
    }

    @Test
    void configure_shouldSetApplicationMetadata() {
        LogForwardingAppender.configure(
                "my-app-id",
                "MyApplication",
                "my-host",
                5000,
                true
        );

        assertTrue(LogForwardingAppender.isEnabled());
    }

    @Test
    void getBuffer_shouldReturnSharedBuffer() {
        LogBuffer buffer1 = LogForwardingAppender.getBuffer();
        LogBuffer buffer2 = LogForwardingAppender.getBuffer();

        assertSame(buffer1, buffer2);
    }

    @Test
    void getBuffer_shouldCreateBufferIfNull() {
        LogForwardingAppender.reset();

        LogBuffer buffer = LogForwardingAppender.getBuffer();

        assertNotNull(buffer);
    }

    @Test
    void setEnabled_shouldToggleForwarding() {
        LogForwardingAppender.setEnabled(true);
        assertTrue(LogForwardingAppender.isEnabled());

        LogForwardingAppender.setEnabled(false);
        assertFalse(LogForwardingAppender.isEnabled());
    }

    @Test
    void append_shouldCaptureLogLevel() {
        ILoggingEvent event = createEvent("Warning message", Level.WARN);

        appender.doAppend(event);

        LogBuffer buffer = LogForwardingAppender.getBuffer();
        var entries = buffer.drain();
        assertEquals(1, entries.size());
        assertEquals("WARN", entries.get(0).level());
    }

    @Test
    void append_shouldCaptureLoggerName() {
        ILoggingEvent event = createEventWithLogger(
                "com.example.MyClass",
                "Test message",
                Level.INFO
        );

        appender.doAppend(event);

        LogBuffer buffer = LogForwardingAppender.getBuffer();
        var entries = buffer.drain();
        assertEquals(1, entries.size());
        assertEquals("com.example.MyClass", entries.get(0).logger());
    }

    @Test
    void append_shouldCaptureThreadName() {
        ILoggingEvent event = createEvent("Test message", Level.INFO);

        appender.doAppend(event);

        LogBuffer buffer = LogForwardingAppender.getBuffer();
        var entries = buffer.drain();
        assertEquals(1, entries.size());
        assertNotNull(entries.get(0).thread());
    }

    @Test
    void append_shouldCaptureMessage() {
        ILoggingEvent event = createEvent("Specific test message content", Level.INFO);

        appender.doAppend(event);

        LogBuffer buffer = LogForwardingAppender.getBuffer();
        var entries = buffer.drain();
        assertEquals(1, entries.size());
        assertEquals("Specific test message content", entries.get(0).message());
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
