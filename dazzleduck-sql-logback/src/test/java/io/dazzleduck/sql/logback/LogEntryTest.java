package io.dazzleduck.sql.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class LogEntryTest {

    @Test
    void from_shouldCreateLogEntryFromEvent() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = context.getLogger("com.example.TestLogger");

        LoggingEvent event = new LoggingEvent();
        event.setLoggerName("com.example.TestLogger");
        event.setLevel(Level.ERROR);
        event.setMessage("Test error message");
        event.setThreadName("test-thread");
        event.setTimeStamp(1705312200000L); // 2024-01-15T10:30:00Z

        LogEntry entry = LogEntry.from(event, "app-id-1", "TestApp", "host-1");

        assertEquals(Instant.ofEpochMilli(1705312200000L), entry.timestamp());
        assertEquals("ERROR", entry.level());
        assertEquals("com.example.TestLogger", entry.logger());
        assertEquals("test-thread", entry.thread());
        assertEquals("Test error message", entry.message());
        assertEquals("app-id-1", entry.applicationId());
        assertEquals("TestApp", entry.applicationName());
        assertEquals("host-1", entry.applicationHost());
    }

    @Test
    void record_shouldSupportEquality() {
        Instant now = Instant.now();

        LogEntry entry1 = new LogEntry(now, "INFO", "Logger1", "main", "msg", "app1", "App1", "host1");
        LogEntry entry2 = new LogEntry(now, "INFO", "Logger1", "main", "msg", "app1", "App1", "host1");
        LogEntry entry3 = new LogEntry(now, "ERROR", "Logger1", "main", "msg", "app1", "App1", "host1");

        assertEquals(entry1, entry2);
        assertNotEquals(entry1, entry3);
    }

    @Test
    void record_shouldGenerateHashCode() {
        Instant now = Instant.now();

        LogEntry entry1 = new LogEntry(now, "INFO", "Logger1", "main", "msg", "app1", "App1", "host1");
        LogEntry entry2 = new LogEntry(now, "INFO", "Logger1", "main", "msg", "app1", "App1", "host1");

        assertEquals(entry1.hashCode(), entry2.hashCode());
    }

    @Test
    void record_shouldGenerateToString() {
        LogEntry entry = new LogEntry(
                Instant.parse("2024-01-15T10:30:00Z"),
                "INFO",
                "TestLogger",
                "main",
                "Test message",
                "app-1",
                "TestApp",
                "localhost"
        );

        String toString = entry.toString();

        assertTrue(toString.contains("INFO"));
        assertTrue(toString.contains("TestLogger"));
        assertTrue(toString.contains("Test message"));
    }

    @Test
    void record_shouldAllowNullValues() {
        LogEntry entry = new LogEntry(null, null, null, null, null, null, null, null);

        assertNull(entry.timestamp());
        assertNull(entry.level());
        assertNull(entry.logger());
        assertNull(entry.thread());
        assertNull(entry.message());
        assertNull(entry.applicationId());
        assertNull(entry.applicationName());
        assertNull(entry.applicationHost());
    }
}
