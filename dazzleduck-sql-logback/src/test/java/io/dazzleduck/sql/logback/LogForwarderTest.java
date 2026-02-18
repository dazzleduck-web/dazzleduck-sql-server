package io.dazzleduck.sql.logback;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class LogForwarderTest {

    private LogForwarderConfig config;

    @BeforeEach
    void setUp() {
        LogForwardingAppender.resetGlobalState();

        config = LogForwarderConfig.builder()
                .baseUrl("http://localhost:9999")  // Non-existent server for testing
                .username("admin")
                .password("admin")
                .ingestionQueue("test-logs")
                .httpClientTimeout(Duration.ofMillis(100))
                .pollInterval(Duration.ofMillis(100))
                .minBatchSize(100)
                .maxSendInterval(Duration.ofMillis(100))
                .maxInMemorySize(1024 * 1024)
                .maxOnDiskSize(10 * 1024 * 1024)
                .enabled(true)
                .build();
    }

    @AfterEach
    void tearDown() {
        LogForwardingAppender.resetGlobalState();
    }

    @Test
    void constructor_shouldStartImmediately() {
        try (LogForwarder forwarder = new LogForwarder(config)) {
            assertTrue(forwarder.isRunning());
        }
    }

    @Test
    void stop_shouldStopForwarder() {
        try (LogForwarder forwarder = new LogForwarder(config)) {
            assertTrue(forwarder.isRunning());

            forwarder.stop();
            assertFalse(forwarder.isRunning());
        }
    }

    @Test
    void close_shouldCloseAllResources() {
        LogForwarder forwarder = new LogForwarder(config);

        forwarder.close();

        assertFalse(forwarder.isRunning());
    }

    @Test
    void addLogEntry_shouldAcceptEntryWhenRunning() {
        try (LogForwarder forwarder = new LogForwarder(config)) {
            LogEntry entry = createEntry("test message");

            boolean accepted = forwarder.addLogEntry(entry);

            assertTrue(accepted);
        }
    }

    @Test
    void addLogEntry_shouldRejectEntryWhenStopped() {
        try (LogForwarder forwarder = new LogForwarder(config)) {
            forwarder.stop();
            LogEntry entry = createEntry("test message");

            boolean accepted = forwarder.addLogEntry(entry);

            assertFalse(accepted);
        }
    }

    @Test
    void addLogEntry_shouldRejectEntryWhenClosed() {
        LogForwarder forwarder = new LogForwarder(config);
        forwarder.close();
        LogEntry entry = createEntry("test message");

        boolean accepted = forwarder.addLogEntry(entry);

        assertFalse(accepted);
    }

    private LogEntry createEntry(String message) {
        return new LogEntry(
                1,
                Instant.now(),
                "INFO",
                "TestLogger",
                "main",
                message
        );
    }
}
