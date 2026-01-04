package io.dazzleduck.sql.logback;


import ch.qos.logback.classic.spi.ILoggingEvent;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Represents a single log entry with all required metadata for forwarding.
 */
public record LogEntry(
        @JsonProperty("timestamp") Instant timestamp,
        @JsonProperty("level") String level,
        @JsonProperty("logger") String logger,
        @JsonProperty("thread") String thread,
        @JsonProperty("message") String message,
        @JsonProperty("application_id") String applicationId,
        @JsonProperty("application_name") String applicationName,
        @JsonProperty("application_host") String applicationHost
) {
    /**
     * Factory method to create LogEntry from Logback event.
     */
    public static LogEntry from(
            ILoggingEvent event,
            String applicationId,
            String applicationName,
            String applicationHost) {
        return new LogEntry(
                Instant.ofEpochMilli(event.getTimeStamp()),
                event.getLevel().toString(),
                event.getLoggerName(),
                event.getThreadName(),
                event.getFormattedMessage(),
                applicationId,
                applicationName,
                applicationHost
        );
    }
}
