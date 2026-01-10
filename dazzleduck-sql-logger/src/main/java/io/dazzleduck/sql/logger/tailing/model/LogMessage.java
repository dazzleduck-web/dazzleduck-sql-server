package io.dazzleduck.sql.logger.tailing.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Model for JSON log message structure
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record LogMessage(
        @JsonProperty("timestamp") String timestamp,
        @JsonProperty("level") String level,
        @JsonProperty("logger") String logger,
        @JsonProperty("thread") String thread,
        @JsonProperty("message") String message,
        @JsonProperty("mdc") String mdc,
        @JsonProperty("marker") String marker
) {
    // Compact constructor for validation
    public LogMessage {
        // Timestamp is required
        if (timestamp == null || timestamp.isBlank()) {
            throw new IllegalArgumentException("timestamp cannot be null or empty");
        }

        // Level defaults to INFO if not provided
        if (level == null || level.isBlank()) {
            level = "INFO";
        }

        // Message defaults to empty string if not provided
        if (message == null) {
            message = "";
        }
    }
}