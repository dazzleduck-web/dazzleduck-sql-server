package io.dazzleduck.sql.logger.tailing.model;

import java.util.List;

/**
 * Container for log lines read from a specific file
 */
public record LogFileWithLines(
        String fileName,
        List<String> lines
) {
    public LogFileWithLines {
        if (fileName == null || fileName.isBlank()) {
            throw new IllegalArgumentException("fileName cannot be null or empty");
        }
        if (lines == null) {
            throw new IllegalArgumentException("lines cannot be null");
        }
    }
}