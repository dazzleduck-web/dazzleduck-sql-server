package io.dazzleduck.sql.client.tailing.model;

import java.util.List;

/**
 * Helper class to group log lines with their source file name
 */
public record LogFileWithLines(String fileName, List<String> lines) {}