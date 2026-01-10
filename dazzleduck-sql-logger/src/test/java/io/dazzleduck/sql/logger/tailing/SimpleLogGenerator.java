package io.dazzleduck.sql.logger.tailing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Utility for generating JSON log files for testing.
 * Writes JSON directly without using SLF4J/Logback to avoid JVM context issues.
 */
public final class SimpleLogGenerator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SimpleLogGenerator() {
        // utility class
    }

    /**
     * Generate multiple log files with specified number of logs per file.
     *
     * @param logDir      directory to create log files in
     * @param numFiles    number of log files to create
     * @param logsPerFile number of log entries per file
     * @throws IOException if file operations fail
     */
    public static void generateLogs(Path logDir, int numFiles, int logsPerFile) throws IOException {
        Files.createDirectories(logDir);

        for (int fileIndex = 1; fileIndex <= numFiles; fileIndex++) {
            Path logFile = logDir.resolve("app-" + fileIndex + ".log");
            String loggerName = "App-" + fileIndex;

            try (BufferedWriter writer = Files.newBufferedWriter(logFile,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                for (int i = 1; i <= logsPerFile; i++) {
                    String jsonLine = createLogEntry(loggerName, "INFO", "log-" + i);
                    writer.write(jsonLine);
                    writer.newLine();
                }
            }
        }
    }

    /**
     * Append a single log entry to an existing file.
     *
     * @param logFile path to the log file
     * @throws IOException if file operations fail
     */
    public static void appendLog(Path logFile) throws IOException {
        Files.createDirectories(logFile.getParent());

        try (BufferedWriter writer = Files.newBufferedWriter(logFile,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            String jsonLine = createLogEntry(
                    SimpleLogGenerator.class.getSimpleName(),
                    "INFO",
                    "Single log entry at " + Instant.now()
            );
            writer.write(jsonLine);
            writer.newLine();
        }
    }

    /**
     * Create a JSON log entry string.
     */
    private static String createLogEntry(String logger, String level, String message) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("timestamp", Instant.now().toString());
        node.put("level", level);
        node.put("logger", logger);
        node.put("thread", Thread.currentThread().getName());
        node.put("message", message);
        return node.toString();
    }

    /**
     * Main method for command-line usage (backward compatibility).
     */
    public static void main(String[] args) throws Exception {
        if (args.length >= 3) {
            // multi-file mode
            Path logDir = Path.of(args[0]);
            int numFiles = Integer.parseInt(args[1]);
            int logsPerFile = Integer.parseInt(args[2]);
            generateLogs(logDir, numFiles, logsPerFile);
        } else if (args.length >= 1) {
            // single-file mode
            Path logFile = Path.of(args[0]);
            appendLog(logFile);
        } else {
            System.err.println("Usage:");
            System.err.println("  java SimpleLogGenerator <filePath>");
            System.err.println("  java SimpleLogGenerator <logDir> <numFiles> <logsPerFile>");
            System.exit(1);
        }
    }
}
