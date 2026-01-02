package io.dazzleduck.sql.client.tailing.E2E;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.client.tailing.model.LogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generates realistic log entries in JSON format for testing
 */
public class LogFileGenerator {

    private static final Logger logger = LoggerFactory.getLogger(LogFileGenerator.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static final String[] LOG_LEVELS = {"INFO", "DEBUG", "WARN", "ERROR"};
    private static final String[] LOGGERS = {
            "com.example.ServiceA",
            "com.example.ServiceB",
            "com.example.DatabasePool",
            "com.example.CacheManager",
            "com.example.ApiController"
    };
    private static final String[] MESSAGES = {
            "Processing request",
            "Database query executed successfully",
            "Cache hit for key",
            "HTTP request received",
            "Transaction completed",
            "Connection pool status check",
            "User authentication successful",
            "Data validation passed",
            "Background job started",
            "Metrics collected"
    };

    private final Path logDirectory;
    private final long maxFileSize;
    private final Random random;
    private final AtomicInteger fileCounter;
    private final AtomicInteger lineCounter;

    public LogFileGenerator(Path logDirectory, long maxFileSize) {
        this.logDirectory = logDirectory;
        this.maxFileSize = maxFileSize;
        this.random = new Random();
        this.fileCounter = new AtomicInteger(1);
        this.lineCounter = new AtomicInteger(0);
    }

    /**
     * Generate log entries continuously until stopped
     */
    public void generateLogs(int entriesPerBatch, long intervalMillis) throws IOException, InterruptedException {
        logger.info("Starting log generation: {} entries every {}ms", entriesPerBatch, intervalMillis);

        Path currentFile = createNewLogFile();
        BufferedWriter writer = Files.newBufferedWriter(currentFile, StandardOpenOption.APPEND);

        try {
            while (!Thread.currentThread().isInterrupted()) {
                for (int i = 0; i < entriesPerBatch; i++) {
                    String logEntry = generateLogEntry();
                    writer.write(logEntry);
                    writer.newLine();
                    lineCounter.incrementAndGet();
                }
                writer.flush();

                // Check if file size exceeded
                if (Files.size(currentFile) >= maxFileSize) {
                    logger.info("File size limit reached for {}, creating new file", currentFile.getFileName());
                    writer.close();
                    currentFile = createNewLogFile();
                    writer = Files.newBufferedWriter(currentFile, StandardOpenOption.APPEND);
                }

                Thread.sleep(intervalMillis);
            }
        } finally {
            writer.close();
        }

        logger.info("Log generation stopped. Total lines generated: {}", lineCounter.get());
    }

    private Path createNewLogFile() throws IOException {
        String fileName = String.format("app-%03d.log", fileCounter.getAndIncrement());
        Path filePath = logDirectory.resolve(fileName);
        Files.createFile(filePath);
        logger.info("Created new log file: {}", fileName);
        return filePath;
    }

    private String generateLogEntry() {
        String timestamp = LocalDateTime.now().format(formatter);
        String level = LOG_LEVELS[random.nextInt(LOG_LEVELS.length)];
        String thread = "thread-" + random.nextInt(10);
        String loggerName = LOGGERS[random.nextInt(LOGGERS.length)];
        String message = MESSAGES[random.nextInt(MESSAGES.length)] + " [id=" + random.nextInt(1000) + "]";

        LogMessage logMessage = new LogMessage(timestamp, level, thread, loggerName, message);

        try {
            return mapper.writeValueAsString(logMessage);
        } catch (IOException e) {
            logger.error("Failed to serialize log message", e);
            return "";
        }
    }

    public int getTotalLinesGenerated() {
        return lineCounter.get();
    }

    public int getCurrentFileNumber() {
        return fileCounter.get() - 1;
    }
}