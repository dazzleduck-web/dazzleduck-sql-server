package io.dazzleduck.sql.logger.tailing;

import io.dazzleduck.sql.client.HttpProducer;
import io.dazzleduck.sql.logger.tailing.model.LogFileWithLines;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Main processor that continuously:
 * 1. Monitors a directory for log files (tracking new file creation)
 * 2. Reads new log lines from all matching files (tracking byte position per file)
 * 3. Converts JSON to Apache Arrow
 * 4. Sends to server via HttpSender
 */
public final class LogTailToArrowProcessor implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LogTailToArrowProcessor.class);

    private final LogFileTailReader tailReader;
    private final JsonToArrowConverter arrowConverter;
    private final HttpProducer httpSender;
    private final ScheduledExecutorService scheduler;
    private final long pollIntervalMs;

    private volatile boolean running = false;

    /**
     * Create processor for directory monitoring
     *
     * @param logDirectory   Directory containing log files
     * @param filePattern    Glob pattern to match files (e.g., "*.log")
     * @param arrowConverter JsonToArrowConverter
     * @param httpSender     HTTP sender for Arrow data
     * @param pollIntervalMs Polling interval in milliseconds
     */
    public LogTailToArrowProcessor(
            String logDirectory,
            String filePattern,
            JsonToArrowConverter arrowConverter,
            HttpProducer httpSender,
            long pollIntervalMs
    ) {
        Objects.requireNonNull(logDirectory, "logDirectory must not be null");
        Objects.requireNonNull(filePattern, "filePattern must not be null");
        Objects.requireNonNull(arrowConverter, "arrowConverter must not be null");
        Objects.requireNonNull(httpSender, "httpSender must not be null");

        if (pollIntervalMs <= 0) {
            throw new IllegalArgumentException("pollIntervalMs must be positive");
        }

        this.tailReader = new LogFileTailReader(logDirectory, filePattern);
        this.arrowConverter = arrowConverter;
        this.httpSender = httpSender;
        this.pollIntervalMs = pollIntervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "log-tail-processor");
            t.setDaemon(true);
            return t;
        });

        logger.info("Initialized LogTailToArrowProcessor: directory={}, pattern={}, pollInterval={}ms",
                logDirectory, filePattern, pollIntervalMs);
    }

    /**
     * Start continuous processing
     */
    public void start() {
        if (running) {
            logger.warn("Processor already running");
            return;
        }

        running = true;
        logger.info("Starting log tail processor with {}ms poll interval", pollIntervalMs);

        scheduler.scheduleAtFixedRate(
                this::processNewLogs,
                0,
                pollIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Stop processing
     */
    public void stop() {
        if (!running) {
            return;
        }

        logger.info("Stopping log tail processor");
        running = false;

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Scheduler did not terminate gracefully, forcing shutdown");
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("Processor stopped");
    }

    private void processNewLogs() {
        try {
            List<LogFileWithLines> filesWithLines = tailReader.readNewLinesGroupedByFile();

            if (filesWithLines.isEmpty()) {
                return;
            }

            int totalLines = filesWithLines.stream().mapToInt(f -> f.lines().size()).sum();
            logger.info("Processing {} new log entries from {} file(s)", totalLines, filesWithLines.size());

            if (logger.isDebugEnabled()) {
                tailReader.getFilePositions().forEach((file, pos) ->
                        logger.debug("  {} -> {} bytes", file, pos));
            }

            for (LogFileWithLines fileWithLines : filesWithLines) {
                // Convert JSON to Arrow
                VectorSchemaRoot arrowBatch = arrowConverter.convertToArrow(fileWithLines.lines(), fileWithLines.fileName());

                if (arrowBatch == null) {
                    logger.error("Failed to convert logs to Arrow format for file: {}", fileWithLines.fileName());
                    continue;
                }

                try {
                    // Send to server using existing HttpSender
                    byte[] arrowBytes = toArrowBytes(arrowBatch);
                    httpSender.enqueue(arrowBytes);

                    logger.debug("Successfully queued {} records ({} bytes) from file {} for sending", arrowBatch.getRowCount(), arrowBytes.length, fileWithLines.fileName());
                } finally {
                    arrowBatch.close();
                }
            }

        } catch (Exception e) {
            logger.error("Error processing logs", e);
        }
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void close() {
        stop();
        arrowConverter.close();
        httpSender.close();
    }

    private static byte[] toArrowBytes(VectorSchemaRoot root) {
        try (
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Arrow batch", e);
        }
    }

}