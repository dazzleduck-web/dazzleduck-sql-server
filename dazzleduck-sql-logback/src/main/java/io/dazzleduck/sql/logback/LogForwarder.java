package io.dazzleduck.sql.logback;

import io.dazzleduck.sql.client.HttpProducer;
import io.dazzleduck.sql.common.types.JavaRow;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Forwards log entries from the LogBuffer to a remote server via HTTP.
 * This component periodically drains the buffer, converts logs to Arrow format,
 * and sends them using HttpSender.
 */
public final class LogForwarder implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(LogForwarder.class);

    /**
     * -- GETTER --
     *  Get the log buffer.
     */
    @Getter
    private final LogBuffer buffer;
    private final LogToArrowConverter converter;
    private final HttpProducer httpProducer;
    private final ScheduledExecutorService scheduler;
    private final LogForwarderConfig config;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a LogForwarder with the given configuration.
     * This constructor creates its own buffer and configures the LogForwardingAppender.
     *
     * @param config Configuration for the forwarder
     */
    public LogForwarder(LogForwarderConfig config) {
        this(config, new LogBuffer(config.getMaxBufferSize()));
    }

    /**
     * Create a LogForwarder with the given configuration and buffer.
     * Useful for testing or when sharing a buffer.
     *
     * @param config Configuration for the forwarder
     * @param buffer The log buffer to drain from
     */
    public LogForwarder(LogForwarderConfig config, LogBuffer buffer) {
        this.config = config;
        this.buffer = buffer;
        this.converter = new LogToArrowConverter();

        // Configure the LogForwardingAppender to use our buffer
        LogForwardingAppender.configure(
                config.getApplicationId(),
                config.getApplicationName(),
                config.getApplicationHost(),
                config.getMaxBufferSize(),
                config.isEnabled()
        );

        // Create transformations for application metadata
        java.util.List<String> transformations = new java.util.ArrayList<>(config.getTransformations());
        transformations.add(String.format("'%s' AS application_id", config.getApplicationId()));
        transformations.add(String.format("'%s' AS application_name", config.getApplicationName()));
        transformations.add(String.format("'%s' AS application_host", config.getApplicationHost()));

        // Create HttpProducer
        this.httpProducer = new HttpProducer(
                converter.getSchema(),
                config.getBaseUrl(),
                config.getUsername(),
                config.getPassword(),
                config.getTargetPath(),
                config.getHttpClientTimeout(),
                config.getMinBatchSize(),
                config.getMaxBatchSize(),
                config.getMaxSendInterval(),
                config.getRetryCount(),
                config.getRetryIntervalMillis(),
                transformations,
                config.getPartitionBy(),
                config.getMaxInMemorySize(),
                config.getMaxOnDiskSize()
        );

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "log-forwarder");
            t.setDaemon(true);
            return t;
        });

        logger.info("LogForwarder initialized with baseUrl={}, targetPath={}, pollInterval={}",
                config.getBaseUrl(), config.getTargetPath(), config.getPollInterval());
    }

    /**
     * Start the log forwarding process.
     * Logs will be polled from the buffer at the configured interval.
     */
    public void start() {
        if (!config.isEnabled()) {
            logger.info("LogForwarder is disabled, not starting");
            return;
        }

        if (closed.get()) {
            throw new IllegalStateException("LogForwarder has been closed");
        }

        if (running.compareAndSet(false, true)) {
            long pollIntervalMs = config.getPollInterval().toMillis();
            scheduler.scheduleAtFixedRate(
                    this::forwardLogs,
                    pollIntervalMs,
                    pollIntervalMs,
                    TimeUnit.MILLISECONDS
            );
            logger.info("LogForwarder started");
        }
    }

    /**
     * Drain buffer and forward logs to server.
     * This method is called periodically by the scheduler.
     */
    void forwardLogs() {
        if (!running.get() || closed.get()) {
            return;
        }

        try {
            List<LogEntry> entries = buffer.drain();
            if (entries.isEmpty()) {
                logger.debug("No log entries to forward");
                return;
            }

            logger.debug("Forwarding {} log entries", entries.size());

            try {
                // Add each log entry as a row
                for (LogEntry entry : entries) {
                    JavaRow row = convertToJavaRow(entry);
                    httpProducer.addRow(row);
                }
                logger.debug("Successfully added {} log entries", entries.size());
            } catch (IllegalStateException e) {
                // Queue is full, return entries for retry
                logger.warn("HttpProducer queue is full, returning entries for retry: {}", e.getMessage());
                buffer.returnForRetry(entries);
            } catch (Exception e) {
                logger.error("Failed to add log entries", e);
                buffer.returnForRetry(entries);
            }

        } catch (Exception e) {
            logger.error("Error during log forwarding", e);
        }
    }

    /**
     * Convert a LogEntry to a JavaRow for the HttpProducer.
     * The field order must match the schema from LogToArrowConverter:
     * timestamp, level, logger, thread, message
     * Note: application_id, application_name, application_host are added via transformations
     */
    private JavaRow convertToJavaRow(LogEntry entry) {
        Object[] fields = new Object[5];
        fields[0] = entry.timestamp() != null ? entry.timestamp().toString() : null;
        fields[1] = entry.level();
        fields[2] = entry.logger();
        fields[3] = entry.thread();
        fields[4] = entry.message();
        return new JavaRow(fields);
    }

    /**
     * Stop the log forwarding process gracefully.
     * Any remaining logs in the buffer will be attempted to send before stopping.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping LogForwarder...");
            // Try to flush remaining logs
            forwardLogs();
            logger.info("LogForwarder stopped");
        }
    }

    /**
     * Check if the forwarder is currently running.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Get the current buffer size.
     */
    public int getBufferSize() {
        return buffer.getSize();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            stop();

            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                    logger.warn("Scheduler did not terminate gracefully");
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }

            try {
                httpProducer.close();
            } catch (Exception e) {
                logger.error("Error closing HttpProducer", e);
            }

            converter.close();
            logger.info("LogForwarder closed");
        }
    }

    /**
     * Create and start a LogForwarder with the given configuration.
     * Convenience method for simple usage.
     *
     * @param config Configuration for the forwarder
     * @return A started LogForwarder instance
     */
    public static LogForwarder createAndStart(LogForwarderConfig config) {
        LogForwarder forwarder = new LogForwarder(config);
        forwarder.start();
        return forwarder;
    }
}
