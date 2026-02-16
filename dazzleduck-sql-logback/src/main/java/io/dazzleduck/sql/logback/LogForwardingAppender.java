package io.dazzleduck.sql.logback;


import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Custom Logback appender that captures log events and forwards them
 * to a remote server via HTTP. Log entries are sent directly to the
 * ArrowProducer which handles batching and sending.
 *
 * <p>Usage in logback.xml:</p>
 * <pre>{@code
 * <appender name="LOG_FORWARDER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
 *     <baseUrl>http://localhost:8081</baseUrl>
 *     <username>admin</username>
 *     <password>admin</password>
 *     <ingestionQueue>log</ingestionQueue>
 *     <project>*,'myhost' AS application_host,CAST(timestamp AS date) AS date</project>
 *     <partitionBy>date</partitionBy>
 * </appender>
 *
 * <root level="INFO">
 *     <appender-ref ref="LOG_FORWARDER"/>
 * </root>
 * }</pre>
 */
public class LogForwardingAppender extends AppenderBase<ILoggingEvent> {

    // Packages to exclude from forwarding to prevent infinite loops
    // Note: excludes the internal forwarder classes but NOT demo classes
    private static final String[] EXCLUDED_PACKAGES = {
            "io.dazzleduck.sql.logback.Log",  // LogForwarder, LogEntry, LogForwardingAppender
            "io.dazzleduck.sql.client",
            "org.apache.arrow"
    };

    // Sequence number counter for generating unique s_no values
    private static final AtomicLong sequenceCounter = new AtomicLong(0);

    // Static forwarder - auto-created when baseUrl is configured
    private static volatile LogForwarder forwarder;

    /**
     * Enable or disable log forwarding.
     */
    private static volatile boolean enabled = true;

    // Logback XML configurable properties
    private String baseUrl;
    private String username = "admin";
    private String password = "admin";
    private String ingestionQueue = "log";
    private long minBatchSize = 1024; // 1 KB default for logs (smaller than metrics)
    private List<String> project = Collections.emptyList();
    private List<String> partitionBy = Collections.emptyList();

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setIngestionQueue(String ingestionQueue) {
        this.ingestionQueue = ingestionQueue;
    }

    /**
     * Set minimum batch size in bytes before sending.
     * Default is 1024 (1 KB).
     */
    public void setMinBatchSize(long minBatchSize) {
        this.minBatchSize = minBatchSize;
    }

    /**
     * Set project expressions (comma-separated).
     * Example: "*,'hostname' AS application_host,CAST(timestamp AS date) AS date"
     */
    public void setProject(String project) {
        if (project != null && !project.trim().isEmpty()) {
            this.project = Arrays.asList(project.split(","));
        }
    }

    /**
     * Set partition columns (comma-separated).
     * Example: "date"
     */
    public void setPartitionBy(String partitionBy) {
        if (partitionBy != null && !partitionBy.trim().isEmpty()) {
            this.partitionBy = Arrays.asList(partitionBy.split(","));
        }
    }

    /**
     * Check if forwarding is enabled.
     *
     * @return true if enabled
     */
    public static boolean isEnabled() {
        return enabled;
    }

    /**
     * Set whether forwarding is enabled.
     *
     * @param enabled true to enable forwarding
     */
    public static void setEnabled(boolean enabled) {
        LogForwardingAppender.enabled = enabled;
    }

    @Override
    public void start() {
        // Auto-create forwarder if baseUrl is configured
        if (baseUrl == null || baseUrl.isEmpty()) {
            addError("LogForwardingAppender baseUrl is not configured - logs will NOT be forwarded. " +
                    "Please set DAZZLEDUCK_BASE_URL environment variable or baseUrl property in logback.xml");
        } else if (baseUrl.contains("${") || baseUrl.contains("}")) {
            addError("LogForwardingAppender baseUrl contains unresolved variables: " + baseUrl +
                    " - logs will NOT be forwarded. Check logback.xml property definitions.");
        } else if (forwarder == null) {
            try {
                synchronized (LogForwardingAppender.class) {
                    if (forwarder == null) {
                        addInfo("Initializing LogForwardingAppender with baseUrl=" + baseUrl +
                                ", ingestionQueue=" + ingestionQueue);

                        LogForwarderConfig config = LogForwarderConfig.builder()
                                .baseUrl(baseUrl)
                                .username(username)
                                .password(password)
                                .ingestionQueue(ingestionQueue)
                                .minBatchSize(minBatchSize)
                                .project(project)
                                .partitionBy(partitionBy)
                                .build();
                        forwarder = new LogForwarder(config);
                        addInfo("LogForwardingAppender successfully initialized");
                    }
                }
            } catch (Exception e) {
                addError("Failed to initialize LogForwardingAppender", e);
            }
        }

        super.start();
    }

    @Override
    public void stop() {
        super.stop();
        if (forwarder != null) {
            forwarder.close();
            forwarder = null;
        }
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (!enabled) {
            return;
        }

        if (forwarder == null) {
            // Only log this error once every 1000 entries to avoid spamming
            long seq = sequenceCounter.get();
            if (seq == 0 || seq % 1000 == 0) {
                addError("LogForwardingAppender forwarder is null - logs are being dropped. " +
                        "Check configuration and initialization errors.");
            }
            return;
        }

        // Avoid logging loops - don't capture logs from our own packages
        String loggerName = event.getLoggerName();
        if (loggerName != null && shouldExclude(loggerName)) {
            return;
        }

        LogEntry entry = LogEntry.from(sequenceCounter.incrementAndGet(), event);

        // Add directly to forwarder - it handles batching via ArrowProducer
        try {
            if (!forwarder.addLogEntry(entry)) {
                // Queue full or forwarder not running - log this warning periodically
                long seq = sequenceCounter.get();
                if (seq == 1 || seq % 100 == 0) {
                    addWarn("Log forwarder queue full, dropping entry (seq=" + seq + ")");
                }
            }
        } catch (Exception e) {
            // Log errors using addError to avoid infinite loop
            // (this error itself will be excluded by the shouldExclude check)
            addError("Failed to forward log entry", e);
        }
    }

    private boolean shouldExclude(String loggerName) {
        for (String excluded : EXCLUDED_PACKAGES) {
            if (loggerName.startsWith(excluded)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reset the appender state. Primarily for testing.
     */
    static void reset() {
        synchronized (LogForwardingAppender.class) {
            if (forwarder != null) {
                forwarder.close();
                forwarder = null;
            }
            enabled = true;
            sequenceCounter.set(0);
        }
    }
}
