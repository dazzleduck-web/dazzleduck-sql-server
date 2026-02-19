package io.dazzleduck.sql.logback;


import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Custom Logback appender that captures log events and forwards them
 * to a remote server via HTTP. Log entries are sent directly to the
 * ArrowProducer which handles batching and sending.
 *
 * <p>Each appender instance maintains its own {@link LogForwarder}, so multiple
 * appenders with different configurations (different servers, queues, etc.)
 * can coexist in the same JVM.</p>
 *
 * <p>There are two ways to configure an appender:</p>
 *
 * <p><b>Option 1 – inline properties in logback.xml:</b></p>
 * <pre>{@code
 * <appender name="LOG_FORWARDER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
 *     <baseUrl>http://localhost:8081</baseUrl>
 *     <username>admin</username>
 *     <password>admin</password>
 *     <ingestionQueue>log</ingestionQueue>
 *     <project>*,'myhost' AS application_host,CAST(timestamp AS date) AS date</project>
 *     <partitionBy>date</partitionBy>
 * </appender>
 * }</pre>
 *
 * <p><b>Option 2 – dedicated conf file per component (recommended for multiple appenders):</b></p>
 * <pre>{@code
 * <appender name="CONTROLLER_FORWARDER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
 *     <configFile>controller-logback.conf</configFile>
 * </appender>
 *
 * <appender name="SERVICE_FORWARDER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
 *     <configFile>/etc/myapp/service-logback.conf</configFile>
 * </appender>
 * }</pre>
 *
 * <p>When {@code configFile} is set it takes full precedence; inline properties are ignored.
 * The conf file must contain a {@code dazzleduck_logback} block in TypeSafe Config format.
 * Keys not present in the file fall back to the defaults in reference.conf.</p>
 */
public class LogForwardingAppender extends AppenderBase<ILoggingEvent> {

    // Packages to exclude from forwarding to prevent infinite loops
    // Note: excludes the internal forwarder classes but NOT demo classes
    private static final String[] EXCLUDED_PACKAGES = {
            "io.dazzleduck.sql.logback.Log",  // LogForwarder, LogEntry, LogForwardingAppender
            "io.dazzleduck.sql.client",
            "org.apache.arrow"
    };

    // Global sequence number counter for unique s_no values across all appender instances
    private static final AtomicLong sequenceCounter = new AtomicLong(0);

    /**
     * Global enable/disable toggle for all appender instances.
     */
    private static volatile boolean enabled = true;

    // Per-instance forwarder - each appender has its own independent forwarder
    private volatile LogForwarder forwarder;

    // Optional path to a dedicated .conf file for this appender.
    // When set, all config is loaded from this file and inline properties are ignored.
    private String configFile;

    // Logback XML configurable properties (used when configFile is not set)
    private String baseUrl;
    private String username = "admin";
    private String password = "admin";
    private String ingestionQueue = "log";
    private long minBatchSize = 1024; // 1 KB default for logs (smaller than metrics)
    private List<String> project = Collections.emptyList();
    private List<String> partitionBy = Collections.emptyList();

    /**
     * Path to a dedicated TypeSafe Config (.conf) file for this appender.
     * Accepts a classpath resource name or an absolute/relative filesystem path.
     * When set, all configuration is loaded from this file and inline properties
     * (baseUrl, username, etc.) are ignored.
     */
    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

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
        try {
            if (configFile != null && !configFile.isEmpty()) {
                addInfo("Initializing LogForwardingAppender from configFile=" + configFile);
                LogForwarderConfig config;
                if (configFile.endsWith(".xml")) {
                    config = loadConfigFromXml(configFile);
                } else {
                    config = LogForwarderConfigFactory.createConfig(configFile);
                }
                forwarder = new LogForwarder(config);
                addInfo("LogForwardingAppender successfully initialized from " + configFile);
            } else if (baseUrl == null || baseUrl.isEmpty()) {
                addError("LogForwardingAppender is not configured - set either <configFile> or <baseUrl>. " +
                        "Logs will NOT be forwarded.");
            } else if (baseUrl.contains("${") || baseUrl.contains("}")) {
                addError("LogForwardingAppender baseUrl contains unresolved variables: " + baseUrl +
                        " - logs will NOT be forwarded. Check logback.xml property definitions.");
            } else {
                // Inline-properties mode: build config from individual XML properties
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
        } catch (Exception e) {
            addError("Failed to initialize LogForwardingAppender", e);
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
     * Reset the global static state. Primarily for testing.
     * Note: per-instance forwarders are cleaned up via {@link #stop()}.
     */
    static void resetGlobalState() {
        enabled = true;
        sequenceCounter.set(0);
    }

    // -------------------------------------------------------------------------
    // XML config-file support
    // -------------------------------------------------------------------------

    /**
     * Load a {@link LogForwarderConfig} from an XML file by delegating to
     * {@link AppenderXmlParser}. Resolves the path as a classpath resource
     * first, then as a filesystem path.
     */
    private LogForwarderConfig loadConfigFromXml(String xmlFile) throws Exception {
        try (InputStream is = openResource(xmlFile)) {
            return AppenderXmlParser.parse(is).config;
        }
    }

    /** Resolves a config file: classpath resource first, then filesystem. */
    private InputStream openResource(String path) throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        if (is != null) return is;
        is = getClass().getClassLoader().getResourceAsStream(path);
        if (is != null) return is;
        File f = new File(path);
        if (f.exists()) return new FileInputStream(f);
        throw new IOException("Cannot find config file: " + path);
    }
}
