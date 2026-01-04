package io.dazzleduck.sql.logback;


import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import lombok.Getter;
import lombok.Setter;

/**
 * Custom Logback appender that captures log events and buffers them
 * for forwarding to a remote server.
 *
 * <p>Usage in logback.xml:</p>
 * <pre>{@code
 * <appender name="LOG_FORWARDER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
 *     <maxBufferSize>10000</maxBufferSize>
 * </appender>
 *
 * <root level="INFO">
 *     <appender-ref ref="LOG_FORWARDER"/>
 * </root>
 * }</pre>
 */
@Setter
public class LogForwardingAppender extends AppenderBase<ILoggingEvent> {

    // Packages to exclude from forwarding to prevent infinite loops
    private static final String[] EXCLUDED_PACKAGES = {
            "io.dazzleduck.sql.logback",
            "io.dazzleduck.sql.client",
            "org.apache.arrow"
    };

    // Static buffer shared across all instances
    private static volatile LogBuffer buffer;
    private static volatile String applicationId;
    private static volatile String applicationName;
    private static volatile String applicationHost;

    /**
     * Enable or disable log forwarding.
     * -- GETTER --
     *  Check if forwarding is enabled.
     *
     * @return true if enabled

     */
    @Getter
    @Setter
    private static volatile boolean enabled = true;

    // Logback XML configurable properties
    private int maxBufferSize = 10000;

    @Override
    public void start() {
        if (buffer == null) {
            synchronized (LogForwardingAppender.class) {
                if (buffer == null) {
                    buffer = new LogBuffer(maxBufferSize);
                }
            }
        }
        super.start();
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (!enabled) {
            return;
        }

        // Avoid logging loops - don't capture logs from our own packages
        String loggerName = event.getLoggerName();
        if (loggerName != null && shouldExclude(loggerName)) {
            return;
        }

        LogEntry entry = LogEntry.from(
                event,
                applicationId,
                applicationName,
                applicationHost
        );

        if (!buffer.offer(entry)) {
            // Buffer full - log to console only (avoid infinite loop)
            addWarn("Log buffer full, dropping entry: " + event.getFormattedMessage());
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
     * Configure the appender with application properties.
     * This should be called before starting the LogForwarder.
     *
     * @param appId Application ID
     * @param appName Application name
     * @param appHost Application host
     * @param bufferSize Maximum buffer size
     * @param isEnabled Whether forwarding is enabled
     */
    public static void configure(String appId, String appName, String appHost, int bufferSize, boolean isEnabled) {
        applicationId = appId;
        applicationName = appName;
        if (appHost != null && !appHost.isEmpty()) {
            applicationHost = appHost;
        }
        enabled = isEnabled;
        if (buffer == null || buffer.getSize() == 0) {
            buffer = new LogBuffer(bufferSize);
        }
    }

    /**
     * Get the shared buffer instance.
     *
     * @return The shared LogBuffer
     */
    public static LogBuffer getBuffer() {
        if (buffer == null) {
            synchronized (LogForwardingAppender.class) {
                if (buffer == null) {
                    buffer = new LogBuffer(10000);
                }
            }
        }
        return buffer;
    }

    /**
     * Reset the appender state. Primarily for testing.
     */
    static void reset() {
        synchronized (LogForwardingAppender.class) {
            buffer = null;
            applicationId = null;
            applicationName = null;
            applicationHost = null;
            enabled = true;
        }
    }

    /**
     * Set whether forwarding is enabled.
     *
     * @param enabled true to enable, false to disable
     */
    public static void setEnabled(boolean enabled) {
        LogForwardingAppender.enabled = enabled;
    }
}
