package io.dazzleduck.sql.logger;

import io.dazzleduck.sql.client.ArrowProducer;
import io.dazzleduck.sql.common.types.JavaRow;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.slf4j.helpers.LegacyAbstractLogger;
import org.slf4j.helpers.MessageFormatter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SLF4J logger that forwards logs to a remote server via Arrow format.
 * Uses a shared ArrowProducer provided by ArrowSimpleLoggerFactory.
 */
public class ArrowSimpleLogger extends LegacyAbstractLogger {

    private static final long serialVersionUID = 1L;

    // Global sequence number generator
    private static final AtomicLong SEQUENCE_GENERATOR = new AtomicLong(0);

    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ISO_INSTANT;

    private final String name;
    private final ArrowProducer producer;
    private final Level configLogLevel;
    private final AsyncConsoleWriter consoleWriter;

    /**
     * Create a logger with the given name, shared producer, and log level.
     *
     * @param name              Logger name
     * @param producer          Shared ArrowProducer (may be null if not configured)
     * @param configLogLevel    Configured log level threshold
     * @param consoleWriter     Async console writer (null to disable console output)
     */
    public ArrowSimpleLogger(String name, ArrowProducer producer, Level configLogLevel, AsyncConsoleWriter consoleWriter) {
        this.name = name;
        this.producer = producer;
        this.configLogLevel = configLogLevel != null ? configLogLevel : Level.INFO;
        this.consoleWriter = consoleWriter;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    protected String getFullyQualifiedCallerName() {
        return name;
    }

    @Override
    protected void handleNormalizedLoggingCall(
            Level level, Marker marker, String messagePattern,
            Object[] args, Throwable throwable) {

        if (!isLevelEnabled(level.toInt())) {
            return;
        }

        String message = format(messagePattern, args);
        if (throwable != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            throwable.printStackTrace(pw);
            message += "\n" + sw;
        }

        if (consoleWriter != null) {
            printToConsole(level, message);
        }

        writeLog(level, marker, message);
    }

    private String format(String pattern, Object[] args) {
        if (args == null || args.length == 0) {
            return pattern;
        }
        return MessageFormatter.arrayFormat(pattern, args).getMessage();
    }

    private void printToConsole(Level level, String message) {
        String timestamp = TS_FORMAT.format(Instant.now());
        String threadName = Thread.currentThread().getName();
        StringBuilder sb = new StringBuilder(timestamp.length() + threadName.length() + name.length() + message.length() + 16);
        sb.append(timestamp).append(" [").append(threadName).append("] ")
                .append(level).append(' ').append(name).append(" - ").append(message);
        consoleWriter.write(sb.toString());
    }

    private void writeLog(Level level, Marker marker, String message) {
        if (producer == null) {
            return;
        }

        try {
            long sequenceNo = SEQUENCE_GENERATOR.incrementAndGet();
            String timestamp = TS_FORMAT.format(Instant.now());
            String threadName = Thread.currentThread().getName();

            // Get MDC context
            Map<String, String> mdcMap = MDC.getCopyOfContextMap();
            if (mdcMap == null) {
                mdcMap = Collections.emptyMap();
            }

            String markerName = marker != null ? marker.getName() : null;

            // Schema: s_no, timestamp, level, logger, thread, message, mdc, marker
            JavaRow row = new JavaRow(new Object[]{
                    sequenceNo,
                    timestamp,
                    level.toString(),
                    name,
                    threadName,
                    message,
                    mdcMap,
                    markerName
            });

            producer.addRow(row);

        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] Failed to log: " + e.getMessage());
        }
    }

    public void log(LoggingEvent event) {
        if (isLevelEnabled(event.getLevel().toInt())) {
            Marker marker = event.getMarkers().isEmpty() ? null : event.getMarkers().get(0);
            writeLog(event.getLevel(), marker, event.getMessage());
        }
    }

    // === Log level controls ===

    @Override
    public boolean isTraceEnabled() {
        return isLevelEnabled(Level.TRACE.toInt());
    }

    @Override
    public boolean isDebugEnabled() {
        return isLevelEnabled(Level.DEBUG.toInt());
    }

    @Override
    public boolean isInfoEnabled() {
        return isLevelEnabled(Level.INFO.toInt());
    }

    @Override
    public boolean isWarnEnabled() {
        return isLevelEnabled(Level.WARN.toInt());
    }

    @Override
    public boolean isErrorEnabled() {
        return isLevelEnabled(Level.ERROR.toInt());
    }

    private boolean isLevelEnabled(int levelInt) {
        return levelInt >= configLogLevel.toInt();
    }
}
