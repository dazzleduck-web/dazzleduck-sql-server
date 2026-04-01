package io.dazzleduck.sql.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import org.slf4j.Marker;
import org.slf4j.event.KeyValuePair;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a single log entry with all metadata for forwarding.
 */
public final class LogEntry {

    /**
     * Caller data captured from the logging call-site stack frame.
     * Only populated when captureCallerData is enabled in the appender config.
     */
    public static final class CallerData {
        private final String className;
        private final String method;
        private final String file;
        private final int line;

        public CallerData(String className, String method, String file, int line) {
            this.className = className;
            this.method = method;
            this.file = file;
            this.line = line;
        }

        public String className() { return className; }
        public String method() { return method; }
        public String file() { return file; }
        public int line() { return line; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CallerData)) return false;
            CallerData that = (CallerData) o;
            return line == that.line &&
                   Objects.equals(className, that.className) &&
                   Objects.equals(method, that.method) &&
                   Objects.equals(file, that.file);
        }

        @Override
        public int hashCode() {
            return Objects.hash(className, method, file, line);
        }

        @Override
        public String toString() {
            return className + "." + method + "(" + file + ":" + line + ")";
        }
    }

    private final long sequenceNumber;
    private final Instant timestamp;
    private final String level;
    private final String logger;
    private final String thread;
    private final String message;
    private final Map<String, String> mdc;
    private final String throwable;
    private final List<String> markers;
    private final Map<String, String> keyValuePairs;
    private final CallerData callerData;

    public LogEntry(long sequenceNumber, Instant timestamp, String level, String logger,
                    String thread, String message, Map<String, String> mdc,
                    String throwable, List<String> markers, Map<String, String> keyValuePairs,
                    CallerData callerData) {
        this.sequenceNumber = sequenceNumber;
        this.timestamp = timestamp;
        this.level = level;
        this.logger = logger;
        this.thread = thread;
        this.message = message;
        this.mdc = mdc != null ? Map.copyOf(mdc) : Collections.emptyMap();
        this.throwable = throwable;
        this.markers = markers != null ? Collections.unmodifiableList(new ArrayList<>(markers)) : Collections.emptyList();
        this.keyValuePairs = keyValuePairs != null ? Map.copyOf(keyValuePairs) : Collections.emptyMap();
        this.callerData = callerData;
    }

    /**
     * Factory method to create a LogEntry from a Logback event.
     *
     * @param event             The Logback logging event
     * @param captureCallerData Whether to capture call-site caller data (expensive: triggers stack walk)
     * @return A new LogEntry instance
     */
    public static LogEntry from(ILoggingEvent event, boolean captureCallerData) {
        // Throwable
        String throwableStr = null;
        if (event.getThrowableProxy() != null) {
            throwableStr = ThrowableProxyUtil.asString(event.getThrowableProxy());
        }

        // Markers (SLF4J 2.x supports multiple markers per event)
        List<String> markerNames = null;
        List<Marker> eventMarkers = event.getMarkerList();
        if (eventMarkers != null && !eventMarkers.isEmpty()) {
            markerNames = new ArrayList<>(eventMarkers.size());
            for (Marker m : eventMarkers) {
                markerNames.add(m.getName());
            }
        }

        // Key-value pairs (SLF4J 2.x fluent API)
        Map<String, String> kvp = null;
        List<KeyValuePair> kvPairs = event.getKeyValuePairs();
        if (kvPairs != null && !kvPairs.isEmpty()) {
            kvp = new LinkedHashMap<>();
            for (KeyValuePair kv : kvPairs) {
                kvp.put(kv.key, kv.value != null ? String.valueOf(kv.value) : null);
            }
        }

        // Caller data (triggers stack walk on first access — only when enabled)
        CallerData callerData = null;
        if (captureCallerData) {
            StackTraceElement[] frames = event.getCallerData();
            if (frames != null && frames.length > 0) {
                StackTraceElement ste = frames[0];
                callerData = new CallerData(
                        ste.getClassName(),
                        ste.getMethodName(),
                        ste.getFileName(),
                        ste.getLineNumber()
                );
            }
        }

        return new LogEntry(
                event.getSequenceNumber(),
                Instant.ofEpochMilli(event.getTimeStamp()),
                event.getLevel().toString(),
                event.getLoggerName(),
                event.getThreadName(),
                event.getFormattedMessage(),
                event.getMDCPropertyMap(),
                throwableStr,
                markerNames,
                kvp,
                callerData
        );
    }

    public long sequenceNumber() { return sequenceNumber; }
    public Instant timestamp() { return timestamp; }
    public String level() { return level; }
    public String logger() { return logger; }
    public String thread() { return thread; }
    public String message() { return message; }
    public Map<String, String> mdc() { return mdc; }
    public String throwable() { return throwable; }
    public List<String> markers() { return markers; }
    public Map<String, String> keyValuePairs() { return keyValuePairs; }
    public CallerData callerData() { return callerData; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry that = (LogEntry) o;
        return sequenceNumber == that.sequenceNumber &&
               Objects.equals(timestamp, that.timestamp) &&
               Objects.equals(level, that.level) &&
               Objects.equals(logger, that.logger) &&
               Objects.equals(thread, that.thread) &&
               Objects.equals(message, that.message) &&
               Objects.equals(mdc, that.mdc) &&
               Objects.equals(throwable, that.throwable) &&
               Objects.equals(markers, that.markers) &&
               Objects.equals(keyValuePairs, that.keyValuePairs) &&
               Objects.equals(callerData, that.callerData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequenceNumber, timestamp, level, logger, thread, message,
                mdc, throwable, markers, keyValuePairs, callerData);
    }

    @Override
    public String toString() {
        return "LogEntry[" +
               "sequenceNumber=" + sequenceNumber +
               ", timestamp=" + timestamp +
               ", level=" + level +
               ", logger=" + logger +
               ", thread=" + thread +
               ", message=" + message +
               ", mdc=" + mdc +
               ", throwable=" + (throwable != null ? "<present>" : "null") +
               ", markers=" + markers +
               ", keyValuePairs=" + keyValuePairs +
               ", callerData=" + callerData +
               "]";
    }
}
