package io.dazzleduck.sql.logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.HttpProducer;
import io.dazzleduck.sql.client.FlightProducer;
import io.dazzleduck.sql.common.types.JavaRow;
import org.apache.arrow.vector.types.pojo.*;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.slf4j.helpers.LegacyAbstractLogger;
import org.slf4j.helpers.MessageFormatter;

import java.io.PrintWriter;
import java.io.Serial;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ArrowSimpleLogger extends LegacyAbstractLogger implements AutoCloseable {

    @Serial
    private static final long serialVersionUID = 1L;

    // Global sequence number generator
    private static final AtomicLong SEQUENCE_GENERATOR = new AtomicLong(0);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ISO_INSTANT;

    // Configuration - initialized FIRST before any Arrow classes to avoid circular initialization
    private static final Config config;
    private static final String CONFIG_APPLICATION_ID;
    private static final String CONFIG_APPLICATION_NAME;
    private static final String CONFIG_APPLICATION_HOST;
    private static final Level CONFIG_LOG_LEVEL;

    // Flag to track if we're in the middle of static initialization (including schema creation)
    private static final boolean fullyInitialized;

    // Thread-local flag to prevent recursion when creating HttpProducer (which uses SLF4J)
    private static final ThreadLocal<Boolean> creatingProducer = ThreadLocal.withInitial(() -> Boolean.FALSE);

    // Schema - must be declared before static block but initialized lazily
    private static final Schema schema;

    static {
        Config tempConfig = null;
        String appId = "unknown";
        String appName = "unknown";
        String appHost = "localhost";
        Level logLevel = Level.INFO;

        try {
            tempConfig = ConfigFactory.load().getConfig("dazzleduck_logger");
            appId = tempConfig.getString("application_id");
            appName = tempConfig.getString("application_name");
            appHost = tempConfig.getString("application_host");

            String levelStr = tempConfig.hasPath("log_level") ? tempConfig.getString("log_level") : "INFO";
            logLevel = Level.valueOf(levelStr.toUpperCase());
        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] Failed to load config, using defaults: " + e.getMessage());
        }

        config = tempConfig;
        CONFIG_APPLICATION_ID = appId;
        CONFIG_APPLICATION_NAME = appName;
        CONFIG_APPLICATION_HOST = appHost;
        CONFIG_LOG_LEVEL = logLevel;

        // Create schema - this may trigger SLF4J logging from Arrow's Field class
        // Any loggers created during this will use NoOpFlightProducer
        schema = createSchema();

        // Now mark as fully initialized - subsequent loggers can use HttpProducer
        fullyInitialized = true;
    }

    private static Schema createSchema() {
        return new Schema(java.util.List.of(
                new Field("s_no", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("level", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("logger", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("thread", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("message", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("mdc", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("marker", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("application_id", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("application_name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("application_host", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
    }

    private final String name;
    private final FlightProducer flightProducer;
    private final String application_id;
    private final String application_name;
    private final String application_host;

    public ArrowSimpleLogger(String name) {
        this(name, createSenderFromConfig());
    }

    public ArrowSimpleLogger(String name, FlightProducer sender) {
        this.name = name;
        this.flightProducer = sender;
        this.application_id = CONFIG_APPLICATION_ID;
        this.application_name = CONFIG_APPLICATION_NAME;
        this.application_host = CONFIG_APPLICATION_HOST;
    }

    private static FlightProducer createSenderFromConfig() {
        // If not fully initialized, config is unavailable, or we're already creating a producer,
        // return a no-op producer. This handles:
        // 1. Circular initialization when Arrow's Field class triggers SLF4J logging
        // 2. Recursive calls when HttpProducer's static initializer uses SLF4J logging
        if (!fullyInitialized || config == null || creatingProducer.get()) {
            return new NoOpFlightProducer();
        }

        // Set flag to prevent recursion
        creatingProducer.set(Boolean.TRUE);
        try {
            Config http = config.getConfig("http");
            String targetPath = http.getString("target_path");
            return new HttpProducer(
                    schema,
                    http.getString("base_url"),
                    http.getString("username"),
                    http.getString("password"),
                    targetPath,
                    Duration.ofMillis(http.getLong("http_client_timeout_ms")),
                    config.getLong("min_batch_size"),
                    config.getLong("max_batch_size"),
                    Duration.ofMillis(config.getLong("max_send_interval_ms")),
                    config.getInt("retry_count"),
                    config.getLong("retry_interval_ms"),
                    config.getStringList("transformations"),
                    config.getStringList("partition_by"),
                    config.getLong("max_in_memory_bytes"),
                    config.getLong("max_on_disk_bytes")
            );
        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] Failed to create HttpProducer: " + e.getMessage());
            return new NoOpFlightProducer();
        } finally {
            creatingProducer.set(Boolean.FALSE);
        }
    }

    /**
     * No-op FlightProducer for use during initialization or when config is unavailable.
     */
    private static class NoOpFlightProducer implements FlightProducer {
        @Override
        public void addRow(JavaRow row) {
            // No-op: discard logs during initialization
        }

        @Override
        public void enqueue(byte[] input) {
            // No-op
        }

        @Override
        public long getMaxInMemorySize() {
            return 0;
        }

        @Override
        public long getMaxOnDiskSize() {
            return 0;
        }

        @Override
        public void close() {
            // No-op
        }
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
            message += "\n" + sw.toString();
        }

        writeArrowAsync(level, marker, message);
    }

    private String format(String pattern, Object[] args) {
        if (args == null || args.length == 0)
            return pattern;
        return MessageFormatter.arrayFormat(pattern, args).getMessage();
    }

    /** Collect logs and send to Flight */
    private void writeArrowAsync(Level level, Marker marker, String message) {
        if (flightProducer == null) {
            return;
        }

        try {
            long sequenceNo = SEQUENCE_GENERATOR.incrementAndGet();
            String timestamp = TS_FORMAT.format(Instant.now());
            String threadName = Thread.currentThread().getName();

            // Get MDC context
            Map<String, String> mdcMap = MDC.getCopyOfContextMap();
            String mdcJson = null;
            if (mdcMap != null && !mdcMap.isEmpty()) {
                try {
                    mdcJson = JSON_MAPPER.writeValueAsString(mdcMap);
                } catch (JsonProcessingException e) {
                    System.err.println("[ArrowSimpleLogger] Failed to serialize MDC: " + e.getMessage());
                }
            }

            // Get marker name
            String markerName = marker != null ? marker.getName() : null;

            JavaRow row = new JavaRow(new Object[]{
                    sequenceNo,
                    timestamp,
                    level.toString(),
                    name,
                    threadName,
                    message,
                    mdcJson,
                    markerName,
                    application_id,
                    application_name,
                    application_host
            });

            // FlightProducer handles batching, serialization, and sending
            flightProducer.addRow(row);

        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] Failed to log:");
            e.printStackTrace(System.err);
        }
    }

    @Override
    public void close() {
        try {
            if (flightProducer != null) {
                ((AutoCloseable) flightProducer).close();
            }
        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] Failed to close producer:");
            e.printStackTrace(System.err);
        }
    }

    public void log(LoggingEvent event) {
        if (isLevelEnabled(event.getLevel().toInt())) {
            writeArrowAsync(event.getLevel(), event.getMarkers().isEmpty() ? null : event.getMarkers().get(0), event.getMessage());
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

    protected boolean isLevelEnabled(int levelInt) {
        return levelInt >= CONFIG_LOG_LEVEL.toInt();
    }

    /**
     * Get the Arrow schema used for log records.
     */
    public static Schema getSchema() {
        return schema;
    }
}
