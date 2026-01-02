package io.dazzleduck.sql.logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.HttpSender;
import io.dazzleduck.sql.common.ingestion.FlightSender;
import io.dazzleduck.sql.common.types.JavaRow;
import org.apache.arrow.vector.types.pojo.*;
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
import java.util.UUID;
public final class ArrowSimpleLogger extends LegacyAbstractLogger implements AutoCloseable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ISO_INSTANT;

    // Static shared resources
    private static volatile Config config;
    private static volatile Schema SCHEMA;
    private static volatile FlightSender flightSender;
    private static String application_id;
    private static String application_name;
    private static String application_host;

    // Instance fields
    private final String name;

    public ArrowSimpleLogger(String name) {
        this.name = name;
    }

    private static FlightSender getSender() {
        if (flightSender == null) {
            synchronized (ArrowSimpleLogger.class) {
                if (flightSender == null) {
                    ensureConfigLoaded();
                    flightSender = createSenderFromConfig();
                }
            }
        }
        return flightSender;
    }

    private static void ensureConfigLoaded() {
        if (config == null) {
            synchronized (ArrowSimpleLogger.class) {
                if (config == null) {
                    config = ConfigFactory.load().getConfig("dazzleduck_logger");
                    application_id = config.getString("application_id");
                    application_name = config.getString("application_name");
                    application_host = config.getString("application_host");
                }
            }
        }
    }

    private static Schema schema() {
        if (SCHEMA == null) {
            synchronized (ArrowSimpleLogger.class) {
                if (SCHEMA == null) {
                    SCHEMA = new Schema(java.util.List.of(
                            new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null),
                            new Field("level", FieldType.nullable(new ArrowType.Utf8()), null),
                            new Field("logger", FieldType.nullable(new ArrowType.Utf8()), null),
                            new Field("thread", FieldType.nullable(new ArrowType.Utf8()), null),
                            new Field("message", FieldType.nullable(new ArrowType.Utf8()), null),
                            new Field("application_id", FieldType.nullable(new ArrowType.Utf8()), null),
                            new Field("application_name", FieldType.nullable(new ArrowType.Utf8()), null),
                            new Field("application_host", FieldType.nullable(new ArrowType.Utf8()), null)
                    ));
                }
            }
        }
        return SCHEMA;
    }

    private static FlightSender createSenderFromConfig() {
        Config http = config.getConfig("http");

        return new HttpSender(
                schema(),
                http.getString("base_url"),
                http.getString("username"),
                http.getString("password"),
                http.getString("target_path"),
                Duration.ofMillis(http.getLong("http_client_timeout_ms")),
                config.getLong("min_batch_size"),
                Duration.ofMillis(config.getLong("max_send_interval_ms")),
                config.getLong("max_in_memory_bytes"),
                config.getLong("max_on_disk_bytes")
        );
    }

    @Override
    protected String getFullyQualifiedCallerName() {
        return ArrowSimpleLogger.class.getName();
    }

    @Override
    protected void handleNormalizedLoggingCall(
            Level level, Marker marker, String messagePattern,
            Object[] args, Throwable throwable) {

        String message = format(messagePattern, args);
        if (throwable != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            throwable.printStackTrace(pw);
            message += "\n" + sw;
        }
        if (marker != null) {
            message = "[Marker:" + marker.getName() + "] " + message;
        }
        writeArrowAsync(level, message);
    }

    private static String format(String pattern, Object[] args) {
        if (pattern == null) return "null";
        if (args == null || args.length == 0) return pattern;
        return MessageFormatter.arrayFormat(pattern, args).getMessage();
    }

    private void writeArrowAsync(Level level, String message) {
        try {
            FlightSender sender = getSender();

            JavaRow row = new JavaRow(new Object[]{
                    TS_FORMAT.format(Instant.now()),
                    level.toString(),
                    name,
                    Thread.currentThread().getName(),
                    message,
                    application_id,
                    application_name,
                    application_host
            });

            sender.addRow(row);

        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] Failed to write log: " + e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        // Individual loggers don't close the shared sender
    }

    static void closeSharedResources() throws Exception {
        if (flightSender != null) {
            synchronized (ArrowSimpleLogger.class) {
                if (flightSender != null) {
                    if (flightSender instanceof AutoCloseable ac) {
                        ac.close();
                    }
                    flightSender = null;
                }
            }
        }
    }

    @Override public boolean isTraceEnabled() { return true; }
    @Override public boolean isDebugEnabled() { return true; }
    @Override public boolean isInfoEnabled()  { return true; }
    @Override public boolean isWarnEnabled()  { return true; }
    @Override public boolean isErrorEnabled() { return true; }

    public String getName() { return name; }
}