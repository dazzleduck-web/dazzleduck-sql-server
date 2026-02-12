package io.dazzleduck.sql.logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.ArrowProducer;
import io.dazzleduck.sql.client.HttpArrowProducer;
import io.dazzleduck.sql.common.ConfigConstants;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating and managing ArrowSimpleLogger instances.
 * All loggers share a single HttpArrowProducer for efficiency.
 */
public class ArrowSimpleLoggerFactory implements ILoggerFactory {

    private final ConcurrentHashMap<String, ArrowSimpleLogger> loggerMap = new ConcurrentHashMap<>();

    // Shared producer for all loggers
    private volatile ArrowProducer sharedProducer;

    // Configuration
    private final Config config;
    private final Level configLogLevel;
    private final AsyncConsoleWriter consoleWriter;
    private final Schema schema;

    public ArrowSimpleLoggerFactory() {
        Config tempConfig = null;
        Level logLevel = Level.INFO;
        boolean showConsole = true;

        try {
            tempConfig = ConfigFactory.load().getConfig("dazzleduck_logger");
            String levelStr = tempConfig.hasPath("log_level") ? tempConfig.getString("log_level") : "INFO";
            logLevel = Level.valueOf(levelStr.toUpperCase());
            showConsole = !tempConfig.hasPath("show_log_in_console") || tempConfig.getBoolean("show_log_in_console");
        } catch (Exception e) {
            System.err.println("[ArrowSimpleLoggerFactory] Failed to load config, using defaults: " + e.getMessage());
        }

        this.config = tempConfig;
        this.configLogLevel = logLevel;
        this.consoleWriter = showConsole ? new AsyncConsoleWriter() : null;
        this.schema = createSchema();
    }

    /**
     * Initialize the shared producer. Called from ArrowSLF4JServiceProvider.initialize().
     */
    void initialize() {
        if (config == null || sharedProducer != null) {
            return;
        }

        try {
            Config http = config.getConfig(ConfigConstants.HTTP_PREFIX);
            sharedProducer = new HttpArrowProducer(
                    schema,
                    http.getString(ConfigConstants.BASE_URL_KEY),
                    http.getString(ConfigConstants.USERNAME_KEY),
                    http.getString(ConfigConstants.PASSWORD_KEY),
                    http.getString(ConfigConstants.INGESTION_QUEUE_KEY),
                    Duration.ofMillis(http.getLong(ConfigConstants.HTTP_CLIENT_TIMEOUT_MS_KEY)),
                    config.getLong(ConfigConstants.MIN_BATCH_SIZE_KEY),
                    config.getLong(ConfigConstants.MAX_BATCH_SIZE_KEY),
                    Duration.ofMillis(config.getLong(ConfigConstants.MAX_SEND_INTERVAL_MS_KEY)),
                    config.getInt(ConfigConstants.RETRY_COUNT_KEY),
                    config.getLong(ConfigConstants.RETRY_INTERVAL_MS_KEY),
                    config.getStringList(ConfigConstants.PROJECT_KEY),
                    config.getStringList(ConfigConstants.PARTITION_BY_KEY),
                    config.getLong(ConfigConstants.MAX_IN_MEMORY_BYTES_KEY),
                    config.getLong(ConfigConstants.MAX_ON_DISK_BYTES_KEY)
            );
        } catch (Exception e) {
            System.err.println("[ArrowSimpleLoggerFactory] Failed to create HttpArrowProducer: " + e.getMessage());
        }
    }

    private static Schema createSchema() {
        // MDC field as Map<String, String>
        Field mdcField = new Field("mdc", FieldType.nullable(new ArrowType.Map(false)),
                List.of(
                        new Field("entries", FieldType.notNullable(new ArrowType.Struct()),
                                List.of(
                                        new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                                        new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)
                                )
                        )
                )
        );

        return new Schema(List.of(
                new Field("s_no", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("level", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("logger", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("thread", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("message", FieldType.nullable(new ArrowType.Utf8()), null),
                mdcField,
                new Field("marker", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
    }

    @Override
    public Logger getLogger(String name) {
        ArrowSimpleLogger logger = loggerMap.get(name);
        if (logger != null) {
            return logger;
        }

        // Create new logger with shared producer
        ArrowSimpleLogger newLogger = new ArrowSimpleLogger(name, sharedProducer, configLogLevel, consoleWriter);

        ArrowSimpleLogger existing = loggerMap.putIfAbsent(name, newLogger);
        return existing != null ? existing : newLogger;
    }

    /**
     * Close all loggers and the shared producer.
     * Called during application shutdown.
     */
    public void shutdown() {
        loggerMap.clear();

        if (sharedProducer != null) {
            try {
                sharedProducer.close();
            } catch (Exception e) {
                System.err.println("[ArrowSimpleLoggerFactory] Failed to close producer: " + e.getMessage());
            }
            sharedProducer = null;
        }

        if (consoleWriter != null) {
            consoleWriter.shutdown();
        }
    }

    /**
     * Get the Arrow schema used for log records.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Get the configured log level.
     */
    public Level getConfigLogLevel() {
        return configLogLevel;
    }

    /**
     * Get count of active loggers.
     */
    public int getLoggerCount() {
        return loggerMap.size();
    }
}
