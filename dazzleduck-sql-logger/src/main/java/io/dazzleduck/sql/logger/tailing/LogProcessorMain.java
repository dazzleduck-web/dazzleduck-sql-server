package io.dazzleduck.sql.logger.tailing;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.HttpSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Main entry point for log tail to Arrow processor.
 */
public final class LogProcessorMain {

    private static final Logger logger = LoggerFactory.getLogger(LogProcessorMain.class);

    private static final Config config = ConfigFactory.load().getConfig("dazzleduck_logger");
    // Application config
    private static final String CONFIG_APPLICATION_ID = config.getString("application_id");
    private static final String CONFIG_APPLICATION_NAME = config.getString("application_name");
    private static final String CONFIG_APPLICATION_HOST = config.getString("application_host");
    // Log monitoring config
    private static final String CONFIG_LOG_DIRECTORY = config.getString("log_directory");
    private static final String CONFIG_LOG_FILE_PATTERN = config.getString("log_file_pattern");
    private static final long CONFIG_POLL_INTERVAL_MS = config.getLong("poll_interval_ms");
    // HTTP config
    private static final String CONFIG_HTTP_BASE_URL = config.getString("http.base_url");
    private static final String CONFIG_HTTP_USERNAME = config.getString("http.username");
    private static final String CONFIG_HTTP_PASSWORD = config.getString("http.password");
    private static final String CONFIG_HTTP_TARGET_PATH = config.getString("http.target_path");
    private static final long CONFIG_HTTP_TIMEOUT_MS = config.getLong("http.http_client_timeout_ms");
    // Batch and memory config
    private static final long CONFIG_MIN_BATCH_SIZE = config.getLong("min_batch_size");
    private static final long CONFIG_MAX_SEND_INTERVAL_MS = config.getLong("max_send_interval_ms");
    private static final long CONFIG_MAX_IN_MEMORY_BYTES = config.getLong("max_in_memory_bytes");
    private static final long CONFIG_MAX_ON_DISK_BYTES = config.getLong("max_on_disk_bytes");

    public static void main(String[] args) throws InterruptedException {
        // Create converter to get schema
        JsonToArrowConverter converter = new JsonToArrowConverter(
                CONFIG_APPLICATION_ID,
                CONFIG_APPLICATION_NAME,
                CONFIG_APPLICATION_HOST
        );

        // Create HttpSender with configuration
        HttpSender httpSender = new HttpSender(
                converter.getSchema(),
                CONFIG_HTTP_BASE_URL,
                CONFIG_HTTP_USERNAME,
                CONFIG_HTTP_PASSWORD,
                CONFIG_HTTP_TARGET_PATH,
                Duration.ofMillis(CONFIG_HTTP_TIMEOUT_MS),
                CONFIG_MIN_BATCH_SIZE,
                Duration.ofMillis(CONFIG_MAX_SEND_INTERVAL_MS),
                config.getInt("retry_count"),
                config.getLong("retry_interval_ms"),
                config.getStringList("transformations"),
                config.getStringList("partition_by"),
                CONFIG_MAX_IN_MEMORY_BYTES,
                CONFIG_MAX_ON_DISK_BYTES
        );
        // Create and start processor with directory monitoring
        LogTailToArrowProcessor processor = new LogTailToArrowProcessor(
                CONFIG_LOG_DIRECTORY,
                CONFIG_LOG_FILE_PATTERN,
                converter,
                httpSender,
                CONFIG_POLL_INTERVAL_MS
        );

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            processor.close();
        }));

        // Start processing
        processor.start();

        logger.info("Processor started. Monitoring directory for log files 💽📂...");

        // Keep main thread alive
        while (processor.isRunning()) {
            Thread.sleep(1000);
        }
    }
}