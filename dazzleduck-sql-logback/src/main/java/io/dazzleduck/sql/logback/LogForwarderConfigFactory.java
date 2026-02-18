package io.dazzleduck.sql.logback;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigConstants;

import java.io.File;
import java.time.Duration;

/**
 * Factory for creating LogForwarderConfig instances configured from application.conf
 * or from a dedicated per-component conf file.
 *
 * <p>Expected configuration format (in application.conf or a separate .conf file):</p>
 * <pre>{@code
 * dazzleduck_logback {
 *   enabled = true
 *
 *   max_buffer_size = 10000
 *   poll_interval_ms = 5000
 *
 *   http {
 *     base_url = "http://localhost:8081"
 *     username = "admin"
 *     password = "admin"
 *     ingestion_queue = "log"
 *     http_client_timeout_ms = 3000
 *   }
 *
 *   min_batch_size = 1048576
 *   max_batch_size = 10485760
 *   max_send_interval_ms = 2000
 *   max_in_memory_bytes = 10485760
 *   max_on_disk_bytes = 1073741824
 *   retry_count = 3
 *   retry_interval_ms = 1000
 *   project = ["*", "CAST (timestamp AS date) AS date"]
 *   partition_by = [date]
 * }
 * }</pre>
 *
 * <p>A separate per-component file only needs to override the keys that differ from the defaults
 * in reference.conf. For example, a minimal controller-logback.conf:</p>
 * <pre>{@code
 * dazzleduck_logback {
 *   http {
 *     base_url = "http://controller-server:8081"
 *     ingestion_queue = "controller-logs"
 *   }
 *   partition_by = [date]
 * }
 * }</pre>
 */
public final class LogForwarderConfigFactory {

    private static final String CONFIG_ROOT = "dazzleduck_logback";

    private LogForwarderConfigFactory() {}

    /**
     * Create a LogForwarder from the default application configuration.
     *
     * @return A configured and started LogForwarder
     */
    public static LogForwarder createForwarder() {
        return new LogForwarder(createConfig());
    }

    /**
     * Create a LogForwarderConfig from the default application configuration
     * (application.conf / reference.conf on the classpath).
     *
     * @return A configured LogForwarderConfig
     */
    public static LogForwarderConfig createConfig() {
        Config root = ConfigFactory.load().getConfig(CONFIG_ROOT);
        return buildConfig(root);
    }

    /**
     * Create a LogForwarderConfig by reading a dedicated conf file.
     * The file must contain a {@code dazzleduck_logback} block; any keys not present
     * in the file fall back to the defaults defined in reference.conf.
     *
     * @param configFilePath path to a TypeSafe Config (.conf) file on the filesystem
     *                       or on the classpath (classpath resources are resolved first)
     * @return A configured LogForwarderConfig
     */
    public static LogForwarderConfig createConfig(String configFilePath) {
        Config fallback = ConfigFactory.load();
        Config fileConfig;

        File file = new File(configFilePath);
        if (file.isAbsolute() || file.exists()) {
            fileConfig = ConfigFactory.parseFile(file).withFallback(fallback);
        } else {
            // Try classpath resource first, then filesystem relative path
            fileConfig = ConfigFactory.parseResources(configFilePath).withFallback(fallback);
            if (!fileConfig.hasPath(CONFIG_ROOT)) {
                fileConfig = ConfigFactory.parseFile(file).withFallback(fallback);
            }
        }

        Config root = fileConfig.resolve().getConfig(CONFIG_ROOT);
        return buildConfig(root);
    }

    private static LogForwarderConfig buildConfig(Config config) {
        Config http = config.getConfig(ConfigConstants.HTTP_PREFIX);

        return LogForwarderConfig.builder()
                .baseUrl(http.getString(ConfigConstants.BASE_URL_KEY))
                .username(http.getString(ConfigConstants.USERNAME_KEY))
                .password(http.getString(ConfigConstants.PASSWORD_KEY))
                .ingestionQueue(http.getString(ConfigConstants.INGESTION_QUEUE_KEY))
                .httpClientTimeout(Duration.ofMillis(http.getLong(ConfigConstants.HTTP_CLIENT_TIMEOUT_MS_KEY)))
                .maxBufferSize(config.getInt(ConfigConstants.MAX_BUFFER_SIZE_KEY))
                .pollInterval(Duration.ofMillis(config.getLong(ConfigConstants.POLL_INTERVAL_MS_KEY)))
                .minBatchSize(config.getLong(ConfigConstants.MIN_BATCH_SIZE_KEY))
                .maxBatchSize(config.getLong(ConfigConstants.MAX_BATCH_SIZE_KEY))
                .maxSendInterval(Duration.ofMillis(config.getLong(ConfigConstants.MAX_SEND_INTERVAL_MS_KEY)))
                .maxInMemorySize(config.getLong(ConfigConstants.MAX_IN_MEMORY_BYTES_KEY))
                .maxOnDiskSize(config.getLong(ConfigConstants.MAX_ON_DISK_BYTES_KEY))
                .retryCount(config.getInt(ConfigConstants.RETRY_COUNT_KEY))
                .retryIntervalMillis(config.getLong(ConfigConstants.RETRY_INTERVAL_MS_KEY))
                .project(config.getStringList(ConfigConstants.PROJECT_KEY))
                .partitionBy(config.getStringList(ConfigConstants.PARTITION_BY_KEY))
                .enabled(config.getBoolean(ConfigConstants.ENABLED_KEY))
                .build();
    }
}
