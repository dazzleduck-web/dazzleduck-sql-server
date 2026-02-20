package io.dazzleduck.sql.scrapper.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.scrapper.CollectorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads collector configuration from HOCON files.
 *
 * Configuration is loaded in the following order (later sources override earlier):
 * 1. application.conf from classpath (defaults)
 * 2. application.conf from current directory (optional override)
 * 3. File specified by -c/--config CLI argument (optional override)
 * 4. System properties (highest priority)
 *
 * Example HOCON configuration:
 * <pre>
 * collector {
 *     enabled = true
 *     targets = ["/actuator/prometheus"]
 *     target-prefix = "http://localhost:8080"
 *     server-url = "http://localhost:8081/ingest"
 *     path = "scraped_metrics"
 *     scrape-interval-ms = 15000
 *
 *     tailing {
 *         flush-threshold = 100
 *         flush-interval-ms = 5000
 *         max-buffer-size = 10000
 *     }
 *
 *     http {
 *         connection-timeout-ms = 5000
 *         read-timeout-ms = 10000
 *         max-retries = 3
 *         retry-delay-ms = 1000
 *     }
 *
 *     identity {
 *         id = "metrics-collector"
 *         name = "Metrics Collector"
 *         host = ""
 *     }
 * }
 * </pre>
 */
public class CollectorConfig {

    private static final Logger log = LoggerFactory.getLogger(CollectorConfig.class);
    private static final String CONFIG_PREFIX = "collector";

    private final Config config;

    /**
     * Load configuration from default locations.
     */
    public CollectorConfig() {
        this.config = ConfigFactory.load().resolve();
        log.debug("Configuration loaded from default locations");
    }

    /**
     * Load configuration with optional external config file.
     *
     * Configuration is loaded in the following priority (highest to lowest):
     * 1. System properties
     * 2. External config file (if specified)
     * 3. application.conf from classpath
     *
     * @param externalConfigPath path to external config file (can be null)
     */
    public CollectorConfig(String externalConfigPath) {
        Config classpathConfig = ConfigFactory.load();
        Config resultConfig;

        // Load external config file if specified
        if (externalConfigPath != null && !externalConfigPath.isEmpty()) {
            File externalFile = new File(externalConfigPath);
            if (externalFile.exists()) {
                log.info("Loading external configuration from: {}", externalConfigPath);
                Config externalConfig = ConfigFactory.parseFile(externalFile);
                // External config overrides classpath config
                Config withExternal = externalConfig.withFallback(classpathConfig);
                // System properties override everything
                resultConfig = ConfigFactory.systemProperties().withFallback(withExternal);
            } else {
                log.warn("External configuration file not found: {}", externalConfigPath);
                resultConfig = classpathConfig;
            }
        } else {
            resultConfig = classpathConfig;
        }

        // Resolve and cache config
        this.config = resultConfig.resolve();

        log.debug("Configuration loaded successfully");
    }

    /**
     * Create CollectorConfig from an existing Config object.
     * Useful for testing.
     *
     * @param config the config object
     */
    public CollectorConfig(Config config) {
        this.config = config;
    }

    /**
     * Get the underlying Config object.
     */
    public Config getConfig() {
        return config;
    }

    /**
     * Check if collector is enabled.
     */
    public boolean isEnabled() {
        return getBoolean("enabled", false);
    }

    /**
     * Get list of target endpoints.
     */
    public List<String> getTargets() {
        return getStringList("targets", new ArrayList<>());
    }

    /**
     * Get target URL prefix.
     */
    public String getTargetPrefix() {
        return getString("target-prefix", "");
    }

    /**
     * Get server URL to send metrics.
     */
    public String getServerUrl() {
        return getString("server-url", "http://localhost:8081/ingest");
    }

    /**
     * Get server URL to send metrics.
     */
    public String getBaseUrl() {
        return getString("base-url", "http://localhost:8081");
    }

    /**
     * Get path parameter for server URL.
     */
    public String getPath() {
        return getString("path", "scraped_metrics");
    }

    /**
     * Get scrape interval in milliseconds.
     */
    public int getScrapeIntervalMs() {
        return getInt("scrape-interval-ms", 15000);
    }

    /**
     * Get flush threshold (tailing mode).
     */
    public int getFlushThreshold() {
        return getInt("tailing.flush-threshold", 100);
    }

    /**
     * Get flush interval in milliseconds (tailing mode).
     */
    public int getFlushIntervalMs() {
        return getInt("tailing.flush-interval-ms", 5000);
    }

    /**
     * Get maximum buffer size (tailing mode).
     */
    public int getMaxBufferSize() {
        return getInt("tailing.max-buffer-size", 10000);
    }

    /**
     * Get HTTP connection timeout in milliseconds.
     */
    public int getConnectionTimeoutMs() {
        return getInt("http.connection-timeout-ms", 5000);
    }

    /**
     * Get HTTP read timeout in milliseconds.
     */
    public int getReadTimeoutMs() {
        return getInt("http.read-timeout-ms", 10000);
    }

    /**
     * Get maximum retry attempts.
     */
    public int getMaxRetries() {
        return getInt("http.max-retries", 3);
    }

    /**
     * Get retry delay in milliseconds.
     */
    public int getRetryDelayMs() {
        return getInt("http.retry-delay-ms", 1000);
    }

    /**
     * Get collector ID.
     */
    public String getCollectorId() {
        return getString("identity.id", "metrics-collector");
    }

    /**
     * Get collector name.
     */
    public String getCollectorName() {
        return getString("identity.name", "Metrics Collector");
    }

    /**
     * Get collector host.
     */
    public String getCollectorHost() {
        return getString("identity.host", "");
    }

    /**
     * Get server authentication username.
     */
    public String getUsername() {
        return getString("auth.username", "admin");
    }

    /**
     * Get server authentication password.
     */
    public String getPassword() {
        return getString("auth.password", "admin");
    }

    /**
     * Get minimum batch size in bytes.
     */
    public long getMinBatchSize() {
        return getLong("producer.min-batch-size", 1024);
    }

    /**
     * Get maximum batch size in bytes.
     */
    public long getMaxBatchSize() {
        return getLong("producer.max-batch-size", 16 * 1024 * 1024);
    }

    /**
     * Get maximum in-memory buffer size in bytes.
     */
    public long getMaxInMemorySize() {
        return getLong("producer.max-in-memory-size", 10 * 1024 * 1024);
    }

    /**
     * Get maximum on-disk buffer size in bytes.
     */
    public long getMaxOnDiskSize() {
        return getLong("producer.max-on-disk-size", 1024 * 1024 * 1024);
    }

    public List<String> getProject() {
        return getStringList("project", new ArrayList<>());
    }

    public List<String> getPartition() {
        return getStringList("partition", new ArrayList<>());
    }

    /**
     * Convert this configuration to CollectorProperties.
     * This provides backward compatibility with existing code.
     */
    public CollectorProperties toProperties() {
        CollectorProperties props = new CollectorProperties();
        props.setEnabled(isEnabled());
        props.setTargets(getTargets());
        props.setTargetPrefix(getTargetPrefix());
        props.setServerUrl(getServerUrl());
        props.setBaseUrl(getBaseUrl());
        props.setPath(getPath());
        props.setScrapeIntervalMs(getScrapeIntervalMs());
        props.setFlushThreshold(getFlushThreshold());
        props.setFlushIntervalMs(getFlushIntervalMs());
        props.setMaxBufferSize(getMaxBufferSize());
        props.setConnectionTimeoutMs(getConnectionTimeoutMs());
        props.setReadTimeoutMs(getReadTimeoutMs());
        props.setMaxRetries(getMaxRetries());
        props.setRetryDelayMs(getRetryDelayMs());
        props.setCollectorId(getCollectorId());
        props.setCollectorName(getCollectorName());
        props.setCollectorHost(getCollectorHost());
        props.setUsername(getUsername());
        props.setPassword(getPassword());
        props.setMinBatchSize(getMinBatchSize());
        props.setMaxBatchSize(getMaxBatchSize());
        props.setMaxInMemorySize(getMaxInMemorySize());
        props.setMaxOnDiskSize(getMaxOnDiskSize());
        props.setProject(getProject());
        props.setPartition(getPartition());
        return props;
    }

    // Helper methods for accessing config with defaults

    private String getString(String path, String defaultValue) {
        String fullPath = CONFIG_PREFIX + "." + path;
        try {
            if (config.hasPath(fullPath)) {
                return config.getString(fullPath);
            }
        } catch (Exception e) {
            log.debug("Error reading config path {}: {}", fullPath, e.getMessage());
        }
        return defaultValue;
    }

    private int getInt(String path, int defaultValue) {
        String fullPath = CONFIG_PREFIX + "." + path;
        try {
            if (config.hasPath(fullPath)) {
                return config.getInt(fullPath);
            }
        } catch (Exception e) {
            log.debug("Error reading config path {}: {}", fullPath, e.getMessage());
        }
        return defaultValue;
    }

    private boolean getBoolean(String path, boolean defaultValue) {
        String fullPath = CONFIG_PREFIX + "." + path;
        try {
            if (config.hasPath(fullPath)) {
                return config.getBoolean(fullPath);
            }
        } catch (Exception e) {
            log.debug("Error reading config path {}: {}", fullPath, e.getMessage());
        }
        return defaultValue;
    }

    private long getLong(String path, long defaultValue) {
        String fullPath = CONFIG_PREFIX + "." + path;
        try {
            if (config.hasPath(fullPath)) {
                return config.getLong(fullPath);
            }
        } catch (Exception e) {
            log.debug("Error reading config path {}: {}", fullPath, e.getMessage());
        }
        return defaultValue;
    }

    private List<String> getStringList(String path, List<String> defaultValue) {
        String fullPath = CONFIG_PREFIX + "." + path;
        try {
            if (config.hasPath(fullPath)) {
                return config.getStringList(fullPath);
            }
        } catch (Exception e) {
            log.debug("Error reading config path {}: {}", fullPath, e.getMessage());
        }
        return defaultValue;
    }

    @Override
    public String toString() {
        return "CollectorConfig{" +
                "enabled=" + isEnabled() +
                ", targets=" + getTargets() +
                ", targetPrefix='" + getTargetPrefix() + '\'' +
                ", serverUrl='" + getServerUrl() + '\'' +
                ", path='" + getPath() + '\'' +
                ", scrapeIntervalMs=" + getScrapeIntervalMs() +
                ", flushThreshold=" + getFlushThreshold() +
                ", flushIntervalMs=" + getFlushIntervalMs() +
                ", maxBufferSize=" + getMaxBufferSize() +
                ", collectorId='" + getCollectorId() + '\'' +
                ", project=" + getProject() + '\'' +
                ", partition=" + getPartition() + '\'' +
                '}';
    }
}
