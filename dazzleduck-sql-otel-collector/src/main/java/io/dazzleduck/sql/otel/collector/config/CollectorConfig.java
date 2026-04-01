package io.dazzleduck.sql.otel.collector.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.config.ConfigBasedProvider;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.IngestionTaskFactoryProvider;
import io.dazzleduck.sql.commons.ingestion.NOOPIngestionTaskFactoryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads OTEL collector configuration from HOCON files.
 *
 * Priority (highest to lowest):
 * 1. System properties
 * 2. Environment variables (otel_collector.* prefix)
 * 3. External config file (CLI -c argument)
 * 4. application.conf / reference.conf from classpath
 *
 * Example application.conf:
 * <pre>
 * otel_collector {
 *     grpc.port = 4317
 *     output.path = "./otel-logs"
 *     flush.threshold = 1000
 *     flush.interval-ms = 5000
 *     partition-by = []
 *     transformations = null
 * }
 * </pre>
 */
public class CollectorConfig {

    private static final Logger log = LoggerFactory.getLogger(CollectorConfig.class);
    private static final String CONFIG_PREFIX = "otel_collector";

    private final Config config;

    public CollectorConfig() {
        this.config = buildConfigFromEnv()
                .withFallback(ConfigFactory.systemProperties())
                .withFallback(ConfigFactory.load())
                .resolve();
    }

    public CollectorConfig(String externalConfigPath) {
        Config envConfig = buildConfigFromEnv();
        Config resultConfig;

        if (externalConfigPath != null && !externalConfigPath.isEmpty()) {
            File externalFile = new File(externalConfigPath);
            if (externalFile.exists()) {
                log.info("Loading external configuration from: {}", externalConfigPath);
                Config externalConfig = ConfigFactory.parseFile(externalFile);
                resultConfig = envConfig
                        .withFallback(ConfigFactory.systemProperties())
                        .withFallback(externalConfig)
                        .withFallback(ConfigFactory.load());
            } else {
                log.warn("External configuration file not found: {}", externalConfigPath);
                resultConfig = envConfig
                        .withFallback(ConfigFactory.systemProperties())
                        .withFallback(ConfigFactory.load());
            }
        } else {
            resultConfig = envConfig
                    .withFallback(ConfigFactory.systemProperties())
                    .withFallback(ConfigFactory.load());
        }

        this.config = resultConfig.resolve();
    }

    public CollectorConfig(Config config) {
        this.config = config;
    }

    private static Config buildConfigFromEnv() {
        StringBuilder hocon = new StringBuilder();
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (!key.startsWith(CONFIG_PREFIX + ".") && !key.equals(CONFIG_PREFIX)) {
                continue;
            }
            String trimmed = value.trim();
            if (trimmed.startsWith("[") || trimmed.startsWith("{")
                    || trimmed.equals("true") || trimmed.equals("false")
                    || trimmed.matches("-?\\d+(\\.\\d+)?")) {
                hocon.append(key).append(" = ").append(trimmed).append("\n");
            } else {
                hocon.append(key).append(" = \"")
                        .append(value.replace("\\", "\\\\").replace("\"", "\\\""))
                        .append("\"\n");
            }
        }
        return hocon.isEmpty() ? ConfigFactory.empty() : ConfigFactory.parseString(hocon.toString());
    }

    public int getGrpcPort() {
        return getInt("grpc_port", 4317);
    }

    public String getStartupScript() {
        return getString("startup_script", "INSTALL arrow FROM community; LOAD arrow;");
    }

    public SignalIngestionConfig getLogIngestionConfig() {
        return readSignalConfig("log_ingestion");
    }

    public SignalIngestionConfig getTraceIngestionConfig() {
        return readSignalConfig("trace_ingestion");
    }

    public SignalIngestionConfig getMetricIngestionConfig() {
        return readSignalConfig("metric_ingestion");
    }

    private SignalIngestionConfig readSignalConfig(String signalKey) {
        String prefix = CONFIG_PREFIX + "." + signalKey;
        String outputPath = config.getString(prefix + ".output_path");
        List<String> partitionBy = List.of();
        String transformation = null;
        long minBucketSizeBytes = getLong("ingestion.min_bucket_size", 1_048_576L);
        long maxDelayMs = getLong("ingestion.max_delay_ms", 5000L);
        try {
            if (config.hasPath(prefix + ".partition_by")) {
                partitionBy = config.getStringList(prefix + ".partition_by");
            }
            if (config.hasPath(prefix + ".transformation")) {
                transformation = config.getString(prefix + ".transformation");
            }
            if (config.hasPath(prefix + ".min_bucket_size")) {
                minBucketSizeBytes = config.getLong(prefix + ".min_bucket_size");
            }
            if (config.hasPath(prefix + ".max_delay_ms")) {
                maxDelayMs = config.getLong(prefix + ".max_delay_ms");
            }
        } catch (Exception e) {
            log.debug("Error reading signal config for {}: {}", signalKey, e.getMessage());
        }
        return new SignalIngestionConfig(outputPath, partitionBy, transformation, minBucketSizeBytes, maxDelayMs);
    }

    public IngestionHandler getLogIngestionTaskFactory() {
        return loadIngestionTaskFactory("log_ingestion_task_factory_provider", getLogIngestionConfig().outputPath());
    }

    public IngestionHandler getTraceIngestionTaskFactory() {
        return loadIngestionTaskFactory("trace_ingestion_task_factory_provider", getTraceIngestionConfig().outputPath());
    }

    public IngestionHandler getMetricIngestionTaskFactory() {
        return loadIngestionTaskFactory("metric_ingestion_task_factory_provider", getMetricIngestionConfig().outputPath());
    }

    private IngestionHandler loadIngestionTaskFactory(String providerKey, String defaultPath) {
        try {
            var defaultProvider = new NOOPIngestionTaskFactoryProvider(defaultPath);
            var provider = ConfigBasedProvider.load(
                    config.getConfig(CONFIG_PREFIX), providerKey,
                    (IngestionTaskFactoryProvider) defaultProvider);
            provider.validate();
            return provider.getIngestionTaskFactory();
        } catch (Exception e) {
            log.warn("Failed to load {}, using NOOP: {}", providerKey, e.getMessage());
            return new NOOPIngestionTaskFactoryProvider(defaultPath).getIngestionTaskFactory();
        }
    }

    public String getServiceName() {
        return getString("service_name", "open-telemetry-collector");
    }

    public String getAuthentication() {
        return getString("authentication", "jwt");
    }

    public String getSecretKey() {
        return getString("secret_key", null);
    }

    public String getLoginUrl() {
        return getString("login_url", null);
    }

    public Map<String, String> getUsers() {
        String fullPath = CONFIG_PREFIX + ".users";
        var users = new HashMap<String, String>();
        try {
            if (config.hasPath(fullPath)) {
                config.getConfigList(fullPath).forEach(c ->
                        users.put(c.getString("username"), c.getString("password")));
            }
        } catch (Exception e) {
            log.debug("Error reading users config: {}", e.getMessage());
        }
        return users;
    }

    public Duration getJwtExpiration() {
        String fullPath = CONFIG_PREFIX + ".jwt_token.expiration";
        try {
            if (config.hasPath(fullPath)) {
                return config.getDuration(fullPath);
            }
        } catch (Exception e) {
            log.debug("Error reading jwt_token.expiration: {}", e.getMessage());
        }
        return Duration.ofHours(1);
    }

    public CollectorProperties toProperties() {
        CollectorProperties props = new CollectorProperties();
        props.setGrpcPort(getGrpcPort());
        props.setStartupScript(getStartupScript());
        props.setAuthentication(getAuthentication());
        props.setSecretKey(getSecretKey());
        props.setLoginUrl(getLoginUrl());
        props.setUsers(getUsers());
        props.setJwtExpiration(getJwtExpiration());
        props.setServiceName(getServiceName());
        props.setLogIngestionConfig(getLogIngestionConfig());
        props.setTraceIngestionConfig(getTraceIngestionConfig());
        props.setMetricIngestionConfig(getMetricIngestionConfig());
        props.setLogIngestionTaskFactory(getLogIngestionTaskFactory());
        props.setTraceIngestionTaskFactory(getTraceIngestionTaskFactory());
        props.setMetricIngestionTaskFactory(getMetricIngestionTaskFactory());
        return props;
    }

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
}
