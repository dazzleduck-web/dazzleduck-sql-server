package io.dazzleduck.sql.otel.collector.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.common.StartupScriptProvider;
import io.dazzleduck.sql.commons.config.ConfigBasedProvider;
import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
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

    public int getHealthPort() {
        return getInt("health.port", 8081);
    }

    public Duration getShutdownGracePeriod() {
        return Duration.ofMillis(getLong("health.shutdown_grace_period_ms", 2_000L));
    }

    /**
     * Returns the startup SQL to execute on the singleton DuckDB connection.
     * Delegates to {@link StartupScriptProvider#load} which reads from the
     * {@code startup_script_provider} block ({@code content} + {@code script_location}).
     * Falls back to the deprecated {@code startup_script} string key.
     */
    public String getStartupScript() {
        try {
            return StartupScriptProvider.load(config.getConfig(CONFIG_PREFIX)).getStartupScript();
        } catch (Exception e) {
            log.warn("Failed to read startup_script_provider, using fallback: {}", e.getMessage());
            return getString("startup_script", "INSTALL arrow FROM community; LOAD arrow;");
        }
    }

    /**
     * Returns queue tuning parameters from the {@code otel_collector.ingestion} block,
     * mirroring the flight module's ingestion config pattern.
     */
    public IngestionConfig getIngestionConfig() {
        String path = CONFIG_PREFIX + ".ingestion";
        if (config.hasPath(path)) {
            return IngestionConfig.fromConfig(config.getConfig(path));
        }
        return new IngestionConfig(1_048_576L, IngestionConfig.DEFAULT_MAX_BUCKET_SIZE,
                IngestionConfig.DEFAULT_MAX_BATCHES, IngestionConfig.DEFAULT_MAX_PENDING_WRITE,
                java.time.Duration.ofSeconds(5), IngestionConfig.DEFAULT_CONFIG_REFRESH);
    }

    /**
     * Returns the single unified {@link IngestionHandler} from the top-level
     * {@code ingestion_task_factory_provider} block.
     */
    public IngestionHandler getIngestionHandler() {
        return loadIngestionTaskFactory("ingestion_task_factory_provider", "./otel-output");
    }

    private IngestionHandler loadIngestionTaskFactory(String providerKey, String defaultPath) {
        try {
            var defaultProvider = new NOOPIngestionTaskFactoryProvider(defaultPath);
            var provider = ConfigBasedProvider.load(
                    config.getConfig(CONFIG_PREFIX), providerKey,
                    (IngestionTaskFactoryProvider) defaultProvider);
            provider.validate();
            return provider.getIngestionHandler();
        } catch (Exception e) {
            log.warn("Failed to load {}, using NOOP: {}", providerKey, e.getMessage());
            return new NOOPIngestionTaskFactoryProvider(defaultPath).getIngestionHandler();
        }
    }

    public String getServiceName() {
        return getString("service_name", "open-telemetry-collector");
    }

    public String getAuthentication() {
        return getString("authentication", "jwt"); // "jwt" is the only supported value
    }

    public String getSecretKey() {
        return getString("secret_key", null);
    }

    public String getLoginUrl() {
        return getString("login_url", null);
    }

    /**
     * Cluster this collector serves. When set, every call must carry a matching
     * {@code x-dd-cluster} JWT claim; when unset, the claim is ignored.
     */
    public String getCluster() {
        return getString("cluster", null);
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
        String fullPath = CONFIG_PREFIX + "." + ConfigConstants.JWT_TOKEN_EXPIRATION_KEY;
        try {
            if (config.hasPath(fullPath)) {
                return config.getDuration(fullPath);
            }
        } catch (Exception e) {
            log.debug("Error reading {}: {}", fullPath, e.getMessage());
        }
        return Duration.ofHours(1);
    }

    public boolean getVerifySignature() {
        String fullPath = CONFIG_PREFIX + "." + ConfigConstants.JWT_TOKEN_VERIFY_SIGNATURE_KEY;
        try {
            if (config.hasPath(fullPath)) {
                return config.getBoolean(fullPath);
            }
        } catch (Exception e) {
            log.debug("Error reading {}: {}", fullPath, e.getMessage());
        }
        return true;
    }

    public CollectorProperties toProperties() {
        CollectorProperties props = new CollectorProperties();
        props.setGrpcPort(getGrpcPort());
        props.setHealthPort(getHealthPort());
        props.setShutdownGracePeriod(getShutdownGracePeriod());
        props.setStartupScript(getStartupScript());
        props.setAuthentication(getAuthentication());
        props.setSecretKey(getSecretKey());
        props.setLoginUrl(getLoginUrl());
        props.setCluster(getCluster());
        props.setUsers(getUsers());
        props.setJwtExpiration(getJwtExpiration());
        props.setServiceName(getServiceName());
        props.setIngestionHandler(getIngestionHandler());
        props.setIngestionConfig(getIngestionConfig());
        props.setVerifySignature(getVerifySignature());
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
