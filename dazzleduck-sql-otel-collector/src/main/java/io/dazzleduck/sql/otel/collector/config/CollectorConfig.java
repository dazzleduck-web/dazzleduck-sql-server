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
import io.dazzleduck.sql.otel.collector.compaction.CompactionSettings;
import io.dazzleduck.sql.otel.collector.query.QuerySettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
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
    /** Matches the {@code temp_write_location} default declared in reference.conf. */
    private static final String DEFAULT_TEMP_SUBDIRECTORY = "dazzleduck-writes";

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
     * Returns the startup SQL to execute on the singleton DuckDB connection, from the
     * {@code startup_script_provider} block ({@code content} + {@code script_location}) via
     * {@link StartupScriptProvider#load}. The deprecated {@code startup_script} string key is used
     * only when that block is absent.
     *
     * <p>Fails rather than falling back when the block is present but cannot produce a script — an
     * unknown provider class, an undefined {@code ${ENV}} reference, or a {@code script_location}
     * that is not a readable file — since running without it (e.g. with catalogs never attached)
     * only surfaces later, far from the cause.
     */
    public String getStartupScript() {
        String providerPath = CONFIG_PREFIX + "." + StartupScriptProvider.STARTUP_SCRIPT_CONFIG_PREFIX;
        if (!config.hasPath(providerPath)) {
            String deprecatedPath = CONFIG_PREFIX + ".startup_script";
            return config.hasPath(deprecatedPath)
                    ? config.getString(deprecatedPath) : "INSTALL arrow FROM community; LOAD arrow;";
        }
        // The built-in provider skips a script_location that is not a file; here that is an error.
        // A custom provider class may resolve script_location differently (S3, classpath), so the
        // check applies only when no class is configured.
        String locationPath = providerPath + ".script_location";
        if (!config.hasPath(providerPath + ".class") && config.hasPath(locationPath)) {
            String location = config.getString(locationPath);
            if (!location.isBlank() && !Files.isRegularFile(Path.of(location))) {
                throw new IllegalArgumentException(
                        "startup_script_provider.script_location is not a readable file: " + location);
            }
        }
        try {
            return StartupScriptProvider.load(config.getConfig(CONFIG_PREFIX)).getStartupScript();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load startup_script_provider: " + e.getMessage(), e);
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
                java.time.Duration.ofSeconds(5));
    }

    /**
     * Raw SQL applied to the ingestion DuckDB instance at startup, from
     * {@code otel_collector.ingestion.connection_settings}. The default lives in
     * {@code reference.conf} so it is visible and overridable rather than hidden in code;
     * an empty list restores DuckDB's own defaults.
     */
    public java.util.List<String> getIngestionConnectionSettings() {
        String path = CONFIG_PREFIX + ".ingestion." + ConfigConstants.CONNECTION_SETTINGS_KEY;
        return config.hasPath(path) ? config.getStringList(path) : java.util.List.of();
    }

    /**
     * Returns the single unified {@link IngestionHandler} from the top-level
     * {@code ingestion_task_factory_provider} block.
     */
    public IngestionHandler getIngestionHandler() {
        return loadIngestionTaskFactory("ingestion_task_factory_provider", "./otel-output");
    }

    /**
     * NOOP (plain Parquet under {@code defaultPath}) when no provider is configured: the block is
     * absent, or — as in the bundled reference.conf — present without {@code class} or
     * {@code ingestion_path}. A block that configures a provider but fails to load or validate fails
     * startup: falling back to NOOP would write data to local disk and never register it in the
     * catalog, while the collector looks healthy.
     */
    private IngestionHandler loadIngestionTaskFactory(String providerKey, String defaultPath) {
        String blockPath = CONFIG_PREFIX + "." + providerKey;
        if (!config.hasPath(blockPath)
                || (!config.hasPath(blockPath + ".class") && !config.hasPath(blockPath + ".ingestion_path"))) {
            return new NOOPIngestionTaskFactoryProvider(defaultPath).getIngestionHandler();
        }
        try {
            var defaultProvider = new NOOPIngestionTaskFactoryProvider(defaultPath);
            var provider = ConfigBasedProvider.load(
                    config.getConfig(CONFIG_PREFIX), providerKey,
                    (IngestionTaskFactoryProvider) defaultProvider);
            provider.validate();
            return provider.getIngestionHandler();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load " + providerKey + ": " + e.getMessage(), e);
        }
    }

    /**
     * Directory under which each signal service creates its scratch directory for temporary Arrow
     * batch files — the same {@code temp_write_location} key the flight module uses for the same
     * purpose, via {@link ConfigConstants#TEMP_WRITE_LOCATION_KEY}.
     *
     * <p>Declared in reference.conf as {@code ${java.io.tmpdir}"/dazzleduck-writes"} — the same
     * default as the flight module, which resolves to {@code /tmp/dazzleduck-writes} on Linux. The
     * explicit fallback here covers a caller supplying its own Config without the bundled
     * reference.conf, and must stay in step with that declared default.
     */
    public String getTempWriteLocation() {
        return getString(ConfigConstants.TEMP_WRITE_LOCATION_KEY,
                Path.of(System.getProperty("java.io.tmpdir"), DEFAULT_TEMP_SUBDIRECTORY).toString());
    }

    /** DuckDB {@code TimeZone} the server applies instance-wide at start; blank (the default) leaves it alone. */
    public String getTimeZone() {
        return getString("time_zone", "");
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

    /**
     * Login delegation target, or null when not configured. When set it must be an absolute
     * {@code http}/{@code https} URL: a value that is not — or not a string — fails startup, since
     * treating it as unset would silently switch authentication to local users.
     */
    public String getLoginUrl() {
        String fullPath = CONFIG_PREFIX + ".login_url";
        if (!config.hasPath(fullPath)) {
            return null;
        }
        String value = config.getString(fullPath);
        URI uri;
        try {
            uri = URI.create(value.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("login_url is not a valid URL: '" + value + "'", e);
        }
        if (!uri.isAbsolute() || uri.getHost() == null
                || !("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))) {
            throw new IllegalArgumentException("login_url must be an absolute http(s) URL, got: '" + value + "'");
        }
        return value.trim();
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

    /**
     * See {@code metrics.write_description} in reference.conf; off unless set. Strict: a value that
     * is not a boolean fails startup (ConfigException naming the key and its origin) rather than
     * silently falling back to off.
     */
    public boolean getMetricsWriteDescription() {
        String fullPath = CONFIG_PREFIX + ".metrics.write_description";
        return config.hasPath(fullPath) && config.getBoolean(fullPath);
    }

    /**
     * See {@code attributes.string_copy} in reference.conf; on unless set to false. Strict like
     * {@link #getMetricsWriteDescription()}: a value that is not a boolean fails startup.
     */
    public boolean getAttributesStringCopy() {
        String fullPath = CONFIG_PREFIX + ".attributes.string_copy";
        return !config.hasPath(fullPath) || config.getBoolean(fullPath);
    }

    /** The {@code compaction} block, parsed strictly; disabled when the block is absent. */
    public CompactionSettings getCompactionSettings() {
        String path = CONFIG_PREFIX + ".compaction";
        return config.hasPath(path) ? CompactionSettings.from(config.getConfig(path)) : CompactionSettings.disabled();
    }

    /** The {@code query} block, parsed strictly; disabled when the block is absent. */
    public QuerySettings getQuerySettings() {
        String path = CONFIG_PREFIX + ".query";
        return config.hasPath(path) ? QuerySettings.from(config.getConfig(path)) : QuerySettings.disabled();
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
        props.setUsers(getUsers());
        props.setJwtExpiration(getJwtExpiration());
        props.setServiceName(getServiceName());
        props.setIngestionHandler(getIngestionHandler());
        props.setIngestionConfig(getIngestionConfig());
        props.setVerifySignature(getVerifySignature());
        props.setTempWriteLocation(getTempWriteLocation());
        props.setTimeZone(getTimeZone());
        props.setMetricsWriteDescription(getMetricsWriteDescription());
        props.setAttributesStringCopy(getAttributesStringCopy());
        props.setCompactionSettings(getCompactionSettings());
        props.setQuerySettings(getQuerySettings());
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
