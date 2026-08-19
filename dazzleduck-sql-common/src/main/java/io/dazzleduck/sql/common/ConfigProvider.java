package io.dazzleduck.sql.common;

import com.typesafe.config.Config;

/**
 * Supplies configuration overrides from somewhere other than the HOCON files — typically a table in
 * the very database the service is about to operate on.
 *
 * <p>The problem it solves: a long-lived service's tunables live in a file baked into its image, so
 * changing one means a rebuild and a redeploy, and nothing can answer "what is this deployment
 * actually running?" without opening a container. Pointing the service at a table it already has a
 * connection to makes the values queryable, auditable, and changeable without a new image.
 *
 * <p>Load via {@link #load(Config)}, which honours a {@code config_provider} block:
 * <pre>{@code
 * dazzleduck_sql_compaction {
 *     minor_compaction_frequency = 1 minute      # fallback when the provider has no such key
 *     config_provider {
 *         class = "io.dazzleduck.sql.commons.TableConfigProvider"
 *         table = "mylake.main.v_config"
 *         key_column = "config_key"
 *         value_column = "value"
 *         prefix = "compaction."                 # stripped from each key before overlaying
 *     }
 * }
 * }</pre>
 *
 * <h2>Ordering</h2>
 *
 * A provider that reads a database must run AFTER {@link StartupScriptProvider} — the startup
 * script is what attaches the catalog. Callers therefore execute the startup script first, then
 * {@link #overrides(Config)}, then build their typed config from the result.
 *
 * <h2>Semantics</h2>
 *
 * The returned {@link Config} is an OVERLAY, not a replacement: the caller applies it with the
 * file-based config as fallback, so a key the provider does not supply keeps its file value. That
 * is what lets a deployment adopt this incrementally, and what makes the bundled
 * {@code application.conf} a meaningful set of defaults rather than dead weight.
 *
 * <p>Absent by default. With no {@code config_provider} block, {@link #load} returns a provider
 * that supplies nothing, and behaviour is byte-identical to before this interface existed.
 */
public interface ConfigProvider {

    String CONFIG_PROVIDER_PREFIX = "config_provider";

    /** Called by {@link #load} for a no-arg implementation; the block itself, not the root config. */
    default void setConfig(Config config) {
    }

    /**
     * The overrides to overlay on the file-based config, possibly empty. Never null.
     *
     * @param serviceConfig the caller's own config block, so a provider can see what it is
     *                      overriding — the compactor passes {@code dazzleduck_sql_compaction}.
     * @throws Exception when the source cannot be read. Whether that is fatal is the CALLER's
     *         decision, not the provider's: a service whose configuration must be authoritative
     *         should refuse to start, while one that can run on its bundled defaults may log and
     *         continue. The provider's job is to report the failure honestly, not to choose.
     */
    Config overrides(Config serviceConfig) throws Exception;

    /** Supplies nothing — the default when no {@code config_provider} block is present. */
    ConfigProvider NONE = serviceConfig -> com.typesafe.config.ConfigFactory.empty();

    /**
     * Loads a {@link ConfigProvider} from {@code config}'s {@code config_provider} block, or
     * {@link #NONE} when the block is absent or names no {@code class}.
     *
     * <p>Mirrors {@link StartupScriptProvider#load}: a {@code Config}-taking constructor is
     * preferred, falling back to a no-arg constructor plus {@link #setConfig}.
     */
    static ConfigProvider load(Config config) throws Exception {
        if (!config.hasPath(CONFIG_PROVIDER_PREFIX)) {
            return NONE;
        }
        Config providerConfig = config.getConfig(CONFIG_PROVIDER_PREFIX);
        if (!providerConfig.hasPath("class")) {
            return NONE;
        }
        String className = providerConfig.getString("class");
        try {
            var ctor = Class.forName(className).getConstructor(Config.class);
            return (ConfigProvider) ctor.newInstance(providerConfig);
        } catch (NoSuchMethodException e) {
            var provider = (ConfigProvider) Class.forName(className).getConstructor().newInstance();
            provider.setConfig(providerConfig);
            return provider;
        }
    }
}
