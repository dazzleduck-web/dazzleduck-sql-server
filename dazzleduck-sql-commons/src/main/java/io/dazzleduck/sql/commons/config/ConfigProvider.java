package io.dazzleduck.sql.commons.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Supplies configuration overrides from somewhere other than the HOCON files — typically a table in
 * the very database the service is about to operate on.
 *
 * <p>The problem it solves: a long-lived service's tunables live in a file baked into its image, so
 * changing one means a rebuild and a redeploy, and nothing can answer "what is this deployment
 * actually running?" without opening a container. Pointing the service at a table it already has a
 * connection to makes the values queryable, auditable, and changeable without a new image.
 *
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
 * A provider that reads a database must run AFTER the startup script — that script is what attaches
 * the catalog. Callers therefore execute the startup script first, then {@link #overrides(Config)},
 * then build their typed config from the result.
 *
 * <h2>Semantics</h2>
 *
 * The returned {@link Config} is an OVERLAY, not a replacement: the caller applies it with the
 * file-based config as fallback, so a key the provider does not supply keeps its file value. That
 * is what lets a deployment adopt this incrementally, and what keeps the bundled
 * {@code application.conf} a meaningful set of defaults rather than dead weight.
 *
 * <p>Absent by default: with no {@code config_provider} block {@link #load} returns {@link #NONE},
 * and behaviour is byte-identical to before this interface existed.
 */
public interface ConfigProvider extends ConfigBasedProvider {

    String CONFIG_PROVIDER_PREFIX = "config_provider";

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
    ConfigProvider NONE = new ConfigProvider() {
        @Override
        public void setConfig(Config config) {
        }

        @Override
        public Config overrides(Config serviceConfig) {
            return ConfigFactory.empty();
        }
    };

    /**
     * Loads the provider named by {@code config}'s {@code config_provider} block, or {@link #NONE}
     * when the block is absent or names no {@code class}.
     *
     * <p>Delegates to {@link ConfigBasedProvider#load(Config, String, ConfigBasedProvider)} — the
     * shared class-name/constructor resolution every other provider SPI here uses — rather than
     * repeating that reflection.
     */
    static ConfigProvider load(Config config) throws Exception {
        return ConfigBasedProvider.load(config, CONFIG_PROVIDER_PREFIX, NONE);
    }
}
