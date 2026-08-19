package io.dazzleduck.sql.flight;

import com.typesafe.config.Config;

/**
 * @deprecated Use {@link io.dazzleduck.sql.common.StartupScriptProvider}.
 *             <p>This interface duplicated the one in {@code dazzleduck-sql-common}, which is the
 *             layer a startup script belongs in: reading SQL to run at connection setup has nothing
 *             to do with Arrow Flight, and a service that needs only that had to depend on the
 *             whole Flight module to get it. It is retained so that any implementation declaring
 *             {@code implements io.dazzleduck.sql.flight.StartupScriptProvider} still compiles and
 *             still loads.
 *             <p>The companion {@link ConfigBasedStartupScriptProvider} shim is the one that
 *             matters at runtime, since HOCON can name that class by string.
 */
@Deprecated(forRemoval = true)
public interface StartupScriptProvider extends io.dazzleduck.sql.common.StartupScriptProvider {

    String STARTUP_SCRIPT_CONFIG_PREFIX =
            io.dazzleduck.sql.common.StartupScriptProvider.STARTUP_SCRIPT_CONFIG_PREFIX;

    /** @deprecated Use {@link io.dazzleduck.sql.common.StartupScriptProvider#load(Config)}. */
    @Deprecated(forRemoval = true)
    static io.dazzleduck.sql.common.StartupScriptProvider load(Config config) throws Exception {
        return io.dazzleduck.sql.common.StartupScriptProvider.load(config);
    }

    /** @deprecated Use {@link io.dazzleduck.sql.common.StartupScriptProvider#replaceEnvVariable}. */
    @Deprecated(forRemoval = true)
    static String replaceEnvVariable(String content) {
        return io.dazzleduck.sql.common.StartupScriptProvider.replaceEnvVariable(content);
    }
}
