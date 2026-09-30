package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.TableConfigProvider;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.common.StartupScriptProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Config rawConfig = CompactionConfig.rawConfig(args);

        // Telemetry comes first so the startup lines below (script, config overlay, failures) are
        // exported too. It reads the file/env config only; config-provider overrides don't apply.
        CompactionTelemetry telemetry = CompactionTelemetry.create(
                rawConfig.getConfig("metrics"), rawConfig.getConfig("logs"));
        // Declared here so a failed start can stop whatever it already started (see the catch).
        CompactionService service = null;
        HealthServer healthServer = null;
        try {
            // The startup script is what ATTACHes the catalog, so it must run before a config provider
            // that reads a table in it. Ordering is the whole trick: file config -> attach -> overlay.
            // Run once here (via the shared commons ConnectionPool, since TableConfigProvider's own table
            // read depends on it) purely so the config-provider table below can be read; the script text
            // itself is captured and handed to every raw compaction/housekeeping connection too, each of
            // which independently re-runs it on its own real DuckDB instance (see RawConnections).
            String startupScript = readStartupScript(rawConfig);
            if (startupScript != null) {
                ConnectionPool.executeOnSingleton(startupScript);
            }

            CompactionConfig config = CompactionConfig.from(withOverrides(rawConfig));

            List<String> tierNames = config.tiers().stream().map(CompactionTier::name).toList();
            CompactionState state = new CompactionState(telemetry.registry(), config.databases(), tierNames);
            TierCompactor tierCompactor = new DuckDbTierCompactor(startupScript, state);
            Housekeeper housekeeper = new DuckLakeHousekeeper(
                    startupScript, config.snapshotRetention(), config.housekeepingConnectionSettings(),
                    config.rewriteDeletesEnabled(), config.rewriteDeleteThreshold(), state);
            CompactionRunLog runLog = new CompactionRunLog(config.runHistorySize());
            service = new CompactionService(config, startupScript, tierCompactor, housekeeper, state, runLog);
            healthServer = new HealthServer(config.healthPort(), service::getStats, runLog);

            healthServer.start();
            service.start();

            final CompactionService startedService = service;
            final HealthServer startedHealthServer = healthServer;

            // Registered only once startup has succeeded, so the catch below is the sole owner of
            // telemetry on a failed start and it is never closed twice.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received");
                startedService.close();
                startedHealthServer.close();
                telemetry.close();
            }, "shutdown-hook"));
        } catch (Throwable t) {
            // Nothing else flushes the exporters before the shutdown hook exists.
            logger.error("Startup failed", t);
            // Stop whatever already started (e.g. service.start() throws after the health server and
            // earlier tiers are running): their non-daemon threads would otherwise keep the JVM alive
            // and compacting, with telemetry closed and no shutdown hook.
            closeQuietly(service);
            closeQuietly(healthServer);
            telemetry.close();
            throw t;
        }

        Thread.currentThread().join();
    }

    /**
     * Overlays a {@link ConfigProvider}'s values on the file-based config, or returns it unchanged
     * when no provider is configured.
     *
     * <p>A configured provider that cannot be read is FATAL. The alternative — start on the bundled
     * defaults — is worse here than it looks: an operator who has moved compaction settings into a
     * table will not be watching the file, so a silent fallback runs the lake on values nobody has
     * reviewed in months, and the symptom (files growing, snapshots expiring early) is invisible
     * until something downstream stalls. Refusing to start is loud, and the previous pod keeps
     * running under an orchestrator.
     */
    private static Config withOverrides(Config rawConfig) throws Exception {
        TableConfigProvider provider = TableConfigProvider.load(rawConfig);
        if (provider == null) {
            return rawConfig;
        }
        Config overrides = provider.overrides();
        logger.info("Applied {} config override(s) from the configured provider",
                overrides.entrySet().size());
        return overrides.withFallback(rawConfig);
    }

    /** Returns the configured startup script text, or {@code null} if none is configured. */
    private static String readStartupScript(Config config) throws Exception {
        StartupScriptProvider provider = StartupScriptProvider.load(config);
        String script = provider.getStartupScript();
        return (script != null && !script.isBlank()) ? script : null;
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            logger.warn("Failed to close {} after a failed start", closeable.getClass().getSimpleName(), e);
        }
    }
}
