package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.TableConfigProvider;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.StartupScriptProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Config rawConfig = CompactionConfig.rawConfig(args);

        // The startup script is what ATTACHes the catalog, so it must run before a config provider
        // that reads a table in it. Ordering is the whole trick: file config -> attach -> overlay.
        executeStartupScript(rawConfig);

        CompactionConfig config = CompactionConfig.from(withOverrides(rawConfig));

        MeterRegistry registry = new LoggingMeterRegistry();
        CompactionState state = new CompactionState(registry, config.databases());
        MajorCompactor majorCompactor = new DuckDbMajorCompactor(
                config.majorCompactionMaxSize(), config.snapshotRetention(), state);
        CompactionService service = new CompactionService(config, majorCompactor, state);
        HealthServer healthServer = new HealthServer(config.healthPort(), service::getStats);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            service.close();
            healthServer.close();
        }, "shutdown-hook"));

        healthServer.start();
        service.start();

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

    private static void executeStartupScript(Config config) throws Exception {
        StartupScriptProvider provider = StartupScriptProvider.load(config);
        String script = provider.getStartupScript();
        if (script != null && !script.isBlank()) {
            logger.info("Executing startup script");
            ConnectionPool.executeOnSingleton(script);
            logger.info("Startup script completed");
        }
    }
}
