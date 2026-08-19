package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.common.StartupScriptProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Config rawConfig = CompactionConfig.rawConfig(args);
        CompactionConfig config = CompactionConfig.from(rawConfig);

        executeStartupScript(rawConfig);

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
