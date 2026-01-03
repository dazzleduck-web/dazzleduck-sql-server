package io.dazzleduck.sql.collector.metricscollector;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dazzleduck.sql.collector.metricscollector.config.CollectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Standalone metrics collector server.
 *
 * This is a long-running process that:
 * 1. Scrapes Prometheus metrics from configured endpoints
 * 2. Buffers metrics with tailing mode
 * 3. Forwards to a remote server in Apache Arrow format
 *
 * Usage:
 * <pre>
 * java -jar dazzleduck-sql-collector.jar
 * java -jar dazzleduck-sql-collector.jar --config /path/to/config.conf
 * java -jar dazzleduck-sql-collector.jar -c /path/to/config.conf
 * </pre>
 *
 * Configuration:
 * The server uses HOCON configuration format. Configuration is loaded from:
 * 1. application.conf from classpath (defaults)
 * 2. External config file specified via --config argument
 * 3. System properties (can override any setting)
 *
 * Example configuration (application.conf):
 * <pre>
 * collector {
 *     enabled = true
 *     targets = ["/actuator/prometheus"]
 *     target-prefix = "http://localhost:8080"
 *     server-url = "http://localhost:8081/ingest"
 *     scrape-interval-ms = 15000
 *     tailing {
 *         flush-threshold = 100
 *         flush-interval-ms = 5000
 *     }
 * }
 * </pre>
 */
public class MetricsCollectorServer {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollectorServer.class);

    private final CollectorConfig config;
    private final AtomicReference<MetricsCollector> collectorRef = new AtomicReference<>();
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    /**
     * CLI arguments.
     */
    public static class Args {
        @Parameter(names = {"-c", "--config"}, description = "Path to HOCON configuration file")
        public String configPath;

        @Parameter(names = {"-h", "--help"}, help = true, description = "Show this help message")
        public boolean help;

        @Parameter(names = {"-v", "--version"}, description = "Show version information")
        public boolean version;
    }

    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        Args cliArgs = new Args();
        JCommander jcommander = JCommander.newBuilder()
                .addObject(cliArgs)
                .programName("dazzleduck-sql-collector")
                .build();

        try {
            jcommander.parse(args);
        } catch (Exception e) {
            System.err.println("Error parsing arguments: " + e.getMessage());
            jcommander.usage();
            System.exit(1);
            return;
        }

        if (cliArgs.help) {
            jcommander.usage();
            return;
        }

        if (cliArgs.version) {
            System.out.println("DazzleDuck Metrics Collector v0.0.13-SNAPSHOT");
            return;
        }

        try {
            MetricsCollectorServer server = new MetricsCollectorServer(cliArgs.configPath);
            server.start();
        } catch (Exception e) {
            log.error("Failed to start metrics collector server", e);
            System.exit(1);
        }
    }

    /**
     * Create a new server with configuration from default locations.
     */
    public MetricsCollectorServer() {
        this.config = new CollectorConfig();
    }

    /**
     * Create a new server with optional external config file.
     *
     * @param configPath path to external config file (can be null)
     */
    public MetricsCollectorServer(String configPath) {
        if (configPath == null || configPath.isEmpty()) {
            this.config = new CollectorConfig();
        } else {
            this.config = new CollectorConfig(configPath);
        }
    }

    /**
     * Create a new server with the given configuration.
     *
     * @param config the collector configuration
     */
    public MetricsCollectorServer(CollectorConfig config) {
        this.config = config;
    }

    /**
     * Start the metrics collector server.
     * This method blocks until shutdown is triggered.
     */
    public void start() {
        log.info("Starting DazzleDuck Metrics Collector Server");
        log.info("Configuration: {}", config);

        if (!config.isEnabled()) {
            log.warn("Collector is disabled in configuration. Set collector.enabled=true to enable.");
            return;
        }

        CollectorProperties properties = config.toProperties();

        if (properties.getTargets().isEmpty()) {
            log.error("No targets configured. Add targets to collector.targets in configuration.");
            return;
        }

        // Create and start the collector
        MetricsCollector collector = new MetricsCollector(properties);
        collectorRef.set(collector);

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "collector-shutdown-hook"));

        collector.start();

        log.info("Metrics Collector Server started successfully");
        log.info("  Targets: {}", properties.getResolvedTargets());
        log.info("  Server URL: {}", properties.getServerUrl());
        log.info("  Scrape Interval: {}ms", properties.getScrapeIntervalMs());
        log.info("  Flush Threshold: {} metrics", properties.getFlushThreshold());
        log.info("  Flush Interval: {}ms", properties.getFlushIntervalMs());

        // Wait for shutdown signal
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Server interrupted");
        }
    }

    /**
     * Trigger graceful shutdown.
     */
    public void shutdown() {
        log.info("Shutting down Metrics Collector Server...");

        MetricsCollector collector = collectorRef.get();
        if (collector != null && collector.isRunning()) {
            collector.stop();
            log.info("Collector stopped. Metrics sent: {}, dropped: {}",
                    collector.getMetricsSentCount(),
                    collector.getMetricsDroppedCount());
        }

        shutdownLatch.countDown();
        log.info("Metrics Collector Server stopped");
    }

    /**
     * Check if the server is running.
     */
    public boolean isRunning() {
        MetricsCollector collector = collectorRef.get();
        return collector != null && collector.isRunning();
    }

    /**
     * Get the underlying collector (for testing or monitoring).
     */
    public MetricsCollector getCollector() {
        return collectorRef.get();
    }

    /**
     * Get the configuration.
     */
    public CollectorConfig getConfig() {
        return config;
    }
}
