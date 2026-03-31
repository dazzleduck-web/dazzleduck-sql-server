package io.dazzleduck.sql.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import static io.dazzleduck.sql.common.ConfigConstants.CONFIG_PATH;
import io.dazzleduck.sql.commons.config.ConfigBasedProvider;
import io.dazzleduck.sql.commons.util.CommandLineConfigUtil;
import io.dazzleduck.sql.flight.ConfigBasedStartupScriptProvider;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.StartupScriptProvider;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer;
import io.dazzleduck.sql.flight.server.FlightSqlProducerFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Runtime implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Runtime.class);

    private final Config config;
    private final BufferAllocator allocator;
    private final DuckDBFlightSqlProducer producer;
    private final boolean httpStarted;
    private final boolean flightSqlStarted;
    private final AutoCloseable metricsForwarder;

    private Runtime(Config config, BufferAllocator allocator, DuckDBFlightSqlProducer producer,
                    boolean httpStarted, boolean flightSqlStarted, AutoCloseable metricsForwarder) {
        this.config = config;
        this.allocator = allocator;
        this.producer = producer;
        this.httpStarted = httpStarted;
        this.flightSqlStarted = flightSqlStarted;
        this.metricsForwarder = metricsForwarder;
    }

    public static Runtime start(String[] args) throws Exception {
        Config overrides = CommandLineConfigUtil.loadCommandLineConfig(args).config();
        Config config = overrides
                .withFallback(ConfigFactory.parseResources("reference.conf"))
                .withFallback(ConfigFactory.systemProperties())
                .resolve()
                .getConfig(CONFIG_PATH);
        return start(config);
    }

    public static Runtime start(Config config) throws Exception {
        String warehousePath = ConfigConstants.getWarehousePath(config);
        validateWarehousePath(warehousePath);

        List<String> networkingModes = config.getStringList("networking_modes");
        validateNetworkingModes(networkingModes);

        logger.info("Starting DazzleDuck Runtime with configuration:");
        logger.info("  Warehouse path: {}", warehousePath);
        logger.info("  Networking modes: {}", networkingModes);

        executeStartupScript(config);

        BufferAllocator allocator = null;
        DuckDBFlightSqlProducer producer = null;
        AutoCloseable metricsForwarder = null;
        boolean httpStarted = false;
        boolean flightSqlStarted = false;

        try {
            metricsForwarder = startMetricsForwarder(warehousePath);

            if (networkingModes.contains("http") || networkingModes.contains("flight-sql")) {
                allocator = new RootAllocator();

                FlightSqlProducerFactory.ProducerBuilder builder = FlightSqlProducerFactory.builder(config)
                        .withAllocator(allocator);

                MeterRegistry forwardingRegistry = getForwardingRegistry(metricsForwarder);
                if (forwardingRegistry != null) {
                    String producerId = builder.getProducerId();
                    MicroMeterFlightRecorder.setupCommonTags(forwardingRegistry, producerId);
                    builder.withFlightRecorder(new MicroMeterFlightRecorder(forwardingRegistry, producerId));
                }

                producer = builder.build();
                logger.info("Created shared FlightSqlProducer for networking services");
            }

            if (networkingModes.contains("http")) {
                logger.info("Starting HTTP server...");
                io.dazzleduck.sql.http.server.Main.start(config, producer, allocator);
                httpStarted = true;
                logger.info("HTTP server started successfully");
            }

            if (networkingModes.contains("flight-sql")) {
                logger.info("Starting Flight SQL server...");
                io.dazzleduck.sql.flight.server.Main.start(config, producer, allocator);
                flightSqlStarted = true;
                logger.info("Flight SQL server started successfully");
            }

            logger.info("DazzleDuck Runtime startup complete");
            logger.info("  HTTP server: {}", httpStarted ? "RUNNING" : "NOT STARTED");
            logger.info("  Flight SQL server: {}", flightSqlStarted ? "RUNNING" : "NOT STARTED");

            return new Runtime(config, allocator, producer, httpStarted, flightSqlStarted, metricsForwarder);

        } catch (Exception e) {
            closeQuietly(metricsForwarder, "metricsForwarder");
            closeQuietly(producer, "producer");
            closeQuietly(allocator, "allocator");
            throw e;
        }
    }

    private static void executeStartupScript(Config config) throws Exception {
        StartupScriptProvider startupScriptProvider = ConfigBasedProvider.load(config,
                StartupScriptProvider.STARTUP_SCRIPT_CONFIG_PREFIX,
                new ConfigBasedStartupScriptProvider());
        String startupScript = startupScriptProvider.getStartupScript();
        if (startupScript != null && !startupScript.isBlank()) {
            logger.info("Executing startup script on singleton connection");
            ConnectionPool.executeOnSingleton(startupScript);
            logger.info("Startup script completed successfully");
        }
    }

    @Override
    public void close() {
        logger.info("Shutting down DazzleDuck Runtime...");

        closeQuietly(producer, "producer");
        closeQuietly(allocator, "allocator");
        closeQuietly(metricsForwarder, "metricsForwarder");

        logger.info("DazzleDuck Runtime shutdown complete");
    }

    /**
     * Reflectively starts the MicrometerForwarder and binds system metrics (CPU, JVM memory,
     * GC, disk) if {@code dazzleduck-sql-micrometer} is on the classpath.
     * Gracefully returns {@code null} when the module is absent.
     */
    private static AutoCloseable startMetricsForwarder(String warehousePath) {
        try {
            Class<?> factoryClass = Class.forName(
                    "io.dazzleduck.sql.micrometer.metrics.MetricsRegistryFactory");
            Object forwarder = factoryClass.getMethod("createForwarder").invoke(null);

            boolean running = (boolean) forwarder.getClass().getMethod("isRunning").invoke(forwarder);
            if (!running) {
                return (AutoCloseable) forwarder;
            }

            MeterRegistry registry =
                    (MeterRegistry) forwarder.getClass().getMethod("getRegistry").invoke(forwarder);

            Class<?> publisherClass = Class.forName(
                    "io.dazzleduck.sql.micrometer.SystemMetricsPublisher");
            publisherClass.getMethod("bind", MeterRegistry.class, String.class)
                    .invoke(null, registry, warehousePath);

            logger.info("Micrometer forwarder started; system metrics bound (CPU, JVM memory, GC, disk)");
            return (AutoCloseable) forwarder;

        } catch (ClassNotFoundException e) {
            logger.info("dazzleduck-sql-micrometer not on classpath — metrics forwarding disabled");
            return null;
        } catch (Exception e) {
            logger.warn("Failed to start metrics forwarder: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Extracts the {@link MeterRegistry} from a forwarder instance via reflection.
     * Returns {@code null} if the forwarder is null or the call fails.
     */
    private static MeterRegistry getForwardingRegistry(AutoCloseable forwarder) {
        if (forwarder == null) return null;
        try {
            return (MeterRegistry) forwarder.getClass().getMethod("getRegistry").invoke(forwarder);
        } catch (Exception e) {
            logger.warn("Could not retrieve MeterRegistry from forwarder: {}", e.getMessage());
            return null;
        }
    }

    private static void closeQuietly(AutoCloseable closeable, String name) {
        if (closeable != null) {
            try {
                closeable.close();
                logger.info("{} closed", name);
            } catch (Exception e) {
                logger.error("Error closing {}", name, e);
            }
        }
    }

    public boolean isHttpStarted() {
        return httpStarted;
    }

    public boolean isFlightSqlStarted() {
        return flightSqlStarted;
    }

    public DuckDBFlightSqlProducer getProducer() {
        return producer;
    }

    private static void validateWarehousePath(String warehousePath) {
        if (warehousePath == null || warehousePath.isEmpty()) {
            throw new IllegalStateException("Warehouse path cannot be null or empty");
        }

        if (warehousePath.startsWith("s3://") || warehousePath.startsWith("s3a://")) {
            validateS3Path(warehousePath);
        } else {
            validateLocalPath(warehousePath);
        }
    }

    private static void validateS3Path(String s3Path) {
        String path = s3Path.replaceFirst("^s3a?://", "");
        if (path.isEmpty()) {
            throw new IllegalStateException("S3 path must include bucket name: " + s3Path);
        }

        String[] parts = path.split("/", 2);
        String bucketName = parts[0];

        if (bucketName.isEmpty()) {
            throw new IllegalStateException("S3 bucket name cannot be empty: " + s3Path);
        }

        if (bucketName.length() < 3 || bucketName.length() > 63) {
            throw new IllegalStateException("S3 bucket name must be between 3 and 63 characters: " + bucketName);
        }

        if (!bucketName.matches("^[a-z0-9][a-z0-9.-]*[a-z0-9]$")) {
            throw new IllegalStateException("Invalid S3 bucket name format: " + bucketName +
                    " (must contain only lowercase letters, numbers, dots, and hyphens)");
        }

        logger.info("Using S3 warehouse path: {}", s3Path);
        logger.warn("S3 path validation is basic - bucket accessibility will be verified at runtime");
    }

    private static void validateLocalPath(String warehousePath) {
        Path path = Paths.get(warehousePath);
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
                logger.info("Created warehouse directory: {}", warehousePath);
            } catch (IOException e) {
                throw new IllegalStateException("Warehouse path does not exist and could not be created: " + warehousePath, e);
            }
        }
        if (!Files.isDirectory(path)) {
            throw new IllegalStateException("Warehouse path is not a directory: " + warehousePath);
        }
        if (!Files.isWritable(path)) {
            throw new IllegalStateException("Warehouse path is not writable: " + warehousePath);
        }
        logger.info("Using local filesystem warehouse path: {}", warehousePath);
    }

    private static void validateNetworkingModes(List<String> networkingModes) {
        if (networkingModes == null || networkingModes.isEmpty()) {
            throw new IllegalStateException("At least one networking mode must be configured. Valid options: 'http', 'flight-sql'");
        }

        for (String mode : networkingModes) {
            if (!mode.equals("http") && !mode.equals("flight-sql")) {
                throw new IllegalStateException("Invalid networking mode: '" + mode + "'. Valid options: 'http', 'flight-sql'");
            }
        }

        logger.info("Validated networking modes: {}", networkingModes);
    }
}
