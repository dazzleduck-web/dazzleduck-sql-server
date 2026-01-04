package io.dazzleduck.sql.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigBasedProvider;
import io.dazzleduck.sql.common.ConfigBasedStartupScriptProvider;
import io.dazzleduck.sql.common.StartupScriptProvider;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer;
import io.dazzleduck.sql.flight.server.FlightSqlProducerFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            printBanner();
            var commandLineConfig = ConfigUtils.loadCommandLineConfig(args).config();
            var config = commandLineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
            start(config);
        } catch (com.typesafe.config.ConfigException.Missing e) {
            logger.error("Missing required configuration: {}", e.getMessage());
            System.err.println("ERROR: Missing required configuration: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            logger.error("Failed to start DazzleDuck Runtime", e);
            System.err.println("ERROR: Failed to start DazzleDuck Runtime: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printBanner() {
        String version = Main.class.getPackage().getImplementationVersion();
        logger.info("=".repeat(60));
        logger.info("DazzleDuck Runtime {}", version != null ? "v" + version : "(development)");
        logger.info("=".repeat(60));
    }

    public static void start(Config config) throws Exception {
        // Validate and load configuration
        String warehousePath = ConfigUtils.getWarehousePath(config);
        validateWarehousePath(warehousePath);

        List<String> networkingModes = config.getStringList("networking_modes");
        validateNetworkingModes(networkingModes);

        logger.info("Starting DazzleDuck Runtime with configuration:");
        logger.info("  Warehouse path: {}", warehousePath);
        logger.info("  Networking modes: {}", networkingModes);

        // Execute startup script if provided
        StartupScriptProvider startupScriptProvider = ConfigBasedProvider.load(config,
                StartupScriptProvider.STARTUP_SCRIPT_CONFIG_PREFIX,
                new ConfigBasedStartupScriptProvider());
        if (startupScriptProvider.getStartupScript() != null) {
            logger.info("Executing startup script");
            ConnectionPool.execute(startupScriptProvider.getStartupScript());
        }

        // Create shared producer instance if any networking mode is enabled
        DuckDBFlightSqlProducer producer = null;
        BufferAllocator allocator = null;

        if (networkingModes.contains("http") || networkingModes.contains("flight-sql")) {
            // Create single shared allocator and producer
            allocator = new RootAllocator();
            try {
                producer = FlightSqlProducerFactory.builder(config)
                        .withAllocator(allocator)
                        .build();
                logger.info("Created shared FlightSqlProducer for networking services");

                // Add shutdown hook for graceful cleanup
                final BufferAllocator finalAllocator = allocator;
                final DuckDBFlightSqlProducer finalProducer = producer;
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    logger.info("Shutdown signal received, cleaning up resources...");
                    try {
                        finalProducer.close();
                        logger.info("Producer closed");
                    } catch (Exception e) {
                        logger.error("Error closing producer", e);
                    }

                    try {
                        finalAllocator.close();
                        logger.info("Allocator closed");
                    } catch (Exception e) {
                        logger.error("Error closing allocator", e);
                    }

                    logger.info("Shutdown complete");
                }, "shutdown-hook"));
            } catch (Exception e) {
                // Cleanup allocator on failure
                try {
                    allocator.close();
                } catch (Exception closeEx) {
                    logger.error("Error closing allocator during cleanup", closeEx);
                }
                throw e;
            }
        }

        // Start servers based on networking modes
        boolean httpStarted = false;
        boolean flightSqlStarted = false;

        if (networkingModes.contains("http")) {
            logger.info("Starting HTTP server...");
            try {
                io.dazzleduck.sql.http.server.Main.start(config, producer, allocator);
                httpStarted = true;
                logger.info("HTTP server started successfully");
            } catch (Exception e) {
                logger.error("Failed to start HTTP server", e);
                throw new RuntimeException("Failed to start HTTP server", e);
            }
        }

        if (networkingModes.contains("flight-sql")) {
            logger.info("Starting Flight SQL server...");
            try {
                io.dazzleduck.sql.flight.server.Main.start(config, producer, allocator);
                flightSqlStarted = true;
                logger.info("Flight SQL server started successfully");
            } catch (Exception e) {
                logger.error("Failed to start Flight SQL server", e);
                throw new RuntimeException("Failed to start Flight SQL server", e);
            }
        }

        logger.info("DazzleDuck Runtime startup complete");
        logger.info("  HTTP server: {}", httpStarted ? "RUNNING" : "NOT STARTED");
        logger.info("  Flight SQL server: {}", flightSqlStarted ? "RUNNING" : "NOT STARTED");
    }

    private static void validateWarehousePath(String warehousePath) {
        if (warehousePath == null || warehousePath.isEmpty()) {
            throw new IllegalStateException("Warehouse path cannot be null or empty");
        }

        // Check if it's an S3 path
        if (warehousePath.startsWith("s3://") || warehousePath.startsWith("s3a://")) {
            validateS3Path(warehousePath);
        } else {
            // Validate local filesystem path
            validateLocalPath(warehousePath);
        }
    }

    private static void validateS3Path(String s3Path) {
        // Validate S3 URI format: s3://bucket/path or s3a://bucket/path
        String path = s3Path.replaceFirst("^s3a?://", "");
        if (path.isEmpty()) {
            throw new IllegalStateException("S3 path must include bucket name: " + s3Path);
        }

        // Check for valid bucket name (basic validation)
        String[] parts = path.split("/", 2);
        String bucketName = parts[0];

        if (bucketName.isEmpty()) {
            throw new IllegalStateException("S3 bucket name cannot be empty: " + s3Path);
        }

        // Basic S3 bucket name validation rules
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
            // Try to create the directory
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

        // Validate each mode
        for (String mode : networkingModes) {
            if (!mode.equals("http") && !mode.equals("flight-sql")) {
                throw new IllegalStateException("Invalid networking mode: '" + mode + "'. Valid options: 'http', 'flight-sql'");
            }
        }

        logger.info("Validated networking modes: {}", networkingModes);
    }
}
