package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.config.ConfigBasedProvider;
import io.dazzleduck.sql.flight.ConfigBasedStartupScriptProvider;
import io.dazzleduck.sql.flight.StartupScriptProvider;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.commons.util.CommandLineConfigUtil;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.auth2.AdvanceJWTTokenAuthenticator;
import io.dazzleduck.sql.flight.server.auth2.AdvanceServerCallHeaderAuthMiddleware;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.dazzleduck.sql.common.ConfigConstants.CONFIG_PATH;


public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            printBanner();
            var commandLineConfig = CommandLineConfigUtil.loadCommandLineConfig(args).config();
            var config = commandLineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
            start(config);
        } catch (com.typesafe.config.ConfigException.Missing e) {
            logger.error("Missing required configuration: {}", e.getMessage());
            System.err.println("ERROR: Missing required configuration: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            logger.error("Failed to start Flight server", e);
            System.err.println("ERROR: Failed to start Flight server: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printBanner() {
        String version = Main.class.getPackage().getImplementationVersion();
        logger.info("=".repeat(60));
        logger.info("DazzleDuck Flight Server {}", version != null ? "v" + version : "(development)");
        logger.info("=".repeat(60));
    }

    public static FlightServer createServer(String[] args) throws Exception {
        var commandLineConfig = CommandLineConfigUtil.loadCommandLineConfig(args).config();
        var config = commandLineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
        return createServer(config);
    }
    public static FlightServer createServer(Config config) throws Exception {
        // Execute startup script if provided
        StartupScriptProvider startupScriptProvider = ConfigBasedProvider.load(config,
                StartupScriptProvider.STARTUP_SCRIPT_CONFIG_PREFIX,
                new ConfigBasedStartupScriptProvider());
        if (startupScriptProvider.getStartupScript() != null) {
            ConnectionPool.executeOnSingleton(startupScriptProvider.getStartupScript());
        }

        // Create allocator and producer using factory
        BufferAllocator allocator = new RootAllocator();
        try {
            DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.builder(config)
                    .withAllocator(allocator)
                    .build();

            try {
                return createServer(config, producer, allocator);
            } catch (Exception e) {
                // Cleanup producer on failure
                try {
                    producer.close();
                } catch (Exception closeEx) {
                    logger.error("Error closing producer during cleanup", closeEx);
                }
                throw e;
            }
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

    /**
     * Creates a FlightServer with a pre-existing producer instance.
     * This allows sharing the same producer between multiple servers.
     *
     * @param config the configuration
     * @param producer the FlightSqlProducer instance to use
     * @param allocator the BufferAllocator associated with the producer
     * @return a configured FlightServer
     * @throws Exception if server creation fails
     */
    public static FlightServer createServer(Config config, DuckDBFlightSqlProducer producer, BufferAllocator allocator) throws Exception {
        // Validate and load configuration
        boolean useEncryption = config.getBoolean(ConfigConstants.FLIGHT_SQL_USE_ENCRYPTION_KEY);
        String keystoreLocation = config.getString(ConfigConstants.KEYSTORE_KEY);
        String serverCertLocation = config.getString(ConfigConstants.SERVER_CERT_KEY);
        String warehousePath = ConfigConstants.getWarehousePath(config);

        // Validate warehouse path
        validateWarehousePath(config, warehousePath);

        // Validate TLS configuration if encryption is enabled
        if (useEncryption) {
            validateTlsConfiguration(keystoreLocation, serverCertLocation);
        }

        // Validate port from producer location
        int port = producer.getExternalLocation().getUri().getPort();
        validatePort(port);

        String host = producer.getExternalLocation().getUri().getHost();

        logger.info("Starting Flight server with configuration:");
        logger.info("  Host: {}", host);
        logger.info("  Port: {}", port);
        logger.info("  Warehouse path: {}", warehousePath);
        logger.info("  TLS enabled: {}", useEncryption);

        AdvanceJWTTokenAuthenticator authenticator = AuthUtils.getAuthenticator(config);

        try (var certStream = getInputStreamForResource(serverCertLocation);
             var keyStream = getInputStreamForResource(keystoreLocation)) {

            var builder = FlightServer.builder(allocator, producer.getExternalLocation(), producer)
                    .middleware(AdvanceServerCallHeaderAuthMiddleware.KEY,
                            new AdvanceServerCallHeaderAuthMiddleware.Factory(authenticator));
            if (useEncryption) {
                builder.useTls(certStream, keyStream);
            }
            return builder.build();
        }
    }


    public static void start(Config config) throws Exception {
        var flightServer = createServer(config);
        startFlightServerThread(flightServer);
    }

    /**
     * Starts the Flight server with a pre-existing producer instance.
     * This allows sharing the same producer between multiple servers.
     *
     * @param config the configuration
     * @param producer the FlightSqlProducer instance to use
     * @param allocator the BufferAllocator associated with the producer
     * @throws Exception if server startup fails
     */
    public static void start(Config config, DuckDBFlightSqlProducer producer, BufferAllocator allocator) throws Exception {
        var flightServer = createServer(config, producer, allocator);
        startFlightServerThread(flightServer, producer, allocator);
    }

    private static void startFlightServerThread(FlightServer flightServer) {
        startFlightServerThread(flightServer, null, null);
    }

    private static void startFlightServerThread(FlightServer flightServer, DuckDBFlightSqlProducer producer, BufferAllocator allocator) {
        // Add shutdown hook for graceful cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received, stopping Flight server...");
            try {
                flightServer.close();
                logger.info("Flight server stopped");
            } catch (Exception e) {
                logger.error("Error stopping Flight server", e);
            }

            if (producer != null) {
                try {
                    producer.close();
                    logger.info("Producer closed");
                } catch (Exception e) {
                    logger.error("Error closing producer", e);
                }
            }

            if (allocator != null) {
                try {
                    allocator.close();
                    logger.info("Allocator closed");
                } catch (Exception e) {
                    logger.error("Error closing allocator", e);
                }
            }

            logger.info("Shutdown complete");
        }, "shutdown-hook"));

        Thread serverThread = new Thread(() -> {
            try {
                flightServer.start();
                logger.info("Flight Server is up: Listening on URI: {}", flightServer.getLocation().getUri());
                flightServer.awaitTermination();
            } catch (IOException e) {
                logger.error("Flight server encountered an IO error during startup or termination", e);
                throw new RuntimeException("Flight server failed to start or terminated unexpectedly", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupt status
                logger.error("Flight server thread was interrupted", e);
                throw new RuntimeException("Flight server thread interrupted", e);
            }
        }, "flight-server");
        serverThread.start();
    }




    private static InputStream getInputStreamForResource(String filename) {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
        if (inputStream == null) {
            throw new IllegalArgumentException("File not found! : " + filename);
        }
        return inputStream;
    }

    private static void validateWarehousePath(Config config, String warehousePath) {
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

    private static void validateTlsConfiguration(String keystoreLocation, String serverCertLocation) {
        if (keystoreLocation == null || keystoreLocation.isEmpty()) {
            throw new IllegalStateException("Keystore location is required when TLS is enabled");
        }
        if (serverCertLocation == null || serverCertLocation.isEmpty()) {
            throw new IllegalStateException("Server certificate location is required when TLS is enabled");
        }

        // Validate that resources exist
        try {
            InputStream certStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(serverCertLocation);
            if (certStream == null) {
                throw new IllegalStateException("Server certificate not found: " + serverCertLocation);
            }
            certStream.close();
        } catch (IOException e) {
            throw new IllegalStateException("Error validating server certificate: " + serverCertLocation, e);
        }

        try {
            InputStream keyStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(keystoreLocation);
            if (keyStream == null) {
                throw new IllegalStateException("Keystore not found: " + keystoreLocation);
            }
            keyStream.close();
        } catch (IOException e) {
            throw new IllegalStateException("Error validating keystore: " + keystoreLocation, e);
        }
    }

    private static void validatePort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid port: " + port + " (must be 1-65535)");
        }
    }
}
