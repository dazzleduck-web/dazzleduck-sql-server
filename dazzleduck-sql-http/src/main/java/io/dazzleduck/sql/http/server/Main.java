
package io.dazzleduck.sql.http.server;


import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.auth.Validator;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.login.LoginService;
import io.dazzleduck.sql.login.ProxyLoginService;
import io.helidon.config.Config;
import io.helidon.cors.CrossOriginConfig;
import io.helidon.logging.common.LogConfig;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.cors.CorsSupport;
import io.helidon.webserver.http.HttpService;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

import java.nio.file.Files;
import java.util.List;
import java.util.UUID;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;


/**
 * The application main class.
 */
public class Main {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Main.class);

    /**
     * Application main entry point.
     * @param args command line arguments.
     */
    public static void main(String[] args) {
        try {
            printBanner();
            var commandlineConfig = io.dazzleduck.sql.common.util.ConfigUtils.loadCommandLineConfig(args).config();
            var appConfig = commandlineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
            start(appConfig);
        } catch (com.typesafe.config.ConfigException.Missing e) {
            logger.error("Missing required configuration: {}", e.getMessage());
            System.err.println("ERROR: Missing required configuration: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            logger.error("Failed to start server", e);
            System.err.println("ERROR: Failed to start server: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printBanner() {
        String version = Main.class.getPackage().getImplementationVersion();
        logger.info("=".repeat(60));
        logger.info("DazzleDuck SQL Server {}", version != null ? "v" + version : "(development)");
        logger.info("=".repeat(60));
    }

    public static void start(com.typesafe.config.Config appConfig) throws Exception {
        // Create allocator and producer
        var allocator = new RootAllocator();

        try {
            // Validate and load configuration
            String warehousePath = ConfigUtils.getWarehousePath(appConfig);
            validateWarehousePath(warehousePath);

            String base64SecretKey = appConfig.getString(ConfigUtils.SECRET_KEY_KEY);
            validateSecretKey(base64SecretKey);

            var tempWriteDir = DuckDBFlightSqlProducer.getTempWriteDir(appConfig);
            validateTempWriteDir(tempWriteDir.toString());

            AccessMode accessMode = DuckDBFlightSqlProducer.getAccessMode(appConfig);

            var httpConfig = appConfig.getConfig("http");
            var port = httpConfig.getInt(ConfigUtils.PORT_KEY);
            validatePort(port);

            var host = httpConfig.getString(ConfigUtils.HOST_KEY);

            logger.info("Starting server with configuration:");
            logger.info("  Host: {}", host);
            logger.info("  Port: {}", port);
            logger.info("  Warehouse path: {}", warehousePath);
            logger.info("  Access mode: {}", accessMode);

            // Create producer using factory with custom settings for HTTP server
            var producer = io.dazzleduck.sql.flight.server.FlightSqlProducerFactory.builder(appConfig)
                    .withLocation(Location.forGrpcInsecure(host, port))
                    .withProducerId(UUID.randomUUID().toString())
                    .withSecretKey(base64SecretKey)
                    .withAllocator(allocator)
                    .withWarehousePath(warehousePath)
                    .withTempWriteDir(tempWriteDir)
                    .withAccessMode(accessMode)
                    .build();

            start(appConfig, producer, allocator);
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
        var path = java.nio.file.Paths.get(warehousePath);
        if (!Files.exists(path)) {
            throw new IllegalStateException("Warehouse path does not exist: " + warehousePath);
        }
        if (!Files.isDirectory(path)) {
            throw new IllegalStateException("Warehouse path is not a directory: " + warehousePath);
        }
        if (!Files.isWritable(path)) {
            throw new IllegalStateException("Warehouse path is not writable: " + warehousePath);
        }
        logger.info("Using local filesystem warehouse path: {}", warehousePath);
    }

    private static void validateTempWriteDir(String tempWriteDir) {
        var path = java.nio.file.Paths.get(tempWriteDir);
        if (!Files.exists(path)) {
            throw new IllegalStateException("Temp write directory does not exist: " + tempWriteDir);
        }
        if (!Files.isDirectory(path)) {
            throw new IllegalStateException("Temp write directory is not a directory: " + tempWriteDir);
        }
        if (!Files.isWritable(path)) {
            throw new IllegalStateException("Temp write directory is not writable: " + tempWriteDir);
        }
    }

    private static void validateSecretKey(String base64SecretKey) {
        if (base64SecretKey == null || base64SecretKey.isEmpty()) {
            throw new IllegalStateException("Secret key is required but not configured");
        }
        // Validate it's valid base64 by trying to decode
        try {
            java.util.Base64.getDecoder().decode(base64SecretKey);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Secret key is not valid base64: " + e.getMessage(), e);
        }
    }

    private static void validatePort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid port: " + port + " (must be 1-65535)");
        }
    }

    /**
     * Starts the HTTP server with a pre-existing producer instance.
     * This allows sharing the same producer between multiple servers.
     *
     * @param appConfig the configuration
     * @param producer the FlightSqlProducer instance to use
     * @param allocator the BufferAllocator associated with the producer
     * @throws Exception if server startup fails
     */
    public static void start(com.typesafe.config.Config appConfig, DuckDBFlightSqlProducer producer, org.apache.arrow.memory.BufferAllocator allocator) throws Exception {
        LogConfig.configureRuntime();
        Config helidonConfig = Config.create();
        var httpConfig =  appConfig.getConfig("http");
        var port = httpConfig.getInt(ConfigUtils.PORT_KEY);
        var host = httpConfig.getString(ConfigUtils.HOST_KEY);
        var auth = httpConfig.hasPath(ConfigUtils.AUTHENTICATION_KEY) ? httpConfig.getString(ConfigUtils.AUTHENTICATION_KEY) : "none";
        String warehousePath = ConfigUtils.getWarehousePath(appConfig);
        String base64SecretKey = appConfig.getString(ConfigUtils.SECRET_KEY_KEY);
        var secretKey = Validator.fromBase64String(base64SecretKey);
        String location = "http://%s:%s".formatted(host, port);
        AccessMode accessMode = DuckDBFlightSqlProducer.getAccessMode(appConfig);
        var jwtExpiration = appConfig.getDuration("jwt_token.expiration");
        var cors = CorsSupport.builder()
                .addCrossOrigin(CrossOriginConfig.builder()
                        .allowOrigins(appConfig.hasPath("allow-origin") ? appConfig.getString("allow-origin") : "*")
                        .allowMethods("GET", "POST")
                        .allowHeaders("Content-Type", "Authorization")
                        .build())
                .build();
        HttpService loginService;
        if (appConfig.hasPath(AuthUtils.PROXY_LOGIN_URL_KEY)) {
            var proxyUrl = appConfig.getString(AuthUtils.PROXY_LOGIN_URL_KEY);
            loginService = new ProxyLoginService(proxyUrl);
        } else {
            loginService = new LoginService(appConfig, secretKey, jwtExpiration);
        }

        WebServer server = WebServer.builder()
                .config(helidonConfig.get("dazzleduck_server"))
                .config(helidonConfig.get("flight_sql"))
                .routing(routing -> {
                    routing.register(cors);
                    var b = routing.register("/health", new HealthCheckService(producer))
                            .register("/query", new QueryService(producer, accessMode))
                            .register("/login", loginService)
                            .register("/plan", new PlaningService(producer, location, allocator, accessMode))
                            .register("/cancel", new CancelService(producer, accessMode))
                            .register("/ingest", new IngestionService(producer, warehousePath, allocator))
                            .register("/ui", new UIService(producer));

                    // JWT filter is required if EITHER:
                    // 1. Authentication is explicitly set to "jwt", OR
                    // 2. Access mode is RESTRICTED (requires JWT for authorization)
                    boolean requiresJwt = "jwt".equals(auth) || accessMode == AccessMode.RESTRICTED;

                    if (requiresJwt) {
                        logger.info("JWT authentication enabled (auth={}, accessMode={})", auth, accessMode);
                        b.addFilter(new JwtAuthenticationFilter(
                                List.of("/query", "/plan", "/ingest", "/cancel", "/ui"),
                                appConfig,
                                secretKey
                        ));
                    } else {
                        logger.warn("JWT authentication disabled - API endpoints are publicly accessible!");
                    }
                })
                .port(port)
                .host(host)
                .build()
                .start();

        var http = server.hasTls() ? "https" : "http";
        String url = "%s://%s:%s".formatted(http, host, port);

        // Add shutdown hook for graceful cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received, stopping server...");
            try {
                server.stop();
                logger.info("HTTP server stopped");
            } catch (Exception e) {
                logger.error("Error stopping server", e);
            }

            try {
                producer.close();
                logger.info("Producer closed");
            } catch (Exception e) {
                logger.error("Error closing producer", e);
            }

            try {
                allocator.close();
                logger.info("Allocator closed");
            } catch (Exception e) {
                logger.error("Error closing allocator", e);
            }

            logger.info("Shutdown complete");
        }, "shutdown-hook"));

        logger.info("HTTP Server started successfully");
        logger.info("Listening on: {}", url);
        logger.info("Health check: {}/health", url);
        logger.info("UI dashboard: {}/ui", url);
    }
}