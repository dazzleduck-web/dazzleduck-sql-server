
package io.dazzleduck.sql.http.server;


import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.commons.util.CommandLineConfigUtil;
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

import static io.dazzleduck.sql.common.ConfigConstants.CONFIG_PATH;


/**
 * The application main class.
 */
public class Main {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Main.class);

    // API versioning
    private static final String API_VERSION = "v1";
    private static final String API_VERSION_PREFIX = "/" + API_VERSION;

    // Endpoint paths
    private static final String ENDPOINT_HEALTH = "/health";
    private static final String ENDPOINT_QUERY = API_VERSION_PREFIX + "/query";
    private static final String ENDPOINT_LOGIN = API_VERSION_PREFIX + "/login";
    private static final String ENDPOINT_PLAN = API_VERSION_PREFIX + "/plan";
    private static final String ENDPOINT_CANCEL = API_VERSION_PREFIX + "/cancel";
    private static final String ENDPOINT_INGEST = API_VERSION_PREFIX + "/ingest";
    private static final String ENDPOINT_UI = API_VERSION_PREFIX + "/ui";

    // Configuration keys
    private static final String CONFIG_HTTP = ConfigConstants.HTTP_PREFIX;
    private static final String CONFIG_JWT_EXPIRATION = ConfigConstants.JWT_TOKEN_EXPIRATION_KEY;
    private static final String CONFIG_ALLOW_ORIGIN = ConfigConstants.ALLOW_ORIGIN_KEY;
    private static final String CONFIG_FLIGHT_SQL = ConfigConstants.FLIGHT_SQL_PREFIX;

    // CORS configuration
    private static final String CORS_DEFAULT_ALLOW_ORIGIN = "*";
    private static final String HTTP_METHOD_GET = "GET";
    private static final String HTTP_METHOD_POST = "POST";
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String HEADER_ARROW_COMPRESSION = "x-dd-arrow-compression";

    // Authentication types
    private static final String AUTH_NONE = ConfigConstants.AUTH_NONE;
    private static final String AUTH_JWT = ConfigConstants.AUTH_JWT;

    // HTTP protocols
    private static final String PROTOCOL_HTTP = "http";
    private static final String PROTOCOL_HTTPS = "https";

    // S3 constants
    private static final String S3_PROTOCOL_PREFIX = "s3://";
    private static final String S3A_PROTOCOL_PREFIX = "s3a://";
    private static final String S3_PROTOCOL_REGEX = "^s3a?://";
    private static final String S3_BUCKET_NAME_REGEX = "^[a-z0-9][a-z0-9.-]*[a-z0-9]$";
    private static final int S3_BUCKET_NAME_MIN_LENGTH = 3;
    private static final int S3_BUCKET_NAME_MAX_LENGTH = 63;
    private static final String PATH_SEPARATOR = "/";
    private static final int PATH_SPLIT_LIMIT = 2;

    // Port validation
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;

    // Banner configuration
    private static final int BANNER_WIDTH = 60;
    private static final String VERSION_PREFIX = "v";
    private static final String VERSION_DEVELOPMENT = "(development)";

    // Thread names
    private static final String SHUTDOWN_HOOK_THREAD_NAME = "shutdown-hook";

    // Error exit code
    private static final int EXIT_CODE_ERROR = 1;

    /**
     * Application main entry point.
     * @param args command line arguments.
     */
    public static void main(String[] args) {
        try {
            printBanner();
            var commandlineConfig = CommandLineConfigUtil.loadCommandLineConfig(args).config();
            var appConfig = commandlineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
            start(appConfig);
        } catch (com.typesafe.config.ConfigException.Missing e) {
            logger.error("Missing required configuration: {}", e.getMessage());
            System.err.println("ERROR: Missing required configuration: " + e.getMessage());
            System.exit(EXIT_CODE_ERROR);
        } catch (Exception e) {
            logger.error("Failed to start server", e);
            System.err.println("ERROR: Failed to start server: " + e.getMessage());
            e.printStackTrace();
            System.exit(EXIT_CODE_ERROR);
        }
    }

    private static void printBanner() {
        String version = Main.class.getPackage().getImplementationVersion();
        logger.info("=".repeat(BANNER_WIDTH));
        logger.info("DazzleDuck SQL Server {}", version != null ? VERSION_PREFIX + version : VERSION_DEVELOPMENT);
        logger.info("=".repeat(BANNER_WIDTH));
    }

    public static void start(com.typesafe.config.Config appConfig) throws Exception {
        // Create allocator and producer
        var allocator = new RootAllocator();

        try {
            // Validate and load configuration
            String warehousePath = ConfigConstants.getWarehousePath(appConfig);
            validateWarehousePath(warehousePath);

            String base64SecretKey = appConfig.getString(ConfigConstants.SECRET_KEY_KEY);
            validateSecretKey(base64SecretKey);

            var tempWriteDir = DuckDBFlightSqlProducer.getTempWriteDir(appConfig);
            validateTempWriteDir(tempWriteDir.toString());

            AccessMode accessMode = DuckDBFlightSqlProducer.getAccessMode(appConfig);

            var httpConfig = appConfig.getConfig(CONFIG_HTTP);
            var port = httpConfig.getInt(ConfigConstants.PORT_KEY);
            validatePort(port);

            var host = httpConfig.getString(ConfigConstants.HOST_KEY);

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
        if (warehousePath.startsWith(S3_PROTOCOL_PREFIX) || warehousePath.startsWith(S3A_PROTOCOL_PREFIX)) {
            validateS3Path(warehousePath);
        } else {
            // Validate local filesystem path
            validateLocalPath(warehousePath);
        }
    }

    private static void validateS3Path(String s3Path) {
        // Validate S3 URI format: s3://bucket/path or s3a://bucket/path
        String path = s3Path.replaceFirst(S3_PROTOCOL_REGEX, "");
        if (path.isEmpty()) {
            throw new IllegalStateException("S3 path must include bucket name: " + s3Path);
        }

        // Check for valid bucket name (basic validation)
        String[] parts = path.split(PATH_SEPARATOR, PATH_SPLIT_LIMIT);
        String bucketName = parts[0];

        if (bucketName.isEmpty()) {
            throw new IllegalStateException("S3 bucket name cannot be empty: " + s3Path);
        }

        // Basic S3 bucket name validation rules
        if (bucketName.length() < S3_BUCKET_NAME_MIN_LENGTH || bucketName.length() > S3_BUCKET_NAME_MAX_LENGTH) {
            throw new IllegalStateException("S3 bucket name must be between " + S3_BUCKET_NAME_MIN_LENGTH +
                " and " + S3_BUCKET_NAME_MAX_LENGTH + " characters: " + bucketName);
        }

        if (!bucketName.matches(S3_BUCKET_NAME_REGEX)) {
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
        if (port < MIN_PORT || port > MAX_PORT) {
            throw new IllegalArgumentException("Invalid port: " + port + " (must be " + MIN_PORT + "-" + MAX_PORT + ")");
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
        var httpConfig = appConfig.getConfig(CONFIG_HTTP);
        var port = httpConfig.getInt(ConfigConstants.PORT_KEY);
        var host = httpConfig.getString(ConfigConstants.HOST_KEY);
        var auth = httpConfig.hasPath(ConfigConstants.AUTHENTICATION_KEY) ? httpConfig.getString(ConfigConstants.AUTHENTICATION_KEY) : AUTH_NONE;
       String base64SecretKey = appConfig.getString(ConfigConstants.SECRET_KEY_KEY);
        var secretKey = Validator.fromBase64String(base64SecretKey);
        String location = PROTOCOL_HTTP + "://%s:%s".formatted(host, port);
        AccessMode accessMode = DuckDBFlightSqlProducer.getAccessMode(appConfig);
        var jwtExpiration = appConfig.getDuration(CONFIG_JWT_EXPIRATION);
        var cors = CorsSupport.builder()
                .addCrossOrigin(CrossOriginConfig.builder()
                        .allowOrigins(appConfig.hasPath(CONFIG_ALLOW_ORIGIN) ? appConfig.getString(CONFIG_ALLOW_ORIGIN) : CORS_DEFAULT_ALLOW_ORIGIN)
                        .allowMethods(HTTP_METHOD_GET, HTTP_METHOD_POST)
                        .allowHeaders(HEADER_CONTENT_TYPE, HEADER_AUTHORIZATION, HEADER_ARROW_COMPRESSION)
                        .build())
                .build();
        HttpService loginService;
        if (appConfig.hasPath(ConfigConstants.LOGIN_URL_KEY)) {
            var proxyUrl = appConfig.getString(ConfigConstants.LOGIN_URL_KEY);
            loginService = new ProxyLoginService(proxyUrl);
        } else {
            loginService = new LoginService(appConfig, secretKey, jwtExpiration);
        }

        // JWT filter is required if EITHER:
        // 1. Authentication is explicitly set to "jwt", OR
        // 2. Access mode is RESTRICTED (requires JWT for authorization)
        boolean requiresJwt = AUTH_JWT.equals(auth) || accessMode == AccessMode.RESTRICTED;

        WebServer server = WebServer.builder()
                .config(helidonConfig.get(ConfigConstants.CONFIG_PATH))
                .config(helidonConfig.get(CONFIG_FLIGHT_SQL))
                .routing(routing -> {
                    routing.register(cors);
                    var b = routing
                            .register(ENDPOINT_HEALTH, new HealthCheckService(producer))
                            .register(ENDPOINT_QUERY, new QueryService(producer))
                            .register(ENDPOINT_LOGIN, loginService)
                            .register(ENDPOINT_PLAN, new PlanningService(producer, FlightToHttpEndpointMapper.fixed(location)))
                            .register(ENDPOINT_CANCEL, new CancelService(producer))
                            .register(ENDPOINT_INGEST, new IngestionService(producer))
                            .register(ENDPOINT_UI, new UIService(producer));

                    // Add JWT filter to versioned endpoints if required
                    if (requiresJwt) {
                        logger.info("JWT authentication enabled (auth={}, accessMode={})", auth, accessMode);
                        b.addFilter(new JwtAuthenticationFilter(
                                List.of(ENDPOINT_QUERY, ENDPOINT_PLAN, ENDPOINT_INGEST, ENDPOINT_CANCEL, ENDPOINT_UI),
                                appConfig,
                                secretKey,
                                producer.getSqlAuthorizer()
                        ));
                    } else {
                        logger.warn("JWT authentication disabled - API endpoints are publicly accessible!");
                    }
                })
                .port(port)
                .host(host)
                .build()
                .start();

        var http = server.hasTls() ? PROTOCOL_HTTPS : PROTOCOL_HTTP;
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
        }, SHUTDOWN_HOOK_THREAD_NAME));

        logger.info("HTTP Server started successfully");
        logger.info("Listening on: {}", url);
        logger.info("Health check: {}{}", url, ENDPOINT_HEALTH);
        logger.info("UI dashboard: {}{}", url, ENDPOINT_UI);
    }
}