package io.dazzleduck.sql.common;

import com.typesafe.config.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ConfigConstants {

    public static final String CONFIG_PATH = "dazzleduck_server";

    public static final String WAREHOUSE_CONFIG_KEY  = "warehouse";
    public static final String AUTHENTICATION_KEY =  "authentication";

    public static final String  PORT_KEY = "port";

    public static final String  HOST_KEY =  "host";

    public static final String SECRET_KEY_KEY = "secret_key";

    public static final String ACCESS_MODE_KEY = "access_mode";

    public static final String TEMP_WRITE_LOCATION_KEY = "temp_write_location";

    // Flight SQL configuration keys
    public static final String FLIGHT_SQL_PREFIX = "flight_sql";
    public static final String USE_ENCRYPTION_KEY = "use_encryption";
    public static final String FLIGHT_SQL_USE_ENCRYPTION_KEY = "flight_sql.use_encryption";
    public static final String FLIGHT_SQL_HOST_KEY = "flight_sql.host";
    public static final String FLIGHT_SQL_PORT_KEY = "flight_sql.port";
    public static final String FLIGHT_SQL_DATA_PROCESSOR_LOCATIONS_KEY = "flight_sql.data_processor_locations";

    // Server configuration keys
    public static final String KEYSTORE_KEY = "keystore";
    public static final String SERVER_CERT_KEY = "server_cert";
    public static final String PRODUCER_ID_KEY = "producer_id";
    public static final String QUERY_TIMEOUT_MS_KEY = "query_timeout_ms";

    // Ingestion configuration keys
    public static final String INGESTION_KEY = "ingestion";
    public static final String MIN_BUCKET_SIZE_KEY = "min_bucket_size";
    public static final String MAX_BUCKET_SIZE_KEY = "max_bucket_size";
    public static final String MAX_BATCHES_KEY = "max_batches";
    public static final String MAX_PENDING_WRITE_KEY = "max_pending_write";
    public static final String MAX_DELAY_MS_KEY = "max_delay_ms";

    // JWT Token configuration keys
    public static final String JWT_TOKEN_PREFIX = "jwt_token";
    public static final String JWT_TOKEN_EXPIRATION_KEY = "jwt_token.expiration";
    public static final String JWT_TOKEN_GENERATION_KEY = "jwt_token.generation";
    public static final String JWT_TOKEN_CLAIMS_GENERATE_HEADERS_KEY = "jwt_token.claims.generate.headers";
    public static final String JWT_TOKEN_CLAIMS_VALIDATE_HEADERS_KEY = "jwt_token.claims.validate.headers";

    // Authentication configuration keys
    public static final String LOGIN_URL_KEY = "login_url";
    public static final String USERNAME_KEY = "username";
    public static final String PASSWORD_KEY = "password";
    public static final String CLAIMS_KEY = "claims";
    public static final String AUTH_NONE = "none";
    public static final String AUTH_JWT = "jwt";

    // HTTP configuration keys
    public static final String HTTP_PREFIX = "http";
    public static final String ALLOW_ORIGIN_KEY = "allow-origin";

    // Application identity keys (for client modules)
    public static final String APPLICATION_ID_KEY = "application_id";
    public static final String APPLICATION_NAME_KEY = "application_name";
    public static final String APPLICATION_HOST_KEY = "application_host";

    // HTTP client keys (for http sub-config)
    public static final String BASE_URL_KEY = "base_url";
    public static final String INGESTION_QUEUE_KEY = "ingestion_queue";
    public static final String HTTP_CLIENT_TIMEOUT_MS_KEY = "http_client_timeout_ms";

    // Batching keys
    public static final String MIN_BATCH_SIZE_KEY = "min_batch_size";
    public static final String MAX_BATCH_SIZE_KEY = "max_batch_size";
    public static final String MAX_SEND_INTERVAL_MS_KEY = "max_send_interval_ms";

    // Buffer/storage keys
    public static final String MAX_IN_MEMORY_BYTES_KEY = "max_in_memory_bytes";
    public static final String MAX_ON_DISK_BYTES_KEY = "max_on_disk_bytes";
    public static final String MAX_BUFFER_SIZE_KEY = "max_buffer_size";
    public static final String POLL_INTERVAL_MS_KEY = "poll_interval_ms";

    // Retry keys
    public static final String RETRY_COUNT_KEY = "retry_count";
    public static final String RETRY_INTERVAL_MS_KEY = "retry_interval_ms";

    // Projection keys
    public static final String PROJECT_KEY = "project";
    public static final String PARTITION_BY_KEY = "partition_by";

    // Feature flags
    public static final String ENABLED_KEY = "enabled";
    public static final String CAPTURE_CALLER_DATA_KEY = "capture_caller_data";

    // Metrics specific
    public static final String STEP_INTERVAL_MS_KEY = "step_interval_ms";
    public static final String INGESTION_CONFIG_PREFIX = "ingestion_task_factory_provider";

    public static String getWarehousePath(Config config) {
        return config.getString(WAREHOUSE_CONFIG_KEY);
    }

    public static Path getTempWriteDir(Config config) throws IOException {
        var tempWriteDir = Path.of(config.getString(TEMP_WRITE_LOCATION_KEY));
        if (!Files.exists(tempWriteDir)) {
            Files.createDirectories(tempWriteDir);
        }
        return tempWriteDir;
    }
}
