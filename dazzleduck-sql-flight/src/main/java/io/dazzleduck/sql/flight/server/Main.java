package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.StartupScriptProvider;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTaskFactory;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTaskFactoryProvider;
import io.dazzleduck.sql.flight.server.auth2.AdvanceJWTTokenAuthenticator;
import io.dazzleduck.sql.flight.server.auth2.AdvanceServerCallHeaderAuthMiddleware;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;


public class Main {

    public static void main(String[] args) throws Exception {
        var commandLineConfig = ConfigUtils.loadCommandLineConfig(args).config();
        var config = commandLineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
        start(config);
    }

    public static FlightServer createServer(String[] args) throws Exception {
        var commandLineConfig = ConfigUtils.loadCommandLineConfig(args).config();
        var config = commandLineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
        return createServer(config);
    }
    public static FlightServer createServer(Config config) throws Exception {
        int port = config.getInt("flight_sql.port");
        String host = config.getString("flight_sql.host");
        AdvanceJWTTokenAuthenticator authenticator = AuthUtils.getAuthenticator(config);
        boolean useEncryption = config.getBoolean("use_encryption");
        Location location = useEncryption ? Location.forGrpcTls(host, port) : Location.forGrpcInsecure(host, port);
        String keystoreLocation = config.getString("keystore");
        String serverCertLocation = config.getString("server_cert");
        String warehousePath = ConfigUtils.getWarehousePath(config);
        String secretKey = config.getString(ConfigUtils.SECRET_KEY_KEY);
        String producerId = config.hasPath("producerId") ? config.getString("producerId") : UUID.randomUUID().toString();
        if(!checkWarehousePath(warehousePath)) {
            System.out.printf("Warehouse dir does not exist %s. Create the dir to proceed", warehousePath);
        }
        AccessMode accessMode = DuckDBFlightSqlProducer.getAccessMode(config);
        var startupContent = StartupScriptProvider.load(config).getStartupScript();
        if (startupContent != null) {
            ConnectionPool.execute(startupContent);
        }
        BufferAllocator allocator = new RootAllocator();
        var postIngestionTaskFactorProvider = PostIngestionTaskFactoryProvider.load(config);
        var postIngestionTaskFactor = postIngestionTaskFactorProvider.getPostIngestionTaskFactory();
        var tempWriteDirector = DuckDBFlightSqlProducer.getTempWriteDir(config);
        var producer = createProducer(location, producerId, secretKey, allocator, warehousePath, tempWriteDirector, accessMode, postIngestionTaskFactor);
        var certStream = getInputStreamForResource(serverCertLocation);
        var keyStream = getInputStreamForResource(keystoreLocation);

        var builder = FlightServer.builder(allocator, location, producer)
                .middleware(AdvanceServerCallHeaderAuthMiddleware.KEY,
                        new AdvanceServerCallHeaderAuthMiddleware.Factory(authenticator));
        if (useEncryption) {
            builder.useTls(certStream, keyStream);
        }
        return builder.build();
    }


    public static void start(Config config) throws Exception {
        var flightServer = createServer(config);
        Thread severThread = new Thread(() -> {
            try {
                flightServer.start();
                System.out.println("Flight Server is up: Listening on URI: " + flightServer.getLocation().getUri());
                flightServer.awaitTermination();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        severThread.start();
    }

    public static DuckDBFlightSqlProducer createProducer(Location location,
                                                         String producerId,
                                                         String secretKey,
                                                         BufferAllocator allocator,
                                                         String warehousePath,
                                                         Path tempWriteDir,
                                                         AccessMode accessMode,
                                                         PostIngestionTaskFactory postIngestionTaskFactory) {
        return new DuckDBFlightSqlProducer(location, producerId, secretKey, allocator, warehousePath, accessMode, tempWriteDir, postIngestionTaskFactory, Executors.newSingleThreadScheduledExecutor(), Duration.ofMinutes(2));
    }

    private static InputStream getInputStreamForResource(String filename) {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
        if (inputStream == null) {
            throw new IllegalArgumentException("File not found! : " + filename);
        }
        return inputStream;
    }

    private static boolean checkWarehousePath(String warehousePath) {
        return true;
    }
}
