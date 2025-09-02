package io.dazzleduck.sql.flight.server;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.StartupScriptProvider;
import io.dazzleduck.sql.common.authorization.AccessMode;
import io.dazzleduck.sql.common.authorization.SimpleAuthorizer;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;


public class Main {

    public static void main(String[] args) throws Exception {
        var flightServer = createServer(args);
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

    public static FlightServer createServer(String[] args) throws Exception {
        var commandLineConfig = ConfigUtils.loadCommandLineConfig(args).config();
        var config = commandLineConfig.withFallback(ConfigFactory.load().getConfig(CONFIG_PATH));
        int port = config.getInt("flight-sql.port");
        String host = config.getString("flight-sql.host");
        CallHeaderAuthenticator authenticator = AuthUtils.getAuthenticator(config);
        boolean useEncryption = config.getBoolean("useEncryption");
        Location location = useEncryption ? Location.forGrpcTls(host, port) : Location.forGrpcInsecure(host, port);
        String keystoreLocation = config.getString("keystore");
        String serverCertLocation = config.getString("serverCert");
        String warehousePath = ConfigUtils.getWarehousePath(config);
        String secretKey = config.getString("secretKey");
        String producerId = config.hasPath("producerId") ? config.getString("producerId") : UUID.randomUUID().toString();
        if(!checkWarehousePath(warehousePath)) {
            System.out.printf("Warehouse dir does not exist %s. Create the dir to proceed", warehousePath);
        }
        AccessMode accessMode = config.hasPath("accessMode") ? AccessMode.valueOf(config.getString("accessMode").toUpperCase()) : AccessMode.COMPLETE;
        var startupContent = StartupScriptProvider.load(config).getStartupScript();
        if (startupContent != null) {
            ConnectionPool.execute(startupContent);
        }

        BufferAllocator allocator = new RootAllocator();
        var authorizer = SimpleAuthorizer.load(ConfigFactory.load("access.conf"));
        var producer = new DuckDBFlightSqlProducer(location, producerId, secretKey, allocator, warehousePath, accessMode, authorizer);
        var certStream =  getInputStreamForResource(serverCertLocation);
        var keyStream = getInputStreamForResource(keystoreLocation);


        var builder = FlightServer.builder(allocator, location, producer)
                .headerAuthenticator(authenticator);
        if (useEncryption) {
            builder.useTls(certStream, keyStream);
        }
        return builder.build();
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
