package io.dazzleduck.sql.flight.server;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.authorization.AccessMode;
import io.dazzleduck.sql.common.authorization.NOOPAuthorizer;
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
import java.security.NoSuchAlgorithmException;
import java.util.UUID;



public class Main {
    public static final String CONFIG_PATH = "dazzleduck-flight-server";

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        var flightServer = createServer(args);
        Thread severThread = new Thread(() -> {
            try {
                flightServer.start();
                System.out.println("S1: Server (Location): Listening on URI and port:  " + flightServer.getLocation().getUri());
                flightServer.awaitTermination();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        severThread.start();
    }

    public static FlightServer createServer(String[] args) throws NoSuchAlgorithmException, IOException {
        var commandLineConfig = ConfigUtils.loadCommandLineConfig(args).config();
        var config = commandLineConfig.withFallback(ConfigFactory.load().getConfig(CONFIG_PATH));
        int port = config.getInt("port");
        String host = config.getString("host");
        CallHeaderAuthenticator authenticator = AuthUtils.getAuthenticator(config);
        boolean useEncryption = config.getBoolean("useEncryption");
        Location location = useEncryption ? Location.forGrpcTls(host, port) : Location.forGrpcInsecure(host, port);
        String keystoreLocation = config.getString("keystore");
        String serverCertLocation = config.getString("serverCert");
        String warehousePath = config.hasPath("warehousePath") ? config.getString("warehousePath") : System.getProperty("user.dir") + "/warehouse";
        String secretKey = config.getString("secretKey");
        String producerId = config.hasPath("producerId") ? config.getString("producerId") : UUID.randomUUID().toString();
        if(!checkWarehousePath(warehousePath)) {
            System.out.printf("Warehouse dir does not exist %s. Create the dir to proceed", warehousePath);
        }
        AccessMode accessMode = config.hasPath("accessMode") ? AccessMode.valueOf(config.getString("accessMode").toUpperCase()) : AccessMode.COMPLETE;
        var startUpFile = config.getString("startupFile");
        ConnectionPool.execute(startUpFile);
        BufferAllocator allocator = new RootAllocator();
        var producer = new DuckDBFlightSqlProducer(location, producerId, secretKey, allocator, warehousePath, accessMode, new NOOPAuthorizer());
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
