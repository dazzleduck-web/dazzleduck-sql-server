package io.dazzleduck.sql.flight.server;

import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ingestion.NOOPPostIngestionTaskFactoryProvider;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.flight.optimizer.QueryOptimizer;
import io.dazzleduck.sql.flight.server.auth2.AdvanceJWTTokenAuthenticator;
import io.dazzleduck.sql.flight.server.auth2.AdvanceServerCallHeaderAuthMiddleware;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.flight.stream.FlightStreamReader;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public interface FlightTestUtils {

    Logger logger = LoggerFactory.getLogger(FlightTestUtils.class);

    String USER = "admin";
    String PASSWORD = "password";
    String testSchema();
    String testCatalog();
    default String testUser() {
        return USER;
    }
    default String testUserPassword() {
        return PASSWORD;
    }


    default ServerClient createRestrictedServerClient(Location serverLocation,
                                                      Map<String, String> additionalClientHeaders) throws IOException, NoSuchAlgorithmException {
        return createRestrictedServerClient(serverLocation, additionalClientHeaders, getTestJWTTokenAuthenticator());
    }



    interface ProducerFactory {
        FlightProducer create(BufferAllocator bufferAllocator, String warehousePath);
    }

    default ServerClient createRestrictedServerClient(ProducerFactory factory,
                                                      Location serverLocation,
                                                      Map<String, String> additionalClientHeaders,
                                                      AdvanceJWTTokenAuthenticator testAuthenticator) throws IOException, NoSuchAlgorithmException {
        var warehousePath = Files.createTempDirectory("duckdb_warehouse_" + DuckDBFlightSqlProducerTest.class.getName()).toString();
        var clientAllocator = new RootAllocator();
        var serverAllocator = new RootAllocator();
        var restrictFlightServer = FlightServer.builder(
                        serverAllocator,
                        serverLocation, factory.create(serverAllocator, warehousePath))
                .middleware(AdvanceServerCallHeaderAuthMiddleware.KEY,
                        new AdvanceServerCallHeaderAuthMiddleware.Factory(testAuthenticator))
                .build()
                .start();

        var allHeader = new HashMap<String, String >();
        allHeader.putAll(Map.of(Headers.HEADER_DATABASE, testCatalog(),
                Headers.HEADER_SCHEMA, testSchema()));
        allHeader.putAll(additionalClientHeaders);
        var restrictSqlClient = new FlightSqlClient(FlightClient.builder(clientAllocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(testUser(),
                        testUserPassword(),
                        allHeader))
                .build());
        return new ServerClient(restrictFlightServer, restrictSqlClient, clientAllocator, warehousePath);
    }
    default ServerClient createRestrictedServerClient(Location serverLocation,
                                                      Map<String, String> additionalClientHeaders,
                                                      AdvanceJWTTokenAuthenticator testAuthenticator) throws IOException, NoSuchAlgorithmException {

        var producerId = UUID.randomUUID().toString();
        return createRestrictedServerClient((allocator, warehousePath) ->
            new RestrictedFlightSqlProducer(serverLocation,
                    producerId,
                    "change me",
                    allocator, warehousePath,
                    Path.of(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString()),
                    NOOPPostIngestionTaskFactoryProvider.NO_OP.getPostIngestionTaskFactory(),
                    Executors.newSingleThreadScheduledExecutor(), Duration.ofMinutes(2), Clock.systemDefaultZone(),
                    DuckDBFlightSqlProducer.buildRecorder(producerId),
                    QueryOptimizer.NOOP_QUERY_OPTIMIZER,  DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG),
                serverLocation, additionalClientHeaders, testAuthenticator);
    }

    static AdvanceJWTTokenAuthenticator getTestJWTTokenAuthenticator() throws NoSuchAlgorithmException {
        var jwtGenerateConfigString = """
                jwt_token.claims.generate.headers=[database,catalog,schema,table,filter,path,function]
                jwt_token.claims.validate.headers=[database,schema]
                """;
        var jwtConfig = ConfigFactory.parseString(jwtGenerateConfigString);
        var config = jwtConfig.withFallback(ConfigFactory.load().getConfig("dazzleduck_server"));
        return AuthUtils.getTestAuthenticator(config);
    }

    static ServerClient setUpFlightServerAndClient(String[] confOverload,
                                                   String user,
                                                   String password,
                                                   Map<String, String> clientHeaders) throws Exception {
        var flightServer = io.dazzleduck.sql.flight.server.Main.createServer(confOverload);
        Thread severThread = new Thread(() -> {
            try {
                flightServer.start();
                logger.info("Flight Server is up: Listening on URI: {}", flightServer.getLocation().getUri());
                flightServer.awaitTermination();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        severThread.start();
        var clientAllocator = new RootAllocator();
        var sqlClient = new FlightSqlClient(FlightClient.builder(clientAllocator, flightServer.getLocation())
                .intercept(AuthUtils.createClientMiddlewareFactory(user, password, clientHeaders))
                .build());
        var commandLineConfig = ConfigUtils.loadCommandLineConfig(confOverload).config();
        return new ServerClient(flightServer, sqlClient, clientAllocator, ConfigUtils.getWarehousePath(commandLineConfig));
    }

    static void testQuery(String testQuery, FlightSqlClient sqlClient, BufferAllocator clientAllocator) throws Exception {
        testQuery(testQuery, testQuery, sqlClient, clientAllocator);
    }

    static void testQuery(String expectedQuery, String query, FlightSqlClient sqlClient, BufferAllocator clientAllocator) throws Exception {
        testStream(expectedQuery, () -> {
            final FlightInfo flightInfo = sqlClient.execute(query);
            return sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket());
        },  clientAllocator);
    }

    static void testStream(String expectedQuery, Supplier<FlightStream> streamSupplier, BufferAllocator clientAllocator) throws Exception {
        try (final FlightStream stream = streamSupplier.get()) {
            TestUtils.isEqual(expectedQuery, clientAllocator, FlightStreamReader.of(stream, clientAllocator));
        }
    }


    public static Location findNextLocation(){
        var port = findFreePortInRange(3200, 65000);
        return Location.forGrpcInsecure("localhost", port);
    }

    /**
     * Finds an available TCP port within a specified range.
     * Uses randomization to reduce conflicts when running parallel tests.
     *
     * @param startPort The starting port number of the range (inclusive).
     * @param endPort The ending port number of the range (inclusive).
     * @return An available port number within the specified range.
     * @throws IllegalArgumentException if the port range is invalid.
     * @throws RuntimeException if no available port is found in the range.
     */
    static int findFreePortInRange(int startPort, int endPort) {
        if (startPort < 1 || endPort > 65535 || startPort > endPort) {
            throw new IllegalArgumentException(
                String.format("Invalid port range [%d, %d]. Ports must be between 1 and 65535, and startPort <= endPort.",
                    startPort, endPort));
        }

        // Create a shuffled list of ports to try in random order
        // This reduces conflicts when multiple tests run concurrently
        List<Integer> ports = new ArrayList<>();
        IntStream.rangeClosed(startPort, endPort).forEach(ports::add);
        Collections.shuffle(ports, ThreadLocalRandom.current());

        int attemptedPorts = 0;
        for (int port : ports) {
            attemptedPorts++;
            if (isPortAvailable(port)) {
                return port;
            }
        }

        throw new RuntimeException(
            String.format("No available port found in range [%d, %d] after trying %d ports",
                startPort, endPort, attemptedPorts));
    }

    /**
     * Checks if a specific port is available for binding.
     *
     * @param port The port number to check.
     * @return true if the port is available, false otherwise.
     */
    static boolean isPortAvailable(int port) {
        try (ServerSocket socket = new ServerSocket()) {
            // Set SO_REUSEADDR to allow immediate reuse of ports
            socket.setReuseAddress(true);
            socket.bind(new java.net.InetSocketAddress(port));
            return true;
        } catch (IOException e) {
            // Port is not available
            return false;
        }
    }

    static FlightTestUtils createForDatabaseSchema(String user, String password, String database, String schema){
        return new Default(user, password, database, schema);
    }

    class Default implements FlightTestUtils {

        private final String user;
        private final String password;
        private final String database;
        private final String schema;

        public Default(String user, String password, String database, String schema) {
            this.user = user;
            this.password = password;
            this.database = database;
            this.schema = schema;
        }
        @Override
        public String testSchema() {
            return schema;
        }

        @Override
        public String testCatalog() {
            return database;
        }

        @Override
        public String testUser() {
            return this.user;
        }

        @Override
        public String testUserPassword() {
            return this.password;
        }
    }
}


