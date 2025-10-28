package io.dazzleduck.sql.flight.server;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.flight.stream.FlightStreamReader;
import io.dazzleduck.sql.flight.server.auth2.AdvanceJWTTokenAuthenticator;
import io.dazzleduck.sql.flight.server.auth2.AdvanceServerCallHeaderAuthMiddleware;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

public interface FlightTestUtils {

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

    default ServerClient createRestrictedServerClient(ProducerFactory producerFactory,
                                                      Location serverLocation,
                                                      Map<String, String> additionalClientHeaders) throws IOException, NoSuchAlgorithmException {
        return createRestrictedServerClient(producerFactory, serverLocation, additionalClientHeaders, getTestJWTTokenAuthenticator());
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

        return createRestrictedServerClient((allocator, warehousePath) ->
            new DuckDBFlightSqlProducer(serverLocation,
                    UUID.randomUUID().toString(),
                    "change me",
                    allocator, warehousePath, AccessMode.RESTRICTED,
                    Path.of(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString())),
                serverLocation, additionalClientHeaders, testAuthenticator);
    }

    public  default AdvanceJWTTokenAuthenticator getTestJWTTokenAuthenticator() throws NoSuchAlgorithmException {
        var jwtGenerateConfigString = """
                jwt.token.claims.generate.headers=[database,catalog,schema,table,filter,path,function]
                jwt.token.claims.validate.headers=[database,schema]
                """;
        var jwtConfig = ConfigFactory.parseString(jwtGenerateConfigString);
        var config = jwtConfig.withFallback(ConfigFactory.load().getConfig("dazzleduck-server"));
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
                System.out.println("Flight Server is up: Listening on URI: " + flightServer.getLocation().getUri());
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
}
