package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTaskFactoryProvider;
import io.dazzleduck.sql.commons.util.MutableClock;
import io.dazzleduck.sql.flight.optimizer.QueryOptimizer;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class DuckDBSqlProducerTimeoutTest {
    protected static final String LOCALHOST = "localhost";
    private static final String USER = "admin";
    private static final String PASSWORD = "password";
    private static final String TEST_CATALOG = "producer_test_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final BufferAllocator clientAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final BufferAllocator serverAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final String LONG_RUNNING_QUERY = "with t as " +
            "(select len(split(concat('abcdefghijklmnopqrstuvwxyz:', generate_series), ':')) as len  from generate_series(1, 1000000000) )" +
            " select count(*) from t where len = 10";
    protected static MutableClock mutableClock;
    protected static DeterministicScheduler executor;
    protected static FlightServer flightServer;
    protected static FlightSqlClient sqlClient;
    protected static String warehousePath;


    @BeforeAll
    public static void beforeAll() throws Exception {
        Path tempDir = Files.createTempDirectory("duckdb_" + DuckDBFlightSqlProducerTest.class.getName());
        warehousePath = Files.createTempDirectory("duckdb_warehouse_" + DuckDBFlightSqlProducerTest.class.getName()).toString();
        String[] sql = {
                String.format("ATTACH '%s/file.db' AS %s", tempDir.toString(), TEST_CATALOG),
                String.format("USE %s", TEST_CATALOG),
                String.format("CREATE SCHEMA %s", TEST_SCHEMA),
                String.format("USE %s.%s", TEST_CATALOG, TEST_SCHEMA)
        };
        ConnectionPool.executeBatch(sql);
        setUpClientServer();
    }


    @AfterAll
    public static void afterAll() {
        String[] sql = { "DETACH  " + TEST_CATALOG };
        ConnectionPool.executeBatch(sql);
        clientAllocator.close();
    }

    private static void setUpClientServer() throws Exception {
        final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55551);
        mutableClock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        executor = new DeterministicScheduler();
        flightServer = FlightServer.builder(
                        serverAllocator,
                        serverLocation,
                        new DuckDBFlightSqlProducer(serverLocation,
                                UUID.randomUUID().toString(),
                                "change me",
                                serverAllocator, warehousePath, AccessMode.COMPLETE,
                                DuckDBFlightSqlProducer.newTempDir(),
                                PostIngestionTaskFactoryProvider.NO_OP.getPostIngestionTaskFactory(),
                                executor,
                                Duration.ofSeconds(5),
                                mutableClock,
                                new NOOPFlightRecorder(),
                                QueryOptimizer.NOOP_QUERY_OPTIMIZER
                        ))
                .headerAuthenticator(AuthUtils.getTestAuthenticator())
                .build()
                .start();
        sqlClient = new FlightSqlClient(FlightClient.builder(clientAllocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER,
                        PASSWORD,
                        Map.of(Headers.HEADER_DATABASE, TEST_CATALOG,
                                Headers.HEADER_SCHEMA, TEST_SCHEMA)))
                .build());
    }

    @Test
    public void testAutoCancelForPreparedStatement() throws Exception {
        try (FlightSqlClient.PreparedStatement preparedStatement = sqlClient.prepare(LONG_RUNNING_QUERY);
             FlightStream stream = sqlClient.getStream(preparedStatement.execute().getEndpoints().get(0).getTicket())) {
            Thread.sleep(100);
            mutableClock.advanceBy(Duration.ofSeconds(10));
            executor.tick(10, TimeUnit.SECONDS);
            assertThrows(Exception.class, stream::next);
        }
    }

    @Test
    public void testAutoCancelForStatement() throws Exception {
        FlightInfo flightInfo = sqlClient.execute(LONG_RUNNING_QUERY);
        try (FlightStream stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
            Thread.sleep(100);
            mutableClock.advanceBy(Duration.ofSeconds(10));
            executor.tick(10, TimeUnit.SECONDS);
            assertThrows(Exception.class, stream::next);
        }
    }
}
