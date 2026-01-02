package io.dazzleduck.sql.flight.server;


import io.dazzleduck.sql.common.ConfigBasedProvider;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.LocalStartupConfigProvider;
import io.dazzleduck.sql.common.StartupScriptProvider;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTaskFactoryProvider;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.flight.optimizer.QueryOptimizer;
import io.dazzleduck.sql.flight.FlightRecorder;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.stream.FlightStreamReader;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.flight.stream.ArrowStreamReaderWrapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

import static io.dazzleduck.sql.common.LocalStartupConfigProvider.SCRIPT_LOCATION_KEY;
import static io.dazzleduck.sql.commons.util.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class DuckDBFlightSqlProducerTest {
    protected static final String LOCALHOST = "localhost";
    private static final Logger logger = LoggerFactory.getLogger(DuckDBFlightSqlProducerTest.class);
    private static final String USER = "admin";
    private static final String PASSWORD = "password";
    private static final String TEST_CATALOG = "producer_test_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final BufferAllocator clientAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final BufferAllocator serverAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final String LONG_RUNNING_QUERY = "with t as " +
            "(select len(split(concat('abcdefghijklmnopqrstuvwxyz:', generate_series), ':')) as len  from generate_series(1, 1000000000) )" +
            " select count(*) from t where len = 10";
    protected static FlightServer flightServer;
    protected static FlightSqlClient sqlClient;
    protected static String warehousePath;
    private static DuckDBFlightSqlProducer producer;
    @TempDir
    Path projectTempDir;
    private Path catalogFile;
    private Path dataPath;
    private final String CATALOG = "test_catalog";

    @BeforeAll
    public static void beforeAll() throws Exception {
        Path tempDir = Files.createTempDirectory("duckdb_" + DuckDBFlightSqlProducerTest.class.getName());
        warehousePath = Files.createTempDirectory("duckdb_warehouse_" + DuckDBFlightSqlProducerTest.class.getName()).toString();
        String[] sqls = {
                "INSTALL arrow FROM community",
                "LOAD arrow",
                String.format("ATTACH '%s/file.db' AS %s", tempDir.toString(), TEST_CATALOG),
                String.format("USE %s", TEST_CATALOG),
                String.format("CREATE SCHEMA %s", TEST_SCHEMA),
                String.format("USE %s.%s", TEST_CATALOG, TEST_SCHEMA),
                String.format("CREATE TABLE %s (key string, value string)", TEST_TABLE),
                String.format("INSERT INTO %s VALUES ('k1', 'v1'), ('k2', 'v2')", TEST_TABLE)
        };
        ConnectionPool.executeBatch(sqls);
        setUpClientServer();
    }

    @AfterAll
    public static void afterAll() {
        clientAllocator.close();
    }

    private static void setUpClientServer() throws Exception {
        final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55556);
        String producerId = UUID.randomUUID().toString();
        FlightRecorder recorder = new MicroMeterFlightRecorder(new SimpleMeterRegistry(), producerId);
        producer = new DuckDBFlightSqlProducer(
                serverLocation,
                producerId,
                "change me",
                serverAllocator,
                warehousePath,
                AccessMode.COMPLETE,
                DuckDBFlightSqlProducer.newTempDir(),
                PostIngestionTaskFactoryProvider.NO_OP.getPostIngestionTaskFactory(),
                Executors.newSingleThreadScheduledExecutor(),
                Duration.ofMinutes(2),
                Clock.systemDefaultZone(),
                recorder, QueryOptimizer.NOOP_QUERY_OPTIMIZER, DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG);

        flightServer = FlightServer.builder(
                        serverAllocator,
                        serverLocation,
                        producer)

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

    @ParameterizedTest
    @ValueSource(strings = {"SELECT * FROM generate_series(10)",
            "SELECT * from " + TEST_CATALOG + "." + TEST_SCHEMA + "." + TEST_TABLE
    })
    public void testSimplePreparedStatementResults(String query) throws Exception {
        try (final FlightSqlClient.PreparedStatement preparedStatement =
                     sqlClient.prepare(query)) {
            try (final FlightStream stream =
                         sqlClient.getStream(preparedStatement.execute().getEndpoints().get(0).getTicket())) {
                TestUtils.isEqual(query, clientAllocator, FlightStreamReader.of(stream, clientAllocator));
            }

            // Read Again
            try (final FlightStream stream =
                         sqlClient.getStream(preparedStatement.execute().getEndpoints().get(0).getTicket())) {
                TestUtils.isEqual(query, clientAllocator, FlightStreamReader.of(stream, clientAllocator));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"SELECT * FROM generate_series(10)",
            "select [1, 2, 3] as \"array\"",
            "SELECT * from " + TEST_CATALOG + "." + TEST_SCHEMA + "." + TEST_TABLE
    })
    public void testStatement(String query) throws Exception {
        FlightTestUtils.testQuery(query, sqlClient, clientAllocator);
    }

    @ParameterizedTest
    @ValueSource(strings = {"SELECT * FROM generate_series(" + Headers.DEFAULT_ARROW_FETCH_SIZE * 3 + ")"
    })
    public void testStatementMultiBatch(String query) throws Exception {
        FlightTestUtils.testQuery(query, sqlClient, clientAllocator);
    }

    @Test
    public void testStatementSplittableHive() throws Exception {
        final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55559);
        try ( var serverClient = createRestrictedServerClient( serverLocation, "admin" )) {
            try (var splittableClient = splittableAdminClientForPath(serverLocation, serverClient.clientAllocator(), "example/hive_table")) {
                var flightCallHeaders = new FlightCallHeaders();
                flightCallHeaders.insert(Headers.HEADER_SPLIT_SIZE, "1");
                var flightInfo = splittableClient.execute(SUPPORTED_HIVE_PATH_QUERY, new HeaderCallOption(flightCallHeaders));
                assertEquals(3, flightInfo.getEndpoints().size());
                var size = 0;
                for (var endpoint : flightInfo.getEndpoints()) {
                    try (final FlightStream stream = splittableClient.getStream(endpoint.getTicket(), new HeaderCallOption(flightCallHeaders))) {
                        while (stream.next()) {
                            size+=stream.getRoot().getRowCount();
                        }
                    }
                }
                assertEquals(6, size);
            }
        }
    }

    @Test
    public void testStatementSplittableDelta() throws Exception {
        var serverLocation = Location.forGrpcInsecure(LOCALHOST, 55577);
        try(var clientServer = createRestrictedServerClient( serverLocation, "admin")) {

            try (var splittableClient = splittableAdminClientForPath(serverLocation, clientServer.clientAllocator(),  "example/delta_table")) {
                var flightCallHeaders = new FlightCallHeaders();
                flightCallHeaders.insert(Headers.HEADER_SPLIT_SIZE, "1");
                var flightInfo = splittableClient.execute(SUPPORTED_DELTA_PATH_QUERY, new HeaderCallOption(flightCallHeaders));
                var size = 0;
                assertEquals(8, flightInfo.getEndpoints().size());
                for (var endpoint : flightInfo.getEndpoints()) {
                    try (final FlightStream stream = splittableClient.getStream(endpoint.getTicket(), new HeaderCallOption(flightCallHeaders))) {
                        while (stream.next()) {
                            size+=stream.getRoot().getRowCount();
                        }
                    }
                }
                assertEquals(11, size);
            }
        }
    }



    @Test
    public void testBadStatement() throws Exception {

        assertThrows(FlightRuntimeException.class, () -> {
            String query = "SELECT x FROM generate_series(10)";
            final FlightInfo flightInfo = sqlClient.execute(query);
            try (final FlightStream stream =
                         sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
                stream.next();
            }
        });
    }

    @Test
    public void testCancelQuery() throws SQLException {
        try (Connection connection = ConnectionPool.getConnection();
             Statement statement = connection.createStatement()) {
            Thread thread = new Thread(() -> {
                try {
                    Thread.sleep(200);
                    statement.cancel();
                } catch (InterruptedException | SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
            try {
                statement.execute(LONG_RUNNING_QUERY);
                // It should not reach here. Expected to throw exception
            } catch (Exception e) {
                // Nothing to do
            }
        }
    }

    @Test
    public void testCancelRemoteStatement() throws Exception {
        final FlightInfo flightInfo = sqlClient.execute(LONG_RUNNING_QUERY);
        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(200);
                var running = producer.getRunningStatementDetails();
                sqlClient.cancelFlightInfo(new CancelFlightInfoRequest(flightInfo));
                // Test the assertion after cancel is  called because if assert will fail
                // then the test will hang for a very long time
                assertEquals(1, producer.getRunningStatementDetails().size());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        try (final FlightStream stream =
                     sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
            while (stream.next()) {
                // It should now reach here
                throw new RuntimeException("Cancellation failed");
            }
        } catch (Exception e) {
            // Expected. Ignore it
        }
        assertEquals(0, producer.getRunningStatementDetails().size());
    }

    @Test
    public void testGetCatalogsResults() throws Exception {
        String expectedSql = "select distinct(database_name) as TABLE_CAT from duckdb_columns() order by database_name";
        FlightTestUtils.testStream(expectedSql,
                () -> sqlClient.getStream(sqlClient.getCatalogs().getEndpoints().get(0).getTicket()),
                clientAllocator);
    }

    @Test
    public void testGetTablesResultNoSchema() throws Exception {
        try (final FlightStream stream =
                     sqlClient.getStream(
                             sqlClient.getTables(null, null, null,
                                     null, false).getEndpoints().get(0).getTicket())) {
            int count = 0;
            while (stream.next()) {
                count += stream.getRoot().getRowCount();
            }
            assertEquals(1, count);
        }
    }

    @Test
    public void testGetSchema() throws Exception {
        try (final FlightStream stream = sqlClient.getStream(
                sqlClient.getSchemas(null, null).getEndpoints().get(0).getTicket())) {
            TestUtils.isEqual( "SELECT catalog_name, schema_name  FROM information_schema.schemata", clientAllocator, FlightStreamReader.of(stream,  clientAllocator));
        }
    }

    @Test
    public void putStream() throws Exception {
        testPutStream("test_123.parquet");
    }

    @Test
    public void putStreamWithError() throws Exception {
        testPutStream("test_456.parquet");
        try {
            testPutStream("test_456.parquet");
        } catch (Exception e ){
          // Exception is expected.
        }
    }

    @Test
    public void testSetFetchSize() throws Exception {
        String query = "select * from generate_series(100)";
        var flightCallHeader = new FlightCallHeaders();
        flightCallHeader.insert(Headers.HEADER_FETCH_SIZE, Integer.toString(10));
        HeaderCallOption callOption = new HeaderCallOption(flightCallHeader);
        final FlightInfo flightInfo = sqlClient.execute(query, callOption);
        int batches = 0;
        try (final FlightStream stream =
                     sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), callOption)) {
            while (stream.next()) {
                batches ++;
            }
            assertEquals(11, batches);
        }
    }

    @Test
    public void testRestrictClientServer() throws Exception {
        var newServerLocation = Location.forGrpcInsecure(LOCALHOST, 55557);
        var restrictedUser = "restricted_user";
        try (var serverClient = createRestrictedServerClient(newServerLocation, restrictedUser)) {
            String expectedSql = "%s where p = '1'".formatted(SUPPORTED_HIVE_PATH_QUERY);
            ConnectionPool.printResult(expectedSql);
            var clientAllocator = serverClient.clientAllocator();
            try (var client = splittableClientForPathAndFilter( newServerLocation, clientAllocator, restrictedUser, "example/hive_table", "p = '1'")) {
                var newFlightInfo = client.execute(SUPPORTED_HIVE_PATH_QUERY);
                try (final FlightStream stream =
                             client.getStream(newFlightInfo.getEndpoints().get(0).getTicket())) {
                    TestUtils.isEqual(expectedSql, clientAllocator, FlightStreamReader.of(stream, clientAllocator));
                }
                final FlightInfo flightInfo = client.execute(SUPPORTED_HIVE_PATH_QUERY);
                try (final FlightStream stream =
                             client.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
                    TestUtils.isEqual(expectedSql, clientAllocator, FlightStreamReader.of(stream, clientAllocator));
                }
            }
        }
    }

    @Test
    public void startUpTest() throws Exception {
        File startUpFile = File.createTempFile("/temp/startup/startUpFile", ".sql");
        startUpFile.deleteOnExit();
        String startUpFileContent = "CREATE TABLE a (key string); INSERT INTO a VALUES('k');\n-- This is a single-line comment \nINSERT INTO a VALUES('k2');\n-- this  is comment \nINSERT INTO a VALUES('k3')";
        try (var writer = new FileWriter(startUpFile)) {
            writer.write(startUpFileContent);
        }
        String startUpFileLocation = startUpFile.getAbsolutePath();
        var classConfig = "%s.%s.%s=%s".formatted(ConfigUtils.CONFIG_PATH, StartupScriptProvider.STARTUP_SCRIPT_CONFIG_PREFIX, ConfigBasedProvider.CLASS_KEY, LocalStartupConfigProvider.class.getName());
        var locationConfig = "%s.%s.%s=%s".formatted(ConfigUtils.CONFIG_PATH, StartupScriptProvider.STARTUP_SCRIPT_CONFIG_PREFIX, SCRIPT_LOCATION_KEY, startUpFileLocation);

        Main.main(new String[]{"--conf", classConfig, "--conf", locationConfig});
        List<String> expected = new ArrayList<>();
        try (Connection conn = ConnectionPool.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT key FROM a")) {
            while (rs.next()) {
                expected.add(rs.getString("key"));
            }
        }
        assertEquals(List.of("k", "k2", "k3"), expected);
        try (Connection conn = ConnectionPool.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE a");
        }
    }

    @Test
    public void testOpenPreparedStatementDetailsLifecycle() throws Exception {
        SqlProducerMBean mbean = producer;
        var preparedStatement = sqlClient.prepare("SELECT * FROM generate_series(10)");
        assertEquals(1, mbean.getOpenPreparedStatementDetails().size());
        var flightInfo = preparedStatement.execute();
        var ticket = flightInfo.getEndpoints().get(0).getTicket();
        var stream = sqlClient.getStream(ticket);
        while (stream.next()) {}
        stream.close();
        preparedStatement.close();
        assertEquals(0, mbean.getOpenPreparedStatementDetails().size());
    }

    @Test
    public void testBytesOut() throws Exception {

        SqlProducerMBean mbean = producer;
        var flightInfo = sqlClient.execute("SELECT * FROM generate_series(1000)");
        try (var stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
            while (stream.next()) { }
        }
        double bytesOut = mbean.getBytesOut();
        assertTrue(bytesOut > 0);
    }

    private ServerClient createRestrictedServerClient(Location serverLocation,
                                                      String user) throws IOException, NoSuchAlgorithmException {
        var testUtil = FlightTestUtils.createForDatabaseSchema(user, "",  TEST_CATALOG, TEST_SCHEMA);
        return testUtil.createRestrictedServerClient(serverLocation, Map.of());
    }

    private void testPutStream(String filename) throws SQLException, IOException {
        Files.createDirectories(Path.of(warehousePath, filename));
        String query = "select * from generate_series(10)";
        try(DuckDBConnection connection = ConnectionPool.getConnection();
            var reader = ConnectionPool.getReader( connection, clientAllocator, query, 1000 )) {
            var streamReader = new ArrowStreamReaderWrapper(reader, clientAllocator);
            var executeIngestOption = new FlightSqlClient.ExecuteIngestOptions("",
                    FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder().build(),
                    false, "", "", Map.of("path", filename));
            sqlClient.executeIngest(streamReader, executeIngestOption);
        }
    }

    private static Connection getConnection() throws SQLException {
        String url = String.format("jdbc:arrow-flight-sql://localhost:%s/?database=memory&useEncryption=0&user=%s&password=%s&retainAuth=true", flightServer.getPort(), USER, PASSWORD );
        return DriverManager.getConnection(url);
    }

    private FlightSqlClient splittableAdminClientForPath( Location location, BufferAllocator allocator, String path) {
        return new FlightSqlClient(FlightClient.builder(allocator, location)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER,
                        PASSWORD,
                        Map.of(Headers.HEADER_DATABASE, TEST_CATALOG,
                                Headers.HEADER_SCHEMA, TEST_SCHEMA,
                                Headers.HEADER_PATH, path)))
                .build());
    }

    private FlightSqlClient splittableClientForPathAndFilter( Location location, BufferAllocator allocator, String user, String path, String filter) {
        return new FlightSqlClient(FlightClient.builder(allocator, location)
                .intercept(AuthUtils.createClientMiddlewareFactory(user,
                        PASSWORD,
                        Map.of(Headers.HEADER_DATABASE, TEST_CATALOG,
                                Headers.HEADER_SCHEMA, TEST_SCHEMA,
                        Headers.HEADER_PATH, path, Headers.HEADER_FILTER, filter)))
                .build());
    }
}