package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DucklakeFlightTest {
    private static final String USER = "admin";
    private static final String PASSWORD = "password";
    private static final String CATALOG = "test_ducklake";
    private static final String SCHEMA = "main";
    private static final String PARTITIONED_TABLE = "tt_p";
    private static final String NON_PARTITIONED_TABLE = "tt";
    private static String workspace;

    @BeforeAll
    public static void setup() throws Exception {
        // Create workspace and warehouse directories
        workspace = Files.createTempDirectory("ducklake_test_workspace_").toString();

        // Setup ducklake database
        setupDucklakeDatabase();
    }

    private static void setupDucklakeDatabase() {
        String[] setups = {
                "INSTALL ducklake",
                "LOAD  ducklake",
                "ATTACH 'ducklake:%s/metadata' AS %s (DATA_PATH '%s/data')".formatted(workspace, CATALOG, workspace),
        };
        ConnectionPool.executeBatch(setups);

        // Create test tables
        createTestTable(CATALOG + "." + SCHEMA + "." + PARTITIONED_TABLE, true);
        addDataFile(PARTITIONED_TABLE);

        createTestTable(CATALOG + "." + SCHEMA + "." + NON_PARTITIONED_TABLE, false);
        addDataFile(NON_PARTITIONED_TABLE);
    }

    private static void createTestTable(String table, boolean partitioned) {
        String[] statements = getStatements(table, partitioned);
        for (var statement : statements) {
            ConnectionPool.execute(statement);
        }
    }

    private static String[] getStatements(String table, boolean partition) {
        var createStatement = "CREATE TABLE %s(key string, value string, partition int)".formatted(table);
        var insertStatement0 = "INSERT INTO %s VALUES ('k00', 'v00', 0), ('k01', 'v01', 0)".formatted(table);
        var partitionTable = partition ? "ALTER TABLE %s SET PARTITIONED BY (partition)".formatted(table)
                : "select 1";
        var insertStatement1 = "INSERT INTO %s VALUES (null, null, 1)".formatted(table);
        var insertStatement2 = "INSERT INTO %s VALUES ('k11', 'v11', 1), ('k21', 'v21', 1)".formatted(table);
        var insertStatement3 = "INSERT INTO %s VALUES ('k31', 'v31', 1), ('k41', 'v41', 1)".formatted(table);
        var insertStatement4 = "INSERT INTO %s VALUES ('k51', 'v51', 1), ('k61', 'v61', 1)".formatted(table);
        var addColumn = "ALTER TABLE %s ADD COLUMN version INT".formatted(table);
        var insertStatement5 = "INSERT INTO %s VALUES ('k51', 'v51', 1, 0), ('k61', 'v61', 1, 0)".formatted(table);
        var insertStatement6 = "INSERT INTO %s VALUES ('k72', 'v72', 2, 0), ('k82', 'v82', 2, 0)".formatted(table);
        return new String[]{
                createStatement,
                insertStatement0,
                partitionTable,
                insertStatement1,
                insertStatement2,
                insertStatement3,
                insertStatement4,
                addColumn,
                insertStatement5,
                insertStatement6,
        };
    }

    private static void addDataFile(String table) {
        String path = workspace + "/data/main/" + table + "/manual.parquet";
        String sql = "COPY (SELECT * FROM (VALUES('k119', 'v119', 9, 0),('k118', 'v118', 8, 0)) AS my_value(key, value, partition, version)) TO '%s' (FORMAT parquet)".formatted(path);
        String addTableSql = "CALL ducklake_add_data_files('%s', '%s', '%s', allow_missing => true)".formatted(CATALOG, table, path);
        String[] batch = {sql, addTableSql};
        ConnectionPool.executeBatch(batch);
    }


    @AfterAll
    public static void cleanup() throws IOException {
        // Detach catalog
        ConnectionPool.execute("DETACH %s".formatted(CATALOG));

        // Clean up workspace directory
        if (workspace != null) {
            try {
                deleteDirectory(new java.io.File(workspace));
            } catch (Exception e) {
                System.err.println("Error deleting workspace: " + e.getMessage());
            }
        }
    }

    private static void deleteDirectory(java.io.File directory) throws IOException {
        if (directory == null || !directory.exists()) {
            return;
        }

        if (directory.isDirectory()) {
            java.io.File[] files = directory.listFiles();
            if (files != null) {
                for (java.io.File file : files) {
                    deleteDirectory(file);
                }
            }
        }

        if (!directory.delete()) {
            throw new IOException("Failed to delete: " + directory.getAbsolutePath());
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testSimpleReadDucklakeQuery() throws Exception {
        try ( var serverClient = createRestrictedServerClient( "admin" )) {
            try (var splittableClient = splittableAdminClientForTable(serverClient.location(), serverClient.clientAllocator(), PARTITIONED_TABLE)) {
                var flightCallHeaders = new FlightCallHeaders();
                flightCallHeaders.insert(Headers.HEADER_SPLIT_SIZE, "1");
                flightCallHeaders.insert(Headers.HEADER_TABLE, PARTITIONED_TABLE);
                var flightInfo = splittableClient.execute("select * from %s where key = 'k51'".formatted(PARTITIONED_TABLE),
                        new HeaderCallOption(flightCallHeaders));
                assertEquals(2, flightInfo.getEndpoints().size());
                var size = 0;
                for (var endpoint : flightInfo.getEndpoints()) {
                    try (final FlightStream stream = splittableClient.getStream(endpoint.getTicket(), new HeaderCallOption(flightCallHeaders))) {
                        while (stream.next()) {
                            size+=stream.getRoot().getRowCount();
                        }
                    }
                }
                assertEquals(2, size);
            }
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testFilterReadDucklakeQuery() throws Exception {
        try ( var serverClient = createRestrictedServerClient(  "admin" )) {
            try (var splittableClient = splittableAdminClientForTable(serverClient.location(), serverClient.clientAllocator(), PARTITIONED_TABLE)) {
                var flightCallHeaders = new FlightCallHeaders();
                flightCallHeaders.insert(Headers.HEADER_SPLIT_SIZE, "1");
                flightCallHeaders.insert(Headers.HEADER_TABLE, PARTITIONED_TABLE);
                flightCallHeaders.insert(Headers.HEADER_FILTER, "key = 'k51'");
                var flightInfo = splittableClient.execute("select * from %s".formatted(PARTITIONED_TABLE),
                        new HeaderCallOption(flightCallHeaders));
                assertEquals(2, flightInfo.getEndpoints().size());
                var size = 0;
                for (var endpoint : flightInfo.getEndpoints()) {
                    try (final FlightStream stream = splittableClient.getStream(endpoint.getTicket(), new HeaderCallOption(flightCallHeaders))) {
                        while (stream.next()) {
                            size+=stream.getRoot().getRowCount();
                        }
                    }
                }
                assertEquals(2, size);
            }
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testNoAccessReadDucklakeQuery() throws Exception {
        try ( var serverClient = createRestrictedServerClient(  "admin" )) {
            try (var splittableClient = splittableAdminClientForTable(serverClient.location(), serverClient.clientAllocator(), PARTITIONED_TABLE)) {
                var flightCallHeaders = new FlightCallHeaders();
                flightCallHeaders.insert(Headers.HEADER_SPLIT_SIZE, "1");
                assertThrows(FlightRuntimeException.class, () -> splittableClient.execute("select * from %s".formatted(NON_PARTITIONED_TABLE)));
            }
        }
    }

    private FlightSqlClient splittableAdminClientForTable(Location location, BufferAllocator allocator, String table) {
        return new FlightSqlClient(FlightClient.builder(allocator, location)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER,
                        PASSWORD,
                        Map.of(Headers.HEADER_DATABASE, CATALOG,
                                Headers.HEADER_SCHEMA, SCHEMA,
                                Headers.HEADER_TABLE, table)))
                .build());
    }


    private ServerClient createRestrictedServerClient(String user) throws IOException, NoSuchAlgorithmException {
        final Location serverLocation = FlightTestUtils.findNextLocation();
        var testUtil = FlightTestUtils.createForDatabaseSchema(user, "",  CATALOG, SCHEMA);
        return testUtil.createRestrictedServerClient(serverLocation, Map.of(Headers.HEADER_SPLIT_SIZE, "1"));
    }
}
