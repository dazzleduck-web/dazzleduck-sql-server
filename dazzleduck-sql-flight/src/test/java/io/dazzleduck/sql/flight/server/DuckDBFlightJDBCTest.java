package io.dazzleduck.sql.flight.server;


import io.dazzleduck.sql.commons.ConnectionPool;
import org.apache.arrow.driver.jdbc.ArrowFlightConnection;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.flight.FlightServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class DuckDBFlightJDBCTest {

    private static final String startupQuery = "\"CREATE TABLE a  (key string); INSERT INTO a VALUES('k');\\n-- This is a single-line comment \\nINSERT INTO a VALUES('k2');/* this  is comment */\\n\\nINSERT INTO a VALUES('k3')\"";
    private static final String LONG_RUNNING_QUERY = "with t as " +
            "(select len(split(concat('abcdefghijklmnopqrstuvwxyz:', generate_series), ':')) as len  from generate_series(1, 1000000000) )" +
            " select count(*) from t where len = 10";
    private static FlightServer flightServer ;
    private static final int port = 55556;
    static String url = String.format("jdbc:arrow-flight-sql://localhost:%s/?database=memory&useEncryption=0&user=admin&password=admin", port);
    @BeforeAll
    public static void beforeAll() throws IOException, NoSuchAlgorithmException {
        String[] args = {"--conf", "port=" + port, "--conf", "useEncryption=false", "--conf", "startupFile=" + startupQuery};
        flightServer = Main.createServer(args);
        Thread severThread = new Thread(() -> {
            try {
                flightServer.start();
                System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
                flightServer.awaitTermination();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        severThread.start();
    }

    @AfterAll
    public static void afterAll() throws InterruptedException {
        flightServer.close();
    }



    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url);
    }

    @Test
    public void executeMulti() {
        ConnectionPool.printResult("select * from a");
    }

    @Test
    public void testExecuteQuery() throws SQLException {
        try(Connection connection = getConnection();
            Statement  st = connection.createStatement()) {
            st.executeQuery("select 1");
            try ( ResultSet resultSet = st.getResultSet()) {
                while (resultSet.next()){
                    resultSet.getInt(1);
                }
            }
        }
    }

    @Test
    public void cancelBeforeExecute() throws SQLException {
        try(Connection connection = getConnection();
            Statement  st = connection.createStatement()) {
            st.cancel();
        }
    }

    @Test
    public void testExecuteQueryWithUserPassword() throws SQLException {
        var numIterator = 1;
        try(Connection connection = getConnection()) {
            for (int i = 0 ; i < numIterator; i ++) {
                try( Statement  st = connection.createStatement()) {
                    var res2 = st.execute("select 1");
                    try (ResultSet resultSet = st.getResultSet()) {
                        while (resultSet.next()) {
                            resultSet.getInt(1);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testExecuteDDLWithUserPassword() throws SQLException {
        var numIterator = 1;
        try (Connection connection = getConnection()) {
            for (int i = 0; i < numIterator; i++) {
                try (Statement st = connection.createStatement()) {
                    var schema = "test_schema_" + System.currentTimeMillis();
                    var res2 = st.execute("create schema " + schema);
                    assertFalse(res2);
                    var res3 = st.execute("DROP SCHEMA " + schema);
                    assertFalse(res3);
                }
            }
        }
    }

    @Test
    public void setAndGetParam() throws SQLException {
        try (Connection connection = getConnection()) {
            try (Statement statement = connection.createStatement()) {
                var res = statement.execute("set variable x = 5");
                assertFalse(res);
            }

            try (Statement statement = connection.createStatement()) {
                var res = statement.execute("select getvariable('x')");
                try (var rs = statement.getResultSet()) {
                    while (rs.next()) {
                        var o = rs.getObject(1);
                        assertNull(o);
                    }
                }
                assertTrue(res);
            }
        }
    }

    @Test
    public void testExecuteQueryWithClientHandler() throws Exception {
        var connection = getConnection(url, new Properties());
        var handler = getPrivateClientHandlerFromConnection(connection);
        for(int i = 0 ; i < 3 ; i ++) {
            var info = handler.getInfo("select 1");
            assertNotNull(info);
        }
    }

    @Test
    @Disabled
    public void cancelAfterExecute() throws SQLException {
        try(Connection connection = getConnection();
            Statement  st = connection.createStatement()) {
            Thread cancelThread = new Thread(() -> {
                try {
                    // Cancel after 1 second.
                    Thread.sleep(1000);
                    st.cancel();
                } catch (SQLException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            cancelThread.start();
            st.execute(LONG_RUNNING_QUERY);
            st.cancel();
        }
    }


    @Test
    public void testSetUnSet() throws SQLException {
        try (Connection connection = getConnection()) {

            try (Statement st = connection.createStatement()) {
                var h = st.executeUpdate("set pivot_limit=9999");
                assertEquals(0, h);
            }
            try (var st2 = connection.createStatement()) {
                var hasResultSet = st2.execute("SELECT cast(current_setting('pivot_limit') as decimal) AS memlimit");
                //var hasResultSet = st2.execute("select 1");
                System.out.println(hasResultSet);
                try (ResultSet set = st2.getResultSet()) {
                    while (set.next()) {
                        System.out.println(set.getObject(1, Long.class));
                    }
                }
            }
        }
    }

    @Test
    public void testMetadata() throws SQLException {
        try(Connection connection = getConnection()) {
            var databaseMetadata = connection.getMetaData();
            assertFalse(databaseMetadata.supportsTransactions());

            try (var rs = connection.getMetaData().getTableTypes()) {
                while (rs.next()) {
                    System.out.printf("%s\n",
                            rs.getString(1));
                }
            }

            try (var rs = databaseMetadata.getSchemas()) {
                while(rs.next()) {
                    System.out.printf("%s, %s\n",
                            rs.getString(1),
                            rs.getString(2));
                }
            }

            //databaseMetadata.getCatalogs();
            try (var rs = databaseMetadata.getCatalogs()) {
                while(rs.next()) {
                    System.out.printf("%s\n",
                            rs.getString(1));
                }
            }

            try( var rs = databaseMetadata.getTables(null, null, null, null)){
                while(rs.next()) {
                    System.out.printf("%s",
                            rs.getString(1));
                }
            }
        }
    }

    @Test
    void testBatchInsertionWithPreparedStatement() throws SQLException {
        var table = "test_table" + System.currentTimeMillis();
        try (var connection = getConnection();
             var st = connection.createStatement()) {
            st.execute(String.format("CREATE TABLE %s(a int)", table));
            var rowsToInsert = 10;
            try (var ps = connection.prepareStatement(String.format("INSERT INTO  %s VALUES(?)", table))) {
                for (int i = 0; i < rowsToInsert; i++) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            assertEquals(rowsToInsert, ConnectionPool.collectFirst(String.format("select count(*) from %s", table), Long.class));
        } finally {
            ConnectionPool.execute(String.format("DROP TABLE IF EXISTS %s", table));
        }
    }

    @Test
    void testInsertionWithPreparedStatement() throws SQLException {
        var table = "test_table" + System.currentTimeMillis();
        var valueToInsert = 10;
        try (var connection = getConnection();
             var st = connection.createStatement()) {
            st.execute(String.format("CREATE TABLE %s(a int)", table));
            try (var ps = connection.prepareStatement(String.format("INSERT INTO  %s VALUES(?)", table))) {
                ps.setInt(1, valueToInsert);
                ps.executeUpdate();
            }
            assertEquals(valueToInsert, ConnectionPool.collectFirst(String.format("select * from %s", table), Integer.class));
        } finally {
            ConnectionPool.execute(String.format("DROP TABLE IF EXISTS %s", table));
        }
    }

    @Test
    void testUpdate() throws SQLException {
        var table = "test_table" + System.currentTimeMillis();
        try (var connection = getConnection();
             var st = connection.createStatement()) {
            st.execute(String.format("CREATE TABLE %s(a int)", table));
            try (var ps = connection.prepareStatement(String.format("INSERT INTO  %s VALUES(?)", table))) {
                ps.setInt(1, 10);
                ps.executeUpdate();
            }
            try (var ps = connection.prepareStatement(String.format("UPDATE %s SET a = ? where a = 10", table))) {
                ps.setInt(1, 20);
                ps.executeUpdate();
            }
        } finally {
            ConnectionPool.execute(String.format("DROP TABLE IF EXISTS %s", table));
        }
    }


    private static ArrowFlightConnection getConnection(String url, Properties properties) throws SQLException {
        return new ArrowFlightJdbcDriver().connect(url, properties);
    }
    private static ArrowFlightSqlClientHandler getPrivateClientHandlerFromConnection(ArrowFlightConnection connection) throws NoSuchFieldException, IllegalAccessException {
        // The class and field name are the same
        Field clientHandler = ArrowFlightConnection.class.getDeclaredField("clientHandler");
        clientHandler.setAccessible(true);
        // Cast to the real class
        return (ArrowFlightSqlClientHandler) clientHandler.get(connection);
    }
}