package io.dazzleduck.sql.commons;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDriver;
import org.duckdb.DuckDBResultSet;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public enum ConnectionPool {
    INSTANCE;

    private static final String DUCKDB_PROPERTY_FILENAME = "duckdb.properties";
    private final DuckDBConnection connection;

    private final ArrayList<String> preGetConnectionStatements = new ArrayList<>();

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    ConnectionPool() {
        try {
            final Properties properties = loadProperties();
            if (!properties.contains(DuckDBDriver.JDBC_STREAM_RESULTS)) {
                properties.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, String.valueOf(true));
            }
            this.connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:", properties);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param connection
     * @param sql sql to be executed
     * @param tClass class of the return object
     * @return fist value of the result set
     * @param <T>
     */
    public static <T> T collectFirst(Connection connection, String sql, Class<T> tClass) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            try (ResultSet resultSet = statement.getResultSet()) {
                resultSet.next();
                return resultSet.getObject(1, tClass);
            }
            catch (SQLException e ){
                throw new RuntimeException("Error collecting result set for sql " + sql, e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error running sql" + sql, e);
        }
    }

    public static <T> Iterable<T> collectFirstColumn(Connection connection, String sql, Class<T> tClass) {
        return collectAll(connection, sql, rs -> rs.getObject(1, tClass), tClass);
    }

    public static <T> Iterable<T> collectAll(Connection connection, String sql, Extractor<T> extractor, Class<T> tClass) {
        List<T> result = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            try (ResultSet resultSet = statement.getResultSet()) {
                while (resultSet.next()) {
                    result.add(extractor.apply(resultSet));
                }
            }
            catch (SQLException e ){
                throw new RuntimeException("Error collecting result set for sql " + sql, e);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Error running sql" + sql, e);
        }
    }

    public static <R extends Record> Iterable<R> collectAll(Connection connection, String sql, Class<R> rClass) throws NoSuchMethodException {
        final var constructor = getCanonicalConstructor(rClass);
        return collectAll(connection, sql, rs -> {
            RecordComponent[] rc  = rClass.getRecordComponents();
            Object[] read = new Object[rc.length];
            for(int i = 0; i <rc.length ; i++  ) {
                var type  = rc[i].getType();
                read[i] = rs.getObject(i + 1 , type);
            }
            return constructor.newInstance(read);
            }, rClass);
    }

    static <T extends Record> Constructor<T> getCanonicalConstructor(Class<T> cls)
            throws NoSuchMethodException {
        Class<?>[] paramTypes =
                Arrays.stream(cls.getRecordComponents())
                        .map(RecordComponent::getType)
                        .toArray(Class<?>[]::new);
        return cls.getDeclaredConstructor(paramTypes);
    }


    /**
     *
     * @param sql
     * @param tClass
     * @return
     * @param <T>
     */
    public static <T> T collectFirst(String sql, Class<T> tClass) throws SQLException {
        try (DuckDBConnection connection = getConnection()) {
            return collectFirst(connection, sql, tClass);
        }
    }

    /**
     *
     * @param sql
     * Used for debugging to print the output of the sql
     */
    public static void printResult(String sql) {
        try (Connection connection = getConnection();
             BufferAllocator rootAllocator = new RootAllocator();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
            try (DuckDBResultSet resultSet = (DuckDBResultSet) statement.getResultSet();
                 ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(rootAllocator, 1000)) {
                while (reader.loadNextBatch()){
                    System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param connection
     * @param allocator
     * @param sql
     * Used for debugging to print the output of the sql
     */
    public static void printResult(Connection connection, BufferAllocator allocator, String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            try (DuckDBResultSet resultSet = (DuckDBResultSet) statement.getResultSet();
                 ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(allocator, 1000)) {
                while (reader.loadNextBatch()){
                    System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param connection
     * @param allocator
     * @param reader
     * @param function
     * @param sourceColumns
     * @param targetField
     * @param tableName
     * @return
     * @throws IOException
     *
     */
    public static Closeable createTempTableWithMap(DuckDBConnection connection,
                                                   BufferAllocator allocator,
                                                   ArrowReader reader,
                                                   MappedReader.Function function,
                                                   List<String> sourceColumns,
                                                   Field targetField,
                                                   String tableName) throws IOException {
        ArrowReader mappedReader = new MappedReader(allocator.newChildAllocator("mmmm", 0, Long.MAX_VALUE), reader, function, sourceColumns,
                targetField);
        final ArrowArrayStream arrow_array_stream = ArrowArrayStream.allocateNew(allocator);
        Data.exportArrayStream(allocator, mappedReader, arrow_array_stream);
        connection.registerArrowStream(tableName, arrow_array_stream);
        return () -> {
            try {
                AutoCloseables.close(mappedReader, arrow_array_stream);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    /**
     *
     * @param connection
     * @param sql
     * @return
     */
    public static boolean execute(Connection connection, String sql)  {
        try(Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Error running sql :" + sql,  e);
        }
    }

    /**
     *
     * @param sql
     * @return
     */
    public static boolean execute(String sql)  {
        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Error running sql :" + sql,  e);
        }
    }

    /**
     *
     * @param sqls
     * @return
     */
    public static int[] executeBatch(String[] sqls) {
        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            for(String sql : sqls) {
                statement.addBatch(sql);
            }
            return statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Error running sqls :",  e);
        }
    }

    public static int[] executeBatch(Connection connection, String[] sqls) {
        try(Statement statement = connection.createStatement()) {
            for(String sql : sqls) {
                statement.addBatch(sql);
            }
            return statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Error running sqls :",  e);
        }
    }

    /**
     *
     * @param connection
     * @param allocator
     * @param sql
     * @param batchSize
     * @return
     */
    public static ArrowReader getReader(DuckDBConnection connection,
                                        BufferAllocator allocator,
                                        String sql,
                                        int batchSize)  throws SQLException {

        final Statement statement = connection.createStatement();
        statement.execute(sql);
        return new ArrowReader(allocator) {
            final Statement _statement = statement;
            final DuckDBResultSet resultSet = (DuckDBResultSet) statement.getResultSet();
            private final ArrowReader internal = (ArrowReader) resultSet.arrowExportStream(allocator, batchSize);

            @Override
            public boolean loadNextBatch() throws IOException {
                return internal.loadNextBatch();
            }

            @Override
            public long bytesRead() {
                return internal.bytesRead();
            }

            @Override
            protected void closeReadSource() throws IOException {
                try {
                    internal.close();
                } catch (NullPointerException e) {
                    // There are some scenarios where Duckdb itself closes the reader
                    // try to close it with closeReadSources
                    internal.close(false);
                }
                try {
                    resultSet.close();
                    _statement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            protected Schema readSchema() throws IOException {
                return internal.getVectorSchemaRoot().getSchema();
            }

            @Override
            public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
                return internal.getVectorSchemaRoot();
            }
        };
    }

    public static DuckDBConnection getConnection()  {
        return INSTANCE.getConnectionInternal();
    }

    /**
     *
     * @param sqls Sql which will be executed on connection before connection is returned.
     *            This is generally used to set parameters as well as database and schema
     * @return
     */
    public static DuckDBConnection getConnection(String[] sqls) {
        DuckDBConnection connection = getConnection();
        for(String sql : sqls) {
            executeBatch(connection, sqls);
        }
        return connection;
    }

    /**
     *
     * @param reader
     * @param allocator
     * @param path
     * @param partitionColumns
     * @param format
     * @throws SQLException
     */
    public static void bulkIngestToFile(ArrowReader reader, BufferAllocator allocator, String path,
                                        List<String> partitionColumns, String format) throws SQLException {
        bulkIngestToFile(reader, allocator, path, partitionColumns, format, null);
    }

    /**
     *
     * @param reader
     * @param allocator
     * @param path
     * @param partitionColumns
     * @param format
     * @throws SQLException
     */
    public static void bulkIngestToFile(ArrowReader reader, BufferAllocator allocator, String path,
                                        List<String> partitionColumns, String format, String transformations) throws SQLException {
        try (var conn = getConnection();
             final ArrowArrayStream arrow_array_stream = ArrowArrayStream.allocateNew(allocator)) {
            Data.exportArrayStream(allocator, reader, arrow_array_stream);
            String partitionByQuery;
            if (!partitionColumns.isEmpty()) {
                partitionByQuery = String.format(", PARTITION_BY (%s)", String.join(",", partitionColumns));
            } else {
                partitionByQuery = "";
            }
            String tempTableName = "_tmp_" + System.currentTimeMillis();
            String innerSql;
            if(transformations == null) {
                innerSql =  "%s".formatted(tempTableName);
            } else {
                innerSql = "( FROM %s SELECT *, %s)".formatted(tempTableName, transformations);
            }
            String sql = String.format("COPY %s TO '%s' (FORMAT %s %s)", innerSql, path, format, partitionByQuery);
            conn.registerArrowStream(tempTableName, arrow_array_stream);
            ConnectionPool.execute(conn, sql);
        }
    }

    private DuckDBConnection getConnectionInternal() {
        try {
            DuckDBConnection result = (DuckDBConnection) connection.duplicate();
            Statement statement = result.createStatement();
            for (String sql : preGetConnectionStatements) {
                statement.execute(sql);
            }
            return result;
        } catch (SQLException e ){
            throw new RuntimeException("Error creating connection " , e);
        }
    }

    /**
     *
     * @param sql add a sql which will be executed before returning the connection by the method getConnection()
     *            This method should be invoked at the beginning of the main function.
     *            Typical use case will be set specific catalog/schema.
     */
    public static void addPreGetConnectionStatement(String sql) {
        INSTANCE.preGetConnectionStatements.add(sql);
    }

    /**
     *
     * @param sql removes a sql which are to be executed when getConnection() is invoked.
     */
    public static void removePreGetConnectionStatement(String sql) {
        INSTANCE.preGetConnectionStatements.remove(sql);
    }


    private static Properties loadProperties() {
        Properties properties = new Properties();

        // Try-with-resources to ensure InputStream is closed
        try (InputStream input = ConnectionPool.class.getClassLoader().getResourceAsStream(DUCKDB_PROPERTY_FILENAME)) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }
}
