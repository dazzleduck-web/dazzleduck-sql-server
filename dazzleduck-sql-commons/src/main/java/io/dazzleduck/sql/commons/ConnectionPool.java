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
import java.util.UUID;

public enum ConnectionPool {
    INSTANCE;

    private static final String DUCKDB_PROPERTY_FILENAME = "duckdb.properties";
    private final DuckDBConnection connection;


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
            if (!properties.containsKey(DuckDBDriver.JDBC_STREAM_RESULTS)) {
                properties.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, String.valueOf(true));
            }
            this.connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:", properties);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static final int DEFAULT_ARROW_BATCH_SIZE = 1000;

    /**
     *
     * @param connection
     * @param sql sql to be executed
     * @param tClass class of the return object
     * @return first value of the result set
     * @param <T>
     */
    public static <T> T collectFirst(Connection connection, String sql, Class<T> tClass) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            try (ResultSet resultSet = statement.getResultSet()) {
                if (!resultSet.next()) {
                    throw new SQLException("Query returned no results: " + sql);
                }
                if(tClass.isArray()) {
                    return (T) resultSet.getArray(1).getArray();
                } else {
                    return resultSet.getObject(1, tClass);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Iterable<T> collectFirstColumn(Connection connection, String sql, Class<T> tClass) {
        if (tClass.isArray()) {
            return collectAll(connection, sql, rs -> {
                Array array = rs.getArray(1);
                return array != null ? (T) array.getArray() : null;
            });
        } else {
            return collectAll(connection, sql, rs -> rs.getObject(1, tClass));
        }
    }

    public static <T> Iterable<T> collectAll(Connection connection, String sql, Extractor<T> extractor) {
        return () -> {
            try {
                Statement statement = connection.createStatement();
                try {
                    statement.execute(sql);
                    ResultSet resultSet = statement.getResultSet();
                    return new ResultSetIterator<>(statement, resultSet, extractor);
                } catch (SQLException e) {
                    try {
                        statement.close();
                    } catch (SQLException closeException) {
                        e.addSuppressed(closeException);
                    }
                    throw new RuntimeException("Error running sql: " + sql, e);
                }
            } catch (SQLException e) {
                throw new RuntimeException("Error running sql: " + sql, e);
            }
        };
    }


    public static <R extends Record> Iterable<R> collectAll(Connection connection, String sql, Class<R> rClass) {

        final Constructor<R> constructor;
        try {
            constructor = getCanonicalConstructor(rClass);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        return collectAll(connection, sql, rs -> {
            RecordComponent[] recordComponents = rClass.getRecordComponents();
            Object[] read = new Object[recordComponents.length];
            for(int i = 0; i < recordComponents.length; i++) {
                var type = recordComponents[i].getType();
                if (type.isArray()) {
                    var a = rs.getArray(i + 1);
                    if (a == null) {
                        read[i] = null;
                    } else {
                        Object[] objArray = (Object[]) a.getArray();
                        Class<?> componentType = type.getComponentType();
                        Object typedArray = java.lang.reflect.Array.newInstance(componentType, objArray.length);
                        for (int j = 0; j < objArray.length; j++) {
                            java.lang.reflect.Array.set(typedArray, j, objArray[j]);
                        }
                        read[i] = typedArray;
                    }
                } else {
                    Object value = rs.getObject(i + 1);
                    if (rs.wasNull()) {
                        read[i] = null;
                    } else {
                        read[i] = value;
                    }
                }
            }
            try {
                return constructor.newInstance(read);
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate record " + rClass.getName(), e);
            }
        });
    }

    private static <T extends Record> Constructor<T> getCanonicalConstructor(Class<T> cls)
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
                 ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(rootAllocator, DEFAULT_ARROW_BATCH_SIZE)) {
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
                 ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(allocator, DEFAULT_ARROW_BATCH_SIZE)) {
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
        ArrowReader mappedReader = new MappedReader(allocator.newChildAllocator("mapped-reader-allocator", 0, Long.MAX_VALUE), reader, function, sourceColumns,
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
            throw new RuntimeSqlException(e);
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
            throw new RuntimeSqlException(e);
        }
    }

    /**
     *
     * @param sqls
     * @return
     */
    public static int[] executeBatch(String[] sqls) {
        try(Connection connection = ConnectionPool.getConnection()) {
            return executeBatch(connection, sqls);
        } catch (SQLException e) {
            throw new RuntimeException("Error running sqls: ", e);
        }
    }

    public static int[] executeBatch(Connection connection, String[] queries) {
        try(Statement statement = connection.createStatement()) {
            for(String sql : queries) {
                statement.addBatch(sql);
            }
            return statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Error running sqls: ", e);
        }
    }

    public static int[] executeBatchInTxn(Connection connection, String[] queries) throws SQLException {
        var oldAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            for (String sql : queries) {
                statement.addBatch(sql);
            }
            var res = statement.executeBatch();
            connection.commit();
            return res;
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(oldAutoCommit);
        }
    }


    public static int[] executeBatchInTxn(String[] queries) throws SQLException {
        try (var connection = ConnectionPool.getConnection()) {
            return executeBatchInTxn(connection, queries);
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
                                        int batchSize) throws SQLException {

        final Statement statement = connection.createStatement();
        try {
            statement.execute(sql);
            final DuckDBResultSet resultSet = (DuckDBResultSet) statement.getResultSet();
            final ArrowReader internal = (ArrowReader) resultSet.arrowExportStream(allocator, batchSize);

            return new ArrowReader(allocator) {
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
                        // DuckDB may close the reader internally in some edge cases
                        // Attempt to close without cleanup if already closed
                        try {
                            internal.close(false);
                        } catch (Exception ignored) {
                            // Reader already closed, nothing to do
                        }
                    }
                    try {
                        resultSet.close();
                        statement.close();
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
        } catch (SQLException e) {
            try {
                statement.close();
            } catch (SQLException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
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
        executeBatch(connection, sqls);
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
     * @param transformations
     * @throws SQLException if there is an error during database operations or file export
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
            return (DuckDBConnection) connection.duplicate();
        } catch (SQLException e ){
            throw new RuntimeException("Error creating connection", e);
        }
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
    private static class ResultSetIterator<T> implements java.util.Iterator<T>, AutoCloseable {
        private final Statement statement;
        private final ResultSet resultSet;
        private final Extractor<T> extractor;
        private Boolean hasNext;
        private boolean closed = false;

        ResultSetIterator(Statement statement, ResultSet resultSet, Extractor<T> extractor) {
            this.statement = statement;
            this.resultSet = resultSet;
            this.extractor = extractor;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (hasNext == null) {
                try {
                    hasNext = resultSet.next();
                    if (!hasNext) {
                        close();
                    }
                } catch (SQLException e) {
                    close();
                    throw new RuntimeException("Error checking for next result", e);
                }
            }
            return hasNext;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                close();
                throw new java.util.NoSuchElementException();
            }
            try {
                T result = extractor.apply(resultSet);
                hasNext = null;
                return result;
            } catch (Exception e) {
                close();
                throw new RuntimeException("Error extracting result", e);
            }
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    // Log but don't throw
                }
                try {
                    statement.close();
                } catch (SQLException e) {
                    // Log but don't throw
                }
            }
        }
    }
}
