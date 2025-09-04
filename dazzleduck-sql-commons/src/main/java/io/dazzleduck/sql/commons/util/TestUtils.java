package io.dazzleduck.sql.commons.util;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.MappedReader;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.duckdb.DuckDBConnection;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

public class TestUtils {

    private static final String IS_EQUAL = "WITH E AS (%s), " +
            " R AS (%s)," +
            " C AS (SELECT * FROM E EXCEPT SELECT * FROM R), " +
            " D AS (SELECT * FROM R EXCEPT SELECT * FROM E) " +
            " SELECT * FROM (SELECT 'L->' as s, * FROM C UNION SELECT 'R->' as s, * FROM D) ORDER BY s";
    ;

    public static void isEqual(String expected, String result) throws SQLException, IOException {
        try (DuckDBConnection connection = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator();) {
            isEqual(connection, allocator, expected, result);
        }
    }

    public static void isEqual(String expected, BufferAllocator allocator, ArrowReader reader) throws SQLException, IOException {
        String tempTable = "_temp_" + System.currentTimeMillis();
        String matTable = String.format("%s_mat", tempTable);
        try( DuckDBConnection connection = ConnectionPool.getConnection();
            final ArrowArrayStream arrow_array_stream = ArrowArrayStream.allocateNew(allocator)) {
            Data.exportArrayStream(allocator, reader, arrow_array_stream);
            connection.registerArrowStream(tempTable, arrow_array_stream);
            ConnectionPool.execute(connection, String.format("CREATE TABLE %s AS SELECT * FROM %s", matTable, tempTable));
            isEqual(connection, allocator, expected, "select * from " + matTable);
            ConnectionPool.execute(connection, String.format("DROP TABLE %s", matTable));
        }
    }

    public static void isEqual(DuckDBConnection connection, BufferAllocator allocator,
                               String expected, String result) throws SQLException, IOException {
        String sql = String.format(IS_EQUAL, expected, result);

        try (ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, 100)) {
            StringBuilder stringBuilder = new StringBuilder();
            boolean failed = false;
            while (reader.loadNextBatch()) {
                failed = true;
                stringBuilder.append(reader.getVectorSchemaRoot().contentToTSVString());
            }
            if (failed) {
                throw new AssertionError(stringBuilder.toString());
            }
        }
    }

    public static void testMappedReader(String sql,
                                        MappedReader.Function function,
                                        List<String> sourceCol,
                                        Field targetField,
                                        String tempTableName,
                                        String testSql,
                                        String expectedSql) throws SQLException, IOException {
        try (DuckDBConnection readConnection = ConnectionPool.getConnection();
             DuckDBConnection writeConnection = ConnectionPool.getConnection();
             RootAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(readConnection, allocator, sql, 10);
             Closeable ignored = ConnectionPool.createTempTableWithMap(writeConnection, allocator, reader, function, sourceCol, targetField, tempTableName)) {
            // the issue with temp tables for now are that they can be only be queries once since it reads the reader once.
            // It does not work even for queries such as `select * from a union select * from a` because it will require reader to be read twice.
            // Therefor we need to store the data of reader into materialized view so that it can be used multiple time
            String materializedTable = tempTableName + "_mat";
            ConnectionPool.execute(writeConnection, String.format("CREATE TABLE %s AS SELECT * FROM %s", materializedTable, tempTableName));
            TestUtils.isEqual(writeConnection, allocator, testSql.replaceAll(tempTableName, materializedTable), expectedSql);
        }
    }

    public interface ConsumerWithException<T> {
        void accept(T t) throws Exception;
    }
    public static void withTempDir(ConsumerWithException<String> consumer) throws Exception {
        var tempDirectory = Files.createTempDirectory("myTempDirPrefix");
        consumer.accept(tempDirectory.toString());
    }
}
