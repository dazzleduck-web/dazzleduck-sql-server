package io.dazzleduck.sql.commons;

import io.dazzleduck.sql.commons.util.TestUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ConnectionPoolTest {

    @Test
    public void testPreConnectionSql() throws SQLException, IOException, InterruptedException {
        String tempLocation = newTempDir();
        try (Connection c = ConnectionPool.getConnection()) {
            ConnectionPool.execute(c, String.format("ATTACH '%s/file1.db' AS db1", tempLocation));
            ConnectionPool.execute(c, String.format("ATTACH '%s/file2.db' AS db2", tempLocation));
            ConnectionPool.execute(c, "use db1");
            ConnectionPool.execute(c, "create table t(id int)");
        }
        try {
            // This should fail
            test();
            throw new AssertionError("it should have failed");
        } catch (RuntimeException e) {
            // ignore the exception
        }
        ConnectionPool.addPreGetConnectionStatement("use db1");
        Thread.sleep(10);
        // This should pass now
        test();
        ConnectionPool.removePreGetConnectionStatement("use db1");
        try {
            // This should fail again
            test();
            throw new AssertionError("it should have failed");
        } catch (RuntimeException e) {
            // ignore the exception
        }
    }

    private void test() {
        try {
            TestUtils.isEqual("select 't' as name", "select * from (show tables)");
        } catch (SQLException | IOException | AssertionError e) {
            throw new RuntimeException(e);
        }
    }

    private static String newTempDir() throws IOException {
        Path tempDir = Files.createTempDirectory("duckdb-sql-commons-");
        return tempDir.toString();
    }

    @Test
    public void testArrowReader() throws SQLException, IOException {
        try(DuckDBConnection connection = ConnectionPool.getConnection();
            BufferAllocator allocator = new RootAllocator();
        ArrowReader reader = ConnectionPool.getReader(connection, allocator, "select * from generate_series(10)", 100)) {
            while (reader.loadNextBatch()){
                System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
            }
        }
    }

    @Test
    public void testBulkIngestionWithPartition() throws IOException, SQLException {
        String tempDir = newTempDir();
        String sql = "select generate_series, generate_series a from generate_series(10)";
        var filename = tempDir + "/bulk";
        try (DuckDBConnection connection = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, 1000)) {
            ConnectionPool.bulkIngestToFile(reader, allocator, filename, List.of("a"), "parquet", "generate_series + 1 as p1");
        }
        TestUtils.isEqual("select generate_series, generate_series a, generate_series + 1 as p1 from generate_series(10) order by generate_series",
                "select generate_series, a, p1 from read_parquet('%s/*/*.parquet') order by generate_series".formatted(filename));
    }

    @Test
    public void testBulkIngestionNoPartition() throws IOException, SQLException {
        String tempDir = newTempDir();
        String sql = "select generate_series, generate_series a from generate_series(10)";
        try(DuckDBConnection connection = ConnectionPool.getConnection();
            BufferAllocator allocator = new RootAllocator();
            ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, 1000)){
            ConnectionPool.bulkIngestToFile(reader, allocator, tempDir + "/bulk", List.of(), "parquet", null);
        }
    }

    @Test
    public void testCollectAll() throws SQLException, NoSuchMethodException {
        record LongAndString( String s, Long l){};
        String sql = "select 's' as s, cast(1 as bigint) as l";
        try( var c = ConnectionPool.getConnection()) {
            var it = ConnectionPool.collectAll(c, sql, LongAndString.class);
            it.forEach( i -> {
                Assertions.assertEquals(new LongAndString( "s", 1L), i);
            });
        }
    }

    @Test
    public void testArrayCollection() throws SQLException {
        try (var c = ConnectionPool.getConnection()) {
            Iterable<Object[]> longArray = (Iterable<Object[]>) ConnectionPool.collectFirstColumn(c, "select [1, 2]", Object.class.arrayType());
            for (var a : longArray) {
                var res = new Object[2];
                res[0] = 1;
                res[1] = 2;
                Assertions.assertArrayEquals(res, a);
            }
        }
    }
}
