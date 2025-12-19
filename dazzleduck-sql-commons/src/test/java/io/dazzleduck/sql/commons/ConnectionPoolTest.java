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
        var sql = "select * from generate_series(10)";
        try(DuckDBConnection connection = ConnectionPool.getConnection();
            BufferAllocator allocator = new RootAllocator();
            ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, 100)) {
            TestUtils.isEqual(sql, allocator, reader);
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
    public void testCollectAll() throws SQLException {
        record LongAndString( String s, long l, long[] longArray, Long nullLong,  long[] nullLongArray, String[] stringArray, String[] nullStringArray){};
        String sql = "select 's' as s, cast(1 as bigint) as l, [cast(1 as bigint)], null, null, ['one', null, 'two'], null";
        try( var c = ConnectionPool.getConnection()) {
            var it = ConnectionPool.collectAll(c, sql, LongAndString.class);
            it.forEach( i -> {
                Assertions.assertEquals("s", i.s());
                Assertions.assertEquals(1L, i.l());
                Assertions.assertArrayEquals(new long[]{1L}, i.longArray());
                Assertions.assertNull(i.nullLong());
                Assertions.assertNull(i.nullLongArray());
                Assertions.assertArrayEquals(new String[]{"one", null, "two"}, i.stringArray);
                Assertions.assertNull(i.nullStringArray());
            });
        }
    }

    @Test
    public void testArrayCollectionInt() throws SQLException {
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

    @Test
    public void testArrayCollectionVarChar() throws SQLException {
        try (var c = ConnectionPool.getConnection()) {
            Iterable<Object[]> varcharArray = (Iterable<Object[]>) ConnectionPool.collectFirstColumn(c, "select ['one', 'two']", Object.class.arrayType());
            for (var a : varcharArray) {
                var res = new Object[2];
                res[0] = "one";
                res[1] = "two";
                Assertions.assertArrayEquals(res, a);
            }
        }
    }

    @Test
    public void testExecuteBatchInTxn() throws SQLException, IOException {
        String[] sqls = {
                "CREATE TABLE batch_in_txn(key int)",
                "INSERT INTO batch_in_txn VALUES (1), (2)"
        };
        ConnectionPool.executeBatchInTxn(sqls);
        TestUtils.isEqual("select 2", "select count(*) from batch_in_txn");
    }

    @Test
    public void testExecuteBatchInTxnFailure() throws SQLException, IOException {
        String[] sqls = {
                "CREATE TABLE batch_in_txn(key int)",
                // Incorrect statement for txn rollback
                "INSERT INTO batch_in_txn VALUES (1,2), (2,  3)"
        };
        try {
            ConnectionPool.executeBatchInTxn(sqls);
        } catch (Exception e) {
            // Expected
            // ignore it
        }
        Assertions.assertThrowsExactly(RuntimeSqlException.class,
                () -> ConnectionPool.execute("desc batch_in_txn"));
    }
}
