package io.dazzleduck.sql.commons;


import io.dazzleduck.sql.commons.hive.HivePartitionPruning;
import io.dazzleduck.sql.commons.util.TestUtils;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class MappedReaderTest {

    private static final String TEST_TABLE_DDL = "x INT, y FLOAT, s VARCHAR";
    static List<String> sourceColumns = List.of("x");
    static Field targetField = new Field("x_x_7", FieldType.notNullable(new ArrowType.Int(32, true)), null);
    MappedReader.Function function = new MappedReader.Function() {
        @Override
        public void apply(List<FieldVector> source, FieldVector target) {
            IntVector sourceIntVector = (IntVector) source.get(0);
            IntVector targetIntVector = (IntVector) target;
            for (int i =0 ; i < sourceIntVector.getValueCount(); i ++){
                targetIntVector.set(i , sourceIntVector.get(i) * 7);
            }
        }
    };

    @Test
    public void testMapReader() throws SQLException, IOException {
        String sql = "select 1 as count";
        MappedReader.Function function = (sources, target) -> {
            IntVector resultVector = (IntVector) target;
            IntVector f = (IntVector) sources.get(0);
            for (int i = 0; i < f.getValueCount(); i++) {
                resultVector.set(i, f.get(i) + 1);
            }
        };
        Field f = new Field("count_p", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        TestUtils.testMappedReader(sql, function, List.of("count"), f,
                "asdf", "select * from asdf", "select 1 as count, 2 as count_p");
    }

    @Test
    public void testMapReaderArray() throws SQLException, IOException {
        String sql = "select 10 as size, 'abc' as filename, 99 as last_modified, ['a%20x', 'b'] as partitions  ";
        Field child = new Field("children", FieldType.notNullable(new ArrowType.Utf8()), null);
        List<String> sourceCol = List.of("partitions");
        Field targetField = new Field("unescaped_partitions", FieldType.notNullable(new ArrowType.List()), List.of(child));
        String[][] partitions = {
                {"a", "string"},
                {"b", "string"}
        };
        String temptableName = "tt";
        String testSql = HivePartitionPruning.getPartitionSql(partitions, temptableName, "true");
        TestUtils.testMappedReader(sql, HivePartitionPruning.UNESCAPE_FN, sourceCol, targetField, temptableName,
                testSql, "select 'abc' as filename, 10 as size,  99 as last_modified, 'a x' as a, 'b' as b");
    }

    @Test
    public void testReader() throws SQLException, IOException {
        String sql = "select 1 as count";
        try (DuckDBConnection connection = ConnectionPool.getConnection();
             RootAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, 10)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                root.contentToTSVString();
            }
        }
    }

    @Test
    public void testMappedReader() throws SQLException, IOException {
        String tableName = "mapped_reader";
        String tempTableName = "temp_mapped_reader";
        createTable(tableName);
        addDataToTable(tableName, 0, 10000);
        TestUtils.testMappedReader(String.format("select * from %s", tableName),
                function, sourceColumns, targetField, tempTableName,
                String.format("select * from %s", tempTableName),
                String.format("select x, y, s, x*7 as x_x_7 from %s", tableName));
    }

    private void createTable(String tableName) throws SQLException {
        try(Connection connection = ConnectionPool.getConnection()) {
            try (var stmt = connection.createStatement()) {
                stmt.execute(String.format("CREATE TABLE %s (%s)", tableName, TEST_TABLE_DDL));
            }
        }
    }

    private void addDataToTable(String tableName, int start, int end) throws SQLException {
        try(DuckDBConnection connection = ConnectionPool.getConnection()) {
            try ( DuckDBAppender appender = connection.createAppender(DuckDBConnection.DEFAULT_SCHEMA, tableName)) {
                for(int i = start ;i < end; i ++) {
                    appender.beginRow();
                    appender.append(i);
                    appender.append((float) i);
                    appender.append("hello- + " + i);
                    appender.endRow();
                }
            }
        }
    }
}
