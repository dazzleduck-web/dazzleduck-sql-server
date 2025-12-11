package io.dazzleduck.sql.search;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexerTest {

    @Test
    public void testFunction() throws IOException, SQLException {
        TokenizationFunction tokenizationFunction = input -> input.split(" ");
        var f1 = new Field("f1", FieldType.notNullable(new ArrowType.Utf8()), null);
        var childArray1 = new Field("f1", FieldType.notNullable(new ArrowType.List()), List.of(f1));
        var f2 = new Field("f2", FieldType.notNullable(new ArrowType.Utf8()), null);
        var childArray2 = new Field("f2", FieldType.notNullable(new ArrowType.List()), List.of(f2));
        var structFieldType = new Field("tokens", FieldType.notNullable(new ArrowType.Struct()), List.of(childArray1, childArray2));
        var mapFunction = Indexer.createMapperFunction(Map.of("f1", tokenizationFunction, "f2", tokenizationFunction));
        TestUtils.testMappedReader("select 'a' as a, 1 as row_num, 'one two three' as f1, 'four five six' as f2", mapFunction, List.of("a", "row_num", "f1", "f2"), structFieldType,
                "asdf", "select * from asdf", "select 'a' as a, 1 as row_num, 'one two three' as f1, 'four five six' as f2, struct_pack(f1:= ['one', 'two', 'three'], f2:= ['four', 'five', 'six'] ) as tokens");
    }

    @Test
    public void testIndexCreationWriteSql() throws SQLException, IOException {
        var fieldToIndex = List.of("f1", "f2");
        try (var connection = ConnectionPool.getConnection()) {
            var inputSql = "create temp view input as select 'd' as filename, 1 as row_num, 'f1' as f1, 'f2' as f2, struct_pack(f1:=['one', 'two'], f2:=['p', 'q']) as tokens, cast('2025-01-01' as timestamp) as time";
            ConnectionPool.execute(connection, inputSql);
            var outputSql = Indexer.constructWriteSql(fieldToIndex, "time","tokens", "input");
            var expected = "select 'd' as filename, unnest(['one', 'p', 'q', 'two']) as token";
            TestUtils.isEqual(connection, new RootAllocator(), expected, "select filename, token from (%s)".formatted(outputSql));
        }
    }

    @Test
    public void testIndexCreation() throws Exception {
        TokenizationFunction tokenizationFunction = input -> input.split(" ");
        TestUtils.withTempDir(dir -> {
            var file1 = "file1.parquet";
            var file2 = "file2.parquet";
            var targetFile = dir + "/target.parquet";
            createFile(Map.of("f1", "one two three", "f2", "p q"), dir, file1);
            createFile(Map.of("f1", "four five six seven", "f2", "r s t u v"), dir, file2);
            var sourceFiles = List.of(file1, file2);
            var fieldToIndex = List.of("f1", "f2");
            Indexer.create(dir, sourceFiles, fieldToIndex, "time", Map.of("f1", tokenizationFunction, "f2", tokenizationFunction), targetFile);
            TestUtils.isEqual("select 14 as distinct_count",
                    "select count(distinct token) as distinct_token from '%s'".formatted(targetFile));
        });
    }

    private void createFile(Map<String, String> input, String prefix, String target) {
        var selectCols = input.entrySet().stream().map(e -> "'%s' AS %s".formatted(e.getValue(), e.getKey())).collect(Collectors.joining(","));
        var t = "%s/%s".formatted(prefix, target);
        var writeSql = "COPY (SELECT %s, cast('2025-04-01' as timestamp) as time) TO '%s' (FORMAT parquet)".formatted(selectCols, t);
        ConnectionPool.execute(writeSql);
    }
}
