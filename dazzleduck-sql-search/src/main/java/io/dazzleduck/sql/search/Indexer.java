package io.dazzleduck.sql.search;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.MappedReader;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.duckdb.DuckDBConnection;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public interface Indexer {

    static MappedReader.Function createMapperFunction(Map<String, TokenizationFunction> tokenizationFunctions) {
        return (sourceList, target) -> {
            target.setInitialCapacity(10);
            target.allocateNew();
            var resultVector = (StructVector) target;
            var sourceMap = new HashMap<String, FieldVector>();
            for (var s : sourceList) {
                sourceMap.put(s.getName(), s);
            }
            for (var entry : tokenizationFunctions.entrySet()) {
                var tokenizationFunction = entry.getValue();
                var column = entry.getKey();
                var inputVector = (VarCharVector) sourceMap.get(column);
                var listVector = resultVector.getChild(column, ListVector.class);
                var listWriter = listVector.getWriter();
                listWriter.allocate();
                int valueCount = 0;
                for (int row = 0; row < inputVector.getValueCount(); row++) {
                    var text = inputVector.get(row);
                    var tokens = tokenizationFunction.apply(new String(text));
                    listWriter.startList();
                    for (var t : tokens) {
                        listWriter.varChar().writeVarChar(new Text(t));
                        valueCount += 1;
                    }
                    listWriter.endList();
                    resultVector.setIndexDefined(row);
                }
                listWriter.setValueCount(valueCount);
                target.setValueCount(inputVector.getValueCount());
            }
        };
    }

    private static String constructInputSql(String sourcePrefix, List<String> sourceFiles, List<String> sourceFields, String timeField) {
        String source = sourceFiles.stream().map(s -> String.format("'%s/%s'", sourcePrefix, s)).collect(Collectors.joining(","));
        String selectCols = String.join(",", sourceFields);
        return "SELECT '%s' AS prefix, substring(filename, len('%s') + 2) as filename, file_row_number as row_num, %s, %s FROM read_parquet([%s])".formatted(sourcePrefix, sourcePrefix, timeField, selectCols, source);
    }


    static String constructWriteSql(List<String> fieldsForIndexing,
                                    String timeField,
                                    String inputStructName,
                                    String inputTable) {
        var fs = fieldsForIndexing.stream().map(f -> String.format("%s.%s as %s", inputStructName, f, f)).collect(Collectors.joining(", "));
        var inner = "SELECT prefix, filename, row_num, %s, %s FROM %s".formatted(timeField, fs, inputTable);
        var fieldList = String.join(",", fieldsForIndexing);
        return "WITH inner0 AS (%s),\n ".formatted(inner) +
                "inner1 AS (UNPIVOT inner0 ON %s INTO NAME field VALUE tokens),\n".formatted(fieldList) +
                "inner2 AS (SELECT prefix, filename, field, row_num, %s, UNNEST(tokens) as token FROM inner1), \n".formatted(timeField) +
                "inner3 AS (SELECT prefix, filename, field, token, list(row_num) as row_nums, count(row_num) as count, min(%s) as start_time, max(%s) as end_time FROM inner2 GROUP BY token, prefix, filename, field),\n".formatted(timeField, timeField) +
                "inner4 AS (SELECT prefix, filename, field, token, CASE WHEN count > 1000 THEN NULL ELSE row_nums END AS row_nums, count, start_time, end_time FROM inner3)\n" +
                "SELECT * FROM inner4 ORDER BY token, field, filename";
    }

    private static void readTransformExecute(String inputSql, MappedReader.Function function, List<String> sourceCol, Field targetField, String tempTableName, String outputSql, int batchSize) throws SQLException, IOException {
        try (DuckDBConnection readConnection = ConnectionPool.getConnection();
             DuckDBConnection writeConnection = ConnectionPool.getConnection();
             RootAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(readConnection, allocator, inputSql, batchSize);
             Closeable ignored = ConnectionPool.createTempTableWithMap(writeConnection, allocator, reader, function, sourceCol, targetField, tempTableName)) {
            ConnectionPool.execute(writeConnection, outputSql);
        }
    }

    static void create(String sourcePrefix, List<String> sourceFiles, List<String> sourceFields, String timeField, Map<String, TokenizationFunction> tokenizationFunctions, String indexFile) throws SQLException, IOException {
        var inputSql = Indexer.constructInputSql(sourcePrefix, sourceFiles, sourceFields, timeField);
        var mapper = Indexer.createMapperFunction(tokenizationFunctions);
        var childArrays = new ArrayList<Field>();
        for (String fieldName : sourceFields) {
            var f = new Field(fieldName, FieldType.notNullable(new ArrowType.Utf8()), null);
            var childArray = new Field(fieldName, FieldType.notNullable(new ArrowType.List()), List.of(f));
            childArrays.add(childArray);
        }
        var outputStructName = "tokens";
        var structFieldType = new Field(outputStructName, FieldType.notNullable(new ArrowType.Struct()), childArrays);
        var temp_table = "__temp_create_index" + System.currentTimeMillis();
        var outputSql = Indexer.constructWriteSql(sourceFields, timeField, outputStructName, temp_table);
        var writeSql = "COPY (%s) TO '%s' (FORMAT parquet)".formatted(outputSql, indexFile);
        Indexer.readTransformExecute(inputSql, mapper, sourceFields, structFieldType, temp_table, writeSql, 5000);
    }
}
