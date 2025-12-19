package io.dazzleduck.sql.commons.planner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dazzleduck.sql.commons.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static io.dazzleduck.sql.commons.ExpressionConstants.TABLE_FUNCTION_TYPE;
import static io.dazzleduck.sql.commons.ExpressionFactory.createFunction;

public interface SplitPlanner {


    static List<List<FileStatus>> getSplitStatus(JsonNode tree,
                                                 long maxSplitSize) throws SQLException, IOException {
        var catalogSchemaAndTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(tree), null, null);

        if (catalogSchemaAndTables.size() != 1) {
            throw new SQLException("unsupported number of tables or path in the query");
        }
        var first = catalogSchemaAndTables.get(0);
        var tableFunction =  first.functionName();
        PartitionPrunerV2 splitPlanner;
        if (tableFunction == null) {
            splitPlanner = PartitionPrunerV2.getPlannerForTable(first);
        } else {
            splitPlanner = PartitionPrunerV2.getPlannerForTableFunction(tableFunction);
        }
        if (splitPlanner == null) {
            throw new SQLException("unsupported type : " + tableFunction);
        }
        var fileStatuses = splitPlanner.pruneFiles(tree, maxSplitSize, Map.of());
        //fileStatuses.sort(Comparator.comparing(FileStatus::lastModified));
        return getSplitStatus(maxSplitSize, fileStatuses);
    }

    private static ArrayList<List<FileStatus>> getSplitStatus(long maxSplitSize, List<FileStatus> fileStatuses) {
        var result = new ArrayList<List<FileStatus>>();
        var current = new ArrayList<FileStatus>();
        long currentSize = 0;
        for (FileStatus fileStatus : fileStatuses) {
            current.add(fileStatus);
            currentSize += fileStatus.size();
            if(currentSize > maxSplitSize) {
                result.add(current);
                current = new ArrayList<>();
                currentSize = 0;
            }
        }
        if(!current.isEmpty()) {
            result.add(current);
        }
        return result;
    }

    private static void replacePathInFromClause(JsonNode tree, String[] paths) {
        var formatToFunction = Map.of("read_delta", "read_parquet");
        var firstStatement = Transformations.getFirstStatementNode(tree);
        var from = firstStatement.get("from_table");
        var type = from.get("type").asText();
        if(type.equals(ExpressionConstants.BASE_TABLE_TYPE)) {
            replacePathInFromTableClause(tree, paths);
        } else {
            replacePathInFromPathClause(tree, paths);
        }
    }

    private static void replacePathInFromTableClause(JsonNode tree, String[] paths) {
        var firstStatement = Transformations.getFirstStatementNode(tree);
        var tableFunction = (ObjectNode)firstStatement.get("from_table");

        var listChildren = new ArrayNode(JsonNodeFactory.instance);
        for (String path : paths) {
            listChildren.add(ExpressionFactory.constant(path));
        }
        var listFunction = createFunction("list_value", "main", "", listChildren);
        var parquetChildren = new ArrayNode(JsonNodeFactory.instance);
        parquetChildren.add(listFunction);

        var readParquetFunction = createFunction("read_parquet", "", "", parquetChildren);
        tableFunction.remove("table_name");
        tableFunction.set("function", readParquetFunction);
        tableFunction.put("type", TABLE_FUNCTION_TYPE);
    }
    private static void replacePathInFromPathClause(JsonNode tree, String[] paths) {
        var formatToFunction = Map.of("read_delta", "read_parquet");
        var firstStatement = Transformations.getFirstStatementNode(tree);
        var tableFunction = Transformations.getTableFunction(firstStatement);
        // TODO
        var format = tableFunction.get("function_name").asText();
        var functionName = formatToFunction.getOrDefault(format, format);
        var from = (ObjectNode) Transformations.getTableFunctionParent(firstStatement);
        var listChildren = new ArrayNode(JsonNodeFactory.instance);
        for (String path : paths) {
            listChildren.add(ExpressionFactory.constant(path));
        }

        JsonNode hiveTypes = null;
        JsonNode unionByName = null;
        for( var child :  (ArrayNode)from.get("function").get("children")) {
            var left = child.get("left");
            if(left != null) {
                var columnNames = (ArrayNode)left.get("column_names");
                if(columnNames != null) {
                    var t = columnNames.get(0).asText();
                    if(t.equals("hive_types")) {
                        hiveTypes = child;
                    } else if(t.equals("union_by_name")) {
                        unionByName = child;
                    }
                }
            }
        }

        var listFunction = createFunction("list_value", "main", "", listChildren);
        var parquetChildren = new ArrayNode(JsonNodeFactory.instance);
        parquetChildren.add(listFunction);
        if(hiveTypes  != null) {
            parquetChildren.add(hiveTypes);
        }
        if(unionByName  != null) {
            parquetChildren.add(unionByName);
        }
        var readParquetFunction = createFunction(functionName, "", "", parquetChildren);
        from.set("function", readParquetFunction);
    }

    static List<TreeAndSize> getSplitTreeAndSize(JsonNode tree,
                                                 long maxSplitSize) throws SQLException, IOException {
        var splits = getSplitStatus(tree, maxSplitSize);
        return splits.stream().map(split -> {
            var copy = tree.deepCopy();
            SplitPlanner.replacePathInFromClause(copy, split.stream().map(FileStatus::fileName).toArray(String[]::new));
            return new TreeAndSize(copy, split.stream().mapToLong(FileStatus::size).sum());
        }).toList();
    }
}
