package io.dazzleduck.sql.commons.planner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dazzleduck.sql.commons.TreeAndSize;
import io.dazzleduck.sql.commons.delta.PartitionPruning;
import io.dazzleduck.sql.commons.ExpressionFactory;
import io.dazzleduck.sql.commons.FileStatus;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.ducklake.DucklakePartitionPruning;
import io.dazzleduck.sql.commons.hive.HivePartitionPruning;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static io.dazzleduck.sql.commons.ExpressionFactory.createFunction;

public interface SplitPlanner {

    enum Pruner {
        INSTANCE;
        final Map<String, PartitionPruner> mapping;
        Pruner(){
            mapping = new HashMap<>();
        }
    }

    static List<List<FileStatus>> getSplitStatus(JsonNode tree,
                                                 long maxSplitSize) throws SQLException, IOException {
        return getSplitStatus(tree, maxSplitSize, Map.of());
    }

    static List<List<FileStatus>> getSplitStatus(JsonNode tree,
                                                 long maxSplitSize,
                                                 Map<String, String> databaseMetadataDatabaseMap) throws SQLException, IOException {
        var statement = Transformations.getFirstStatementNode(tree);
        var filterExpression = Transformations.getWhereClauseForTableFunction(statement);
        var catalogSchemaAndTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(tree), null, null);

        if (catalogSchemaAndTables.size() != 1) {
            throw new SQLException("unsupported number of tables or path in the query");
        }
        var path = catalogSchemaAndTables.get(0).tableOrPath();
        var tableFunction =  catalogSchemaAndTables.get(0).functionName();
        List<FileStatus> fileStatuses;
        switch (tableFunction) {
            case "read_parquet" -> {
                var partitionDataTypes  = Transformations.getHivePartition(tree);
                fileStatuses = HivePartitionPruning.pruneFiles(path,
                        tree, partitionDataTypes);
            }
            case "read_delta" ->
                    fileStatuses = PartitionPruning.pruneFiles(path, filterExpression);

            case "read_ducklake" -> {
                //
                // Get the metadataDatabase from
                var first = catalogSchemaAndTables.get(0);
                var catalog = first.catalog();
                var metadata = databaseMetadataDatabaseMap.get(catalog);
                try {
                    fileStatuses = DucklakePartitionPruning.pruneFiles(first.schema(), first.tableOrPath(), "", metadata);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            }


            default ->
            {
                var planner = Pruner.INSTANCE.mapping.get(tableFunction);
                if (planner == null) {
                    throw new SQLException("unsupported type : " + tableFunction);
                } else {
                    fileStatuses = planner.pruneFiles(tree, maxSplitSize, Map.of());
                }
            }


        }

        fileStatuses.sort(Comparator.comparing(FileStatus::lastModified));
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
        var tableFunction = Transformations.getTableFunction(firstStatement);
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
