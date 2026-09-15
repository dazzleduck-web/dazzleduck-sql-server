package io.dazzleduck.sql.commons.planner;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.FileStatus;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.delta.PartitionPruning;
import io.dazzleduck.sql.commons.ducklake.DucklakePartitionPruning;
import io.dazzleduck.sql.commons.hive.HivePartitionPruning;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public interface PartitionPrunerV2 {

    Set<String> ducklakeDatabases = new HashSet<>();

    // DuckLake catalogs are discovered by catalog type. (Earlier versions were detected via a
    // listed '__ducklake_metadata_<catalog>' database, but since DuckDB/DuckLake 1.5.4 that metadata
    // database is no longer shown by duckdb_databases() — the catalog itself reports type='ducklake'.)
    static final String DUCKLAKE_DATABASE_QUERY = "SELECT database_name from duckdb_databases() where type = 'ducklake'";
    Map<String, PartitionPrunerV2> tableFunctionPlanners = Map.of(
            "read_hive", new HiveSplitPlanner(),
            "read_parquet", new HiveSplitPlanner(),
            "read_delta", new DeltaLakeSplitPlanner()
    );

    static PartitionPrunerV2 getPlannerForTableFunction(String functionName) {
        return tableFunctionPlanners.get(functionName);
    }

    static PartitionPrunerV2 getPlannerForTable(Transformations.CatalogSchemaTable table) throws SQLException {
        if (ducklakeDatabases.isEmpty()) {
            try (Connection c = ConnectionPool.getConnection()) {
                var database = ConnectionPool.collectFirstColumn(c, DUCKLAKE_DATABASE_QUERY, String.class);
                for (var d : database) {
                    ducklakeDatabases.add(d);
                }
            }
        }
        if (ducklakeDatabases.contains(table.catalog())) {
            return new DucklakeSplitPlanner();
        } else {
            throw new IllegalStateException("Database Not supported" + table.catalog());
        }
    }

    /**
     *
     * @param tree
     * @param maxSplitSize
     * @param properties
     * @return Sorted FileStatus by lastModified time
     * @throws SQLException
     * @throws IOException
     */
    List<FileStatus> pruneFiles(JsonNode tree,
                                long maxSplitSize,
                                Map<String, String> properties,
                                List<String> sessionSetupSqls) throws SQLException, IOException;


    static String getPath(JsonNode tree) {
        var catalogSchemaAndTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(tree), null, null);
        return catalogSchemaAndTables.get(0).tableOrPath();
    }

}


class HiveSplitPlanner implements PartitionPrunerV2 {

    @Override
    public List<FileStatus> pruneFiles(JsonNode tree, long maxSplitSize, Map<String, String> properties,
                                       List<String> sessionSetupSqls) throws SQLException, IOException {
        var partitionDataTypes  = Transformations.getHivePartition(tree);
        var path  = PartitionPrunerV2.getPath(tree);
        return HivePartitionPruning.pruneFiles(path,
                tree, partitionDataTypes, sessionSetupSqls);
    }
}

class DeltaLakeSplitPlanner implements PartitionPrunerV2 {

    @Override
    public List<FileStatus> pruneFiles(JsonNode tree, long maxSplitSize, Map<String, String> properties,
                                       List<String> sessionSetupSqls) throws SQLException, IOException {
        var statement = Transformations.getFirstStatementNode(tree);
        var path  = PartitionPrunerV2.getPath(tree);
        var filterExpression = Transformations.getWhereClauseForTableFunction(statement);
        // The Delta path converts the filter to a Delta Kernel Predicate, which has no notion of
        // DuckDB session variables — there is nowhere to apply sessionSetupSqls. Fail loudly rather
        // than hand the Kernel an expression it will mis-evaluate.
        if (filterExpression != null
                && !Transformations.collectFunction(filterExpression, "getvariable").isEmpty()) {
            throw new SQLException("getvariable() is not supported in a filter over read_delta; "
                    + "split planning for Delta cannot evaluate session variables");
        }
        return PartitionPruning.pruneFiles(path, filterExpression);
    }
}

class DucklakeSplitPlanner implements PartitionPrunerV2 {

    private Map<String, DucklakePartitionPruning> cache = new ConcurrentHashMap<>();
    @Override
    public List<FileStatus> pruneFiles( JsonNode tree, long maxSplitSize, Map<String, String> properties,
                                        List<String> sessionSetupSqls) throws SQLException, IOException {
        var catalogSchemaAndTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(tree), null, null);
        var first = catalogSchemaAndTables.get(0);
        var catalog = first.catalog();
        var metadata = "__ducklake_metadata_" + catalog;
        var pruner = cache.compute(metadata, (key, oldValue) -> {
            if (oldValue == null){
                return new DucklakePartitionPruning(metadata);
            } else {
                return oldValue;
            }
        });
       return pruner.pruneFiles(first.schema(), first.tableOrPath(), tree, sessionSetupSqls);

    }
}