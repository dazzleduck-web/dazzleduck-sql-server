package io.dazzleduck.sql.commons.ducklake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.FileStatus;
import io.dazzleduck.sql.commons.Transformations;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DucklakePartitionPruning {

    private static final String NESTED = "{'key' : concat('min_', column_id), 'value' : min_value }," +
            " {'key' : concat('max_', column_id), 'value' :  max_value}," +
            " {'key' : concat('null_count_', column_id), 'value' : cast(null_count as varchar)}," +
            " {'key' : concat('contains_nan_', column_id), 'value' : cast(contains_nan as varchar)}";
    private static final String COLUMN_INFO_QUERY =
            """
                     WITH s AS (SELECT schema_id FROM %s.ducklake_schema  WHERE end_snapshot IS NULL AND schema_name = '%s'),
                     t AS (SELECT table_id, schema_id FROM %s.ducklake_table WHERE end_snapshot IS NULL AND table_name = '%s'),
                     c AS  (SELECT  column_id, table_id, column_name, column_type FROM %s.ducklake_column WHERE end_snapshot IS NULL ),
                     sv AS (SELECT max(schema_version) as version FROM %s.ducklake_snapshot )
                     SELECT c.column_id as id, c.column_name as name, c.column_type as "type", sv.version as schemaVersion FROM c, s, t, sv WHERE c.table_id = t.table_id AND  s.schema_id = t.schema_id ;
                    """;
    private static final String SCHEMA_VERSION_QUERY = "SELECT max(schema_version) FROM %s.ducklake_snapshot";
    private static final String TABLE_ID_QUERY = "select table_id from %s.ducklake_table t, %s.ducklake_schema s " +
            "where s.schema_name = '%s' and t.table_name = '%s' " +
            "and s.schema_id  = t.schema_id";
    private static final String QUERY =
            " WITH L AS (SELECT data_file_id, path, path_is_relative, file_format,record_count, file_size_bytes, mapping_id, table_id FROM %s.ducklake_data_file WHERE table_id = %s and end_snapshot is null),\n" +
                    "M AS (SELECT mapping_id, type FROM %s.ducklake_column_mapping WHERE table_id = %s),\n" +
                    "B AS (SELECT data_file_id, unnest([" + NESTED + "]) AS  nested, column_id FROM %s.ducklake_file_column_stats WHERE table_id = %s and column_id in (%s)),\n" +
                    " AA AS (SELECT  data_file_id, nested.key as column_id, nested.value as value, column_id from B),\n" +
                    " P AS (PIVOT AA ON column_id IN (%s) USING first(value) GROUP BY data_file_id),\n" +
                    " R AS (%s)\n" +
                    " SELECT L.path, L.file_size_bytes, cast(0  as  bigint), L.path_is_relative, L.table_id, L.mapping_id FROM L INNER JOIN R ON L.data_file_id = R.data_file_id LEFT OUTER JOIN M ON M.mapping_id = L.mapping_id ORDER BY L.data_file_id";
    private static final String NO_FILTER_QUERY = "SELECT L.path, L.file_size_bytes, cast(0  as  bigint), L.path_is_relative, L.table_id, L.mapping_id FROM %s.ducklake_data_file L WHERE table_id = %s ORDER by L.data_file_id";
    private static final String RELATIVE_PATH_QUERY = "select if(s.path_is_relative, concat(m.\"value\", s.path, t.path), concat(s.path, t.path)) as path from %s.main.ducklake_schema s join %s.main.ducklake_table t on  (s.schema_id = t.schema_id) cross join  %s.main.ducklake_metadata m where m.key =  'data_path' and t.table_id = %s";
    private static final String PIVOT_TABLE_ALIAS = "P";

    private final String metadataDatabase;
    private final Map<String, VersionEntity<Map<String, ColumnInfo>>> columnInfoCache = new ConcurrentHashMap<>();
    private final Map<Long, VersionEntity<String>> relativePathCache = new ConcurrentHashMap<>();
    public DucklakePartitionPruning(String metadataDatabase) {
        this.metadataDatabase = metadataDatabase;
    }

    private String getNoFilterQuery(long tableId) {
        return NO_FILTER_QUERY.formatted(metadataDatabase, tableId);
    }

    private String partitionSql(String innerSql, Long tableId, Set<Long> columnId) {
        var columnString = columnId.stream().map(l -> "min_%s, max_%s".formatted(l, l)).collect(Collectors.joining(","));
        var columnIdString = columnId.stream().map("%s"::formatted).collect(Collectors.joining(","));
        return QUERY.formatted(metadataDatabase, tableId, metadataDatabase, tableId, metadataDatabase, tableId, columnIdString, columnString, innerSql);
    }

    private String getColumnInfoQuery(String schema, String table) {
        return COLUMN_INFO_QUERY.formatted(metadataDatabase, schema, metadataDatabase, table, metadataDatabase, metadataDatabase);
    }

    private String getSchemaVersionQuery() {
        return SCHEMA_VERSION_QUERY.formatted(metadataDatabase);
    }

    private String getTableIdQuery(String schema, String table) {
        return TABLE_ID_QUERY.formatted(metadataDatabase, metadataDatabase, schema, table);
    }

    private String getTablePathQuery(long tableId) {
        return RELATIVE_PATH_QUERY.formatted(metadataDatabase, metadataDatabase, metadataDatabase, tableId);
    }

    private String getTablePath(long tableId) {
        var versionEntity = relativePathCache.compute(tableId, (key, oldValue) -> {
            Long latestSchemaVersion;
            try {
                var schemaVersionQuery = getSchemaVersionQuery();
                latestSchemaVersion = ConnectionPool.collectFirst(schemaVersionQuery, Long.class);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to get schema version", e);
            }
            if (oldValue == null || oldValue.version() < latestSchemaVersion) {
                return new VersionEntity<>(latestSchemaVersion, getRelativePathFromDB(tableId));
            } else {
                return oldValue;
            }
        });
        return versionEntity.entity;
    }

    private String getRelativePathFromDB(long tableId) {
        try (var connection = ConnectionPool.getConnection()) {
            return ConnectionPool.collectFirst(connection, getTablePathQuery(tableId), String.class);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, ColumnInfo> getColumnIdMap(String schema, String table) {
        var schemaTable = "%s.%s".formatted(schema, table);
        var versionEntity = columnInfoCache.compute(schemaTable, (key, oldValue) -> {
            Long latestSchemaVersion;
            try {
                var schemaVersionQuery = getSchemaVersionQuery();
                latestSchemaVersion = ConnectionPool.collectFirst(schemaVersionQuery, Long.class);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to get schema version", e);
            }
            if (oldValue == null || oldValue.version() < latestSchemaVersion) {
                List<ColumnInfo> info;
                try {
                    info = getInfo(schema, table);
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to get column info", e);
                }
                var res = new HashMap<String, ColumnInfo>();
                for (var i : info) {
                    res.put(i.name, i);
                }
                return new VersionEntity<>(latestSchemaVersion, res);
            } else {
                return oldValue;
            }
        });
        return versionEntity.entity;
    }

    private List<ColumnInfo> getInfo(String schema, String table) throws SQLException {
        var query = getColumnInfoQuery(schema, table);
        try (var connection = ConnectionPool.getConnection()) {
            var result = new java.util.ArrayList<ColumnInfo>();
            for (var info : ConnectionPool.collectAll(connection, query, ColumnInfo.class)) {
                result.add(info);
            }
            return result;
        }
    }

    public List<FileStatus> pruneFiles(String schema,
                                       String table,
                                       JsonNode tree) throws SQLException {
        var where = Transformations.identity()
                .andThen(Transformations::getFirstStatementNode)
                .andThen(Transformations::getWhereClauseForBaseTable)
                .apply(tree);
        var tableId = getTableId(schema, table);
        if (tableId == null) {
            throw new SQLException("Table Not Found :%s".formatted(table));
        }
        String tableRelativePath = getTablePath(tableId);
        String toRun;
        if (where == null || where instanceof NullNode) {
            toRun = getNoFilterQuery(tableId);
        } else {
            var columnMap = getColumnIdMap(schema, table);
            var maxMap = new HashMap<String, String>();
            var minMap = new HashMap<String, String>();
            var typeMap = new HashMap<String, String>();
            for (var e : columnMap.entrySet()) {
                minMap.put(e.getKey(), "min_" + e.getValue().id());
                maxMap.put(e.getKey(), "max_" + e.getValue().id());
                typeMap.put(e.getKey(), e.getValue().type());
            }

            var partitionQuery = Transformations.replaceEqualMinMaxInQuery(PIVOT_TABLE_ALIAS, minMap, maxMap, typeMap)
                    .apply(tree);

            var references = Transformations.collectReferences(where);
            if (references.isEmpty()) {
                toRun = getNoFilterQuery(tableId);
            } else {
                var columnIds = references.stream().map(n -> columnMap.get(Transformations.getReferenceName(n)[0]).id())
                        .collect(Collectors.toSet());
                var partitionSql = Transformations.parseToSql(partitionQuery);
                toRun = partitionSql(partitionSql, tableId, columnIds);
            }
        }
        try (var connection = ConnectionPool.getConnection()) {
            var res = ConnectionPool.collectAll(connection, toRun, DucklakeFileStatus.class);
            var result = new java.util.ArrayList<FileStatus>();
            for (var x : res) {
                result.add(x.resolvedFileStatue(tableRelativePath));
            }
            return result;
        }
    }

    public List<FileStatus> pruneFiles(String schema,
                                       String table,
                                       String sql) throws SQLException, JsonProcessingException {
        var tree = Transformations.parseToTree(sql);
        return pruneFiles(schema, table, tree);
    }

    private Long getTableId(String schema, String table) throws SQLException {
        var query = getTableIdQuery(schema, table);
        try {
            return ConnectionPool.collectFirst(query, Long.class);
        } catch (RuntimeException e  ){
            if (e.getCause() instanceof SQLException s) {
                throw s;
            } else {
                throw e;
            }
        }
    }

    public record ColumnInfo(Long id, String name, String type, Long schemaVersion) {
    }

    public record VersionEntity<E>(long version, E entity) {
    }
}
