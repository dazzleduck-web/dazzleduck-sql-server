package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating DuckLakePostIngestionTask instances.
 * Uses a map for fast lookup based on ingestion path.
 */
public class DuckLakeIngestionHandler implements IngestionHandler {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeIngestionHandler.class);

    private static final String TABLE_PATH_QUERY =
            """
            SELECT CASE WHEN s.path_is_relative
                        THEN concat(rtrim(m."value", '/'), '/', s.path, '/', t.path)
                        ELSE concat(s.path, '/', t.path)
                   END AS path
            FROM %s.ducklake_schema s
            JOIN %s.ducklake_table t ON (s.schema_id = t.schema_id)
            CROSS JOIN %s.ducklake_metadata m
            WHERE m.key = 'data_path'
              AND s.schema_name = '%s'
              AND t.table_name = '%s'
              AND s.end_snapshot IS NULL
              AND t.end_snapshot IS NULL
            """;

    private final Map<String, QueueIdToTableMapping> queueIdsToTableMappings;

    private final Map<String, String> queueIdToPathMapping;



    public DuckLakeIngestionHandler(Map<String, QueueIdToTableMapping> queueIdToTableMappingMap
    ) {
        this.queueIdsToTableMappings = queueIdToTableMappingMap;
        var map  =  new HashMap<String, String>();
        queueIdToTableMappingMap.values().forEach(m -> {
            var queueId = m.ingestionQueue();
            var path = getPathFromCatalog(m.catalog(), m.schema(), m.table());
            map.put(queueId, path);

    });
        this.queueIdToPathMapping = map;

    }

    private String getPathFromCatalog(String catalogName, String schema, String table) {
        String metadataDatabase = "__ducklake_metadata_" + catalogName;
        String query = TABLE_PATH_QUERY.formatted(metadataDatabase, metadataDatabase, metadataDatabase, schema, table);
        try {
            return ConnectionPool.collectFirst(query, String.class);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get path for table %s.%s.%s".formatted(catalogName, schema, table), e);
        }
    }

    @Override
    public PostIngestionTask createPostIngestionTask(IngestionResult result) {
        QueueIdToTableMapping mapping = queueIdsToTableMappings.get(result.queueName());
        // Fallback: try suffix (last path segment)
        if (mapping == null) {
            mapping = queueIdsToTableMappings.get(extractSuffix(result.queueName()));
        }

        if (mapping == null) {
            throw new IllegalArgumentException("No mapping found for ingestion queue: " + result.queueName() +
                    ". Available mappings: " + queueIdsToTableMappings.keySet());
        }

        return new DuckLakePostIngestionTask(result, mapping.catalog(), mapping.table(), mapping.schema(), mapping.additionalParameters());
    }

    @Override
    public String getTargetPath(String queueId) {
        return queueIdToPathMapping.get(queueId);
    }

    @Override
    public String getTransformation(String queueId) {
        QueueIdToTableMapping mapping = queueIdsToTableMappings.get(queueId);
        if (mapping == null) {
            mapping = queueIdsToTableMappings.get(extractSuffix(queueId));
        }
        return mapping != null ? mapping.transformation() : null;
    }

    // extracting last path segment of queueName (eg. log, metric)
    private String extractSuffix(String queueName) {
        String normalized = queueName.replace("\\", "/");
        int lastSlash = normalized.lastIndexOf('/');
        return lastSlash >= 0 ? normalized.substring(lastSlash + 1) : normalized;
    }
}