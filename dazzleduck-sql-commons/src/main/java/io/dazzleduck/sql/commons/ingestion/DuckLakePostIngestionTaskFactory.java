package io.dazzleduck.sql.commons.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Factory for creating DuckLakePostIngestionTask instances.
 * Supports multiple table mappings based on ingestion path.
 */
public class DuckLakePostIngestionTaskFactory implements PostIngestionTaskFactory {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakePostIngestionTaskFactory.class);

    private final List<PathToTableMapping> pathToTableMappings;
    private final String metadataDatabase;
    private final String defaultCatalogName;

    public DuckLakePostIngestionTaskFactory(List<PathToTableMapping> pathToTableMappings,
                                            String metadataDatabase,
                                            String defaultCatalogName) {
        this.pathToTableMappings = pathToTableMappings;
        this.metadataDatabase = metadataDatabase;
        this.defaultCatalogName = defaultCatalogName;
    }

    @Override
    public PostIngestionTask create(IngestionResult result) {
        return pathToTableMappings.stream()
                .filter(m -> m.matches(result.queueName()))
                .findFirst()
                .<PostIngestionTask>map(m -> new DuckLakePostIngestionTask(
                        result,
                        defaultCatalogName,
                        m.tableName(),
                        m.schemaName(),
                        metadataDatabase
                ))
                .orElse(PostIngestionTask.NOOP);
    }
}