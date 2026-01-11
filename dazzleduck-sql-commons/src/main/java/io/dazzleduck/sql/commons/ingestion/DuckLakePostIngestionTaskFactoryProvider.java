package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Provider for DuckLakePostIngestionTaskFactory.
 * Reads configuration and creates factory instances for DuckLake table ingestion.
 */
public class DuckLakePostIngestionTaskFactoryProvider implements PostIngestionTaskFactoryProvider {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakePostIngestionTaskFactoryProvider.class);

    private Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public PostIngestionTaskFactory getPostIngestionTaskFactory() {
        if (config == null) {
            return ingestionResult -> PostIngestionTask.NOOP;
        }

        try {
            // Get catalog name from config
            String catalogName = config.hasPath("catalog_name") ? config.getString("catalog_name") : config.hasPath("database") ? config.getString("database") : "main";
            // Construct metadata database name from catalog name
            String metadataDatabase = "__ducklake_metadata_" + catalogName;
            // Build list of path-to-table mappings
            List<PathToTableMapping> pathToTableMappings = loadMappings(config);
            if (pathToTableMappings.isEmpty()) {
                return ingestionResult -> PostIngestionTask.NOOP;
            }
            logger.info("Creating DuckLakePostIngestionTaskFactory with catalog={}, metadataDb={}, {} mapping(s)", catalogName, metadataDatabase, pathToTableMappings.size());

            // Log each mapping for debugging
            for (int i = 0; i < pathToTableMappings.size(); i++) {
                PathToTableMapping mapping = pathToTableMappings.get(i);
            }

            return new DuckLakePostIngestionTaskFactory(pathToTableMappings, metadataDatabase, catalogName);

        } catch (Exception e) {
            return ingestionResult -> PostIngestionTask.NOOP;
        }
    }

    private List<PathToTableMapping> loadMappings(Config config) {
        if (!config.hasPath("path_to_table_mapping")) return List.of();
        return config.getConfigList("path_to_table_mapping").stream().map(c -> new PathToTableMapping(c.getString("base_path"), c.getString("table_name"), c.hasPath("schema_name") ? c.getString("schema_name") : "main")).toList();
    }
}