package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.Transformations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * Provider for DuckLakeIngestionTaskFactory.
 * Reads configuration and creates factory instances for DuckLake table ingestion.
 */
public class DuckLakeIngestionTaskFactoryProvider implements IngestionTaskFactoryProvider {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeIngestionTaskFactoryProvider.class);

    private Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public IngestionHandler getIngestionTaskFactory() {
        Map<String, QueueIdToTableMapping> pathToTableMappings = loadMappings(config);
        return new DuckLakeIngestionHandler(pathToTableMappings);
    }

    @Override
    public void validate() {
        if (!config.hasPath("ingestion_queue_table_mapping")) return;
        config.getConfigList("ingestion_queue_table_mapping").forEach(c -> {
            if (c.hasPath("transformation")) {
                validateTransformation(c.getString("ingestion_queue"), c.getString("transformation"));
            }
        });
    }

    /**
     * Validates that the transformation SQL is parseable and references {@code __this} as a base table.
     *
     * @throws IllegalArgumentException if the SQL cannot be parsed or does not reference {@code __this}
     */
    static void validateTransformation(String ingestionQueue, String transformation) {
        try {
            var ast = Transformations.parseToTree(transformation);
            var statementNode = Transformations.getFirstStatementNode(ast);
            List<Transformations.CatalogSchemaTable> tables =
                    Transformations.getAllTablesOrPathsFromSelect(statementNode, "", "");
            boolean hasThis = tables.stream()
                    .anyMatch(t -> Transformations.TableType.BASE_TABLE == t.type()
                            && "__this".equals(t.tableOrPath()));
            if (!hasThis) {
                throw new IllegalArgumentException(
                        "transformation for queue '%s' must reference '__this' as a table (e.g. SELECT ... FROM __this), but found tables: %s"
                                .formatted(ingestionQueue, tables.stream().map(Transformations.CatalogSchemaTable::tableOrPath).toList()));
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid transformation SQL for queue '%s': %s".formatted(ingestionQueue, e.getMessage()), e);
        }
    }

    private Map<String, QueueIdToTableMapping> loadMappings(Config config) {
        if (!config.hasPath("ingestion_queue_table_mapping")) return Map.of();
        Map<String, QueueIdToTableMapping> mappings = new LinkedHashMap<>();
        config.getConfigList("ingestion_queue_table_mapping").forEach(c -> {
            Map<String, String> additionalParameters = new LinkedHashMap<>();
            if (c.hasPath("additional_parameters")) {
                c.getConfig("additional_parameters").entrySet().forEach(e ->
                    additionalParameters.put(e.getKey(), e.getValue().unwrapped().toString()));
            }
            String ingestionQueue = c.getString("ingestion_queue");
            String transformation = c.hasPath("transformation") ? c.getString("transformation") : null;
            QueueIdToTableMapping mapping = new QueueIdToTableMapping(
                    ingestionQueue,
                    c.getString("catalog"),
                    c.getString("schema"),
                    c.getString("table"),
                    additionalParameters,
                    transformation);
            mappings.put(mapping.ingestionQueue(), mapping);
        });
        return mappings;
    }
}