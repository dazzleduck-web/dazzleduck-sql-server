package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.commons.Transformations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * Provider for DuckLakeIngestionTaskFactory.
 * Reads configuration and creates factory instances for DuckLake table ingestion.
 */
public class DuckLakeIngestionTaskFactoryProvider extends AbstractIngestionTaskFactoryProvider {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeIngestionTaskFactoryProvider.class);

    @Override
    protected Map<String, QueueIdToTableMapping> loadMappings() {
        if (config == null || !config.hasPath(ConfigConstants.INGESTION_QUEUE_TABLE_MAPPING_KEY))
            return Map.of();
        Map<String, QueueIdToTableMapping> mappings = new LinkedHashMap<>();
        config.getConfigList(ConfigConstants.INGESTION_QUEUE_TABLE_MAPPING_KEY).forEach(c -> {
            String ingestionQueue = c.getString(ConfigConstants.INGESTION_QUEUE_KEY);
            if (!c.hasPath("catalog") || !c.hasPath("schema") || !c.hasPath("table")) {
                logger.debug("Skipping DuckLake mapping for queue '{}': missing catalog/schema/table", ingestionQueue);
                return;
            }
            Map<String, String> additionalParameters = new LinkedHashMap<>();
            if (c.hasPath("additional_parameters")) {
                c.getConfig("additional_parameters").entrySet()
                        .forEach(e -> additionalParameters.put(e.getKey(), e.getValue().unwrapped().toString()));
            }
            String transformation = c.hasPath(ConfigConstants.TRANSFORMATION_KEY)
                    ? c.getString(ConfigConstants.TRANSFORMATION_KEY) : null;
            if (transformation != null && !transformation.trim().toUpperCase().startsWith("SELECT")) {
                transformation = "SELECT *, " + transformation + " FROM __this";
            }
            String view       = c.hasPath(ConfigConstants.VIEW_KEY)        ? c.getString(ConfigConstants.VIEW_KEY)        : null;
            String inputTable = c.hasPath(ConfigConstants.INPUT_TABLE_KEY) ? c.getString(ConfigConstants.INPUT_TABLE_KEY) : null;
            boolean extractClaims = c.hasPath(ConfigConstants.EXTRACT_CLAIMS_KEY)
                    && c.getBoolean(ConfigConstants.EXTRACT_CLAIMS_KEY);
            QueueIdToTableMapping mapping = new QueueIdToTableMapping(
                    ingestionQueue, c.getString("catalog"), c.getString("schema"), c.getString("table"),
                    additionalParameters, transformation, view, inputTable)
                    .withExtractClaims(extractClaims);
            mappings.put(mapping.ingestionQueue(), mapping);
        });
        return mappings;
    }

    @Override
    public IngestionHandler getIngestionHandler() {
        Duration refreshInterval = config != null && config.hasPath(ConfigConstants.QUEUE_CONFIG_REFRESH_DELAY_MS_KEY)
                ? Duration.ofMillis(config.getLong(ConfigConstants.QUEUE_CONFIG_REFRESH_DELAY_MS_KEY))
                : Duration.ofMinutes(2);
        return new DuckLakeIngestionHandler(loadMappings(), refreshInterval);
    }
}