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
import java.util.regex.Pattern;

/**
 * Provider for DuckLakeIngestionTaskFactory.
 * Reads configuration and creates factory instances for DuckLake table ingestion.
 */
public class DuckLakeIngestionTaskFactoryProvider extends AbstractIngestionTaskFactoryProvider {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeIngestionTaskFactoryProvider.class);

    /**
     * A transformation that is already a full query. Matched as a whole word, so a shorthand
     * expression on a column like {@code with_flag} or {@code selected_at} is still wrapped.
     */
    private static final Pattern FULL_QUERY = Pattern.compile("\\s*(SELECT|WITH)\\b", Pattern.CASE_INSENSITIVE);

    /**
     * Whether {@code transformation} is a full query rather than the {@code SELECT *, <expr> FROM
     * __this} shorthand. One that starts with {@code SELECT} or {@code WITH} is; otherwise it is
     * one if DuckDB parses it as a statement — which covers a leading comment, a parenthesized
     * query and FROM-first syntax ({@code FROM __this SELECT a}) — while a select-list expression
     * such as {@code upper(level) AS lvl} does not parse and is shorthand.
     */
    static boolean isFullQuery(String transformation) {
        if (FULL_QUERY.matcher(transformation).lookingAt()) {
            return true;
        }
        try {
            var tree = io.dazzleduck.sql.commons.Transformations.parseToTree(transformation);
            return !tree.path("error").asBoolean(false) && tree.path("statements").size() == 1;
        } catch (Exception e) {
            return false;
        }
    }

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
            if (transformation != null && !isFullQuery(transformation)) {
                // Trailing ';' or a trailing comment would otherwise end the statement early or
                // swallow the appended FROM.
                transformation = "SELECT *, " + TransformationText.stripTrailing(transformation) + " FROM __this";
            }
            String view       = c.hasPath(ConfigConstants.VIEW_KEY)        ? c.getString(ConfigConstants.VIEW_KEY)        : null;
            String inputTable = c.hasPath(ConfigConstants.INPUT_TABLE_KEY) ? c.getString(ConfigConstants.INPUT_TABLE_KEY) : null;
            boolean extractClaims = c.hasPath(ConfigConstants.EXTRACT_CLAIMS_KEY)
                    && c.getBoolean(ConfigConstants.EXTRACT_CLAIMS_KEY);
            int numPartitions = c.hasPath(ConfigConstants.NUM_PARTITIONS_KEY)
                    ? c.getInt(ConfigConstants.NUM_PARTITIONS_KEY) : 1;
            String partitionExpression = c.hasPath(ConfigConstants.PARTITION_EXPRESSION_KEY)
                    ? c.getString(ConfigConstants.PARTITION_EXPRESSION_KEY) : null;
            QueueIdToTableMapping mapping = new QueueIdToTableMapping(
                    ingestionQueue, c.getString("catalog"), c.getString("schema"), c.getString("table"),
                    additionalParameters, transformation, view, inputTable)
                    .withExtractClaims(extractClaims)
                    .withPartitioning(numPartitions, partitionExpression)
                    .withVariables(IngestionVariables.fromConfig(c));
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