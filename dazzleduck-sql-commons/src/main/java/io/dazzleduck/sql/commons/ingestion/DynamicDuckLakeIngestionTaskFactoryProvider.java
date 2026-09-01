package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.common.ConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;

/**
 * {@link io.dazzleduck.sql.commons.ingestion.IngestionTaskFactoryProvider} that reads
 * queue configuration from a SQLite database.
 *
 * <p>Required config key: {@code db_path} — filesystem path to the SQLite file.
 * Optional config key: {@code config_load_interval_ms} — how often to poll for schema
 * changes (default 5 000 ms).
 *
 * <p>Example HOCON:
 * <pre>
 *   ingestion_task_factory_provider = {
 *     class = "io.dazzleduck.sql.sqlite.ingestion.DynamicDuckLakeIngestionTaskFactoryProvider"
 *     db_path = "/var/data/ingestion.db"
 *     config_load_interval_ms = 5000
 *   }
 * </pre>
 */
public class DynamicDuckLakeIngestionTaskFactoryProvider extends AbstractIngestionTaskFactoryProvider {

    private static final Logger logger = LoggerFactory.getLogger(DynamicDuckLakeIngestionTaskFactoryProvider.class);

    static final String DB_PATH_KEY              = "db_path";
    static final String CONFIG_LOAD_INTERVAL_KEY = "config_load_interval_ms";
    static final String MANAGE_TABLES_KEY        = "manage_tables";
    static final long   DEFAULT_INTERVAL_MS      = 5_000L;

    @Override
    protected Map<String, QueueIdToTableMapping> loadMappings() {
        if (config == null || !config.hasPath(DB_PATH_KEY)) return Map.of();
        String dbPath = config.getString(DB_PATH_KEY);
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            try (Connection conn = repo.openReadOnlyConnection()) {
                return DynamicQueueRepository.loadAll(conn);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load ingestion queue mappings from: " + dbPath, e);
        }
    }

    @Override
    public void validate() {
        super.validate();
        if (config == null || !config.hasPath(ConfigConstants.INGESTION_QUEUE_TABLE_MAPPING_KEY)) return;
        if (config.getConfigList(ConfigConstants.INGESTION_QUEUE_TABLE_MAPPING_KEY).stream()
                .anyMatch(c -> c.hasPath(ConfigConstants.EXTRACT_CLAIMS_KEY)
                        && c.getBoolean(ConfigConstants.EXTRACT_CLAIMS_KEY))) {
            logger.warn("ingestion_queue_table_mapping is ignored by the dynamic provider — queue "
                    + "configuration comes from the SQLite registry only; extract_claims will not take "
                    + "effect and no claims column will be written");
        }
    }

    @Override
    public IngestionHandler getIngestionHandler() {
        if (config == null || !config.hasPath(DB_PATH_KEY)) {
            throw new IllegalStateException("'db_path' must be set for DynamicDuckLakeIngestionTaskFactoryProvider");
        }
        String dbPath = config.getString(DB_PATH_KEY);
        long intervalMs = config.hasPath(CONFIG_LOAD_INTERVAL_KEY)
                ? config.getLong(CONFIG_LOAD_INTERVAL_KEY)
                : DEFAULT_INTERVAL_MS;
        boolean manageTables = config.hasPath(MANAGE_TABLES_KEY) && config.getBoolean(MANAGE_TABLES_KEY);

        DynamicQueueRepository repo = new DynamicQueueRepository(dbPath);
        try {
            repo.init();
            Connection readConn = repo.openReadOnlyConnection();
            Map<String, QueueIdToTableMapping> initial = DynamicQueueRepository.loadAll(readConn);
            logger.info("DynamicIngestionHandler: {} queue(s) loaded from {} (manage_tables={})",
                    initial.size(), dbPath, manageTables);
            return new DynamicIngestionHandler(dbPath, readConn, initial, Duration.ofMillis(intervalMs),
                    manageTables);
        } catch (SQLException e) {
            repo.close();
            throw new RuntimeException("Failed to initialise DynamicIngestionHandler for: " + dbPath, e);
        }
    }
}
