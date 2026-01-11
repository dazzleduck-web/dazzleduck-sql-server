package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Post-ingestion task that adds newly ingested files to a DuckLake table.
 * This task executes the ducklake_add_data_files procedure for each ingested file
 * within a transaction to ensure atomicity.
 */
public class DuckLakePostIngestionTask implements PostIngestionTask {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakePostIngestionTask.class);

    private static final String ADD_FILE_QUERY = "CALL ducklake_add_data_files('%s', '%s', '%s', schema => '%s', ignore_extra_columns => true, allow_missing => true);";

    private final IngestionResult ingestionResult;
    private final String catalogName;
    private final String tableName;
    private final String schemaName;
    private final String metadataDatabase;

    public DuckLakePostIngestionTask(IngestionResult ingestionResult,
                                     String catalogName,
                                     String tableName,
                                     String schemaName,
                                     String metadataDatabase) {
        this.ingestionResult = ingestionResult;
        this.catalogName = catalogName;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.metadataDatabase = metadataDatabase;
    }

    @Override
    public void execute() {
        List<String> files = ingestionResult.filesCreated();
        if (files == null || files.isEmpty()) {
            logger.debug("No files to add to DuckLake for catalog={}, table={}", catalogName, tableName);
            return;
        }

        try {
            addFilesInTransaction(files);
            logger.info("Successfully added {} files to DuckLake table {}.{}.{}", files.size(), catalogName, schemaName, tableName);
        } catch (SQLException e) {
            logger.error("Failed to add files to DuckLake table {}.{}.{}", catalogName, schemaName, tableName, e);
            throw new RuntimeException("Failed to execute DuckLake post-ingestion task for table " + tableName, e);
        }
    }

    /**
     * Adds files to DuckLake table within a transaction.
     * All files are added atomically - if any file fails, all changes are rolled back.
     */
    private void addFilesInTransaction(List<String> files) throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            String[] queries = files.stream().map(file -> ADD_FILE_QUERY.formatted(catalogName, tableName, file, schemaName)).toArray(String[]::new);
            ConnectionPool.executeBatchInTxn(conn, queries);
        }
    }
}