package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Post-ingestion task that adds newly ingested files to a DuckLake table.
 * This task executes the ducklake_add_data_files procedure for each ingested file
 * within a transaction to ensure atomicity.
 *
 * <p>Optionally appends a watermark row set in the same transaction, configured through the
 * queue mapping's {@code additional_parameters}:
 * <ul>
 *   <li>{@code watermark_table} — unqualified table name (same catalog/schema as the target)
 *       receiving one row per group with the MIN of the timestamp column across the newly
 *       added files. Appended atomically with the file registration: a rollback undoes both.</li>
 *   <li>{@code watermark_timestamp_column} — timestamp column to MIN over; required when
 *       {@code watermark_table} is set. The result lands in a watermark-table column of the
 *       same name (matched BY NAME).</li>
 *   <li>{@code watermark_group_columns} — optional comma-separated grouping columns
 *       (e.g. {@code "county,state"}); each must exist in both the Parquet files and
 *       the watermark table. Empty/absent produces a single global-MIN row per batch.</li>
 * </ul>
 * The MIN is computed with a projected scan of only the newly written files
 * ({@code read_parquet}), never a rescan of the target table.
 */
public class DuckLakePostIngestionTask implements PostIngestionTask {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakePostIngestionTask.class);

    private static final String ADD_FILE_QUERY = "CALL ducklake_add_data_files('%s', '%s', '%s', schema => '%s', ignore_extra_columns => true, allow_missing => true);";

    public static final String WATERMARK_TABLE_KEY = "watermark_table";
    public static final String WATERMARK_TIMESTAMP_COLUMN_KEY = "watermark_timestamp_column";
    public static final String WATERMARK_GROUP_COLUMNS_KEY = "watermark_group_columns";

    private final IngestionResult ingestionResult;
    private final String catalogName;
    private final String tableName;
    private final String schemaName;
    private final String watermarkTable;
    private final String watermarkTimestampColumn;
    private final List<String> watermarkGroupColumns;

    public DuckLakePostIngestionTask(IngestionResult ingestionResult,
                                     String catalogName,
                                     String tableName,
                                     String schemaName,
                                     Map<String, String> additionalParameters) {
        this.ingestionResult = ingestionResult;
        this.catalogName = catalogName;
        this.tableName = tableName;
        this.schemaName = schemaName;
        Map<String, String> params = additionalParameters == null ? Map.of() : additionalParameters;
        this.watermarkTable = params.get(WATERMARK_TABLE_KEY);
        this.watermarkTimestampColumn = params.get(WATERMARK_TIMESTAMP_COLUMN_KEY);
        this.watermarkGroupColumns = params.containsKey(WATERMARK_GROUP_COLUMNS_KEY)
                ? Arrays.stream(params.get(WATERMARK_GROUP_COLUMNS_KEY).split(","))
                        .map(String::trim).filter(s -> !s.isEmpty()).toList()
                : List.of();
        if (watermarkTable != null && watermarkTimestampColumn == null) {
            throw new IllegalArgumentException(
                    "Queue '%s': '%s' requires '%s'".formatted(
                            ingestionResult.queueName(), WATERMARK_TABLE_KEY, WATERMARK_TIMESTAMP_COLUMN_KEY));
        }
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
     * When a watermark table is configured its INSERT joins the same transaction, so the
     * registered files and their watermark rows commit or roll back together.
     */
    private void addFilesInTransaction(List<String> files) throws SQLException {
        List<String> queries = new ArrayList<>(files.stream()
                .map(file -> ADD_FILE_QUERY.formatted(catalogName, tableName, file, schemaName))
                .toList());
        if (watermarkTable != null) {
            queries.add(watermarkQuery(files));
        }
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, queries.toArray(String[]::new));
        }
    }

    private String watermarkQuery(List<String> files) {
        String fileList = files.stream()
                .map(f -> "'" + f.replace("'", "''") + "'")
                .collect(Collectors.joining(", ", "[", "]"));
        String tsColumn = quoteIdentifier(watermarkTimestampColumn);
        String groupColumns = watermarkGroupColumns.stream()
                .map(DuckLakePostIngestionTask::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String selectPrefix = groupColumns.isEmpty() ? "" : groupColumns + ", ";
        String groupBy = groupColumns.isEmpty() ? "" : " GROUP BY " + groupColumns;
        return "INSERT INTO %s.%s.%s BY NAME SELECT %sMIN(%s) AS %s FROM read_parquet(%s)%s".formatted(
                catalogName, schemaName, watermarkTable, selectPrefix, tsColumn, tsColumn, fileList, groupBy);
    }

    private static String quoteIdentifier(String identifier) {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }
}
