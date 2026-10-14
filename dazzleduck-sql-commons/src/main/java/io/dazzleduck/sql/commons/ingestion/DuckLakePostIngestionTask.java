package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Post-ingestion task that adds newly ingested files to a DuckLake table.
 * This task executes the ducklake_add_data_files procedure for each ingested file
 * within a transaction to ensure atomicity.
 *
 * <p>When the queue mapping configures a watermark (see {@link WatermarkSpec}), the rows
 * precomputed at write time and carried on {@link IngestionResult#watermarkRows()} are appended
 * to the watermark table via a plain {@code INSERT ... VALUES} in the SAME transaction, so file
 * registration and watermark commit or roll back together. This task never re-reads the written
 * files.
 *
 * <p>The snapshot id ({@code watermark_snapshot_id_column}) is written by that same INSERT, so it
 * commits with the rows rather than being patched in afterwards. DuckLake assigns a snapshot id at
 * COMMIT and does not expose the pending one, so what is written is
 * {@code max(snapshot_id) + 1} — the same formula DuckLake uses, read just before the transaction
 * opens.
 *
 * <p><strong>The column is therefore a lower bound, and is specified as one.</strong> The committed
 * id is always {@code >=} what was written: DuckLake takes {@code max + 1} at commit, {@code max}
 * only ever grows (even aggressive snapshot expiry retains the newest snapshot and ids are never
 * reused), and a competing writer that takes the id first only pushes our commit higher. So the
 * recorded value can be stale but never ahead of the truth, whatever happens — including a crash
 * between the commit and the verification below.
 *
 * <p>{@link #verifySnapshotId} then tightens the bound to the exact snapshot, which is the normal
 * outcome. That step is an accuracy improvement, not a correctness requirement: if it is skipped or
 * fails, the column still satisfies its contract.
 *
 * <p>Limitation: queues registered through the dynamic SQLite registry
 * ({@link DynamicQueueRepository}) do not carry {@code additional_parameters}, so watermarks are
 * only available for statically configured queue mappings.
 */
public class DuckLakePostIngestionTask implements PostIngestionTask {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakePostIngestionTask.class);

    private static final String ADD_FILE_QUERY = "CALL ducklake_add_data_files('%s', '%s', '%s', schema => '%s', ignore_extra_columns => true, allow_missing => true);";

    private final IngestionResult ingestionResult;
    private final String catalogName;
    private final String tableName;
    private final String schemaName;
    private final WatermarkSpec watermarkSpec;

    public DuckLakePostIngestionTask(IngestionResult ingestionResult,
                                     String catalogName,
                                     String tableName,
                                     String schemaName,
                                     Map<String, String> additionalParameters) {
        this.ingestionResult = ingestionResult;
        this.catalogName = catalogName;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.watermarkSpec = WatermarkSpec.fromParameters(ingestionResult.queueName(), additionalParameters);
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
        } catch (FilesNotRegisteredException e) {
            logger.error("DuckLake table {}.{}.{} rejected {} files", catalogName, schemaName, tableName, files.size(), e);
            throw e;
        } catch (SQLException e) {
            logger.error("Failed to add files to DuckLake table {}.{}.{}", catalogName, schemaName, tableName, e);
            throw new RuntimeException("Failed to execute DuckLake post-ingestion task for table " + tableName, e);
        }
    }

    /**
     * Adds files to DuckLake table within a transaction.
     * All files are added atomically - if any file fails, all changes are rolled back.
     * Precomputed watermark rows join the same transaction, so the registered files and their
     * watermark commit or roll back together.
     */
    private void addFilesInTransaction(List<String> files) throws SQLException {
        List<String> queries = new ArrayList<>(files.stream()
                .map(file -> ADD_FILE_QUERY.formatted(
                        escapeLiteral(catalogName), escapeLiteral(tableName), escapeLiteral(file), escapeLiteral(schemaName)))
                .toList());
        List<List<String>> watermarkRows = ingestionResult.watermarkRows();
        boolean hasWatermark = watermarkSpec != null && watermarkRows != null && !watermarkRows.isEmpty();
        try (Connection conn = ConnectionPool.getConnection()) {
            if (!hasWatermark) {
                executeInTxn(conn, queries, files);
                return;
            }
            long predictedSnapshotId = predictNextSnapshotId(conn);
            queries.add(watermarkSpec.insertSql(catalogName, schemaName, watermarkRows, predictedSnapshotId));
            executeInTxn(conn, queries, files);
            verifySnapshotId(conn, files, predictedSnapshotId);
        }
    }

    /**
     * Runs {@code queries} in one transaction, telling apart a statement that failed (nothing
     * registered: {@link FilesNotRegisteredException}, so the caller may delete the files) from a
     * failed commit, whose outcome is unknown and which is rethrown as a plain {@link SQLException}.
     *
     * <p>The error that is thrown is always the one that decides that outcome. A rollback that fails
     * after it, or restoring auto-commit afterwards, is attached as suppressed rather than replacing
     * it: a statement failure is still {@code FilesNotRegisteredException} when its rollback fails
     * (commit was never called, so nothing can have been registered), and a failed commit is still
     * reported as itself.
     */
    static void executeInTxn(Connection conn, List<String> queries, List<String> files) throws SQLException {
        boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        Throwable primary = null;
        try {
            try (Statement statement = conn.createStatement()) {
                for (String sql : queries) {
                    statement.addBatch(sql);
                }
                statement.executeBatch();
            } catch (SQLException e) {
                var rejected = new FilesNotRegisteredException("Files were rejected before commit: " + e.getMessage(), files, e);
                rollbackQuietly(conn, rejected);
                throw rejected;
            }
            try {
                conn.commit();
            } catch (SQLException e) {
                // The commit may or may not have landed; end whatever is left of the transaction.
                rollbackQuietly(conn, e);
                throw e;
            }
        } catch (Throwable t) {
            primary = t;
            throw t;
        } finally {
            try {
                conn.setAutoCommit(oldAutoCommit);
            } catch (SQLException e) {
                if (primary == null) throw e;
                primary.addSuppressed(e);
            }
        }
    }

    private static void rollbackQuietly(Connection conn, Throwable failure) {
        try {
            conn.rollback();
        } catch (SQLException e) {
            failure.addSuppressed(e);
        }
    }

    /**
     * The id this transaction will commit as, barring a competing writer: DuckLake derives the next
     * snapshot id the same way and enforces it with a primary key on {@code ducklake_snapshot}.
     */
    private long predictNextSnapshotId(Connection conn) throws SQLException {
        String query = "SELECT coalesce(max(snapshot_id), -1) + 1 AS next_id FROM __ducklake_metadata_%s.ducklake_snapshot"
                .formatted(escapeLiteral(catalogName));
        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            resultSet.next();
            return resultSet.getLong("next_id");
        }
    }

    /**
     * Tightens the recorded lower bound to the exact snapshot the batch committed in.
     *
     * <p>One query settles it: the file we just registered carries the committed id in its
     * {@code begin_snapshot}. A cheaper pre-check was tried and removed — both a global
     * {@code current_snapshot()} and a per-table high-water mark cost about as much as this lookup
     * (0.15-0.21ms vs 0.16ms at 1M files), and when either says "something moved" this query still
     * has to run. One unconditional exact lookup is simpler and no slower.
     *
     * <p>Failures here are logged rather than thrown. The batch is already durable, and the column
     * is specified as a lower bound, so a skipped repair leaves a value that is merely loose rather
     * than wrong — whereas failing an ingest that succeeded would be a real loss.
     */
    private void verifySnapshotId(Connection conn, List<String> files, long predicted) {
        try {
            Long actual = resolveCommittedSnapshotId(conn, files.get(0), predicted);
            if (actual == null || actual == predicted) {
                return;
            }
            logger.info("Queue '{}' committed as snapshot {} rather than the predicted {} (a concurrent "
                    + "writer took the id); tightening the watermark rows", ingestionResult.queueName(), actual, predicted);
            ConnectionPool.execute(conn, watermarkSpec.updateSnapshotIdSql(catalogName, schemaName, actual));
        } catch (Exception e) {
            logger.warn("Could not tighten the snapshot id written to {}.{}.{}; rows keep the lower bound {}",
                    catalogName, schemaName, watermarkSpec.table(), predicted, e);
        }
    }

    /**
     * The snapshot a registered file became visible in. One file is enough: the whole batch is
     * registered by a single transaction, so every file of it carries the same
     * {@code begin_snapshot}.
     *
     * <p>{@code begin_snapshot >= lowerBound} is free pruning rather than a filter on correctness —
     * the committed id is always {@code >=} the predicted one, and {@code begin_snapshot} rises with
     * time, so zone maps skip almost the whole table instead of scanning every path in the catalog.
     */
    private Long resolveCommittedSnapshotId(Connection conn, String file, long lowerBound) throws SQLException {
        String query = ("SELECT begin_snapshot FROM __ducklake_metadata_%s.ducklake_data_file "
                + "WHERE begin_snapshot >= %d AND path = '%s'")
                .formatted(escapeLiteral(catalogName), lowerBound, escapeLiteral(file));
        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            if (!resultSet.next()) {
                logger.warn("Could not locate registered file {} for queue '{}'; leaving '{}' as written",
                        file, ingestionResult.queueName(), watermarkSpec.snapshotIdColumn());
                return null;
            }
            return resultSet.getLong("begin_snapshot");
        }
    }

    private static String escapeLiteral(String value) {
        return value.replace("'", "''");
    }
}
