package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Post-ingestion task that adds newly ingested files to a DuckLake table, and each sidecar's files
 * to its table. This task executes the ducklake_add_data_files procedure for each file within one
 * transaction, so a batch and its sidecars commit or roll back together.
 */
public class DuckLakePostIngestionTask implements PostIngestionTask {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakePostIngestionTask.class);

    private static final String ADD_FILE_QUERY = "CALL ducklake_add_data_files('%s', '%s', '%s', schema => '%s', ignore_extra_columns => true, allow_missing => true);";

    private final IngestionResult ingestionResult;
    private final String catalogName;
    private final String tableName;
    private final String schemaName;
    /** Runs in each attempt's transaction, after its statements and before its commit; tests only. */
    private final Runnable beforeCommit;

    /**
     * Registration attempts when the commit fails because a concurrent DELETE on the table committed
     * first. Without the duckdb/ducklake#1895 fix every such DELETE (a retention job's, say)
     * conflicts with a batch being registered. The conflict is decided before DuckLake writes
     * anything, so nothing was registered, and the same files are registered again in a new
     * transaction, whose snapshot includes the DELETE: a short delay instead of a batch the client
     * must resend. Other conflicts, such as with an ALTER or DROP of the table, are not retried:
     * registering the same files again under a changed schema could silently drop their columns.
     */
    static final int MAX_ATTEMPTS = 5;

    public DuckLakePostIngestionTask(IngestionResult ingestionResult,
                                     String catalogName,
                                     String tableName,
                                     String schemaName) {
        this(ingestionResult, catalogName, tableName, schemaName, () -> { });
    }

    DuckLakePostIngestionTask(IngestionResult ingestionResult,
                              String catalogName,
                              String tableName,
                              String schemaName,
                              Runnable beforeCommit) {
        this.beforeCommit = beforeCommit;
        this.ingestionResult = ingestionResult;
        this.catalogName = catalogName;
        this.tableName = tableName;
        this.schemaName = schemaName;
    }

    @Override
    public void execute() {
        List<String> files = ingestionResult.allFiles();
        if (files.isEmpty()) {
            logger.debug("No files to add to DuckLake for catalog={}, table={}", catalogName, tableName);
            return;
        }

        try {
            addFilesInTransaction(files);
            logger.info("Successfully added {} files to DuckLake table {}.{}.{}{}", files.size(), catalogName, schemaName, tableName,
                    ingestionResult.sidecarFiles().isEmpty() ? "" : " and its sidecars " + ingestionResult.sidecarFiles().keySet());
        } catch (FilesNotRegisteredException e) {
            logger.error("DuckLake table {}.{}.{} rejected {} files", catalogName, schemaName, tableName, files.size(), e);
            throw e;
        } catch (SQLException e) {
            logger.error("Failed to add files to DuckLake table {}.{}.{}", catalogName, schemaName, tableName, e);
            throw new RuntimeException("Failed to execute DuckLake post-ingestion task for table " + tableName, e);
        }
    }

    /**
     * Registers the files in one transaction, retried as a whole when its commit fails because a
     * concurrent DELETE committed first (see {@link #MAX_ATTEMPTS}). Such a conflict registered
     * nothing, so when the attempts run out it is reported as {@link FilesNotRegisteredException},
     * and the queue may delete the files and retry the batch. Any other failure is thrown at once: a
     * rejected statement is already {@code FilesNotRegisteredException}, and any other commit error
     * has an unknown outcome, so retrying it could register the batch twice.
     */
    private void addFilesInTransaction(List<String> files) throws SQLException {
        for (int attempt = 1; ; attempt++) {
            try {
                addFilesOnce(files);
                return;
            } catch (SQLException e) {
                if (!isConcurrentDeleteConflict(e)) {
                    throw e;
                }
                if (attempt >= MAX_ATTEMPTS) {
                    throw new FilesNotRegisteredException("Registration conflicted with a concurrent delete "
                            + MAX_ATTEMPTS + " times: " + e.getMessage(), files, e);
                }
                logger.info("Registering {} files in {}.{}.{} conflicted with a concurrent delete; retrying ({}/{})",
                        files.size(), catalogName, schemaName, tableName, attempt + 1, MAX_ATTEMPTS);
                backOff(attempt, files, e);
            }
        }
    }

    /**
     * DuckLake's error for a commit refused because another transaction deleted from the table
     * since this one's snapshot; the check runs before anything is written. Matched in any case,
     * through the cause chain.
     */
    static boolean isConcurrentDeleteConflict(SQLException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String message = t.getMessage() == null ? "" : t.getMessage().toLowerCase(java.util.Locale.ROOT);
            // "has deleted from it", or "has deleted inlined data from it" for rows inlined in the catalog
            if (message.contains("transaction conflict") && message.contains("another transaction has deleted")) {
                return true;
            }
        }
        return false;
    }

    /** A short, growing, jittered pause, so writers that conflicted together do not again. */
    private static void backOff(int attempt, List<String> files, SQLException conflict) {
        long millis = 20L * attempt + java.util.concurrent.ThreadLocalRandom.current().nextLong(20);
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            var notRegistered = new FilesNotRegisteredException(
                    "Interrupted before retrying a registration that conflicted: " + conflict.getMessage(), files, conflict);
            notRegistered.addSuppressed(interrupted);
            throw notRegistered;
        }
    }

    /**
     * Adds the batch's files to the DuckLake table, and each sidecar's files to its table, within one
     * transaction: if any file fails, all changes are rolled back.
     */
    private void addFilesOnce(List<String> files) throws SQLException {
        List<String> queries = new java.util.ArrayList<>();
        List<String> baseFiles = ingestionResult.filesCreated() == null ? List.of() : ingestionResult.filesCreated();
        baseFiles.forEach(file -> queries.add(addFileQuery(tableName, file)));
        ingestionResult.sidecarFiles().forEach((sidecarTable, sidecarFiles) ->
                sidecarFiles.forEach(file -> queries.add(addFileQuery(sidecarTable, file))));
        try (Connection conn = ConnectionPool.getConnection()) {
            executeInTxn(conn, queries, files, beforeCommit);
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
     *
     * <p>{@code beforeCommit} (tests only) runs between the statements and the commit; if it fails,
     * the transaction is rolled back, so restoring auto-commit cannot commit it.
     */
    static void executeInTxn(Connection conn, List<String> queries, List<String> files, Runnable beforeCommit)
            throws SQLException {
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
                beforeCommit.run();
            } catch (RuntimeException | Error e) {
                rollbackQuietly(conn, e);
                throw e;
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

    private String addFileQuery(String table, String file) {
        return ADD_FILE_QUERY.formatted(escapeLiteral(catalogName), escapeLiteral(table), escapeLiteral(file),
                escapeLiteral(schemaName));
    }

    private static String escapeLiteral(String value) {
        return value.replace("'", "''");
    }
}
