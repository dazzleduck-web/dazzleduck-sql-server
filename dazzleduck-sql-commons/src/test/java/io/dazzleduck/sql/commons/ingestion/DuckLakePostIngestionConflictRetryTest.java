package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A DELETE on the table that commits while a batch is being registered makes the registration's
 * commit fail with a transaction conflict on stock DuckLake (duckdb/ducklake#1895), as a retention
 * job's would. Nothing was registered, so the task registers the same files again in a new
 * transaction rather than fail the batch back to the client.
 */
class DuckLakePostIngestionConflictRetryTest {

    @TempDir
    Path tempDir;

    static final String CATALOG = "conflict_retry_lake";
    static final String TABLE = CATALOG + ".main.logs";

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(tempDir.resolve("data"));
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0)".formatted(
                            tempDir.resolve("catalog"), CATALOG, tempDir.resolve("data")),
                    "CREATE TABLE %s (b VARCHAR, a VARCHAR)".formatted(TABLE)
            });
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    @Test
    void aConcurrentDeleteMakesTheRegistrationRetryNotFail() throws Exception {
        register(file("old", "('apple','fruit'), ('carrot','vegetable')"), () -> { });

        AtomicInteger attempts = new AtomicInteger();
        register(file("new", "('banana','fruit')"), () -> {
            if (attempts.incrementAndGet() == 1) {
                // the DELETE commits while this registration's transaction is open
                execute("DELETE FROM %s WHERE b = 'apple'".formatted(TABLE));
            }
        });

        assertEquals(2, attempts.get(), "the first commit conflicted, the second registered the files");
        assertEquals(List.of("banana", "carrot"), names());
    }

    @Test
    void aConflictOnEveryAttemptFailsAfterTheLastOne() throws Exception {
        register(file("old", "('apple','fruit')"), () -> { });

        AtomicInteger attempts = new AtomicInteger();
        String batch = file("new", "('banana','fruit')");
        RuntimeException e = assertThrows(RuntimeException.class, () -> register(batch, () -> {
            attempts.incrementAndGet();
            execute("INSERT INTO %s VALUES ('x', 'y')".formatted(TABLE));
            execute("DELETE FROM %s WHERE b = 'x'".formatted(TABLE));
        }));

        assertEquals(DuckLakePostIngestionTask.MAX_ATTEMPTS, attempts.get());
        assertTrue(e.getCause() instanceof SQLException sql && DuckLakePostIngestionTask.isTransactionConflict(sql),
                () -> "the last conflict is reported: " + e.getCause());
        assertEquals(List.of("apple"), names(), "nothing of the batch was registered");
    }

    @Test
    void onlyATransactionConflictIsRetried() {
        assertTrue(DuckLakePostIngestionTask.isTransactionConflict(new SQLException(
                "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\n"
                        + "Transaction conflict - attempting to insert into table with index \"1\" - but another transaction has deleted from it")));
        assertTrue(DuckLakePostIngestionTask.isTransactionConflict(new SQLException("commit failed",
                new SQLException("Transaction conflict - attempting to insert"))), "found in a cause too");
        assertFalse(DuckLakePostIngestionTask.isTransactionConflict(new SQLException(
                "IO Error: Could not connect to the metadata catalog")), "an unknown commit outcome is never retried");
    }

    /** Writes a Parquet file of {@code (b, a)} rows and returns its path. */
    private String file(String name, String values) throws SQLException {
        Path path = tempDir.resolve("data").resolve(name + ".parquet");
        execute("COPY (SELECT * FROM (VALUES %s) AS t(b, a)) TO '%s' (FORMAT parquet)".formatted(values, path));
        return path.toString();
    }

    private void register(String file, Runnable beforeCommit) {
        var result = new IngestionResult("q", 1L, "app", Map.of(), 1L, List.of(file));
        var task = new DuckLakePostIngestionTask(result, CATALOG, "logs", "main", Map.of());
        task.beforeCommitForTesting(beforeCommit);
        task.execute();
    }

    private static void execute(String sql) {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> names() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection();
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery("SELECT b FROM %s ORDER BY b".formatted(TABLE))) {
            var names = new java.util.ArrayList<String>();
            while (rs.next()) {
                names.add(rs.getString(1));
            }
            return names;
        }
    }
}
