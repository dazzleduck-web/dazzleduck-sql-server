package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DuckLakePostIngestionTaskTest {

    @TempDir
    Path tempDir;

    String catalog = "test_ducklake";
    String metadataDb = "__ducklake_metadata_" + catalog;

    String tableName = "logs";
    Path parquetFile;

    @BeforeEach
    void setupDuckLakeAndParquet() throws Exception {
        Files.createDirectories(tempDir.resolve("data"));

        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "LOAD arrow",
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(tempDir.resolve("catalog"), catalog, tempDir.resolve("data")),
                    "USE " + catalog,
                    "CREATE TABLE %s (b VARCHAR, a VARCHAR)".formatted(tableName)
            });
            parquetFile = tempDir.resolve("data").resolve("test.parquet");
            ConnectionPool.execute(conn, "COPY (SELECT * FROM (VALUES ('apple','fruit'), ('banana','fruit'), ('carrot','vegetable')) AS t(b,a)) TO '%s' (FORMAT 'parquet')".formatted(parquetFile));
        }
    }

    @AfterEach
    void detachDuckLake() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + catalog);
        }
    }

    @Test
    void shouldAddFilesToDuckLakeTableTest() throws Exception {
        IngestionResult ingestionResult = new IngestionResult("test-queue", 1L, "test-app", Map.of(), 3L, List.of(parquetFile.toString()));
        DuckLakePostIngestionTask task = new DuckLakePostIngestionTask(ingestionResult, catalog, tableName, "main", metadataDb);
        // execute (from DuckLakePostIngestionTask)
        task.execute();

        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator()) {

            ConnectionPool.execute(conn, "USE " + catalog);
            TestUtils.isEqual(conn, allocator, "SELECT * FROM logs ORDER BY b", "SELECT * FROM (VALUES ('apple','fruit'), ('banana','fruit'), ('carrot','vegetable')) AS t(b,a) ORDER BY b");
        }
    }

    @Test
    void shouldFailWhenParquetFileDoesNotExistTest() {
        IngestionResult ingestionResult = new IngestionResult("test-queue", 1L, "test-app", Map.of(), 3L, List.of(tempDir.resolve("missing.parquet").toString()));
        DuckLakePostIngestionTask task = new DuckLakePostIngestionTask(ingestionResult, catalog, tableName, "main", metadataDb);
        assertThrows(RuntimeException.class, task::execute);
    }

    @Test
    void shouldNoOpWhenFileListIsEmptyTest() throws Exception {
        IngestionResult ingestionResult = new IngestionResult("test-queue", 1L, "test-app", Map.of(), 0L, List.of());
        DuckLakePostIngestionTask task = new DuckLakePostIngestionTask(ingestionResult, catalog, tableName, "main", metadataDb);
        task.execute(); // should not throw
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "USE " + catalog);
            Long count = ConnectionPool.collectFirst(conn, "SELECT COUNT(*) FROM " + tableName, Long.class);
            assertEquals(0L, count);
        }
    }
}
