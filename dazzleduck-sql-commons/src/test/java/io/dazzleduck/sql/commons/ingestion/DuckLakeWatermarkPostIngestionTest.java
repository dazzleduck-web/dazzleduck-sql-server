package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * DuckLakePostIngestionTask's optional watermark append: when the queue mapping's
 * {@code additional_parameters} configure a watermark table, timestamp column and group columns,
 * the task inserts one MIN-timestamp row per group — computed from only the newly registered
 * Parquet files — in the SAME transaction as {@code ducklake_add_data_files}, so file
 * registration and watermark rows commit or roll back together.
 */
class DuckLakeWatermarkPostIngestionTest {

    @TempDir Path tempDir;
    static final String CATALOG = "watermark_lake";

    @BeforeEach
    void setUp() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), CATALOG, tempDir.resolve("data")),
                    "CREATE TABLE %s.main.facts (county VARCHAR, state VARCHAR, ts TIMESTAMP, v DOUBLE)".formatted(CATALOG),
                    "CREATE TABLE %s.main.ingest_watermark (county VARCHAR, state VARCHAR, ts TIMESTAMP)".formatted(CATALOG)
            });
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    private List<String> writeBatchFiles() throws Exception {
        String f1 = tempDir.resolve("b1.parquet").toString();
        String f2 = tempDir.resolve("b2.parquet").toString();
        try (Connection conn = ConnectionPool.getConnection()) {
            // (king, wa) spans both files; its true min ts (01:00) is in the second file.
            ConnectionPool.execute(conn, ("COPY (SELECT * FROM (VALUES "
                    + "('king', 'wa', TIMESTAMP '2026-08-01 03:00', 1.0::DOUBLE), "
                    + "('cook', 'il', TIMESTAMP '2026-08-02 05:00', 2.0::DOUBLE)"
                    + ") AS t(county, state, ts, v)) TO '%s' (FORMAT parquet)").formatted(f1));
            ConnectionPool.execute(conn, ("COPY (SELECT * FROM (VALUES "
                    + "('king', 'wa', TIMESTAMP '2026-08-01 01:00', 3.0::DOUBLE), "
                    + "('cook', 'in', TIMESTAMP '2026-08-03 09:00', 4.0::DOUBLE)"
                    + ") AS t(county, state, ts, v)) TO '%s' (FORMAT parquet)").formatted(f2));
        }
        return List.of(f1, f2);
    }

    private static IngestionResult result(List<String> files) {
        return new IngestionResult("q1", 1, "test-app", Map.of(), 4, files, null);
    }

    private static Map<String, String> watermarkParams() {
        return Map.of(
                DuckLakePostIngestionTask.WATERMARK_TABLE_KEY, "ingest_watermark",
                DuckLakePostIngestionTask.WATERMARK_TIMESTAMP_COLUMN_KEY, "ts",
                DuckLakePostIngestionTask.WATERMARK_GROUP_COLUMNS_KEY, "county, state");
    }

    @Test
    void appendsMinTimestampPerGroupInSameTransaction() throws Exception {
        List<String> files = writeBatchFiles();
        new DuckLakePostIngestionTask(result(files), CATALOG, "facts", "main", watermarkParams()).execute();

        try (Connection conn = ConnectionPool.getConnection()) {
            List<String> facts = collect(conn, "SELECT count(*)::VARCHAR AS r FROM %s.main.facts".formatted(CATALOG));
            assertEquals(List.of("4"), facts);

            List<String> watermarks = collect(conn,
                    ("SELECT county || '|' || state || '|' || strftime(ts, '%%Y-%%m-%%d %%H:%%M') AS r "
                            + "FROM %s.main.ingest_watermark ORDER BY county, state").formatted(CATALOG));
            assertEquals(List.of(
                    "cook|il|2026-08-02 05:00",
                    "cook|in|2026-08-03 09:00",
                    "king|wa|2026-08-01 01:00"),  // min across BOTH files, not per file
                    watermarks);
        }
    }

    @Test
    void watermarkFailureRollsBackFileRegistration() throws Exception {
        List<String> files = writeBatchFiles();
        Map<String, String> params = Map.of(
                DuckLakePostIngestionTask.WATERMARK_TABLE_KEY, "missing_watermark_table",
                DuckLakePostIngestionTask.WATERMARK_TIMESTAMP_COLUMN_KEY, "ts");
        var task = new DuckLakePostIngestionTask(result(files), CATALOG, "facts", "main", params);
        assertThrows(RuntimeException.class, task::execute);

        try (Connection conn = ConnectionPool.getConnection()) {
            // The add_data_files calls succeeded individually but the batch rolled back as one txn.
            assertEquals(List.of("0"), collect(conn, "SELECT count(*)::VARCHAR AS r FROM %s.main.facts".formatted(CATALOG)));
        }
    }

    @Test
    void noWatermarkParametersRegistersFilesOnly() throws Exception {
        List<String> files = writeBatchFiles();
        new DuckLakePostIngestionTask(result(files), CATALOG, "facts", "main", Map.of()).execute();

        try (Connection conn = ConnectionPool.getConnection()) {
            assertEquals(List.of("4"), collect(conn, "SELECT count(*)::VARCHAR AS r FROM %s.main.facts".formatted(CATALOG)));
            assertEquals(List.of("0"), collect(conn, "SELECT count(*)::VARCHAR AS r FROM %s.main.ingest_watermark".formatted(CATALOG)));
        }
    }

    @Test
    void watermarkTableWithoutTimestampColumnIsRejected() throws Exception {
        List<String> files = writeBatchFiles();
        Map<String, String> params = Map.of(DuckLakePostIngestionTask.WATERMARK_TABLE_KEY, "ingest_watermark");
        assertThrows(IllegalArgumentException.class,
                () -> new DuckLakePostIngestionTask(result(files), CATALOG, "facts", "main", params));
    }

    private static List<String> collect(Connection conn, String sql) throws Exception {
        List<String> rows = new ArrayList<>();
        ConnectionPool.collectAll(conn, sql, rs -> rs.getString("r")).forEach(rows::add);
        return rows;
    }
}
