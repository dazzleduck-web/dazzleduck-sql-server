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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * DuckLakePostIngestionTask's watermark append: watermark rows are precomputed at write time
 * (see {@link WatermarkSpec#computeRows}) and carried on {@link IngestionResult#watermarkRows()};
 * the task appends them via INSERT ... VALUES in the SAME transaction as
 * {@code ducklake_add_data_files}, so file registration and watermark rows commit or roll back
 * together. The task itself never re-reads the written files.
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
                    "CREATE TABLE %s.main.ingest_watermark (county VARCHAR, state VARCHAR, min_ts TIMESTAMP, max_ts TIMESTAMP, row_count BIGINT)".formatted(CATALOG)
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

    private static final WatermarkSpec SPEC = new WatermarkSpec("ingest_watermark", "ts", List.of("county", "state"), "min_ts", "max_ts", "row_count");

    private static Map<String, String> watermarkParams(String table) {
        return Map.of(
                WatermarkSpec.TABLE_KEY, table,
                WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts",
                WatermarkSpec.GROUP_COLUMNS_KEY, "county, state",
                WatermarkSpec.MIN_TIMESTAMP_COLUMN_KEY, "min_ts",
                WatermarkSpec.MAX_TIMESTAMP_COLUMN_KEY, "max_ts",
                WatermarkSpec.ROW_COUNT_COLUMN_KEY, "row_count");
    }

    /** Computes the rows the way ParquetIngestionQueue does at write time: over the source relation. */
    private List<List<String>> computeRows(List<String> files) throws Exception {
        String relation = "SELECT * FROM read_parquet(['%s', '%s'])".formatted(files.get(0), files.get(1));
        try (Connection conn = ConnectionPool.getConnection()) {
            return SPEC.computeRows(conn, relation);
        }
    }

    private static IngestionResult result(List<String> files, List<List<String>> watermarkRows) {
        return new IngestionResult("q1", 1, "test-app", Map.of(), 4, files, null, watermarkRows);
    }

    @Test
    void allNullTimestampGroupIsWrittenWithNullMinMaxAndRealCount() throws Exception {
        String f = tempDir.resolve("allnull.parquet").toString();
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, ("COPY (SELECT * FROM (VALUES "
                    + "('king', 'wa', NULL::TIMESTAMP, 1.0::DOUBLE), "
                    + "('king', 'wa', NULL::TIMESTAMP, 2.0::DOUBLE)"
                    + ") AS t(county, state, ts, v)) TO '%s' (FORMAT parquet)").formatted(f));
        }
        List<String> files = List.of(f);
        List<List<String>> rows;
        try (Connection conn = ConnectionPool.getConnection()) {
            rows = SPEC.computeRows(conn, "SELECT * FROM read_parquet(['%s'])".formatted(f));
        }
        new DuckLakePostIngestionTask(result(files, rows), CATALOG, "facts", "main",
                watermarkParams("ingest_watermark")).execute();

        try (Connection conn = ConnectionPool.getConnection()) {
            // the batch is recorded: NULL min/max, but the two rows are counted
            assertEquals(List.of("king|wa|NULL|NULL|2"), collect(conn,
                    ("SELECT county || '|' || state || '|' || coalesce(min_ts::VARCHAR, 'NULL')"
                            + " || '|' || coalesce(max_ts::VARCHAR, 'NULL') || '|' || row_count::VARCHAR AS r "
                            + "FROM %s.main.ingest_watermark").formatted(CATALOG)));
        }
    }

    @Test
    void appendsPrecomputedRowsInSameTransaction() throws Exception {
        List<String> files = writeBatchFiles();
        List<List<String>> rows = computeRows(files);
        new DuckLakePostIngestionTask(result(files, rows), CATALOG, "facts", "main",
                watermarkParams("ingest_watermark")).execute();

        try (Connection conn = ConnectionPool.getConnection()) {
            assertEquals(List.of("4"), collect(conn, "SELECT count(*)::VARCHAR AS r FROM %s.main.facts".formatted(CATALOG)));

            List<String> watermarks = collect(conn,
                    ("SELECT county || '|' || state || '|' || strftime(min_ts, '%%Y-%%m-%%d %%H:%%M') "
                            + "|| '|' || strftime(max_ts, '%%Y-%%m-%%d %%H:%%M') || '|' || row_count::VARCHAR AS r "
                            + "FROM %s.main.ingest_watermark ORDER BY county, state").formatted(CATALOG));
            // county|state|MIN|MAX|count — aggregated across BOTH files, not per file
            assertEquals(List.of(
                    "cook|il|2026-08-02 05:00|2026-08-02 05:00|1",
                    "cook|in|2026-08-03 09:00|2026-08-03 09:00|1",
                    "king|wa|2026-08-01 01:00|2026-08-01 03:00|2"),
                    watermarks);
        }
    }

    @Test
    void watermarkFailureRollsBackFileRegistration() throws Exception {
        List<String> files = writeBatchFiles();
        List<List<String>> rows = computeRows(files);
        var task = new DuckLakePostIngestionTask(result(files, rows), CATALOG, "facts", "main",
                watermarkParams("missing_watermark_table"));
        assertThrows(RuntimeException.class, task::execute);

        try (Connection conn = ConnectionPool.getConnection()) {
            // The add_data_files calls succeeded individually but the batch rolled back as one txn.
            assertEquals(List.of("0"), collect(conn, "SELECT count(*)::VARCHAR AS r FROM %s.main.facts".formatted(CATALOG)));
        }
    }

    @Test
    void emptyWatermarkRowsSkipTheInsert() throws Exception {
        List<String> files = writeBatchFiles();
        new DuckLakePostIngestionTask(result(files, List.of()), CATALOG, "facts", "main",
                watermarkParams("ingest_watermark")).execute();

        try (Connection conn = ConnectionPool.getConnection()) {
            assertEquals(List.of("4"), collect(conn, "SELECT count(*)::VARCHAR AS r FROM %s.main.facts".formatted(CATALOG)));
            assertEquals(List.of("0"), collect(conn, "SELECT count(*)::VARCHAR AS r FROM %s.main.ingest_watermark".formatted(CATALOG)));
        }
    }

    @Test
    void noWatermarkParametersRegistersFilesOnly() throws Exception {
        List<String> files = writeBatchFiles();
        new DuckLakePostIngestionTask(result(files, null), CATALOG, "facts", "main", Map.of()).execute();

        try (Connection conn = ConnectionPool.getConnection()) {
            assertEquals(List.of("4"), collect(conn, "SELECT count(*)::VARCHAR AS r FROM %s.main.facts".formatted(CATALOG)));
            assertEquals(List.of("0"), collect(conn, "SELECT count(*)::VARCHAR AS r FROM %s.main.ingest_watermark".formatted(CATALOG)));
        }
    }

    private static List<String> collect(Connection conn, String sql) throws Exception {
        List<String> rows = new ArrayList<>();
        ConnectionPool.collectAll(conn, sql, rs -> rs.getString("r")).forEach(rows::add);
        return rows;
    }

    /**
     * With {@code watermark_snapshot_id_column} configured, the row records the snapshot the batch
     * committed at — the same one that made the data files visible. The value cannot come from
     * inside the ingest transaction (a transaction cannot learn the snapshot it is creating), so
     * the task stamps it immediately afterwards on the same connection.
     */
    @Test
    void recordsTheSnapshotTheBatchCommittedAt() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "ALTER TABLE %s.main.ingest_watermark ADD COLUMN snapshot_id BIGINT".formatted(CATALOG));
        }
        List<String> files = writeBatchFiles();

        Map<String, String> params = new java.util.HashMap<>(watermarkParams("ingest_watermark"));
        params.put(WatermarkSpec.SNAPSHOT_ID_COLUMN_KEY, "snapshot_id");
        new DuckLakePostIngestionTask(result(files, computeRows(files)), CATALOG, "facts", "main", params).execute();

        try (Connection conn = ConnectionPool.getConnection();
             var st = conn.createStatement()) {
            // Every watermark row carries an id, and they all carry the SAME one: a batch is one
            // transaction, so one snapshot.
            try (var rs = st.executeQuery(
                    "SELECT count(*), count(snapshot_id), count(DISTINCT snapshot_id) FROM %s.main.ingest_watermark".formatted(CATALOG))) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1), "three groups in the batch");
                assertEquals(3, rs.getInt(2), "none left NULL");
                assertEquals(1, rs.getInt(3), "one batch, one snapshot");
            }
            // And it is the snapshot the DATA landed in, not a later one: reading the facts table
            // at that version must already show the batch's rows. AT (VERSION => ...) takes no
            // subquery, so the id is read out first.
            long stamped;
            try (var rs = st.executeQuery("SELECT max(snapshot_id) FROM %s.main.ingest_watermark".formatted(CATALOG))) {
                assertTrue(rs.next());
                stamped = rs.getLong(1);
            }
            try (var rs = st.executeQuery(
                    "SELECT count(*) FROM %s.main.facts AT (VERSION => %d)".formatted(CATALOG, stamped))) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1), "the stamped snapshot already contains the batch's rows");
            }
        }
    }

    /** Without the key, the column is never touched — the shape every existing queue keeps. */
    @Test
    void leavesTheSnapshotColumnAloneWhenUnconfigured() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "ALTER TABLE %s.main.ingest_watermark ADD COLUMN snapshot_id BIGINT".formatted(CATALOG));
        }
        List<String> files = writeBatchFiles();

        new DuckLakePostIngestionTask(result(files, computeRows(files)), CATALOG, "facts", "main",
                watermarkParams("ingest_watermark")).execute();

        try (Connection conn = ConnectionPool.getConnection();
             var st = conn.createStatement();
             var rs = st.executeQuery("SELECT count(*), count(snapshot_id) FROM %s.main.ingest_watermark".formatted(CATALOG))) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(0, rs.getInt(2), "unconfigured: the column stays NULL");
        }
    }
}
