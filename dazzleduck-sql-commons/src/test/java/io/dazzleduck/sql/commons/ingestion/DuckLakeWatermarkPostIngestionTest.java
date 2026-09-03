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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                    "CREATE TABLE %s.main.ingest_watermark (county VARCHAR, state VARCHAR, min_ts TIMESTAMP, max_ts TIMESTAMP, row_count BIGINT, min_commit_snapshot_id BIGINT)".formatted(CATALOG)
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

    private static final WatermarkSpec SPEC = new WatermarkSpec("ingest_watermark", "ts", List.of("county", "state"), "min_ts", "max_ts", "row_count", "min_commit_snapshot_id");

    private static Map<String, String> watermarkParams(String table) {
        return Map.of(
                WatermarkSpec.TABLE_KEY, table,
                WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts",
                WatermarkSpec.GROUP_COLUMNS_KEY, "county, state",
                WatermarkSpec.MIN_TIMESTAMP_COLUMN_KEY, "min_ts",
                WatermarkSpec.MAX_TIMESTAMP_COLUMN_KEY, "max_ts",
                WatermarkSpec.ROW_COUNT_COLUMN_KEY, "row_count",
                WatermarkSpec.SNAPSHOT_ID_COLUMN_KEY, "min_commit_snapshot_id");
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

    // ── watermark_snapshot_id_column ────────────────────────────────────────

    /** One file with a single row, so a batch can be run more than once with distinct files. */
    private List<String> writeSingleFile(String name, String county) throws Exception {
        String f = tempDir.resolve(name).toString();
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, ("COPY (SELECT * FROM (VALUES "
                    + "('%s', 'wa', TIMESTAMP '2026-08-01 03:00', 1.0::DOUBLE)"
                    + ") AS t(county, state, ts, v)) TO '%s' (FORMAT parquet)").formatted(county, f));
        }
        return List.of(f);
    }

    private List<List<String>> computeRowsFor(List<String> files) throws Exception {
        String relation = "SELECT * FROM read_parquet(['%s'])".formatted(String.join("', '", files));
        try (Connection conn = ConnectionPool.getConnection()) {
            return SPEC.computeRows(conn, relation);
        }
    }

    @Test
    void stampsTheSnapshotTheFilesWereRegisteredIn() throws Exception {
        List<String> files = writeBatchFiles();
        List<List<String>> rows = computeRows(files);
        new DuckLakePostIngestionTask(result(files, rows), CATALOG, "facts", "main",
                watermarkParams("ingest_watermark")).execute();

        try (Connection conn = ConnectionPool.getConnection()) {
            // every watermark row is stamped — no NULL left behind
            assertEquals(List.of("0"), collect(conn,
                    "SELECT count(*)::VARCHAR AS r FROM %s.main.ingest_watermark WHERE min_commit_snapshot_id IS NULL".formatted(CATALOG)));
            // and the value is exactly the snapshot the data files became visible in
            assertEquals(List.of("1"), collect(conn, ("""
                    SELECT count(DISTINCT min_commit_snapshot_id)::VARCHAR AS r FROM (
                      SELECT min_commit_snapshot_id FROM %s.main.ingest_watermark
                      UNION
                      SELECT DISTINCT begin_snapshot FROM __ducklake_metadata_%s.ducklake_data_file
                      WHERE path IN ('%s', '%s'))""")
                    .formatted(CATALOG, CATALOG, files.get(0), files.get(1))));
        }
    }

    /** Consecutive batches each land on their own snapshot rather than sharing or overwriting one. */
    @Test
    void eachBatchIsStampedWithItsOwnSnapshot() throws Exception {
        Map<String, String> params = watermarkParams("ingest_watermark");

        List<String> first = writeSingleFile("s1.parquet", "king");
        new DuckLakePostIngestionTask(result(first, computeRowsFor(first)), CATALOG, "facts", "main", params).execute();

        List<String> second = writeSingleFile("s2.parquet", "pierce");
        new DuckLakePostIngestionTask(result(second, computeRowsFor(second)), CATALOG, "facts", "main", params).execute();

        try (Connection conn = ConnectionPool.getConnection()) {
            // two batches, two rows, two different snapshots — the first was not re-stamped
            assertEquals(List.of("2"), collect(conn,
                    "SELECT count(DISTINCT min_commit_snapshot_id)::VARCHAR AS r FROM %s.main.ingest_watermark".formatted(CATALOG)));
            assertEquals(List.of("0"), collect(conn,
                    "SELECT count(*)::VARCHAR AS r FROM %s.main.ingest_watermark WHERE min_commit_snapshot_id IS NULL".formatted(CATALOG)));
            // each row carries the snapshot of its own file
            assertEquals(List.of("king", "pierce"), collect(conn, ("""
                    SELECT w.county AS r FROM %s.main.ingest_watermark w
                    JOIN __ducklake_metadata_%s.ducklake_data_file f ON f.begin_snapshot = w.min_commit_snapshot_id
                    ORDER BY w.county""").formatted(CATALOG, CATALOG)));
        }
    }

    /**
     * The stamping must touch only the rows of its own batch. A pre-existing unstamped row — a
     * concurrent queue's in-flight batch on a shared watermark table, or a row from before the
     * column was added — must be left alone. This is precisely what rowid scoping buys over a
     * blanket {@code WHERE min_commit_snapshot_id IS NULL}, which would sweep the stray row up too.
     */
    @Test
    void aStrayUnstampedRowIsNotClaimedByThisBatch() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, ("INSERT INTO %s.main.ingest_watermark "
                    + "(county, state, min_ts, max_ts, row_count, min_commit_snapshot_id) VALUES "
                    + "('stray', 'zz', TIMESTAMP '2026-01-01 00:00', TIMESTAMP '2026-01-01 00:00', 7, NULL)")
                    .formatted(CATALOG));
        }

        List<String> files = writeBatchFiles();
        new DuckLakePostIngestionTask(result(files, computeRows(files)), CATALOG, "facts", "main",
                watermarkParams("ingest_watermark")).execute();

        try (Connection conn = ConnectionPool.getConnection()) {
            // the stray row is still NULL; only this batch's 3 rows were stamped
            assertEquals(List.of("stray"), collect(conn,
                    ("SELECT county AS r FROM %s.main.ingest_watermark WHERE min_commit_snapshot_id IS NULL")
                            .formatted(CATALOG)));
            assertEquals(List.of("3"), collect(conn,
                    ("SELECT count(*)::VARCHAR AS r FROM %s.main.ingest_watermark WHERE min_commit_snapshot_id IS NOT NULL")
                            .formatted(CATALOG)));
        }
    }

    /**
     * The whole point of writing the id inline: the batch must still be ONE transaction, i.e. one
     * snapshot. A patch-it-up-afterwards implementation commits twice and shows up here as two.
     */
    @Test
    void theWholeBatchIncludingTheSnapshotIdIsASingleSnapshot() throws Exception {
        long before;
        try (Connection conn = ConnectionPool.getConnection()) {
            before = Long.parseLong(collect(conn,
                    "SELECT max(snapshot_id)::VARCHAR AS r FROM __ducklake_metadata_%s.ducklake_snapshot".formatted(CATALOG)).get(0));
        }

        List<String> files = writeBatchFiles();
        new DuckLakePostIngestionTask(result(files, computeRows(files)), CATALOG, "facts", "main",
                watermarkParams("ingest_watermark")).execute();

        try (Connection conn = ConnectionPool.getConnection()) {
            long after = Long.parseLong(collect(conn,
                    "SELECT max(snapshot_id)::VARCHAR AS r FROM __ducklake_metadata_%s.ducklake_snapshot".formatted(CATALOG)).get(0));
            assertEquals(1, after - before, "ingest must advance the catalog by exactly one snapshot");
            // and that single snapshot is the one recorded on the rows
            assertEquals(List.of(String.valueOf(after)), collect(conn,
                    "SELECT DISTINCT min_commit_snapshot_id::VARCHAR AS r FROM %s.main.ingest_watermark".formatted(CATALOG)));
        }
    }

    /** The key is required whenever a watermark is configured — a spec without it fails to parse. */
    @Test
    void watermarkWithoutSnapshotColumnIsRejected() {
        Map<String, String> withoutKey = Map.of(
                WatermarkSpec.TABLE_KEY, "ingest_watermark",
                WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts",
                WatermarkSpec.MIN_TIMESTAMP_COLUMN_KEY, "min_ts",
                WatermarkSpec.MAX_TIMESTAMP_COLUMN_KEY, "max_ts",
                WatermarkSpec.ROW_COUNT_COLUMN_KEY, "row_count");
        var e = assertThrows(IllegalArgumentException.class,
                () -> WatermarkSpec.fromParameters("q1", withoutKey));
        assertTrue(e.getMessage().contains(WatermarkSpec.SNAPSHOT_ID_COLUMN_KEY),
                "the error should name the missing key, got: " + e.getMessage());
    }

    /**
     * The repair path, which only runs when a concurrent writer takes the predicted id. Every other
     * test here commits unopposed, so the prediction is exact, {@code verifySnapshotId} returns
     * early and {@link WatermarkSpec#updateSnapshotIdSql} never executes — leaving the subtlest
     * query in the feature unexercised, and silently so, since the verify step logs its failures
     * rather than throwing.
     *
     * <p>The lost race is reproduced by inserting a batch with a deliberately stale predicted id
     * (the value an earlier batch legitimately holds) and then running the repair, which is the
     * state a losing transaction actually commits in. The three rows discriminate between the
     * rowid scoping and both simpler predicates that look adequate:
     * <ul>
     *   <li>{@code WHERE min_commit_snapshot_id IS NULL} would sweep up the stray row belonging to
     *       a concurrent queue's in-flight batch on the same table.</li>
     *   <li>{@code WHERE min_commit_snapshot_id = predicted} would clobber the earlier batch's
     *       rows, which correctly hold exactly that id.</li>
     * </ul>
     */
    @Test
    void repairStampsOnlyTheRowsOfTheBatchThatLostTheRace() throws Exception {
        // An earlier batch commits unopposed: its rows hold the exact snapshot it committed as.
        List<String> first = writeSingleFile("r1.parquet", "king");
        new DuckLakePostIngestionTask(result(first, computeRowsFor(first)), CATALOG, "facts", "main",
                watermarkParams("ingest_watermark")).execute();
        long firstSnapshot = maxSnapshot();

        // A concurrent queue's in-flight batch on the shared watermark table: inserted, unstamped.
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, ("INSERT INTO %s.main.ingest_watermark "
                    + "(county, state, min_ts, max_ts, row_count, min_commit_snapshot_id) VALUES "
                    + "('stray', 'zz', TIMESTAMP '2026-01-01 00:00', TIMESTAMP '2026-01-01 00:00', 7, NULL)")
                    .formatted(CATALOG));
        }

        // The losing batch: it predicted firstSnapshot, which was already taken by the time it
        // committed, so its rows land carrying an id that belongs to someone else.
        List<String> second = writeSingleFile("r2.parquet", "pierce");
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    SPEC.insertSql(CATALOG, "main", computeRowsFor(second), firstSnapshot)});
        }
        long secondSnapshot = maxSnapshot();
        assertTrue(secondSnapshot > firstSnapshot,
                "the losing batch must commit after the id it predicted, got " + secondSnapshot + " <= " + firstSnapshot);

        // The repair, as DuckLakePostIngestionTask's verify step issues it.
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, SPEC.updateSnapshotIdSql(CATALOG, "main", secondSnapshot));
        }

        try (Connection conn = ConnectionPool.getConnection()) {
            assertEquals(
                    List.of("king=" + firstSnapshot, "pierce=" + secondSnapshot, "stray=null"),
                    collect(conn, ("""
                            SELECT county || '=' || coalesce(min_commit_snapshot_id::VARCHAR, 'null') AS r
                            FROM %s.main.ingest_watermark ORDER BY county""").formatted(CATALOG)),
                    "the repair must tighten only the losing batch's rows");
        }
    }

    private long maxSnapshot() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            return Long.parseLong(collect(conn,
                    "SELECT max(snapshot_id)::VARCHAR AS r FROM __ducklake_metadata_%s.ducklake_snapshot"
                            .formatted(CATALOG)).get(0));
        }
    }

    private static List<String> collect(Connection conn, String sql) throws Exception {
        List<String> rows = new ArrayList<>();
        ConnectionPool.collectAll(conn, sql, rs -> rs.getString("r")).forEach(rows::add);
        return rows;
    }
}
