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
import java.util.UUID;

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
                    "CREATE TABLE %s.main.ingest_watermark (county VARCHAR, state VARCHAR, min_ts TIMESTAMP, max_ts TIMESTAMP, row_count BIGINT, min_commit_snapshot_id BIGINT, batch_id UUID)".formatted(CATALOG)
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

    // Recognisable in assertions, and still well-formed UUIDs, which the column now requires.
    private static final UUID BATCH_1 = UUID.fromString("00000000-0000-4000-8000-0000000000b1");
    private static final UUID BATCH_2 = UUID.fromString("00000000-0000-4000-8000-0000000000b2");

    private static final WatermarkSpec SPEC = new WatermarkSpec("ingest_watermark", "ts",
            List.of("county", "state"), "min_ts", "max_ts", "row_count", "min_commit_snapshot_id", "batch_id");

    private static Map<String, String> watermarkParams(String table) {
        return Map.of(
                WatermarkSpec.TABLE_KEY, table,
                WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts",
                WatermarkSpec.GROUP_COLUMNS_KEY, "county, state",
                WatermarkSpec.MIN_TIMESTAMP_COLUMN_KEY, "min_ts",
                WatermarkSpec.MAX_TIMESTAMP_COLUMN_KEY, "max_ts",
                WatermarkSpec.ROW_COUNT_COLUMN_KEY, "row_count",
                WatermarkSpec.SNAPSHOT_ID_COLUMN_KEY, "min_commit_snapshot_id",
                WatermarkSpec.BATCH_ID_COLUMN_KEY, "batch_id");
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

    /**
     * A TIMESTAMPTZ source's MIN/MAX are rendered in UTC, so whatever the zones (here a Tokyo JVM and a
     * Los Angeles DuckDB) a TIMESTAMPTZ watermark column stores the exact instant and a TIMESTAMP column
     * the UTC clock time. Whole-minute values matter: the JDBC driver's own rendering drops zero seconds
     * in a form DuckDB cannot parse.
     */
    @Test
    void timestampTzSourceIsWrittenInUtcInAnyZone() throws Exception {
        String f = tempDir.resolve("tz.parquet").toString();
        String watermarkColumns = "county VARCHAR, state VARCHAR, min_ts %1$s, max_ts %1$s, row_count BIGINT,"
                + " min_commit_snapshot_id BIGINT, batch_id UUID";
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "CREATE TABLE %s.main.facts_tz (county VARCHAR, state VARCHAR, ts TIMESTAMPTZ, v DOUBLE)".formatted(CATALOG),
                    "CREATE TABLE %s.main.watermark_tz (%s)".formatted(CATALOG, watermarkColumns.formatted("TIMESTAMPTZ")),
                    "CREATE TABLE %s.main.watermark_plain (%s)".formatted(CATALOG, watermarkColumns.formatted("TIMESTAMP"))
            });
            ConnectionPool.execute(conn, ("COPY (SELECT * FROM (VALUES "
                    + "('king', 'wa', TIMESTAMPTZ '2026-08-01 03:00:00+00', 1.0::DOUBLE), "
                    + "('king', 'wa', TIMESTAMPTZ '2026-08-01 22:30:00+00', 2.0::DOUBLE)"
                    + ") AS t(county, state, ts, v)) TO '%s' (FORMAT parquet)").formatted(f));
        }
        java.util.TimeZone jvmZone = java.util.TimeZone.getDefault();
        String duckDbZone = ConnectionPool.collectFirst("SELECT current_setting('TimeZone')", String.class);
        try {
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("Asia/Tokyo"));
            ConnectionPool.executeOnSingleton("SET GLOBAL TimeZone = 'America/Los_Angeles'");
            List<List<String>> rows;
            try (Connection conn = ConnectionPool.getConnection()) {
                rows = SPEC.computeRows(conn, "SELECT * FROM read_parquet(['%s'])".formatted(f));
            }
            assertEquals(List.of(List.of("king", "wa", "2026-08-01 03:00:00.000000+00", "2026-08-01 22:30:00.000000+00", "2")),
                    rows);

            // Each batch registers its own copy of the file, so the two tasks don't share one.
            String f2 = tempDir.resolve("tz2.parquet").toString();
            java.nio.file.Files.copy(Path.of(f), Path.of(f2));
            new DuckLakePostIngestionTask(result(List.of(f), rows), CATALOG, "facts_tz", "main",
                    watermarkParams("watermark_tz")).execute();
            new DuckLakePostIngestionTask(result(List.of(f2), rows), CATALOG, "facts_tz", "main",
                    watermarkParams("watermark_plain")).execute();

            try (Connection conn = ConnectionPool.getConnection()) {
                assertEquals(List.of("king|wa|2026-08-01 03:00|2026-08-01 22:30|2"), collect(conn,
                        ("SELECT county || '|' || state || '|' || strftime(timezone('UTC', min_ts), '%%Y-%%m-%%d %%H:%%M')"
                                + " || '|' || strftime(timezone('UTC', max_ts), '%%Y-%%m-%%d %%H:%%M')"
                                + " || '|' || row_count::VARCHAR AS r FROM %s.main.watermark_tz").formatted(CATALOG)),
                        "TIMESTAMPTZ column: the exact instants");
                assertEquals(List.of("king|wa|2026-08-01 03:00|2026-08-01 22:30|2"), collect(conn,
                        ("SELECT county || '|' || state || '|' || strftime(min_ts, '%%Y-%%m-%%d %%H:%%M')"
                                + " || '|' || strftime(max_ts, '%%Y-%%m-%%d %%H:%%M')"
                                + " || '|' || row_count::VARCHAR AS r FROM %s.main.watermark_plain").formatted(CATALOG)),
                        "TIMESTAMP column: the UTC clock time");
            }
        } finally {
            java.util.TimeZone.setDefault(jvmZone);
            ConnectionPool.executeOnSingleton("SET GLOBAL TimeZone = '%s'".formatted(duckDbZone));
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
     * The correction path, which only runs when a concurrent writer takes the predicted id. Every
     * other test here commits unopposed, so the prediction is exact, {@code verifySnapshotId}
     * returns early and {@link WatermarkSpec#snapshotCorrectionSql} never executes — leaving the
     * subtlest write in the feature unexercised, and silently so, since the verify step logs its
     * failures rather than throwing.
     *
     * <p>The lost race is reproduced by inserting a batch with a deliberately stale predicted id
     * (the value an earlier batch legitimately holds) and then running the correction, which is the
     * state a losing transaction actually commits in.
     *
     * <p>What the three rows discriminate is that the correction is an APPEND scoped by batch id,
     * so it must leave every pre-existing row exactly as it was — including a concurrent queue's
     * unstamped in-flight row on the same table, which an {@code IS NULL} predicate would have
     * swept up, and the earlier batch's rows, which hold precisely the stale id the loser predicted
     * and which a {@code = predicted} predicate would have clobbered.
     */
    @Test
    void correctionIsAppendedAndTouchesNothingThatWasAlreadyThere() throws Exception {
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
        UUID batchId = BATCH_1;
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    SPEC.insertSql(CATALOG, "main", computeRowsFor(second), firstSnapshot, batchId)});
        }
        long secondSnapshot = maxSnapshot();
        assertTrue(secondSnapshot > firstSnapshot,
                "the losing batch must commit after the id it predicted, got " + secondSnapshot + " <= " + firstSnapshot);

        // The correction, as DuckLakePostIngestionTask's verify step issues it.
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn,
                    SPEC.snapshotCorrectionSql(CATALOG, "main", batchId, secondSnapshot));
        }

        try (Connection conn = ConnectionPool.getConnection()) {
            assertEquals(
                    List.of("king=" + firstSnapshot, "pierce=" + firstSnapshot, "stray=null", "null=" + secondSnapshot),
                    collect(conn, ("""
                            SELECT coalesce(county, 'null') || '=' || coalesce(min_commit_snapshot_id::VARCHAR, 'null') AS r
                            FROM %s.main.ingest_watermark ORDER BY county NULLS LAST, min_commit_snapshot_id""").formatted(CATALOG)),
                    "the correction must be a new row, leaving the loser's own row and everyone else's untouched");

            // The reader rule: the exact snapshot of a batch is max(snapshot_id) over its batch id.
            assertEquals(List.of(String.valueOf(secondSnapshot)),
                    collect(conn, ("SELECT max(min_commit_snapshot_id)::VARCHAR AS r FROM %s.main.ingest_watermark "
                            + "WHERE batch_id = UUID '%s'").formatted(CATALOG, batchId)),
                    "max(snapshot_id) over the batch id must give the snapshot it really committed in");

            // And the correction carries no aggregates, so it cannot disturb an existing sum.
            assertEquals(List.of("9"),
                    collect(conn, "SELECT sum(row_count)::VARCHAR AS r FROM %s.main.ingest_watermark".formatted(CATALOG)),
                    "a sparse correction row must leave sum(row_count) alone: 1 (king) + 1 (pierce) + 7 (stray)");
        }
    }

    /**
     * The reason the correction needs a batch id at all: two batches in the same group can
     * aggregate identically, so without one a correction is indistinguishable from a second batch.
     * This pins the reader rules against a case that has both — a corrected batch and a
     * same-aggregate neighbour — where the true row count is 2.
     *
     * <p>Note which rule is which. Because a correction row is SPARSE, the aggregates need no
     * per-batch deduplication at all: a plain {@code sum(row_count)} is correct, since the
     * correction contributes NULL. The batch id is needed for the SNAPSHOT, not for the counts.
     * Grouping the aggregates by batch id is both unnecessary and wrong once a batch has more than
     * one group — see {@link #aggregatesMustNotBeGroupedByBatchId}.
     */
    @Test
    void theSnapshotRuleIsPerBatchAndTheCountsNeedNoDeduplication() throws Exception {
        List<String> a = writeSingleFile("d1.parquet", "same");
        List<String> b = writeSingleFile("d2.parquet", "same");
        try (Connection conn = ConnectionPool.getConnection()) {
            // Two batches whose rows are identical in every value column, one of them corrected.
            ConnectionPool.execute(conn, SPEC.insertSql(CATALOG, "main", computeRowsFor(a), 4, BATCH_1));
            ConnectionPool.execute(conn, SPEC.snapshotCorrectionSql(CATALOG, "main", BATCH_1, 7));
            ConnectionPool.execute(conn, SPEC.insertSql(CATALOG, "main", computeRowsFor(b), 9, BATCH_2));

            assertEquals(List.of(BATCH_1 + "=7", BATCH_2 + "=9"), collect(conn, ("""
                    SELECT batch_id::VARCHAR || '=' || max(min_commit_snapshot_id)::VARCHAR AS r
                    FROM %s.main.ingest_watermark WHERE batch_id IS NOT NULL
                    GROUP BY batch_id ORDER BY batch_id""").formatted(CATALOG)),
                    "the exact snapshot of a batch is max(snapshot_id) over its batch id");

            assertEquals(List.of("2"), collect(conn,
                    "SELECT sum(row_count)::VARCHAR AS r FROM %s.main.ingest_watermark".formatted(CATALOG)),
                    "a plain sum is correct because the correction row's row_count is NULL");

            assertEquals(List.of("1"), collect(conn, ("""
                    SELECT sum(row_count)::VARCHAR AS r FROM (
                        SELECT county, state, min_ts, max_ts, row_count
                        FROM %s.main.ingest_watermark WHERE row_count IS NOT NULL
                        GROUP BY county, state, min_ts, max_ts, row_count)""").formatted(CATALOG)),
                    "deduplicating by the value columns instead merges the two batches: wrong");
        }
    }

    /**
     * A batch holds one row per group, so an aggregate folded into a {@code GROUP BY batch_id}
     * keeps one group and silently drops the rest. Pinned because the obvious-looking query —
     * {@code any_value(row_count) ... GROUP BY batch_id} — is correct for single-group batches and
     * wrong for every other kind, which is the worst way for a reader rule to be wrong.
     */
    @Test
    void aggregatesMustNotBeGroupedByBatchId() throws Exception {
        List<String> files = writeBatchFiles();   // three groups: king/wa, cook/il, cook/in
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, SPEC.insertSql(CATALOG, "main", computeRows(files), 4, BATCH_1));
            ConnectionPool.execute(conn, SPEC.snapshotCorrectionSql(CATALOG, "main", BATCH_1, 7));

            assertEquals(List.of("4"), collect(conn,
                    "SELECT sum(row_count)::VARCHAR AS r FROM %s.main.ingest_watermark".formatted(CATALOG)),
                    "the batch ingested 4 rows across its three groups");

            // any_value picks an arbitrary group, so assert the property rather than which one it
            // picked: whatever it returns is one group's count, and so short of the batch's total.
            assertEquals(List.of("true"), collect(conn, ("""
                    SELECT (folded < 4)::VARCHAR AS r FROM (
                        SELECT sum(row_count) AS folded FROM (
                            SELECT batch_id, any_value(row_count) AS row_count
                            FROM %s.main.ingest_watermark WHERE batch_id IS NOT NULL GROUP BY batch_id))""")
                    .formatted(CATALOG)),
                    "folding the count into GROUP BY batch_id keeps one group of three, undercounting the batch");

            assertEquals(List.of("7"), collect(conn, ("""
                    SELECT max(min_commit_snapshot_id)::VARCHAR AS r
                    FROM %s.main.ingest_watermark WHERE batch_id = UUID '%s'""").formatted(CATALOG, BATCH_1)),
                    "the snapshot rule is unaffected by the group count");
        }
    }

    /**
     * "Is this batch visible as of snapshot N" must be asked of the GREATEST value recorded for the
     * batch, not of the raw column. Pinned because appending the correction rather than updating
     * the row in place made this a live trap: the row as first written keeps its predicted id
     * forever, so the raw column reports a corrected batch as visible earlier than it is. The
     * earlier in-place UPDATE overwrote that value, which is why the question did not arise before.
     */
    @Test
    void visibilityMustBeAskedOfThePerBatchMaximum() throws Exception {
        List<String> files = writeSingleFile("v1.parquet", "king");
        try (Connection conn = ConnectionPool.getConnection()) {
            // Predicted 4, really committed in 7: the shape a lost race leaves behind.
            ConnectionPool.execute(conn, SPEC.insertSql(CATALOG, "main", computeRowsFor(files), 4, BATCH_1));
            ConnectionPool.execute(conn, SPEC.snapshotCorrectionSql(CATALOG, "main", BATCH_1, 7));

            assertEquals(List.of(), collect(conn, ("""
                    SELECT batch_id::VARCHAR AS r FROM %s.main.ingest_watermark
                    GROUP BY batch_id HAVING max(min_commit_snapshot_id) <= 5""").formatted(CATALOG)),
                    "the batch committed in 7, so it is not visible as of 5");

            assertEquals(List.of("king"), collect(conn, ("""
                    SELECT county AS r FROM %s.main.ingest_watermark
                    WHERE min_commit_snapshot_id <= 5 AND county IS NOT NULL""").formatted(CATALOG)),
                    "reading the raw column instead claims visibility at 5: the answer the docs warn against");

            // A migrated table also holds rows from before the column existed, which carry no id.
            // SQL groups those into ONE null batch, so grouping them admits or excludes the whole
            // history together — here a single historical batch at 9 would hide the one at 2.
            ConnectionPool.execute(conn, ("INSERT INTO %s.main.ingest_watermark "
                    + "(county, state, min_ts, max_ts, row_count, min_commit_snapshot_id) VALUES "
                    + "('legacy-early', 'zz', NULL, NULL, 1, 2), ('legacy-late', 'zz', NULL, NULL, 1, 9)")
                    .formatted(CATALOG));

            assertEquals(List.of(), collect(conn, ("""
                    SELECT 'grouped' AS r FROM %s.main.ingest_watermark
                    WHERE batch_id IS NULL GROUP BY batch_id HAVING max(min_commit_snapshot_id) <= 5""")
                    .formatted(CATALOG)),
                    "grouping the id-less rows hides the batch at snapshot 2 behind the one at 9");

            assertEquals(List.of("legacy-early"), collect(conn, ("""
                    SELECT county AS r FROM %s.main.ingest_watermark
                    WHERE batch_id IS NULL AND min_commit_snapshot_id <= 5 AND county IS NOT NULL""")
                    .formatted(CATALOG)),
                    "read directly, they are correct: the earlier code corrected those rows in place");
        }
    }

    /**
     * The batch id survives the write-then-register path, which is where
     * {@code doc/DUCKLAKE_NO_INLINING_SPEC.md} takes these rows next: a UUID column is written to
     * Parquet as {@code FIXED_LEN_BYTE_ARRAY(16)} with the standard UUID logical type, and
     * {@code ducklake_add_data_files} accepts that file into a UUID column.
     *
     * <p>Pinned here because {@code add_data_files} does NOT cast — the same file with the id as
     * VARCHAR is rejected with {@code Expected UUID, found type VARCHAR} — so this is the fact that
     * the sidecar writer will have to respect, and it is cheaper to catch its loss here than in
     * that conversion.
     */
    @Test
    void theBatchIdRoundTripsThroughParquetIntoAUuidColumn() throws Exception {
        String file = tempDir.resolve("sidecar.parquet").toString();
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, ("""
                    COPY (SELECT 'king' AS county, 'wa' AS state,
                                 TIMESTAMP '2026-08-01 01:00' AS min_ts, TIMESTAMP '2026-08-01 03:00' AS max_ts,
                                 2::BIGINT AS row_count, 4::BIGINT AS min_commit_snapshot_id,
                                 UUID '%s' AS batch_id)
                    TO '%s' (FORMAT parquet)""").formatted(BATCH_1, file));

            assertEquals(List.of("FIXED_LEN_BYTE_ARRAY|16"), collect(conn,
                    "SELECT type || '|' || type_length AS r FROM parquet_schema('%s') WHERE name = 'batch_id'"
                            .formatted(file)),
                    "a UUID column must be 16 raw bytes in the file, not 36 characters of text");

            ConnectionPool.execute(conn, ("CALL ducklake_add_data_files('%s', 'ingest_watermark', '%s', "
                    + "schema => 'main', ignore_extra_columns => true, allow_missing => true)")
                    .formatted(CATALOG, file));

            assertEquals(List.of(BATCH_1 + "|UUID"), collect(conn, ("""
                    SELECT batch_id::VARCHAR || '|' || typeof(batch_id) AS r
                    FROM %s.main.ingest_watermark WHERE county = 'king'""").formatted(CATALOG)),
                    "the registered file's batch id reads back as a UUID");
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
