package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WatermarkSpecTest {

    private static final UUID BATCH = UUID.fromString("0199e0a0-1111-7000-8000-00000000000b");

    @TempDir Path tempDir;

    // ------------------------------------------------------------------ parsing

    @Test
    void absentKeysParseToNull() {
        assertNull(WatermarkSpec.fromParameters("q", null));
        assertNull(WatermarkSpec.fromParameters("q", Map.of()));
        assertNull(WatermarkSpec.fromParameters("q", Map.of("unrelated_key", "x")));
    }

    @Test
    void parsesFullSpecAndTrimsGroupColumns() {
        WatermarkSpec spec = WatermarkSpec.fromParameters("q", Map.of(
                WatermarkSpec.TABLE_KEY, "wm",
                WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts",
                WatermarkSpec.GROUP_COLUMNS_KEY, " county , state ",
                WatermarkSpec.MIN_TIMESTAMP_COLUMN_KEY, "min_ts",
                WatermarkSpec.MAX_TIMESTAMP_COLUMN_KEY, "max_ts",
                WatermarkSpec.ROW_COUNT_COLUMN_KEY, "row_count",
                WatermarkSpec.SNAPSHOT_ID_COLUMN_KEY, "min_commit_snapshot_id",
                WatermarkSpec.BATCH_ID_COLUMN_KEY, "batch_id"));
        assertEquals(new WatermarkSpec("wm", "ts", List.of("county", "state"), "min_ts", "max_ts", "row_count", "min_commit_snapshot_id", "batch_id"), spec);
    }

    @Test
    void toleratesFlattenedHoconListSyntax() {
        // A HOCON list arrives via unwrapped().toString() as "[county, state]".
        WatermarkSpec spec = WatermarkSpec.fromParameters("q", Map.of(
                WatermarkSpec.TABLE_KEY, "wm",
                WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts",
                WatermarkSpec.GROUP_COLUMNS_KEY, "[county, state]",
                WatermarkSpec.MIN_TIMESTAMP_COLUMN_KEY, "min_ts",
                WatermarkSpec.MAX_TIMESTAMP_COLUMN_KEY, "max_ts",
                WatermarkSpec.ROW_COUNT_COLUMN_KEY, "row_count",
                WatermarkSpec.SNAPSHOT_ID_COLUMN_KEY, "min_commit_snapshot_id",
                WatermarkSpec.BATCH_ID_COLUMN_KEY, "batch_id"));
        assertEquals(List.of("county", "state"), spec.groupColumns());
    }

    @Test
    void rejectsPartialBlankAndTypodSpecs() {
        // table without timestamp column
        assertThrows(IllegalArgumentException.class, () -> WatermarkSpec.fromParameters("q",
                Map.of(WatermarkSpec.TABLE_KEY, "wm")));
        // blank values pass no-null checks but must still be rejected
        assertThrows(IllegalArgumentException.class, () -> WatermarkSpec.fromParameters("q",
                Map.of(WatermarkSpec.TABLE_KEY, "wm", WatermarkSpec.TIMESTAMP_COLUMN_KEY, " ")));
        assertThrows(IllegalArgumentException.class, () -> WatermarkSpec.fromParameters("q",
                Map.of(WatermarkSpec.TABLE_KEY, "", WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts")));
        // typo'd watermark_ key must not silently disable the feature
        assertThrows(IllegalArgumentException.class, () -> WatermarkSpec.fromParameters("q",
                Map.of("watermark_tabel", "wm")));
        // malformed group entry
        assertThrows(IllegalArgumentException.class, () -> WatermarkSpec.fromParameters("q",
                Map.of(WatermarkSpec.TABLE_KEY, "wm", WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts",
                        WatermarkSpec.GROUP_COLUMNS_KEY, "county,,state")));
    }

    @Test
    void queueMappingValidatesWatermarkAtConstruction() {
        // The same validation runs from QueueIdToTableMapping so misconfig fails at config load.
        assertThrows(IllegalArgumentException.class, () -> new QueueIdToTableMapping(
                "q", "cat", "main", "t", Map.of(WatermarkSpec.TABLE_KEY, "wm"), null));
    }

    // ------------------------------------------------------- batch id and corrections

    /**
     * Required, not optional, so there is one behaviour: the table is always append-only and the
     * correction is always an append. A watermark table that predates the key must add the column
     * before the queue will start, which is a startup error naming the key rather than a surprise
     * at flush time.
     */
    @Test
    void batchIdColumnIsRequiredWhenAWatermarkIsConfigured() {
        Map<String, String> withoutKey = Map.of(
                WatermarkSpec.TABLE_KEY, "wm",
                WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts",
                WatermarkSpec.MIN_TIMESTAMP_COLUMN_KEY, "min_ts",
                WatermarkSpec.MAX_TIMESTAMP_COLUMN_KEY, "max_ts",
                WatermarkSpec.ROW_COUNT_COLUMN_KEY, "row_count",
                WatermarkSpec.SNAPSHOT_ID_COLUMN_KEY, "snap");
        var missing = assertThrows(IllegalArgumentException.class,
                () -> WatermarkSpec.fromParameters("q", withoutKey));
        assertTrue(missing.getMessage().contains(WatermarkSpec.BATCH_ID_COLUMN_KEY),
                "the error should name the missing key, got: " + missing.getMessage());

        Map<String, String> blank = new java.util.HashMap<>(withoutKey);
        blank.put(WatermarkSpec.BATCH_ID_COLUMN_KEY, "   ");
        var blankValue = assertThrows(IllegalArgumentException.class,
                () -> WatermarkSpec.fromParameters("q", blank));
        assertTrue(blankValue.getMessage().contains(WatermarkSpec.BATCH_ID_COLUMN_KEY), blankValue.getMessage());

        Map<String, String> set = new java.util.HashMap<>(withoutKey);
        set.put(WatermarkSpec.BATCH_ID_COLUMN_KEY, " batch_id ");
        assertEquals("batch_id", WatermarkSpec.fromParameters("q", set).batchIdColumn(), "trimmed");

        assertThrows(IllegalArgumentException.class, () -> new WatermarkSpec("wm", "ts", List.of(),
                "min_ts", "max_ts", "row_count", "snap", null), "the record itself must require it");
    }

    @Test
    void insertSqlCarriesTheBatchIdWhenTheColumnIsConfigured() {
        WatermarkSpec with = new WatermarkSpec("wm", "ts", List.of("county"), "min_ts", "max_ts", "row_count",
                "snap", "batch_id");
        assertEquals("INSERT INTO \"cat\".\"main\".\"wm\""
                        + " (\"county\", \"min_ts\", \"max_ts\", \"row_count\", \"snap\", \"batch_id\") "
                        + "VALUES ('king', '1', '9', '2', 7, UUID '" + BATCH + "')",
                with.insertSql("cat", "main", List.of(List.of("king", "1", "9", "2")), 7, BATCH));
    }

    /**
     * Every batch must carry an id, checked rather than quietly omitted: rows written without one
     * would leave nothing for a correction to attach to, losing the exact snapshot rarely and
     * invisibly. Only null is possible now — a malformed id is no longer representable, which is
     * part of what the {@link UUID} type buys over a string.
     */
    @Test
    void insertSqlRefusesRowsWithNoBatchId() {
        List<List<String>> rows = List.of(List.of("king", "1", "9", "2"));
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of("county"), "min_ts", "max_ts", "row_count",
                "snap", "batch_id");
        var e = assertThrows(IllegalArgumentException.class,
                () -> spec.insertSql("cat", "main", rows, 7, null));
        assertTrue(e.getMessage().contains(WatermarkSpec.BATCH_ID_COLUMN_KEY), e.getMessage());
    }

    /**
     * The correction is an INSERT of one sparse row, not an UPDATE: only the batch id and the
     * snapshot are set, so every aggregate column stays NULL and existing consumer queries
     * (sum of row counts, max of timestamps) are unaffected by it.
     */
    @Test
    void snapshotCorrectionIsASparseInsertKeyedByBatchId() {
        WatermarkSpec spec = new WatermarkSpec("ingest-watermark", "ts", List.of("county"), "min_ts", "max_ts",
                "row_count", "min_commit_snapshot_id", "batch id");
        String sql = spec.snapshotCorrectionSql("cat", "main", BATCH, 11);
        assertEquals("INSERT INTO \"cat\".\"main\".\"ingest-watermark\" (\"batch id\", \"min_commit_snapshot_id\") "
                + "VALUES (UUID '" + BATCH + "', 11)", sql);
        assertFalse(sql.contains("UPDATE"), "the correction must never be an in-place update");
    }

    // ------------------------------------------------------------------ computation

    private String relationOver(Connection conn, String values, String path) throws Exception {
        ConnectionPool.execute(conn,
                "COPY (SELECT * FROM (VALUES %s) AS t(county, state, ts)) TO '%s' (FORMAT parquet)"
                        .formatted(values, path));
        return "SELECT * FROM read_parquet('%s')".formatted(path);
    }

    @Test
    void keepsAllNullTimestampGroupSoItsRowCountIsRecorded() throws Exception {
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of("county", "state"), "min_ts", "max_ts", "row_count", "min_commit_snapshot_id", "batch_id");
        try (Connection conn = ConnectionPool.getConnection()) {
            String relation = relationOver(conn,
                    "('king','wa',TIMESTAMP '2026-08-01 03:00'),"
                            + "('king','wa',TIMESTAMP '2026-08-01 01:00'),"
                            + "('cook','il',TIMESTAMP '2026-08-02 05:00'),"
                            + "('null','nv',NULL::TIMESTAMP)",   // all-NULL group is kept, with a count
                    tempDir.resolve("grouped.parquet").toString());
            List<List<String>> rows = spec.computeRows(conn, relation);
            rows.sort(java.util.Comparator.comparing(r -> r.get(0)));
            assertEquals(3, rows.size());
            // JDBC renders TIMESTAMP via java.sql.Timestamp.toString (trailing ".0"); the string
            // round-trips through DuckDB's implicit VARCHAR cast on INSERT.
            // group, MIN, MAX, COUNT
            assertEquals(List.of("cook", "il", "2026-08-02 05:00:00.0", "2026-08-02 05:00:00.0", "1"), rows.get(0));
            assertEquals(List.of("king", "wa", "2026-08-01 01:00:00.0", "2026-08-01 03:00:00.0", "2"), rows.get(1));
            // all timestamps NULL: no min/max, but the rows still happened and are counted
            assertEquals(Arrays.asList("null", "nv", null, null, "1"), rows.get(2));
        }
    }

    @Test
    void globalModeSkipsOnlyGenuinelyEmptyInput() throws Exception {
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of(), "min_ts", "max_ts", "row_count", "min_commit_snapshot_id", "batch_id");
        try (Connection conn = ConnectionPool.getConnection()) {
            // zero-row relation: a global MIN would be a single NULL row — must be suppressed
            String empty = relationOver(conn, "('x','y',TIMESTAMP '2026-01-01')",
                    tempDir.resolve("empty.parquet").toString()) + " WHERE county = 'nope'";
            assertTrue(spec.computeRows(conn, empty).isEmpty());

            // all-NULL is NOT empty: one row, NULL min/max, real count
            String allNull = relationOver(conn, "('x','y',NULL::TIMESTAMP)",
                    tempDir.resolve("allnull.parquet").toString());
            assertEquals(List.of(Arrays.asList(null, null, "1")), spec.computeRows(conn, allNull));

            String real = relationOver(conn, "('x','y',TIMESTAMP '2026-01-01 08:00')",
                    tempDir.resolve("real.parquet").toString());
            assertEquals(List.of(List.of("2026-01-01 08:00:00.0", "2026-01-01 08:00:00.0", "1")),
                    spec.computeRows(conn, real));
        }
    }

    // ------------------------------------------------------------------ rendering

    @Test
    void insertSqlQuotesIdentifiersAndEscapesValues() {
        WatermarkSpec spec = new WatermarkSpec("ingest-watermark", "timestamp", List.of("county"), "min_ts", "max_ts", "row_count", "min_commit_snapshot_id", "batch_id");
        String sql = spec.insertSql("cat", "main",
                List.of(List.of("o'brien", "2026-01-01 00:00:00", "2026-01-01 06:00:00", "3")), 7, BATCH);
        assertEquals("INSERT INTO \"cat\".\"main\".\"ingest-watermark\""
                + " (\"county\", \"min_ts\", \"max_ts\", \"row_count\", \"min_commit_snapshot_id\", \"batch_id\") "
                + "VALUES ('o''brien', '2026-01-01 00:00:00', '2026-01-01 06:00:00', '3', 7, UUID '" + BATCH + "')", sql);
    }

    // ------------------------------------------------ MAX timestamp and row count

    @Test
    void aggregationAlwaysIncludesMinMaxAndCount() {
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of("county"), "min_ts", "max_ts", "rows", "min_commit_snapshot_id", "batch_id");
        assertEquals("SELECT \"county\", MIN(\"ts\") AS \"min_ts\", MAX(\"ts\") AS \"max_ts\","
                + " COUNT(*) AS \"rows\" FROM (SELECT 1) GROUP BY \"county\""
                + " HAVING COUNT(*) > 0",
                spec.aggregationSql("SELECT 1"));
    }

    @Test
    void insertColumnOrderMatchesAggregateOrder() {
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of("county"), "min_ts", "max_ts", "rows", "min_commit_snapshot_id", "batch_id");
        assertEquals("INSERT INTO \"cat\".\"main\".\"wm\""
                + " (\"county\", \"min_ts\", \"max_ts\", \"rows\", \"min_commit_snapshot_id\", \"batch_id\")"
                + " VALUES ('a', '1', '9', '42', 7, UUID '" + BATCH + "')",
                spec.insertSql("cat", "main", List.of(List.of("a", "1", "9", "42")), 7, BATCH));
    }

    @Test
    void blankMaxOrRowCountColumnIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new WatermarkSpec("wm", "ts", List.of(), "min_ts", "  ", "rows", "min_commit_snapshot_id", "batch_id"));
        assertThrows(IllegalArgumentException.class,
                () -> new WatermarkSpec("wm", "ts", List.of(), "min_ts", "max_ts", null, "min_commit_snapshot_id", "batch_id"));
        assertThrows(IllegalArgumentException.class,
                () -> new WatermarkSpec("wm", "ts", List.of(), "min_ts", "max_ts", "rows", null, "batch_id"));
    }

    @Test
    void missingMaxOrRowCountKeyIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> WatermarkSpec.fromParameters("q", Map.of(
                "watermark_table", "wm",
                "watermark_timestamp_column", "ts")));
        assertThrows(IllegalArgumentException.class, () -> WatermarkSpec.fromParameters("q", Map.of(
                "watermark_table", "wm",
                "watermark_timestamp_column", "ts",
                "watermark_max_timestamp_column", "max_ts")));
    }
}
