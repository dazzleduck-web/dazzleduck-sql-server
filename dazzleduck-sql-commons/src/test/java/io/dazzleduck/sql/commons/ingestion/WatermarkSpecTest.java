package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WatermarkSpecTest {

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
                WatermarkSpec.ROW_COUNT_COLUMN_KEY, "row_count"));
        assertEquals(new WatermarkSpec("wm", "ts", List.of("county", "state"), "min_ts", "max_ts", "row_count"), spec);
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
                WatermarkSpec.ROW_COUNT_COLUMN_KEY, "row_count"));
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

    // ------------------------------------------------------------------ computation

    private String relationOver(Connection conn, String values, String path) throws Exception {
        ConnectionPool.execute(conn,
                "COPY (SELECT * FROM (VALUES %s) AS t(county, state, ts)) TO '%s' (FORMAT parquet)"
                        .formatted(values, path));
        return "SELECT * FROM read_parquet('%s')".formatted(path);
    }

    @Test
    void keepsAllNullTimestampGroupSoItsRowCountIsRecorded() throws Exception {
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of("county", "state"), "min_ts", "max_ts", "row_count");
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
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of(), "min_ts", "max_ts", "row_count");
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
        WatermarkSpec spec = new WatermarkSpec("ingest-watermark", "timestamp", List.of("county"), "min_ts", "max_ts", "row_count");
        String sql = spec.insertSql("cat", "main",
                List.of(List.of("o'brien", "2026-01-01 00:00:00", "2026-01-01 06:00:00", "3")));
        assertEquals("INSERT INTO \"cat\".\"main\".\"ingest-watermark\""
                + " (\"county\", \"min_ts\", \"max_ts\", \"row_count\") "
                + "VALUES ('o''brien', '2026-01-01 00:00:00', '2026-01-01 06:00:00', '3')", sql);
    }

    // ------------------------------------------------ MAX timestamp and row count

    @Test
    void aggregationAlwaysIncludesMinMaxAndCount() {
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of("county"), "min_ts", "max_ts", "rows");
        assertEquals("SELECT \"county\", MIN(\"ts\") AS \"min_ts\", MAX(\"ts\") AS \"max_ts\","
                + " COUNT(*) AS \"rows\" FROM (SELECT 1) GROUP BY \"county\""
                + " HAVING COUNT(*) > 0",
                spec.aggregationSql("SELECT 1"));
    }

    @Test
    void insertColumnOrderMatchesAggregateOrder() {
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of("county"), "min_ts", "max_ts", "rows");
        assertEquals("INSERT INTO \"cat\".\"main\".\"wm\""
                + " (\"county\", \"min_ts\", \"max_ts\", \"rows\")"
                + " VALUES ('a', '1', '9', '42')",
                spec.insertSql("cat", "main", List.of(List.of("a", "1", "9", "42"))));
    }

    @Test
    void blankMaxOrRowCountColumnIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new WatermarkSpec("wm", "ts", List.of(), "min_ts", "  ", "rows"));
        assertThrows(IllegalArgumentException.class,
                () -> new WatermarkSpec("wm", "ts", List.of(), "min_ts", "max_ts", null));
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

    @Test
    void parsesTheOptionalSnapshotIdColumn() {
        Map<String, String> params = new java.util.HashMap<>(Map.of(
                WatermarkSpec.TABLE_KEY, "wm",
                WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts",
                WatermarkSpec.MIN_TIMESTAMP_COLUMN_KEY, "min_ts",
                WatermarkSpec.MAX_TIMESTAMP_COLUMN_KEY, "max_ts",
                WatermarkSpec.ROW_COUNT_COLUMN_KEY, "row_count"));
        params.put(WatermarkSpec.SNAPSHOT_ID_COLUMN_KEY, " snapshot_id ");

        assertEquals("snapshot_id", WatermarkSpec.fromParameters("q", params).snapshotIdColumn());
    }

    @Test
    void snapshotIdColumnIsOptional() {
        Map<String, String> params = Map.of(
                WatermarkSpec.TABLE_KEY, "wm",
                WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts",
                WatermarkSpec.MIN_TIMESTAMP_COLUMN_KEY, "min_ts",
                WatermarkSpec.MAX_TIMESTAMP_COLUMN_KEY, "max_ts",
                WatermarkSpec.ROW_COUNT_COLUMN_KEY, "row_count");

        WatermarkSpec spec = WatermarkSpec.fromParameters("q", params);

        // Absent means the column is never written: every pre-existing configuration is unchanged.
        assertNull(spec.snapshotIdColumn());
        assertNull(spec.snapshotStampSql("cat", "main"));
    }

    @Test
    void blankSnapshotIdColumnIsRejected() {
        Map<String, String> params = new java.util.HashMap<>(Map.of(
                WatermarkSpec.TABLE_KEY, "wm",
                WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts",
                WatermarkSpec.MIN_TIMESTAMP_COLUMN_KEY, "min_ts",
                WatermarkSpec.MAX_TIMESTAMP_COLUMN_KEY, "max_ts",
                WatermarkSpec.ROW_COUNT_COLUMN_KEY, "row_count"));
        params.put(WatermarkSpec.SNAPSHOT_ID_COLUMN_KEY, "  ");

        // Present-but-blank is a typo, not a way to disable it.
        assertThrows(IllegalArgumentException.class, () -> WatermarkSpec.fromParameters("q", params));
    }

    @Test
    void rendersTheSnapshotStamp() {
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of(), "min_ts", "max_ts", "row_count", "snapshot_id");

        // IS NULL scopes it to rows not yet stamped, which also sweeps up anything a crash between
        // the two commits left behind. The catalog is a literal inside the function call.
        assertEquals("UPDATE \"cat\".\"main\".\"wm\" SET \"snapshot_id\" = "
                        + "(SELECT * FROM ducklake_last_committed_snapshot('cat')) WHERE \"snapshot_id\" IS NULL",
                spec.snapshotStampSql("cat", "main"));
    }

    @Test
    void snapshotIdColumnIsNotWrittenByTheInsert() {
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of(), "min_ts", "max_ts", "row_count", "snapshot_id");

        // The INSERT shape is unchanged: the id does not exist yet when this statement is built.
        String sql = spec.insertSql("cat", "main", List.of(List.of("2026-08-01 00:00:00", "2026-08-01 00:05:00", "3")));

        assertFalse(sql.contains("snapshot_id"), sql);
    }
}
