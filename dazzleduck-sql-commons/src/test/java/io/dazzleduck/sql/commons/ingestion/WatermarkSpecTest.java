package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
                WatermarkSpec.GROUP_COLUMNS_KEY, " county , state "));
        assertEquals(new WatermarkSpec("wm", "ts", List.of("county", "state")), spec);
    }

    @Test
    void toleratesFlattenedHoconListSyntax() {
        // A HOCON list arrives via unwrapped().toString() as "[county, state]".
        WatermarkSpec spec = WatermarkSpec.fromParameters("q", Map.of(
                WatermarkSpec.TABLE_KEY, "wm",
                WatermarkSpec.TIMESTAMP_COLUMN_KEY, "ts",
                WatermarkSpec.GROUP_COLUMNS_KEY, "[county, state]"));
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
    void computesMinPerGroupExcludingNullTimestamps() throws Exception {
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of("county", "state"));
        try (Connection conn = ConnectionPool.getConnection()) {
            String relation = relationOver(conn,
                    "('king','wa',TIMESTAMP '2026-08-01 03:00'),"
                            + "('king','wa',TIMESTAMP '2026-08-01 01:00'),"
                            + "('cook','il',TIMESTAMP '2026-08-02 05:00'),"
                            + "('null','nv',NULL::TIMESTAMP)",   // all-NULL group must be dropped
                    tempDir.resolve("grouped.parquet").toString());
            List<List<String>> rows = spec.computeRows(conn, relation);
            rows.sort(java.util.Comparator.comparing(r -> r.get(0)));
            assertEquals(2, rows.size());
            // JDBC renders TIMESTAMP via java.sql.Timestamp.toString (trailing ".0"); the string
            // round-trips through DuckDB's implicit VARCHAR cast on INSERT.
            assertEquals(List.of("cook", "il", "2026-08-02 05:00:00.0"), rows.get(0));
            assertEquals(List.of("king", "wa", "2026-08-01 01:00:00.0"), rows.get(1));
        }
    }

    @Test
    void globalModeProducesNoRowForEmptyOrAllNullInput() throws Exception {
        WatermarkSpec spec = new WatermarkSpec("wm", "ts", List.of());
        try (Connection conn = ConnectionPool.getConnection()) {
            // zero-row relation: a global MIN would be a single NULL row — must be suppressed
            String empty = relationOver(conn, "('x','y',TIMESTAMP '2026-01-01')",
                    tempDir.resolve("empty.parquet").toString()) + " WHERE county = 'nope'";
            assertTrue(spec.computeRows(conn, empty).isEmpty());

            String allNull = relationOver(conn, "('x','y',NULL::TIMESTAMP)",
                    tempDir.resolve("allnull.parquet").toString());
            assertTrue(spec.computeRows(conn, allNull).isEmpty());

            String real = relationOver(conn, "('x','y',TIMESTAMP '2026-01-01 08:00')",
                    tempDir.resolve("real.parquet").toString());
            assertEquals(List.of(List.of("2026-01-01 08:00:00.0")), spec.computeRows(conn, real));
        }
    }

    // ------------------------------------------------------------------ rendering

    @Test
    void insertSqlQuotesIdentifiersAndEscapesValues() {
        WatermarkSpec spec = new WatermarkSpec("ingest-watermark", "timestamp", List.of("county"));
        String sql = spec.insertSql("cat", "main", List.of(List.of("o'brien", "2026-01-01 00:00:00")));
        assertEquals("INSERT INTO \"cat\".\"main\".\"ingest-watermark\" (\"county\", \"timestamp\") "
                + "VALUES ('o''brien', '2026-01-01 00:00:00')", sql);
    }
}
