package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SidecarRows}: the SQL it generates, run against DuckDB, and the input it refuses. The rows
 * are written the way data is — a Parquet file, which the transaction then registers — so what is
 * checked here is the file that {@code copySql} produces. A statement that fails fails the batch,
 * which is why every identifier is checked in the record and the values are the producer's
 * already-rendered literals. Registering the file in place is covered end to end against a real
 * DuckLake catalog by the collector's own test.
 */
public class SidecarRowsTest {

    private static final String TABLE = "sidecar_rows_test";

    private static final List<String> COLUMNS = List.of("queue", "name", "seen");

    @TempDir
    Path directory;

    private static SidecarRows rows(List<List<String>> values) {
        return new SidecarRows(TABLE, COLUMNS, values);
    }

    private static List<String> row(String queue, String name) {
        return List.of("'" + queue + "'", "'" + name + "'", "TIMESTAMPTZ '2026-10-16 00:00:00+00'");
    }

    /** Writes the rows as Parquet, the way a batch does, and returns the file. */
    private String write(SidecarRows sidecar) {
        String file = directory.resolve(sidecar.fileName()).toString();
        ConnectionPool.execute(sidecar.copySql(file));
        return file;
    }

    /** The file's rows as sorted {@code queue/name} text. */
    private static String contents(String file) throws Exception {
        String text = ConnectionPool.collectFirst(
                ("SELECT coalesce(string_agg(r, ' ' ORDER BY r), '') FROM (SELECT queue || '/' || name AS r"
                        + " FROM read_parquet('%s'))").formatted(file), String.class);
        return text == null ? "" : text;
    }

    @Test
    void theRowsAreWrittenToAParquetFile() throws Exception {
        String file = write(rows(List.of(row("logs", "a"), row("logs", "b"))));
        assertTrue(file.endsWith(".parquet"), file);
        assertEquals("logs/a logs/b", contents(file));
    }

    /** The file's schema is the table's: the record's column names, the producer's literal types. */
    @Test
    void theFilesSchemaIsTheTables() throws Exception {
        String file = write(rows(List.of(row("logs", "a"))));
        assertEquals("VARCHAR, VARCHAR, TIMESTAMP WITH TIME ZONE", ConnectionPool.collectFirst(
                ("SELECT concat_ws(', ', typeof(queue), typeof(name), typeof(seen))"
                        + " FROM read_parquet('%s') LIMIT 1").formatted(file), String.class),
                "the producer's literals decide the types, so TIMESTAMPTZ stays TIMESTAMPTZ");
    }

    /** Each batch writes its own file, so two batches never contend for one. */
    @Test
    void everyBatchGetsItsOwnFileName() {
        var sidecar = rows(List.of(row("logs", "a")));
        assertTrue(!sidecar.fileName().equals(sidecar.fileName()), "names are unique per call");
    }

    /**
     * Append-only, and duplicates are tolerated: a write adds a file and reads nothing, so two
     * processes recording the same value cannot conflict. Deduplicating is the reader's job, and
     * {@code min} over the duplicates gives the first sighting.
     */
    @Test
    void duplicatesAccumulateAsFilesAndDeduplicateOnRead() throws Exception {
        write(rows(List.of(row("logs", "a"))));
        write(rows(List.of(List.of("'logs'", "'a'", "TIMESTAMPTZ '2027-01-01 00:00:00+00'"))));
        String glob = directory.resolve("*.parquet").toString();
        assertEquals(2L, (long) ConnectionPool.collectFirst(
                "SELECT count(*) FROM read_parquet('%s')".formatted(glob), Long.class),
                "both rows are there, in separate files");
        assertEquals("logs|a|2026", ConnectionPool.collectFirst(
                ("SELECT queue || '|' || name || '|' || strftime(min(seen), '%%Y') FROM read_parquet('%s')"
                        + " GROUP BY queue, name").formatted(glob), String.class));
    }

    /** No DDL and no read — the statement writes a file and nothing else. */
    @Test
    void theStatementIsACopyAndNothingElse() {
        String sql = rows(List.of(row("logs", "a"))).copySql("/tmp/x.parquet");
        assertTrue(sql.startsWith("COPY (SELECT * FROM (VALUES "), sql);
        // Quoted, so a column named like a reserved word works; see the typed test below.
        assertTrue(sql.contains("AS t (\"queue\", \"name\", \"seen\")"), sql);
        assertTrue(sql.endsWith("TO '/tmp/x.parquet' (FORMAT parquet)"), sql);
        assertTrue(!sql.contains("INSERT") && !sql.contains("CREATE"), sql);
    }

    // --- what it refuses -----------------------------------------------------------------

    @Test
    void aTableOrColumnNameThatIsNotAPlainIdentifierIsRefused() {
        for (String bad : new String[]{"a.b", "my-table", "t; DROP TABLE x", "1t", "", " "}) {
            assertThrows(IllegalArgumentException.class,
                    () -> new SidecarRows(bad, COLUMNS, List.of()), "table '" + bad + "'");
            assertThrows(IllegalArgumentException.class,
                    () -> new SidecarRows(TABLE, List.of(bad), List.of()), "column '" + bad + "'");
        }
    }

    @Test
    void noColumnsAtAllIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new SidecarRows(TABLE, List.of(), List.of()));
    }

    @Test
    void aRowWithTheWrongNumberOfValuesIsRefused() {
        var e = assertThrows(IllegalArgumentException.class, () -> rows(List.of(List.of("'logs'", "'a'"))));
        assertTrue(e.getMessage().contains("2 value(s) for 3 column(s)"), e.getMessage());
    }

    // --- union across a bucket's batches -------------------------------------------------

    /** Several batches become one COPY, so their rows are unioned and identical ones collapsed. */
    @Test
    void aBucketsRowsAreUnionedAndDeduplicated() {
        var union = SidecarRows.union(List.of(
                rows(List.of(row("logs", "a"), row("logs", "b"))),
                rows(List.of(row("logs", "b"), row("logs", "c")))));
        assertEquals(List.of(row("logs", "a"), row("logs", "b"), row("logs", "c")), union.rows());
    }

    @Test
    void batchesWithoutRowsContributeNothingAndAllOfThemMeansNone() {
        assertNull(SidecarRows.union(List.of()));
        assertNull(SidecarRows.union(java.util.Arrays.asList(null, rows(List.of()))));
        var union = SidecarRows.union(java.util.Arrays.asList(null, rows(List.of(row("logs", "a")))));
        assertEquals(List.of(row("logs", "a")), union.rows());
    }

    /** One bucket is one queue's batches, so two tables in it would mean a bug upstream. */
    @Test
    void aBucketCarryingTwoDifferentTablesIsRefused() {
        var other = new SidecarRows("other_table", COLUMNS, List.of(row("logs", "a")));
        var e = assertThrows(IllegalArgumentException.class,
                () -> SidecarRows.union(List.of(rows(List.of(row("logs", "a"))), other)));
        assertTrue(e.getMessage().contains("other_table"), e.getMessage());
    }

    // --- declared column types -----------------------------------------------------------

    /**
     * Why types are declared rather than inferred from the literals: registration does not cast.
     * A VARCHAR file will not register into a TIMESTAMP or UUID column, and a {@code VALUES} list
     * of quoted strings is all-VARCHAR however the target is declared.
     */
    @Test
    void aTypedSidecarRegistersIntoATypedTableAndAnUntypedOneDoesNot() throws Exception {
        String catalog = "sidecar_types_lake";
        Path data = directory.resolve("data");
        java.nio.file.Files.createDirectories(data.resolve("main").resolve("wm"));
        try (var conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatch(new String[]{
                    "INSTALL ducklake", "LOAD ducklake",
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')"
                            .formatted(directory.resolve("catalog"), catalog, data),
                    "CREATE TABLE %s.main.wm (grp VARCHAR, \"at\" TIMESTAMP, n BIGINT, batch_id UUID)"
                            .formatted(catalog)});

            var one = new SidecarRows("wm", List.of("grp", "at", "n", "batch_id"),
                    List.of("VARCHAR", "TIMESTAMP", "BIGINT", "UUID"),
                    List.of(List.of("'king'", "'2026-08-01 03:00:00'", "'2'",
                            "'0199e0a0-1111-7000-8000-0000000000b1'")));
            // Through union, because that is the only path a real batch takes to the COPY: it
            // rebuilds the record, so declared types have to survive it.
            var typed = SidecarRows.union(List.of(one));
            assertEquals(one.columnTypes(), typed.columnTypes(), "union must carry the declared types");
            String file = data.resolve("main").resolve("wm").resolve("typed.parquet").toString();
            ConnectionPool.execute(conn, typed.copySql(file));
            ConnectionPool.execute(conn, ("CALL ducklake_add_data_files('%s', 'wm', '%s', schema => 'main',"
                    + " ignore_extra_columns => true, allow_missing => true)").formatted(catalog, file));
            assertEquals("king|2026-08-01 03:00:00|2|0199e0a0-1111-7000-8000-0000000000b1",
                    ConnectionPool.collectFirst(conn, ("SELECT grp || '|' || \"at\"::VARCHAR || '|' || n::VARCHAR"
                            + " || '|' || batch_id::VARCHAR AS r FROM %s.main.wm").formatted(catalog),
                            String.class),
                    "declared types make the file's schema the table's, reserved column name and all");

            // The same rows without declared types: an all-VARCHAR file, which is refused.
            var untyped = new SidecarRows("wm", List.of("grp", "at", "n", "batch_id"),
                    List.of(List.of("'king'", "'2026-08-01 03:00:00'", "'2'",
                            "'0199e0a0-1111-7000-8000-0000000000b1'")));
            String bad = data.resolve("main").resolve("wm").resolve("untyped.parquet").toString();
            ConnectionPool.execute(conn, untyped.copySql(bad));
            var e = assertThrows(Exception.class, () -> ConnectionPool.execute(conn,
                    ("CALL ducklake_add_data_files('%s', 'wm', '%s', schema => 'main',"
                            + " ignore_extra_columns => true, allow_missing => true)").formatted(catalog, bad)));
            assertTrue(e.getMessage().contains("Failed to map column"), e.getMessage());

            ConnectionPool.execute(conn, "DETACH " + catalog);
        }
    }

    /** Two batches of the same columns but different declared types are not one file's worth. */
    @Test
    void unionRefusesRowsWhoseDeclaredTypesDisagree() {
        var rows = List.of(List.of("'a'"));
        var asText = new SidecarRows("t", List.of("c"), List.of("VARCHAR"), rows);
        var asNumber = new SidecarRows("t", List.of("c"), List.of("BIGINT"), rows);
        assertThrows(IllegalArgumentException.class, () -> SidecarRows.union(List.of(asText, asNumber)));
        // And the untyped/typed pair is equally a mismatch, not a silent fallback to untyped.
        var untyped = new SidecarRows("t", List.of("c"), rows);
        assertThrows(IllegalArgumentException.class, () -> SidecarRows.union(List.of(asText, untyped)));
    }

    @Test
    void columnTypesMustMatchTheColumnsAndCarryNoSql() {
        var rows = List.of(List.of("'a'"));
        assertThrows(IllegalArgumentException.class,
                () -> new SidecarRows("t", List.of("a"), List.of("VARCHAR", "BIGINT"), rows),
                "one type per column");
        assertThrows(NullPointerException.class,
                () -> new SidecarRows("t", List.of("a"), java.util.Arrays.asList((String) null), rows),
                "a null type is rejected by List.copyOf before validation reaches it");
        for (String bad : new String[]{"VARCHAR; DROP TABLE x", "'VARCHAR'", "", "1BIGINT"}) {
            assertThrows(IllegalArgumentException.class,
                    () -> new SidecarRows("t", List.of("a"), List.of(bad), rows), String.valueOf(bad));
        }
        // Precision and scale are ordinary type syntax and must still work.
        assertTrue(new SidecarRows("t", List.of("a"), List.of("DECIMAL(10, 2)"), rows)
                .copySql("/tmp/x.parquet").contains("CAST(\"a\" AS DECIMAL(10, 2))"));
    }

    // --- the per-queue parameter that names the table ------------------------------------

    /**
     * Optional, per stream, and validated at config load — not per batch: a bad name would
     * otherwise fail every batch of that queue.
     */
    @Test
    void theAttributeTypesTableParameterIsOptionalAndValidatedAtConfigLoad() {
        assertNull(mapping(java.util.Map.of()).attributeTypesTable(),
                "a queue that names no table records nothing");
        assertEquals("otel_attribute_types", mapping(java.util.Map.of(
                QueueIdToTableMapping.ATTRIBUTE_TYPES_TABLE_KEY, " otel_attribute_types "))
                .attributeTypesTable(), "trimmed");
        for (String bad : new String[]{"main.types", "my-types", "types; DROP TABLE x", "1t", ""}) {
            assertThrows(IllegalArgumentException.class, () -> mapping(java.util.Map.of(
                    QueueIdToTableMapping.ATTRIBUTE_TYPES_TABLE_KEY, bad)), "'" + bad + "'");
        }
    }

    private static QueueIdToTableMapping mapping(java.util.Map<String, String> parameters) {
        return new QueueIdToTableMapping("logs", "cat", "main", "logs", parameters, null);
    }
}
