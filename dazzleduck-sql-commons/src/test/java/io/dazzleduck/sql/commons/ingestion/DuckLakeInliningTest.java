package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The startup check in {@link DuckLakeInlining}, end to end through {@code validate()}: a catalog a watermark table is
 * written to must have inlining off from every source, or startup fails naming the source and its fix.
 */
public class DuckLakeInliningTest {

    @TempDir
    Path dir;

    private String catalog;
    private String settingBefore;

    /**
     * {@link DuckLakeInlining#SETTING} belongs to the whole DuckDB instance, and validate() moves it. Surefire runs the
     * module in one JVM against one instance, so it is restored after each test to keep other tests' file counts stable.
     */
    @BeforeEach
    void rememberSetting() throws Exception {
        settingBefore = ConnectionPool.collectFirst(
                "SELECT current_setting('%s')::VARCHAR".formatted(DuckLakeInlining.SETTING), String.class);
    }

    @AfterEach
    void detachAndRestoreSetting() {
        if (catalog != null) {
            ConnectionPool.execute("DETACH " + catalog);
            catalog = null;
        }
        ConnectionPool.execute("SET GLOBAL %s = %s".formatted(DuckLakeInlining.SETTING, settingBefore));
    }

    private void attach(String name, Integer attachLimit) throws Exception {
        Files.createDirectories(dir.resolve("data"));
        catalog = name;
        ConnectionPool.execute("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s'%s)".formatted(
                dir.resolve("catalog"), name, dir.resolve("data"),
                attachLimit == null ? "" : ", DATA_INLINING_ROW_LIMIT " + attachLimit));
        ConnectionPool.execute("CREATE SCHEMA IF NOT EXISTS %s.other".formatted(name));
        ConnectionPool.execute("CREATE TABLE %s.main.wm (a INT)".formatted(name));
        ConnectionPool.execute("CREATE TABLE %s.other.t (a INT)".formatted(name));
    }

    /**
     * Writes the option row {@code CALL c.set_option(...)} produces. Written directly because that call is not
     * reachable through the JDBC driver in this DuckLake build; {@code ducklake_options()}, which the check reads,
     * reports it the same way.
     *
     * @param scope      {@code table}, {@code schema} or {@code global}
     * @param scopeEntry for {@code table}, {@code schema.table}; for {@code schema}, its name
     */
    private void setOption(String scope, String scopeEntry, int limit) {
        String scopeId = switch (scope) {
            case "table" -> ("(SELECT t.table_id FROM __ducklake_metadata_%1$s.ducklake_table t"
                    + " JOIN __ducklake_metadata_%1$s.ducklake_schema s ON (s.schema_id = t.schema_id)"
                    + " WHERE s.schema_name = '%2$s' AND t.table_name = '%3$s'"
                    + " AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL)").formatted(catalog,
                    scopeEntry.substring(0, scopeEntry.indexOf('.')), scopeEntry.substring(scopeEntry.indexOf('.') + 1));
            case "schema" -> ("(SELECT schema_id FROM __ducklake_metadata_%s.ducklake_schema"
                    + " WHERE schema_name = '%s' AND end_snapshot IS NULL)").formatted(catalog, scopeEntry);
            default -> "NULL";
        };
        ConnectionPool.execute(("INSERT INTO __ducklake_metadata_%s.ducklake_metadata (key, value, scope, scope_id)"
                + " VALUES ('%s', '%d', %s, %s)").formatted(catalog, DuckLakeInlining.OPTION, limit,
                scope.equals("global") ? "NULL" : "'" + scope + "'", scopeId));
    }

    private DuckLakeIngestionTaskFactoryProvider provider(boolean withWatermark) {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(com.typesafe.config.ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [{
                    ingestion_queue = "q"
                    catalog = "%s"
                    schema  = "main"
                    table   = "t"
                    %s
                }]
                """.formatted(catalog, withWatermark ? """
                    additional_parameters {
                        watermark_table                = "wm"
                        watermark_timestamp_column     = "ts"
                        watermark_min_timestamp_column = "min_ts"
                        watermark_max_timestamp_column = "max_ts"
                        watermark_row_count_column     = "row_count"
                        watermark_snapshot_id_column   = "snap"
                        watermark_batch_id_column      = "batch_id"
                    }""" : "")));
        return provider;
    }

    private String setting() throws Exception {
        return ConnectionPool.collectFirst(
                "SELECT current_setting('%s')::VARCHAR".formatted(DuckLakeInlining.SETTING), String.class);
    }

    private int dataFiles(String table) throws Exception {
        return ConnectionPool.collectFirst(("SELECT count(*)::INTEGER FROM __ducklake_metadata_%1$s.ducklake_data_file f"
                + " JOIN __ducklake_metadata_%1$s.ducklake_table t ON (t.table_id = f.table_id)"
                + " WHERE t.table_name = '%2$s' AND t.end_snapshot IS NULL").formatted(catalog, table), Integer.class);
    }

    /** Nothing enables inlining: startup passes, the default is zeroed, and an insert really writes a file. */
    @Test
    void passesAndTurnsOffTheDefault() throws Exception {
        attach("inl_ok", null);
        ConnectionPool.execute("SET GLOBAL %s = 10".formatted(DuckLakeInlining.SETTING));
        assertDoesNotThrow(() -> provider(true).validate());
        assertEquals("0", setting());
        ConnectionPool.execute("INSERT INTO %s.main.wm VALUES (1)".formatted(catalog));
        assertEquals(1, dataFiles("wm"), "with inlining off, a one-row insert is a Parquet file, not a catalog row");
    }

    /** The connection parameter is the first place an operator would set it. */
    @Test
    void failsOnTheAttachParameter() throws Exception {
        attach("inl_attach", 10);
        var e = assertThrows(IllegalStateException.class, () -> provider(true).validate());
        assertTrue(e.getMessage().contains("ATTACH parameter DATA_INLINING_ROW_LIMIT 10"), e.getMessage());
    }

    /**
     * A stored option overrides the ATTACH parameter, so a connection with inlining off is not enough: a catalog-wide
     * option of 10 still inlines.
     */
    @Test
    void failsOnACatalogOptionEvenWithTheAttachParameterAtZero() throws Exception {
        attach("inl_catalog", 0);
        setOption("global", null, 10);
        var e = assertThrows(IllegalStateException.class, () -> provider(true).validate());
        assertTrue(e.getMessage().contains("the catalog option data_inlining_row_limit = 10"), e.getMessage());
        assertTrue(e.getMessage().contains("CALL inl_catalog.set_option('data_inlining_row_limit', 0)"), e.getMessage());
    }

    @Test
    void failsOnASchemaOption() throws Exception {
        attach("inl_schema", null);
        setOption("schema", "other", 5);
        var e = assertThrows(IllegalStateException.class, () -> provider(true).validate());
        assertTrue(e.getMessage().contains("schema 'other'"), e.getMessage());
        assertTrue(e.getMessage().contains("schema => 'other')"), e.getMessage());
    }

    /** Strict by design: a table option anywhere in the catalog fails, not only on the watermark table. */
    @Test
    void failsOnATableOptionOnAnyTable() throws Exception {
        attach("inl_table", null);
        setOption("table", "other.t", 3);
        var e = assertThrows(IllegalStateException.class, () -> provider(true).validate());
        assertTrue(e.getMessage().contains("table 'other.t'"), e.getMessage());
        assertTrue(e.getMessage().contains("schema => 'other', table_name => 't')"), e.getMessage());
    }

    /** Every source is reported at once, so one restart fixes them all. */
    @Test
    void reportsEverySource() throws Exception {
        attach("inl_all", 10);
        setOption("global", null, 10);
        setOption("table", "main.wm", 2);
        var e = assertThrows(IllegalStateException.class, () -> provider(true).validate());
        assertTrue(e.getMessage().contains("ATTACH parameter"), e.getMessage());
        assertTrue(e.getMessage().contains("the catalog option"), e.getMessage());
        assertTrue(e.getMessage().contains("table 'main.wm'"), e.getMessage());
    }

    /** An explicit zero, on the connection or in the catalog, is what turning it off looks like. */
    @Test
    void explicitZerosPass() throws Exception {
        attach("inl_zero", 0);
        setOption("global", null, 0);
        setOption("table", "main.wm", 0);
        assertDoesNotThrow(() -> provider(true).validate());
    }

    /** No watermark table: nothing to protect, so no check and the instance-wide setting is left alone. */
    @Test
    void withoutAWatermarkNothingIsCheckedOrChanged() throws Exception {
        attach("inl_none", 10);
        ConnectionPool.execute("SET GLOBAL %s = 7".formatted(DuckLakeInlining.SETTING));
        assertDoesNotThrow(() -> provider(false).validate());
        assertEquals("7", setting());
    }
}
