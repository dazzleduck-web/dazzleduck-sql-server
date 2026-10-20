package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterEach;
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
 * written to must be attached with {@code DATA_INLINING_ROW_LIMIT 0}, or startup fails naming the catalog and its fix.
 */
public class DuckLakeInliningTest {

    @TempDir
    Path dir;

    private String catalog;

    @AfterEach
    void detach() {
        if (catalog != null) {
            ConnectionPool.execute("DETACH " + catalog);
            catalog = null;
        }
    }

    private void attach(String name, Integer attachLimit) throws Exception {
        Files.createDirectories(dir.resolve("data"));
        catalog = name;
        ConnectionPool.execute("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s'%s)".formatted(
                dir.resolve("catalog"), name, dir.resolve("data"),
                attachLimit == null ? "" : ", DATA_INLINING_ROW_LIMIT " + attachLimit));
        ConnectionPool.execute("CREATE SCHEMA %s.other".formatted(name));
        ConnectionPool.execute("CREATE TABLE %s.main.wm (a INT)".formatted(name));
        ConnectionPool.execute("CREATE TABLE %s.other.t (a INT)".formatted(name));
    }

    /** {@code CALL catalog.set_option(...)}, with {@code scope} the named arguments that narrow it, if any. */
    private void setOption(int limit, String scope) {
        ConnectionPool.execute("CALL %s.set_option('%s', %d%s)".formatted(
                catalog, DuckLakeInlining.OPTION, limit, scope == null ? "" : ", " + scope));
    }

    private DuckLakeIngestionTaskFactoryProvider provider(boolean withWatermark) {
        return provider(withWatermark, catalog);
    }

    /** @param configCatalog the catalog name as the config file spells it */
    private DuckLakeIngestionTaskFactoryProvider provider(boolean withWatermark, String configCatalog) {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(com.typesafe.config.ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [{
                    ingestion_queue = "q"
                    catalog = "%s"
                    schema  = "main"
                    table   = "t"
                    %s
                }]
                """.formatted(configCatalog, withWatermark ? """
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

    private int dataFiles(String table) throws Exception {
        return ConnectionPool.collectFirst(("SELECT count(*)::INTEGER FROM __ducklake_metadata_%1$s.ducklake_data_file f"
                + " JOIN __ducklake_metadata_%1$s.ducklake_table t ON (t.table_id = f.table_id)"
                + " WHERE t.table_name = '%2$s' AND t.end_snapshot IS NULL").formatted(catalog, table), Integer.class);
    }

    /** Attached with the limit at zero: startup passes, and an insert really writes a file. */
    @Test
    void passesWhenAttachedWithInliningDisabled() throws Exception {
        attach("inl_ok", 0);
        assertDoesNotThrow(() -> provider(true).validate());
        ConnectionPool.execute("INSERT INTO %s.main.wm VALUES (1)".formatted(catalog));
        assertEquals(1, dataFiles("wm"), "with inlining off, a one-row insert is a Parquet file, not a catalog row");
    }

    @Test
    void failsOnTheAttachParameter() throws Exception {
        attach("inl_attach", 10);
        var e = assertThrows(IllegalStateException.class, () -> provider(true).validate());
        assertTrue(e.getMessage().contains("ATTACH parameter DATA_INLINING_ROW_LIMIT 10"), e.getMessage());
    }

    /** Leaving the parameter out means the default of 10, so it is not the same as disabling inlining. */
    @Test
    void failsWithoutTheAttachParameter() throws Exception {
        attach("inl_missing", null);
        var e = assertThrows(IllegalStateException.class, () -> provider(true).validate());
        assertTrue(e.getMessage().contains("Add DATA_INLINING_ROW_LIMIT 0 to its ATTACH"), e.getMessage());
    }

    /** DuckDB resolves catalog names case-insensitively, so a config spelling them differently writes to the same one. */
    @Test
    void failsOnTheAttachParameterWhenTheConfigSpellsTheCatalogInAnotherCase() throws Exception {
        attach("inl_case", 10);
        var e = assertThrows(IllegalStateException.class, () -> provider(true, "INL_Case").validate());
        assertTrue(e.getMessage().contains("ATTACH parameter DATA_INLINING_ROW_LIMIT 10"), e.getMessage());
    }

    /**
     * A stored option takes precedence over the ATTACH parameter, so an ATTACH at zero is not enough: the insert on
     * another table shows the row really is inlined, not written as a file.
     */
    @Test
    void failsOnACatalogOption() throws Exception {
        attach("inl_opt_catalog", 0);
        setOption(10, null);
        ConnectionPool.execute("INSERT INTO %s.other.t VALUES (1)".formatted(catalog));
        assertEquals(0, dataFiles("t"), "the catalog option of 10 beats the ATTACH parameter of 0");
        var e = assertThrows(IllegalStateException.class, () -> provider(true).validate());
        assertTrue(e.getMessage().contains("the catalog option data_inlining_row_limit = 10"), e.getMessage());
        assertTrue(e.getMessage().contains(
                "CALL \"inl_opt_catalog\".set_option('data_inlining_row_limit', 0, schema => 'main', table_name => 'wm')"),
                e.getMessage());
    }

    @Test
    void failsOnTheWatermarkSchemasOption() throws Exception {
        attach("inl_opt_schema", 0);
        setOption(5, "schema => 'main'");
        var e = assertThrows(IllegalStateException.class, () -> provider(true).validate());
        assertTrue(e.getMessage().contains("schema 'main' option data_inlining_row_limit = 5"), e.getMessage());
    }

    @Test
    void failsOnTheWatermarkTablesOwnOption() throws Exception {
        attach("inl_opt_table", 0);
        setOption(3, "schema => 'main', table_name => 'wm'");
        var e = assertThrows(IllegalStateException.class, () -> provider(true).validate());
        assertTrue(e.getMessage().contains("its own option data_inlining_row_limit = 3"), e.getMessage());
    }

    /**
     * The most specific scope wins: the table's own zero overrides the catalog's ten. An insert really writes a file,
     * so the check agrees with what DuckLake does.
     */
    @Test
    void passesWhenTheTablesOwnZeroOverridesTheCatalog() throws Exception {
        attach("inl_opt_override", 0);
        setOption(10, null);
        setOption(0, "schema => 'main', table_name => 'wm'");
        assertDoesNotThrow(() -> provider(true).validate());
        ConnectionPool.execute("INSERT INTO %s.main.wm VALUES (1)".formatted(catalog));
        assertEquals(1, dataFiles("wm"), "the table's own zero applies, so a one-row insert is a Parquet file");
    }

    /** Options on other schemas and tables do not apply to the watermark table, so they do not fail startup. */
    @Test
    void ignoresOptionsOnOtherTables() throws Exception {
        attach("inl_opt_other", 0);
        setOption(5, "schema => 'other'");
        setOption(3, "schema => 'other', table_name => 't'");
        assertDoesNotThrow(() -> provider(true).validate());
    }

    /** No watermark table: nothing to protect, so no check. */
    @Test
    void withoutAWatermarkNothingIsChecked() throws Exception {
        attach("inl_none", 10);
        assertDoesNotThrow(() -> provider(false).validate());
    }
}
