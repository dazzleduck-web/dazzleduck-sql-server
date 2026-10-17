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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link DuckLakeInlining} resolves the limit that actually applies to a table, over five sources
 * that are not all visible in the same view. One test per source, because a resolver that misses
 * one is wrong in both directions: it refuses a table that is fine, or passes a table that inlines.
 *
 * <p>For the three sources stored as option rows, the resolver's answer is also checked against a
 * real write: an insert must become a file exactly when the resolver says inlining is off. Reading
 * the rows back only proves the query finds them, and DuckLake loads a catalog's options at ATTACH,
 * so a row written afterwards is visible in {@code ducklake_options()} while the writes still use
 * the attach parameter — which is why these tests reattach before writing.
 */
public class DuckLakeInliningTest {

    @TempDir
    Path dir;

    private String catalog;
    private Integer attachLimit;
    private String settingBefore;

    /**
     * {@link DuckLakeInlining#SETTING} is process-wide, and these tests move it. Surefire runs the
     * whole module in one JVM against one DuckDB instance, so leaving it changed alters how many
     * files OTHER tests' inserts produce — which it did, before this was restored.
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
        if (settingBefore != null) {
            ConnectionPool.execute("SET GLOBAL %s = %s".formatted(DuckLakeInlining.SETTING, settingBefore));
        }
    }

    /** Attaches a catalog, optionally with the inlining parameter on the ATTACH itself. */
    private String attach(String name, Integer attachLimit) throws Exception {
        Files.createDirectories(dir.resolve("data"));
        catalog = name;
        this.attachLimit = attachLimit;
        ConnectionPool.execute("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s'%s)".formatted(
                dir.resolve("catalog"), name, dir.resolve("data"),
                attachLimit == null ? "" : ", DATA_INLINING_ROW_LIMIT " + attachLimit));
        ConnectionPool.execute("CREATE SCHEMA IF NOT EXISTS %s.other".formatted(name));
        ConnectionPool.execute("CREATE TABLE %s.main.t (a INT)".formatted(name));
        ConnectionPool.execute("CREATE TABLE %s.other.t (a INT)".formatted(name));
        return name;
    }

    /**
     * Writes the option row that {@code CALL c.set_option(...)} produces. Written directly because
     * that call is not reachable through the JDBC driver in this DuckLake build, though it works in
     * the CLI; the stored form was read back from a catalog the CLI had configured.
     *
     * @param scope      {@code table}, {@code schema} or {@code global}
     * @param scopeEntry for {@code table}, {@code schema.table}; for {@code schema}, its name
     */
    private void setOption(String scope, String scopeEntry, int limit) {
        String scopeId = switch (scope) {
            // Qualified, because two schemas here hold a table of the same name.
            case "table" -> ("(SELECT t.table_id FROM __ducklake_metadata_%s.ducklake_table t"
                    + " JOIN __ducklake_metadata_%s.ducklake_schema s ON (s.schema_id = t.schema_id)"
                    + " WHERE s.schema_name = '%s' AND t.table_name = '%s'"
                    + " AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL)");
            case "schema" -> "(SELECT schema_id FROM __ducklake_metadata_%s.ducklake_schema"
                    + " WHERE schema_name = '%s' AND end_snapshot IS NULL)";
            default -> "NULL";
        };
        ConnectionPool.execute(("INSERT INTO __ducklake_metadata_%s.ducklake_metadata"
                + " (key, value, scope, scope_id) VALUES ('%s', '%d', %s, %s)").formatted(
                        catalog, DuckLakeInlining.OPTION, limit,
                        scope.equals("global") ? "NULL" : "'" + scope + "'",
                        switch (scope) {
                            case "table" -> scopeId.formatted(catalog, catalog,
                                    scopeEntry.substring(0, scopeEntry.indexOf('.')),
                                    scopeEntry.substring(scopeEntry.indexOf('.') + 1));
                            case "schema" -> scopeId.formatted(catalog, scopeEntry);
                            default -> scopeId;
                        }));
    }

    /**
     * DuckLake reads a catalog's options when it ATTACHes, so an option row written afterwards is
     * visible in {@code ducklake_options()} but does NOT affect writes until the catalog is
     * attached again. Reattaching is therefore not tidiness: without it a behavioural assertion
     * would test the attach parameter rather than the option under test.
     */
    private void reattach() {
        ConnectionPool.execute("DETACH " + catalog);
        ConnectionPool.execute("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s'%s)".formatted(
                dir.resolve("catalog"), catalog, dir.resolve("data"),
                attachLimit == null ? "" : ", DATA_INLINING_ROW_LIMIT " + attachLimit));
    }

    /** Data files DuckLake holds for one table, which is what an insert either produces or not. */
    private int dataFiles(String schema, String table) throws Exception {
        return ConnectionPool.collectFirst(("SELECT count(*)::INTEGER FROM __ducklake_metadata_%s.ducklake_data_file f"
                + " JOIN __ducklake_metadata_%s.ducklake_table t ON (t.table_id = f.table_id)"
                + " JOIN __ducklake_metadata_%s.ducklake_schema s ON (s.schema_id = t.schema_id)"
                + " WHERE s.schema_name = '%s' AND t.table_name = '%s' AND t.end_snapshot IS NULL")
                .formatted(catalog, catalog, catalog, schema, table), Integer.class);
    }

    /**
     * The assertion that matters: the resolver's answer must agree with what a write actually does.
     * Reading the option rows back only proves the query finds them — a wrong precedence order, or
     * a {@code scope_entry} spelled differently from the stored one, would pass that and still
     * report a limit the writes do not use.
     */
    private void assertResolverMatchesAWrite(String schema, String table) throws Exception {
        reattach();
        boolean inliningExpected = DuckLakeInlining.effectiveRowLimit(catalog, schema, table).inliningEnabled();
        int before = dataFiles(schema, table);
        ConnectionPool.execute("INSERT INTO %s.%s.%s VALUES (1)".formatted(catalog, schema, table));
        int after = dataFiles(schema, table);
        if (inliningExpected) {
            assertEquals(before, after,
                    "the resolver says inlining is on for %s.%s, so the row must NOT become a file"
                            .formatted(schema, table));
        } else {
            assertEquals(before + 1, after,
                    "the resolver says inlining is off for %s.%s, so the row must become a file"
                            .formatted(schema, table));
        }
    }

    @Test
    void withNothingSetTheInstanceSettingApplies() throws Exception {
        attach("inlining_setting_test", null);
        ConnectionPool.execute("SET GLOBAL %s = 7".formatted(DuckLakeInlining.SETTING));
        var limit = DuckLakeInlining.effectiveRowLimit(catalog, "main", "t");
        assertEquals(7L, limit.rowLimit());
        assertEquals(DuckLakeInlining.SETTING, limit.source());
        assertTrue(limit.inliningEnabled());
    }

    /**
     * Instance-wide, which is the only scope this setting really has here: a per-connection
     * {@code SET} on a {@link ConnectionPool} duplicate moved it for the others too.
     */
    @Test
    void disableZeroesTheSettingForTheInstance() throws Exception {
        attach("inlining_disable_test", null);
        ConnectionPool.execute("SET GLOBAL %s = 7".formatted(DuckLakeInlining.SETTING));
        DuckLakeInlining.disable();
        var limit = DuckLakeInlining.effectiveRowLimit(catalog, "main", "t");
        assertEquals(0L, limit.rowLimit());
        assertFalse(limit.inliningEnabled());
    }

    /**
     * The source a resolver is most likely to miss: it is in {@code duckdb_databases().options} and
     * in neither {@code ducklake_options()} nor {@code duckdb_settings()}, and it beats the setting
     * — so zeroing the setting is not sufficient.
     */
    @Test
    void theAttachParameterBeatsTheInstanceSetting() throws Exception {
        attach("inlining_attach_test", 10);
        var limit = DuckLakeInlining.effectiveRowLimit(catalog, "main", "t");
        assertEquals(10L, limit.rowLimit(), "the ATTACH parameter still applies");
        assertTrue(limit.source().contains("ATTACH"), limit.source());
        assertEquals(0L, (long) ConnectionPool.collectFirst(
                "SELECT count(*) FROM ducklake_options('%s') WHERE option_name = '%s'"
                        .formatted(catalog, DuckLakeInlining.OPTION), Long.class),
                "and it is invisible to ducklake_options, which is what makes it the trap");

        // The case the per-table check exists for: the instance setting zeroed, exactly as
        // disable() does, and a write still inlined because the ATTACH parameter wins.
        ConnectionPool.execute("SET GLOBAL %s = 0".formatted(DuckLakeInlining.SETTING));
        assertEquals(10L, DuckLakeInlining.effectiveRowLimit(catalog, "main", "t").rowLimit(),
                "zeroing the setting does not change the effective limit");
        assertResolverMatchesAWrite("main", "t");
    }

    @Test
    void aCatalogOptionBeatsTheAttachParameter() throws Exception {
        attach("inlining_catalog_test", 10);
        setOption("global", null, 3);
        var limit = DuckLakeInlining.effectiveRowLimit(catalog, "main", "t");
        assertEquals(3L, limit.rowLimit());
        assertTrue(limit.source().contains("catalog"), limit.source());
        assertResolverMatchesAWrite("main", "t");
    }

    /** The fifth source, which sits between table and catalog. */
    @Test
    void aSchemaOptionBeatsTheCatalogAndAppliesOnlyToThatSchema() throws Exception {
        attach("inlining_schema_test", 10);
        setOption("global", null, 3);
        setOption("schema", "other", 0);
        var other = DuckLakeInlining.effectiveRowLimit(catalog, "other", "t");
        assertEquals(0L, other.rowLimit(), "the schema's own zero");
        assertTrue(other.source().contains("schema 'other'"), other.source());
        assertEquals(3L, DuckLakeInlining.effectiveRowLimit(catalog, "main", "t").rowLimit(),
                "and the other schema keeps the catalog's value");
        assertResolverMatchesAWrite("other", "t");
        assertResolverMatchesAWrite("main", "t");
    }

    @Test
    void aTableOptionBeatsEverythingElse() throws Exception {
        attach("inlining_table_test", 10);
        setOption("global", null, 3);
        setOption("schema", "main", 5);
        setOption("table", "main.t", 0);
        var limit = DuckLakeInlining.effectiveRowLimit(catalog, "main", "t");
        assertEquals(0L, limit.rowLimit());
        assertTrue(limit.source().contains("table"), limit.source());
        assertResolverMatchesAWrite("main", "t");
    }

    /**
     * The two sides of the comparison come from different places: DuckLake records the object's own
     * spelling, while the resolver is given the identifier a config file used. A table created as
     * {@code "Mixed"} is stored as {@code main.Mixed}, so an exact match on {@code main.mixed}
     * would miss its option and fall through to the wider scope — a false pass exactly when a
     * table option re-enables inlining under a catalog option of zero.
     */
    @Test
    void aTableOptionIsFoundWhateverCaseTheConfigUses() throws Exception {
        attach("inlining_case_test", null);
        ConnectionPool.execute("CREATE TABLE %s.main.\"Mixed\" (a INT)".formatted(catalog));
        setOption("global", null, 0);
        setOption("table", "main.Mixed", 10);

        for (String spelling : new String[]{"Mixed", "mixed", "MIXED"}) {
            var limit = DuckLakeInlining.effectiveRowLimit(catalog, "main", spelling);
            assertEquals(10L, limit.rowLimit(), "configured as '" + spelling + "'");
            assertTrue(limit.inliningEnabled(), "the table option must be found, not the catalog's zero");
        }
        // And the resolver agrees with what a write does, which is the point of finding it.
        assertResolverMatchesAWrite("main", "Mixed");
    }

    /** A value that is not a number means the real limit is unknown, not that it is unset. */
    @Test
    void aNonNumericAttachValueIsReportedRatherThanIgnored() throws Exception {
        Files.createDirectories(dir.resolve("data"));
        catalog = "inlining_bad_value_test";
        // DuckLake accepts the ATTACH; what it stores is what the resolver has to cope with.
        try {
            ConnectionPool.execute("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 'lots')"
                    .formatted(dir.resolve("catalog"), catalog, dir.resolve("data")));
        } catch (RuntimeException e) {
            catalog = null;
            return; // DuckLake rejected it at ATTACH time, which is just as good
        }
        ConnectionPool.execute("CREATE TABLE %s.main.t (a INT)".formatted(catalog));
        assertThrows(java.sql.SQLException.class,
                () -> DuckLakeInlining.effectiveRowLimit(catalog, "main", "t"));
    }

    /** A static provider whose one queue writes watermark rows into {@code main.wm}. */
    private DuckLakeIngestionTaskFactoryProvider providerWithWatermark() {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(com.typesafe.config.ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [{
                    ingestion_queue = "q"
                    catalog = "%s"
                    schema  = "main"
                    table   = "t"
                    additional_parameters {
                        watermark_table                = "wm"
                        watermark_timestamp_column     = "ts"
                        watermark_min_timestamp_column = "min_ts"
                        watermark_max_timestamp_column = "max_ts"
                        watermark_row_count_column     = "row_count"
                        watermark_snapshot_id_column   = "snap"
                        watermark_batch_id_column      = "batch_id"
                    }
                }]
                """.formatted(catalog)));
        return provider;
    }

    /**
     * The feature itself, end to end through {@code validate()}: a watermark table that would
     * inline must stop startup, naming the table and the fix. The resolver and the remedy are
     * tested above in isolation; without this, removing the check from validate() would leave the
     * suite green. It also covers the positive side of the gate — a watermark is configured, so the
     * instance setting is zeroed before the check runs.
     */
    @Test
    void validateRefusesToStartWhenAWatermarkTableWouldInline() throws Exception {
        attach("inlining_validate_test", 10);
        ConnectionPool.execute("CREATE TABLE %s.main.wm (a INT)".formatted(catalog));
        ConnectionPool.execute("SET GLOBAL %s = 7".formatted(DuckLakeInlining.SETTING));

        var e = assertThrows(IllegalStateException.class, () -> providerWithWatermark().validate());
        assertTrue(e.getMessage().contains("%s.main.wm".formatted(catalog)), e.getMessage());
        assertTrue(e.getMessage().contains("remove DATA_INLINING_ROW_LIMIT"), e.getMessage());

        assertEquals("0", ConnectionPool.collectFirst(
                "SELECT current_setting('%s')::VARCHAR".formatted(DuckLakeInlining.SETTING), String.class),
                "a watermark is configured, so the instance setting was zeroed before the check");
    }

    /** And the same provider starts once the table is exempted where the stored option lives. */
    @Test
    void validatePassesOnceTheWatermarkTableIsExempted() throws Exception {
        attach("inlining_validate_ok_test", 10);
        ConnectionPool.execute("CREATE TABLE %s.main.wm (a INT)".formatted(catalog));
        setOption("table", "main.wm", 0);
        reattach();
        assertDoesNotThrow(() -> providerWithWatermark().validate());
    }

    /** When the table's own option is the cause, the fix and the fallback would be one statement. */
    @Test
    void theRemedyForATableOptionIsStatedOnce() {
        String remedy = DuckLakeInlining.remedy("c", "main", "t", "the table's own " + DuckLakeInlining.OPTION);
        assertEquals(1, remedy.split("set_option", -1).length - 1,
                "one statement, not the same one twice: " + remedy);
        assertFalse(remedy.contains("not yours to change"), remedy);
    }

    /** The remedy names the source's own fix first, since it is the narrower change. */
    @Test
    void theRemedyNamesTheSourcesOwnFixBeforeTheStoredOption() {
        String forAttach = DuckLakeInlining.remedy("c", "main", "t", "the ATTACH parameter DATA_INLINING_ROW_LIMIT");
        assertTrue(forAttach.indexOf("remove DATA_INLINING_ROW_LIMIT")
                < forAttach.indexOf("set_option"), forAttach);
        assertTrue(DuckLakeInlining.remedy("c", "main", "t", "the catalog's data_inlining_row_limit")
                .contains("set it to 0 where it is set"));
    }
}
