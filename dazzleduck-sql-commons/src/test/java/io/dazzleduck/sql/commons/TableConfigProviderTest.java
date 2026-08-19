package io.dazzleduck.sql.commons;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class TableConfigProviderTest {

    private static final String TABLE = "main.t_config";

    @BeforeEach
    void createTable() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS " + TABLE + "(config_key VARCHAR, value VARCHAR);"
                + "DELETE FROM " + TABLE + ";");
    }

    @AfterEach
    void dropTable() throws SQLException {
        exec("DROP TABLE IF EXISTS " + TABLE + ";");
    }

    private static void exec(String sql) throws SQLException {
        try (Connection c = ConnectionPool.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    private static void insert(String key, String value) throws SQLException {
        exec("INSERT INTO " + TABLE + " VALUES ('" + key + "', "
                + (value == null ? "NULL" : "'" + value + "'") + ");");
    }

    private static Config providerConfig(String extra) {
        return ConfigFactory.parseString(
                "table = \"" + TABLE + "\"\n" + (extra == null ? "" : extra));
    }

    // ── the values a service actually asks for ──────────────────────────────

    @Test
    void overlaysValuesTheServiceReadsWithItsOwnAccessors() throws Exception {
        insert("compaction.snapshot_retention", "90 minutes");
        insert("compaction.minor_compaction_max_size", "16MB");
        insert("compaction.health_port", "9091");

        Config file = ConfigFactory.parseString(
                "snapshot_retention = 60 minutes\nminor_compaction_max_size = 8MB\nhealth_port = 9090");
        Config merged = new TableConfigProvider(providerConfig("prefix = \"compaction.\""))
                .overrides(file).withFallback(file);

        // Everything arrives as a string; HOCON converts on demand, so the service's existing
        // getDuration/getBytes/getInt calls keep working unchanged.
        assertEquals(Duration.ofMinutes(90), merged.getDuration("snapshot_retention"));
        assertEquals(16_000_000L, merged.getBytes("minor_compaction_max_size"));
        assertEquals(9091, merged.getInt("health_port"));
    }

    @Test
    void aKeyTheTableDoesNotSupplyKeepsItsFileValue() throws Exception {
        insert("compaction.snapshot_retention", "90 minutes");

        Config file = ConfigFactory.parseString(
                "snapshot_retention = 60 minutes\nhousekeeping_frequency = 5 minutes");
        Config merged = new TableConfigProvider(providerConfig("prefix = \"compaction.\""))
                .overrides(file).withFallback(file);

        assertEquals(Duration.ofMinutes(90), merged.getDuration("snapshot_retention"));
        assertEquals(Duration.ofMinutes(5), merged.getDuration("housekeeping_frequency"),
                "an overlay, not a replacement — this is what makes adoption incremental");
    }

    // ── living in a SHARED registry ─────────────────────────────────────────

    @Test
    void prefixSelectsThisServicesNamespaceAndStripsIt() throws Exception {
        insert("compaction.snapshot_retention", "90 minutes");
        insert("aggregator.analytics.cron", "0 */5 * * * *");   // another service's row
        insert("visibility.fact.max_lookback_days", "15");      // and another's

        Config overrides = new TableConfigProvider(providerConfig("prefix = \"compaction.\""))
                .overrides(ConfigFactory.empty());

        assertEquals(1, overrides.entrySet().size(), "only this service's namespace");
        assertEquals("90 minutes", overrides.getString("snapshot_retention"));
        assertFalse(overrides.hasPath("aggregator.analytics.cron"));
    }

    @Test
    void withoutAPrefixEveryRowIsTakenVerbatim() throws Exception {
        insert("snapshot_retention", "90 minutes");
        insert("nested.thing", "x");

        Config overrides = new TableConfigProvider(providerConfig(null))
                .overrides(ConfigFactory.empty());
        assertEquals("90 minutes", overrides.getString("snapshot_retention"));
        assertEquals("x", overrides.getString("nested.thing"));
    }

    @Test
    void unusableRowsAreSkippedRatherThanFailingTheRead() throws Exception {
        insert("compaction.snapshot_retention", "90 minutes");
        insert("compaction.", "empty remainder");
        insert("compaction..double", "dotted");
        insert("compaction.has space", "quoting-sensitive");
        insert("compaction.null_value", null);

        // A shared registry holds other people's rows, and a key that needs quoting is far more
        // likely to be one of those than a path this service meant to set. Skipping keeps one odd
        // row from stopping a service from starting.
        Config overrides = new TableConfigProvider(providerConfig("prefix = \"compaction.\""))
                .overrides(ConfigFactory.empty());
        assertEquals(1, overrides.entrySet().size(), overrides.toString());
        assertEquals("90 minutes", overrides.getString("snapshot_retention"));
    }

    @Test
    void anEmptyRelationOverlaysNothing() throws Exception {
        Config overrides = new TableConfigProvider(providerConfig("prefix = \"compaction.\""))
                .overrides(ConfigFactory.empty());
        assertTrue(overrides.isEmpty());
    }

    // ── failure and configuration errors ────────────────────────────────────

    @Test
    void aMissingRelationThrowsSoTheCallerCanDecide() {
        TableConfigProvider p = new TableConfigProvider(
                ConfigFactory.parseString("table = \"main.does_not_exist\""));
        // The provider reports; whether that is fatal belongs to the service (the compactor exits).
        assertThrows(SQLException.class, () -> p.overrides(ConfigFactory.empty()));
    }

    @Test
    void identifiersAreValidatedAtConstructionNotInterpolatedBlindly() {
        assertThrows(IllegalArgumentException.class, () -> new TableConfigProvider(
                ConfigFactory.parseString("table = \"t; DROP TABLE main.t_config; --\"")));
        assertThrows(IllegalArgumentException.class, () -> new TableConfigProvider(
                ConfigFactory.parseString("table = \"main.t\"\nkey_column = \"k FROM x; --\"")));
    }

    @Test
    void customColumnNamesArePossibleBecauseSchemasDiffer() throws Exception {
        exec("CREATE TABLE main.t_other(name VARCHAR, val VARCHAR);"
                + "INSERT INTO main.t_other VALUES ('snapshot_retention', '90 minutes');");
        try {
            Config overrides = new TableConfigProvider(ConfigFactory.parseString(
                    "table = \"main.t_other\"\nkey_column = \"name\"\nvalue_column = \"val\""))
                    .overrides(ConfigFactory.empty());
            assertEquals("90 minutes", overrides.getString("snapshot_retention"));
        } finally {
            exec("DROP TABLE main.t_other;");
        }
    }

    // ── the SPI ─────────────────────────────────────────────────────────────

    @Test
    void absentBlockMeansNoProviderAndByteIdenticalBehaviour() throws Exception {
        assertSame(ConfigProvider.NONE, ConfigProvider.load(ConfigFactory.empty()));
        assertSame(ConfigProvider.NONE,
                ConfigProvider.load(ConfigFactory.parseString("config_provider { table = \"x\" }")),
                "a block naming no class is not a provider");
        assertTrue(ConfigProvider.NONE.overrides(ConfigFactory.empty()).isEmpty());
    }

    @Test
    void loadConstructsTheNamedProvider() throws Exception {
        Config config = ConfigFactory.parseString(
                "config_provider {\n"
                        + "  class = \"io.dazzleduck.sql.commons.TableConfigProvider\"\n"
                        + "  table = \"" + TABLE + "\"\n"
                        + "  prefix = \"compaction.\"\n"
                        + "}");
        insert("compaction.snapshot_retention", "90 minutes");

        ConfigProvider provider = ConfigProvider.load(config);
        assertInstanceOf(TableConfigProvider.class, provider);
        assertEquals("90 minutes",
                provider.overrides(ConfigFactory.empty()).getString("snapshot_retention"));
    }
}
