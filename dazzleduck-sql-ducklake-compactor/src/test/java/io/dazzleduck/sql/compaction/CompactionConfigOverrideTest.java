package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.TableConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The compactor reading its tunables from a table instead of its baked file.
 *
 * <p>These exercise {@link CompactionConfig#from} over an overlaid config — the same composition
 * {@code Main} performs — rather than booting the service, so the assertions are about which values
 * win rather than about scheduling.
 */
class CompactionConfigOverrideTest {

    private static final String TABLE = "main.compactor_config";

    /** The bundled application.conf's shape, as the fallback layer. */
    private static final String FILE_CONFIG = """
            databases = ["mylake"]
            compaction_tiers {
              minor {
                enabled = true
                frequency = 1 minute
                min_file_size = 0
                max_file_size = 8MB
                max_compacted_files = 1000
                connection_settings = ["SET threads=2"]
              }
              major {
                enabled = true
                frequency = 1 hour
                min_file_size = 8MB
                max_file_size = 64MB
                max_compacted_files = 1000
                connection_settings = []
              }
            }
            housekeeping_frequency = 5 minutes
            housekeeping_connection_settings = []
            snapshot_retention = 60 minutes
            health_port = 9090
            """;

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
        exec("INSERT INTO " + TABLE + " VALUES ('" + key + "', '" + value + "');");
    }

    /** The composition Main performs: provider overrides, file as fallback. */
    private static CompactionConfig resolve(Config raw) throws Exception {
        TableConfigProvider provider = TableConfigProvider.load(raw);
        Config merged = provider == null ? raw : provider.overrides().withFallback(raw);
        return CompactionConfig.from(merged);
    }

    private static Config withProvider(String prefix) {
        return ConfigFactory.parseString(FILE_CONFIG + """
                config_provider {
                  class = "io.dazzleduck.sql.commons.TableConfigProvider"
                  table = "%s"
                  prefix = "%s"
                }
                """.formatted(TABLE, prefix));
    }

    private static CompactionTier tier(CompactionConfig config, String name) {
        return config.tiers().stream().filter(t -> t.name().equals(name)).findFirst().orElseThrow();
    }

    @Test
    void withNoProviderTheFileIsAuthoritativeAndNothingChanges() throws Exception {
        CompactionConfig config = resolve(ConfigFactory.parseString(FILE_CONFIG));
        assertEquals(Duration.ofMinutes(60), config.snapshotRetention());
        assertEquals(8_000_000L, tier(config, "minor").maxFileSize());
        assertEquals(9090, config.healthPort());
    }

    @Test
    void tableValuesWinOverTheBakedFile() throws Exception {
        insert("compaction.snapshot_retention", "120 minutes");
        insert("compaction.housekeeping_frequency", "10 minutes");

        CompactionConfig config = resolve(withProvider("compaction."));
        assertEquals(Duration.ofMinutes(120), config.snapshotRetention(),
                "the point of the phase: change a compaction setting without a new image");
        assertEquals(Duration.ofMinutes(10), config.housekeepingFrequency());
        // untouched keys keep the file's values, so adoption is incremental
        assertEquals(9090, config.healthPort());
    }

    @Test
    void otherServicesRowsInASharedRegistryAreIgnored() throws Exception {
        insert("compaction.snapshot_retention", "120 minutes");
        insert("aggregator.analytics.cron", "0 */15 * * * *");
        insert("visibility.fact.max_lookback_days", "15");
        insert("retention.ai_txn.days", "unlimited");

        CompactionConfig config = resolve(withProvider("compaction."));
        assertEquals(Duration.ofMinutes(120), config.snapshotRetention());
        assertEquals(Duration.ofMinutes(5), config.housekeepingFrequency(),
                "a registry shared with other services must not disturb this one");
    }

    @Test
    void aConfiguredButUnreadableTableFailsRatherThanRunningOnStaleDefaults() {
        Config raw = ConfigFactory.parseString(FILE_CONFIG + """
                config_provider {
                  class = "io.dazzleduck.sql.commons.TableConfigProvider"
                  table = "main.no_such_config_table"
                }
                """);
        // An operator who has moved these settings into a table is not watching the file, so
        // starting on its values would run the lake on numbers nobody has reviewed — and the
        // symptom (files growing, snapshots expiring early) stays invisible until something
        // downstream stalls. Refusing to start is the loud failure.
        assertThrows(SQLException.class, () -> resolve(raw));
    }

    @Test
    void aTypedValueIsParsedByTheServicesOwnAccessor() throws Exception {
        // Values are stored as an operator would have written them in the file; HOCON converts.
        insert("compaction.housekeeping_frequency", "30s");
        insert("compaction.health_port", "9191");

        CompactionConfig config = resolve(withProvider("compaction."));
        assertEquals(Duration.ofSeconds(30), config.housekeepingFrequency());
        assertEquals(9191, config.healthPort());
    }

    @Test
    void tiersParseWithAllFieldsAndArbitraryCountIsSupported() throws Exception {
        // Three tiers, not the usual two, proves this isn't secretly still hardcoded to minor/major.
        Config raw = ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers = [
                  { name = "tier1", enabled = true,  frequency = 30 seconds, min_file_size = 0,    max_file_size = 1MB,  max_compacted_files = 100, connection_settings = ["SET threads=1"] }
                  { name = "tier2", enabled = false, frequency = 5 minutes,  min_file_size = 1MB,   max_file_size = 16MB, max_compacted_files = 0,   connection_settings = [] }
                  { name = "tier3", enabled = true,  frequency = 1 hour,     min_file_size = 16MB,  max_file_size = 1GB,  max_compacted_files = 50,  connection_settings = [] }
                ]
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = ["SET memory_limit='4GB'"]
                snapshot_retention = 60 minutes
                health_port = 9090
                """);
        CompactionConfig config = resolve(raw);

        assertEquals(3, config.tiers().size());
        CompactionTier tier1 = tier(config, "tier1");
        assertTrue(tier1.enabled());
        assertEquals(Duration.ofSeconds(30), tier1.frequency());
        assertEquals(0L, tier1.minFileSize());
        assertEquals(1_000_000L, tier1.maxFileSize());
        assertEquals(100L, tier1.maxCompactedFiles());
        assertEquals(List.of("SET threads=1"), tier1.connectionSettings());

        assertFalse(tier(config, "tier2").enabled());
        assertEquals(List.of("SET memory_limit='4GB'"), config.housekeepingConnectionSettings());
    }

    @Test
    void duplicateTierNamesAreRejected() {
        Config raw = ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers = [
                  { name = "minor", enabled = true, frequency = 1 minute, min_file_size = 0,   max_file_size = 8MB,  max_compacted_files = 0, connection_settings = [] }
                  { name = "minor", enabled = true, frequency = 1 hour,   min_file_size = 8MB,  max_file_size = 64MB, max_compacted_files = 0, connection_settings = [] }
                ]
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """);
        var e = assertThrows(IllegalArgumentException.class, () -> resolve(raw));
        assertTrue(e.getMessage().contains("Duplicate"), e.getMessage());
    }

    @Test
    void aTiersMaxFileSizeMustExceedItsOwnMinFileSize() {
        Config raw = ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers = [
                  { name = "broken", enabled = true, frequency = 1 minute, min_file_size = 8MB, max_file_size = 4MB, max_compacted_files = 0, connection_settings = [] }
                ]
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """);
        var e = assertThrows(IllegalArgumentException.class, () -> resolve(raw));
        assertTrue(e.getMessage().contains("broken"), e.getMessage());
    }

    @Test
    void overlappingEnabledTiersAreRejected() {
        // minor's range [0, 8MB) overlaps major's [4MB, 64MB) at [4MB, 8MB) — tiers run
        // concurrently with no lock, so overlap must refuse to start rather than risk a race.
        Config raw = ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers = [
                  { name = "minor", enabled = true, frequency = 1 minute, min_file_size = 0,   max_file_size = 8MB,  max_compacted_files = 0, connection_settings = [] }
                  { name = "major", enabled = true, frequency = 1 hour,   min_file_size = 4MB,  max_file_size = 64MB, max_compacted_files = 0, connection_settings = [] }
                ]
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """);
        var e = assertThrows(IllegalArgumentException.class, () -> resolve(raw));
        assertTrue(e.getMessage().contains("overlap"), e.getMessage());
    }

    @Test
    void overlapValidationIsSkippedWhenEitherTierIsDisabled() throws Exception {
        // No exception: with "major" disabled there's nothing for "minor"'s range to overlap.
        Config raw = ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers = [
                  { name = "minor", enabled = true,  frequency = 1 minute, min_file_size = 0,   max_file_size = 8MB,  max_compacted_files = 0, connection_settings = [] }
                  { name = "major", enabled = false, frequency = 1 hour,   min_file_size = 4MB,  max_file_size = 64MB, max_compacted_files = 0, connection_settings = [] }
                ]
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """);
        CompactionConfig config = resolve(raw);
        assertFalse(tier(config, "major").enabled());
    }

    @Test
    void listValuedKeysCannotBeOverriddenAndDatabasesShouldNotBe() throws Exception {
        // Every table value is a string, and HOCON will not widen a string to a LIST — so a
        // list-valued key cannot be set this way. That limit lands in the right place:
        // `databases` names the catalogs to compact, and the config table lives INSIDE one of
        // them, so a deployment must already know its lake before it can read any of this. It is
        // bootstrap identity, like a connection string, and belongs in the file that gets the
        // process to the database in the first place.
        insert("compaction.databases", "[\"lake_a\"]");
        Config raw = withProvider("compaction.");
        var e = assertThrows(com.typesafe.config.ConfigException.WrongType.class,
                () -> resolve(raw));
        assertTrue(e.getMessage().contains("databases"), e.getMessage());
    }

    @Test
    void keyedTiersTakeTheirNameFromTheKey() throws Exception {
        CompactionConfig config = resolve(ConfigFactory.parseString(FILE_CONFIG));
        assertEquals(List.of("minor", "major"),
                config.tiers().stream().map(CompactionTier::name).toList());
        assertEquals(Duration.ofMinutes(1), tier(config, "minor").frequency());
        assertEquals(64_000_000L, tier(config, "major").maxFileSize());
    }

    @Test
    void oneTierFieldCanBeRetunedFromTheTableWithoutRestatingTheTier() throws Exception {
        // The reason tiers are keyed rather than listed. HOCON merges objects field-by-field, so
        // this row changes a cadence and nothing else — with a list it would have replaced every
        // tier and startup would fail on the first field the row did not restate.
        insert("compaction.compaction_tiers.minor.frequency", "10 seconds");

        CompactionConfig config = resolve(withProvider("compaction."));
        CompactionTier minor = tier(config, "minor");
        assertEquals(Duration.ofSeconds(10), minor.frequency(), "the overridden field");
        assertEquals(8_000_000L, minor.maxFileSize(), "its siblings survive the merge");
        assertEquals(1000L, minor.maxCompactedFiles());
        assertEquals(List.of("SET threads=2"), minor.connectionSettings(),
                "a list INSIDE the tier is untouched by overriding a scalar beside it");
        assertEquals(Duration.ofHours(1), tier(config, "major").frequency(), "other tiers untouched");
    }

    @Test
    void aTierCanBeDisabledFromTheTable() throws Exception {
        // The per-process case: one shared config, each process turning off the tiers it does not
        // own. Equivalent on the command line as
        // --conf 'dazzleduck_sql_compaction.compaction_tiers.minor.enabled = false'.
        insert("compaction.compaction_tiers.minor.enabled", "false");

        CompactionConfig config = resolve(withProvider("compaction."));
        assertFalse(tier(config, "minor").enabled());
        assertTrue(tier(config, "major").enabled());
        assertEquals(2, config.tiers().size(),
                "a disabled tier stays configured so its file-count gauge still reports its band");
    }

    @Test
    void aBrandNewTierCanBeAddedFromTheTable() throws Exception {
        insert("compaction.compaction_tiers.mega.enabled", "true");
        insert("compaction.compaction_tiers.mega.frequency", "1 minute");
        insert("compaction.compaction_tiers.mega.min_file_size", "64MB");
        insert("compaction.compaction_tiers.mega.max_file_size", "256MB");
        insert("compaction.compaction_tiers.mega.max_compacted_files", "5");
        // deliberately no connection_settings row: it is list-valued, and a key/value table can
        // only carry scalars, so the field has to be optional for this to be possible at all

        CompactionConfig config = resolve(withProvider("compaction."));
        assertEquals(3, config.tiers().size());
        assertEquals(256_000_000L, tier(config, "mega").maxFileSize());
        assertEquals(List.of(), tier(config, "mega").connectionSettings());
    }

    @Test
    void keyedTiersAreOrderedByBandNotByKey() throws Exception {
        // An object's key order is not meaningful, so band order is imposed — otherwise the logs
        // and the per-tier gauges would come out in whatever order the parser happened to produce.
        Config raw = ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers {
                  mega  { enabled = true, frequency = 1 hour,   min_file_size = 64MB, max_file_size = 256MB, max_compacted_files = 5, connection_settings = [] }
                  minor { enabled = true, frequency = 1 minute, min_file_size = 0,    max_file_size = 8MB,   max_compacted_files = 0, connection_settings = [] }
                  major { enabled = true, frequency = 5 minutes, min_file_size = 8MB, max_file_size = 64MB,  max_compacted_files = 0, connection_settings = [] }
                }
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """);
        assertEquals(List.of("minor", "major", "mega"),
                resolve(raw).tiers().stream().map(CompactionTier::name).toList());
    }

    @Test
    void aKeyedTierDeclaringADifferentNameIsRejected() {
        // Catches a half-finished migration from the list shape, which would otherwise run under
        // the key while being tuned under the field.
        Config raw = ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers {
                  minor { name = "major", enabled = true, frequency = 1 minute, min_file_size = 0, max_file_size = 8MB, max_compacted_files = 0, connection_settings = [] }
                }
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """);
        var e = assertThrows(IllegalArgumentException.class, () -> resolve(raw));
        assertTrue(e.getMessage().contains("the key is the tier name"), e.getMessage());
    }

    @Test
    void aKeyedTierThatIsNotAnObjectIsRejected() {
        Config raw = ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers { minor = "1 minute" }
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """);
        var e = assertThrows(IllegalArgumentException.class, () -> resolve(raw));
        assertTrue(e.getMessage().contains("must be an object"), e.getMessage());
    }

    @Test
    void theListShapeStillParsesSoExistingConfigsKeepWorking() throws Exception {
        // 0.2.19 shape, verbatim: `name` as a field, declaration order preserved.
        Config raw = ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers = [
                  { name = "major", enabled = true, frequency = 1 hour,   min_file_size = 8MB, max_file_size = 64MB, max_compacted_files = 0, connection_settings = [] }
                  { name = "minor", enabled = true, frequency = 1 minute, min_file_size = 0,   max_file_size = 8MB,  max_compacted_files = 0, connection_settings = [] }
                ]
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """);
        assertEquals(List.of("major", "minor"),
                resolve(raw).tiers().stream().map(CompactionTier::name).toList(),
                "declaration order, not band order, is what a list always had");
    }
}
