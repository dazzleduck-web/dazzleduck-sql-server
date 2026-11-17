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
                target_file_size = 16MB
                max_compacted_files = 1000
                connection_settings = ["SET threads=2"]
              }
              major {
                enabled = true
                frequency = 1 hour
                min_file_size = 8MB
                max_file_size = 64MB
                target_file_size = 64MB
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
                  { name = "tier1", enabled = true,  frequency = 30 seconds, min_file_size = 0,    max_file_size = 1MB,  target_file_size = 1MB,  max_compacted_files = 100, connection_settings = ["SET threads=1"] }
                  { name = "tier2", enabled = false, frequency = 5 minutes,  min_file_size = 1MB,   max_file_size = 16MB, target_file_size = 16MB, max_compacted_files = 5,   connection_settings = [] }
                  { name = "tier3", enabled = true,  frequency = 1 hour,     min_file_size = 16MB,  max_file_size = 1GB,  target_file_size = 1GB,  max_compacted_files = 50,  connection_settings = [] }
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
        assertEquals(1_000_000L, tier1.targetFileSize());
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
                  { name = "minor", enabled = true, frequency = 1 minute, min_file_size = 0,   max_file_size = 8MB,  target_file_size = 8MB,  max_compacted_files = 5, connection_settings = [] }
                  { name = "minor", enabled = true, frequency = 1 hour,   min_file_size = 8MB,  max_file_size = 64MB, target_file_size = 64MB, max_compacted_files = 5, connection_settings = [] }
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
                  { name = "broken", enabled = true, frequency = 1 minute, min_file_size = 8MB, max_file_size = 4MB, target_file_size = 4MB, max_compacted_files = 5, connection_settings = [] }
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
                  { name = "minor", enabled = true, frequency = 1 minute, min_file_size = 0,   max_file_size = 8MB,  target_file_size = 8MB,  max_compacted_files = 5, connection_settings = [] }
                  { name = "major", enabled = true, frequency = 1 hour,   min_file_size = 4MB,  max_file_size = 64MB, target_file_size = 64MB, max_compacted_files = 5, connection_settings = [] }
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
                  { name = "minor", enabled = true,  frequency = 1 minute, min_file_size = 0,   max_file_size = 8MB,  target_file_size = 8MB,  max_compacted_files = 5, connection_settings = [] }
                  { name = "major", enabled = false, frequency = 1 hour,   min_file_size = 4MB,  max_file_size = 64MB, target_file_size = 64MB, max_compacted_files = 5, connection_settings = [] }
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
        assertEquals(16_000_000L, tier(config, "minor").targetFileSize());
        assertEquals(64_000_000L, tier(config, "major").maxFileSize());
    }

    @Test
    void oneTierFieldCanBeRetunedFromTheTableWithoutRestatingTheTier() throws Exception {
        // The reason tiers are keyed rather than listed. HOCON merges objects field-by-field, so
        // this row changes a cap and nothing else — with a list it would have replaced every
        // tier and startup would fail on the first field the row did not restate.
        insert("compaction.compaction_tiers.minor.max_compacted_files", "7");

        CompactionConfig config = resolve(withProvider("compaction."));
        CompactionTier minor = tier(config, "minor");
        assertEquals(7L, minor.maxCompactedFiles(), "the overridden field");
        assertEquals(8_000_000L, minor.maxFileSize(), "its siblings survive the merge");
        assertEquals(16_000_000L, minor.targetFileSize());
        assertEquals(List.of("SET threads=2"), minor.connectionSettings(),
                "a list INSIDE the tier is untouched by overriding a scalar beside it");
        assertEquals(1000L, tier(config, "major").maxCompactedFiles(), "other tiers untouched");
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
        insert("compaction.compaction_tiers.mega.target_file_size", "256MB");
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
                  mega  { enabled = true, frequency = 1 hour,   min_file_size = 64MB, max_file_size = 256MB, target_file_size = 256MB, max_compacted_files = 5, connection_settings = [] }
                  minor { enabled = true, frequency = 1 minute, min_file_size = 0,    max_file_size = 8MB,   target_file_size = 8MB,   max_compacted_files = 5, connection_settings = [] }
                  major { enabled = true, frequency = 5 minutes, min_file_size = 8MB, max_file_size = 64MB,  target_file_size = 64MB,  max_compacted_files = 5, connection_settings = [] }
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
                  minor { name = "major", enabled = true, frequency = 1 minute, min_file_size = 0, max_file_size = 8MB, target_file_size = 8MB, max_compacted_files = 5, connection_settings = [] }
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
                  { name = "major", enabled = true, frequency = 1 hour,   min_file_size = 8MB, max_file_size = 64MB, target_file_size = 64MB, max_compacted_files = 5, connection_settings = [] }
                  { name = "minor", enabled = true, frequency = 1 minute, min_file_size = 0,   max_file_size = 8MB,  target_file_size = 8MB,  max_compacted_files = 5, connection_settings = [] }
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

    @Test
    void aTiersBlockThatIsNeitherKeyedNorAListIsRejectedClearly() {
        Config raw = ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers = 5
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """);
        // Not a ClassCastException: this module refuses to start on bad config *and says why*.
        var e = assertThrows(IllegalArgumentException.class, () -> resolve(raw));
        assertTrue(e.getMessage().contains("keyed by name"), e.getMessage());
    }

    @Test
    void overridingATierWhileStillOnTheListShapeSaysWhatWentWrong() throws Exception {
        // The trap this change introduces: the docs now advertise per-tier overrides, but against
        // a LIST an override is an object that REPLACES the list, leaving one tier with one field.
        // The bare HOCON error ("No configuration setting found for key 'enabled'") names neither
        // the tier nor the cause.
        insert("compaction.compaction_tiers.minor.frequency", "10 seconds");
        Config listShaped = ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers = [
                  { name = "minor", enabled = true, frequency = 1 minute, min_file_size = 0, max_file_size = 8MB, target_file_size = 8MB, max_compacted_files = 5, connection_settings = [] }
                ]
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """ + """
                config_provider {
                  class = "io.dazzleduck.sql.commons.TableConfigProvider"
                  table = "%s"
                  prefix = "compaction."
                }
                """.formatted(TABLE));

        var e = assertThrows(IllegalArgumentException.class, () -> resolve(listShaped));
        assertTrue(e.getMessage().contains("minor"), e.getMessage());
        assertTrue(e.getMessage().contains("key the tiers by name first"), e.getMessage());
    }

    @Test
    void deleteFileRewriteIsOnByDefaultAndDefersTheThresholdToTheCatalog() throws Exception {
        CompactionConfig config = resolve(ConfigFactory.parseString(FILE_CONFIG));
        assertTrue(config.rewriteDeletesEnabled());
        assertNull(config.rewriteDeleteThreshold(),
                "unset must mean DuckLake's own rewrite_delete_threshold, not a value baked in here");
    }

    @Test
    void deleteFileRewriteSettingsCanComeFromTheTable() throws Exception {
        insert("compaction.rewrite_deletes_enabled", "false");
        insert("compaction.rewrite_delete_threshold", "0.5");

        CompactionConfig config = resolve(withProvider("compaction."));
        assertFalse(config.rewriteDeletesEnabled());
        assertEquals(0.5, config.rewriteDeleteThreshold());
    }

    @Test
    void aRewriteDeleteThresholdOutsideZeroToOneIsRejected() {
        var e = assertThrows(IllegalArgumentException.class,
                () -> resolve(ConfigFactory.parseString(FILE_CONFIG + "rewrite_delete_threshold = 1.5\n")));
        assertTrue(e.getMessage().contains("rewrite_delete_threshold"), e.getMessage());
    }

    @Test
    void rewriteSqlIsPerTableAndPassesTheThresholdOnlyWhenSet() {
        assertEquals("CALL ducklake_rewrite_data_files('mylake', 'events', schema := 'main')",
                DuckLakeHousekeeper.rewriteDataFilesSql("mylake", "main", "events", null));
        assertEquals("CALL ducklake_rewrite_data_files('mylake', 'o''brien', schema := 'main', delete_threshold => 0.5)",
                DuckLakeHousekeeper.rewriteDataFilesSql("mylake", "main", "o'brien", 0.5));
    }

    // ── Dynamic compaction startup checks (DYNAMIC_COMPACTION_SPEC.md) ──────────────────────────

    private static Config oneTier(String fields) {
        return ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers { minor { enabled = true, min_file_size = 0, max_file_size = 8MB, %s } }
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """.formatted(fields));
    }

    @Test
    void anEnabledTierNeedsATarget() {
        var e = assertThrows(IllegalArgumentException.class, () -> resolve(oneTier("max_compacted_files = 12")));
        assertTrue(e.getMessage().contains("target_file_size"), e.getMessage());
    }

    @Test
    void aDisabledTierNeedsNoTarget() throws Exception {
        Config raw = ConfigFactory.parseString("""
                databases = ["mylake"]
                compaction_tiers { minor { enabled = false, min_file_size = 0, max_file_size = 8MB, max_compacted_files = 0 } }
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """);
        assertFalse(tier(resolve(raw), "minor").enabled());
    }

    @Test
    void aTargetBelowTheBandsMaximumIsRejected() {
        // DuckLake skips a file at least as large as the target, so [4MB, 8MB) would never merge.
        var e = assertThrows(IllegalArgumentException.class,
                () -> resolve(oneTier("target_file_size = 4MB, max_compacted_files = 12")));
        assertTrue(e.getMessage().contains("at least"), e.getMessage());
    }

    @Test
    void theStaticCapMustBePositive() {
        var e = assertThrows(IllegalArgumentException.class,
                () -> resolve(oneTier("target_file_size = 16MB, max_compacted_files = 0")));
        assertTrue(e.getMessage().contains("max_compacted_files"), e.getMessage());
    }

    @Test
    void minFilesBelowTwoIsRejected() {
        var e = assertThrows(IllegalArgumentException.class,
                () -> resolve(oneTier("target_file_size = 16MB, max_compacted_files = 12, min_files = 1")));
        assertTrue(e.getMessage().contains("min_files"), e.getMessage());
    }

    @Test
    void theSpecsTierSettingsParseAndTheRestDefault() throws Exception {
        CompactionTier minor = tier(resolve(oneTier("""
                target_file_size = 16MB, max_compacted_files = 12, min_files = 4, max_wait = 5 minutes,
                memory_share = 0.4, threads_per_cpu = 6, max_files_per_call = 3000,
                pass_time_limit = 1 minute, idle_sleep_max = 5 seconds, call_time_limit = 8 minutes""")), "minor");
        assertEquals(0.4, minor.memoryShare());
        assertEquals(6, minor.threadsPerCpu());
        assertEquals(16_000_000L, minor.targetFileSize());
        assertEquals(12L, minor.maxCompactedFiles());
        assertEquals(4, minor.minFiles());
        assertEquals(Duration.ofMinutes(5), minor.maxWait());
        assertEquals(3000L, minor.maxFilesPerCall());
        assertEquals(Duration.ofMinutes(1), minor.passTimeLimit());
        assertEquals(Duration.ofSeconds(5), minor.idleSleep());
        assertEquals(Duration.ofMinutes(8), minor.callTimeLimit());

        CompactionTier defaults = tier(resolve(oneTier("target_file_size = 16MB, max_compacted_files = 12")), "minor");
        assertEquals(CompactionTier.DEFAULT_MIN_FILES, defaults.minFiles());
        assertEquals(Duration.ZERO, defaults.maxWait(), "no time trigger unless one is set");
        assertEquals(CompactionTier.DEFAULT_MAX_FILES_PER_CALL, defaults.maxFilesPerCall());
        assertEquals(CompactionTier.DEFAULT_PASS_TIME_LIMIT, defaults.passTimeLimit());
        assertEquals(CompactionTier.DEFAULT_IDLE_SLEEP, defaults.idleSleep());
        assertEquals(CompactionTier.DEFAULT_CALL_TIME_LIMIT, defaults.callTimeLimit());
        assertEquals(1.0, defaults.memoryShare(), "the only enabled tier gets all the tiers' memory");
        assertEquals(CompactionTier.DEFAULT_THREADS_PER_CPU, defaults.threadsPerCpu());

        var e = assertThrows(IllegalArgumentException.class,
                () -> resolve(oneTier("target_file_size = 16MB, max_compacted_files = 12, threads_per_cpu = 0")));
        assertTrue(e.getMessage().contains("threads_per_cpu"), e.getMessage());

        CompactionTier legacy = tier(resolve(oneTier("target_file_size = 16MB, max_compacted_files = 12, cpu_share = 0.5")),
                "minor");
        assertEquals(CompactionTier.DEFAULT_THREADS_PER_CPU, legacy.threadsPerCpu(), "cpu_share is ignored, not refused");
    }

    @Test
    void memorySharesAreSplitAndChecked() throws Exception {
        String tiers = """
                databases = ["mylake"]
                compaction_tiers {
                  minor { enabled = true, min_file_size = 0, max_file_size = 8MB, target_file_size = 16MB, max_compacted_files = 12, memory_share = %s }
                  major { enabled = true, min_file_size = 8MB, max_file_size = 64MB, target_file_size = 64MB, max_compacted_files = 16 }
                  mega  { enabled = true, min_file_size = 64MB, max_file_size = 256MB, target_file_size = 256MB, max_compacted_files = 5 }
                }
                housekeeping_frequency = 5 minutes
                housekeeping_connection_settings = []
                snapshot_retention = 60 minutes
                health_port = 9090
                """;
        CompactionConfig config = resolve(ConfigFactory.parseString(tiers.formatted("0.4")));
        assertEquals(0.3, tier(config, "major").memoryShare(), 1e-9, "an equal part of what minor leaves");

        var e = assertThrows(IllegalArgumentException.class, () -> resolve(ConfigFactory.parseString(tiers.formatted("1.5"))));
        assertTrue(e.getMessage().contains("memory_share"), e.getMessage());
    }

    @Test
    void theSpillDirectoryDefaultsAndCanBeTurnedOff() throws Exception {
        CompactionConfig defaults = resolve(oneTier("target_file_size = 16MB, max_compacted_files = 12"));
        assertEquals(SpillBudget.DEFAULT_DIRECTORY, defaults.spillDirectory());
        assertEquals(-1L, defaults.spillMaxSize());

        CompactionConfig set = resolve(oneTier("target_file_size = 16MB, max_compacted_files = 12")
                .withFallback(ConfigFactory.parseString("spill_directory = \"\", spill_max_size = 20GB")));
        assertEquals("", set.spillDirectory());
        assertEquals(20_000_000_000L, set.spillMaxSize());
        assertSame(SpillBudget.NONE, SpillBudget.create(set.spillDirectory(), set.spillMaxSize()));
    }

    @Test
    void theBundledConfigShipsTheSpecsThreeTiers() throws Exception {
        CompactionConfig config = resolve(ConfigFactory.load("application").getConfig("dazzleduck_sql_compaction"));
        assertEquals(List.of("minor", "major", "mega"), config.tiers().stream().map(CompactionTier::name).toList());
        CompactionTier minor = tier(config, "minor");
        assertEquals(16_000_000L, minor.targetFileSize());
        assertEquals(12L, minor.maxCompactedFiles());
        assertEquals(4, minor.minFiles());
        assertEquals(Duration.ofMinutes(5), minor.maxWait());
        assertEquals(16L, tier(config, "major").maxCompactedFiles());
        assertEquals(5L, tier(config, "mega").maxCompactedFiles());
        assertEquals(256_000_000L, tier(config, "mega").targetFileSize());
        assertEquals(1.0, config.tiers().stream().mapToDouble(CompactionTier::memoryShare).sum(), 1e-9);
        assertEquals(0.4, minor.memoryShare(), 1e-9);
        assertEquals(CompactionTier.DEFAULT_THREADS_PER_CPU, minor.threadsPerCpu());
    }
}
