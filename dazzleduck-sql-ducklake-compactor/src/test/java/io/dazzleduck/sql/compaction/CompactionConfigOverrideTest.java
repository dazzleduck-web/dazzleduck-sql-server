package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigProvider;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.TableConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

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
            minor_compaction_frequency = 1 minute
            major_compaction_frequency = 1 hour
            minor_compaction_max_size = 8MB
            major_compaction_max_size = 64MB
            housekeeping_frequency = 5 minutes
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
        ConfigProvider provider = ConfigProvider.load(raw);
        Config merged = provider == ConfigProvider.NONE
                ? raw
                : provider.overrides(raw).withFallback(raw);
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

    @Test
    void withNoProviderTheFileIsAuthoritativeAndNothingChanges() throws Exception {
        CompactionConfig config = resolve(ConfigFactory.parseString(FILE_CONFIG));
        assertEquals(Duration.ofMinutes(60), config.snapshotRetention());
        assertEquals(8_000_000L, config.minorCompactionMaxSize());
        assertEquals(9090, config.healthPort());
    }

    @Test
    void tableValuesWinOverTheBakedFile() throws Exception {
        insert("compaction.snapshot_retention", "120 minutes");
        insert("compaction.major_compaction_max_size", "128MB");

        CompactionConfig config = resolve(withProvider("compaction."));
        assertEquals(Duration.ofMinutes(120), config.snapshotRetention(),
                "the point of the phase: change a compaction setting without a new image");
        assertEquals(128_000_000L, config.majorCompactionMaxSize());
        // untouched keys keep the file's values, so adoption is incremental
        assertEquals(Duration.ofMinutes(1), config.minorCompactionFrequency());
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
        assertEquals(Duration.ofHours(1), config.majorCompactionFrequency(),
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
        insert("compaction.minor_compaction_frequency", "30s");
        insert("compaction.minor_compaction_max_size", "16MiB");
        insert("compaction.health_port", "9191");

        CompactionConfig config = resolve(withProvider("compaction."));
        assertEquals(Duration.ofSeconds(30), config.minorCompactionFrequency());
        assertEquals(16L * 1024 * 1024, config.minorCompactionMaxSize(), "MiB is binary, MB is not");
        assertEquals(9191, config.healthPort());
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
}
