package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import io.dazzleduck.sql.commons.util.CommandLineConfigUtil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public record CompactionConfig(
        List<String> databases,
        List<CompactionTier> tiers,
        Duration housekeepingFrequency,
        Duration snapshotRetention,
        List<String> housekeepingConnectionSettings,
        int healthPort,
        int runHistorySize
) {
    private static final String CONFIG_PATH = "dazzleduck_sql_compaction";
    private static final String TIERS_KEY = "compaction_tiers";

    public static Config rawConfig(String[] args) throws Exception {
        Config overrides = CommandLineConfigUtil.loadCommandLineConfig(args).config();
        return overrides
                .withFallback(ConfigFactory.load("application"))
                .withFallback(ConfigFactory.systemProperties())
                .resolve()
                .getConfig(CONFIG_PATH);
    }

    public static CompactionConfig load(String[] args) throws Exception {
        return from(rawConfig(args));
    }

    static CompactionConfig from(Config c) {
        List<CompactionTier> tiers = parseTiers(c);
        validateTiers(tiers);

        return new CompactionConfig(
                c.getStringList("databases"),
                tiers,
                c.getDuration("housekeeping_frequency"),
                c.getDuration("snapshot_retention"),
                c.getStringList("housekeeping_connection_settings"),
                c.getInt("health_port"),
                c.hasPath("run_history_size")
                        ? c.getInt("run_history_size") : CompactionRunLog.DEFAULT_CAPACITY
        );
    }

    /**
     * Reads {@code compaction_tiers} in either shape.
     *
     * <p>The preferred shape is an OBJECT keyed by tier name, because HOCON merges objects
     * field-by-field but replaces lists wholesale. Keyed by name, one override retunes one field
     * and leaves the rest of that tier — and every other tier — alone:
     *
     * <pre>{@code
     * --conf 'dazzleduck_sql_compaction.compaction_tiers.minor.enabled = false'
     * }</pre>
     *
     * With a list there is no such override: {@code compaction_tiers.0.enabled} parses as an
     * object, which replaces the whole list, and startup then fails on the first field the
     * replacement did not restate. That same asymmetry decides whether a tier field can come from
     * a {@code config_provider} table — see the override section of this module's README.
     *
     * <p>The LIST shape is still accepted, so configs written against 0.2.19 keep working.
     */
    private static List<CompactionTier> parseTiers(Config c) {
        ConfigValue configured = c.getValue(TIERS_KEY);
        if (configured.valueType() == ConfigValueType.LIST) {
            return parseTierList(c);
        }
        if (configured.valueType() != ConfigValueType.OBJECT) {
            throw new IllegalArgumentException(
                    "%s must be tiers keyed by name, or a list of tiers, but is %s"
                            .formatted(TIERS_KEY, configured.valueType()));
        }
        return parseTierObject(configured);
    }

    /**
     * The tier name comes from the key, so it can be neither duplicated nor omitted. Ordered by
     * {@code min_file_size} rather than by key, since an object's key order is not meaningful —
     * that keeps logs and the per-tier file-count gauges in band order.
     */
    private static List<CompactionTier> parseTierObject(ConfigValue configured) {
        ConfigObject root = (ConfigObject) configured;
        List<CompactionTier> tiers = new ArrayList<>(root.size());
        for (Map.Entry<String, ConfigValue> entry : root.entrySet()) {
            String name = entry.getKey();
            if (entry.getValue().valueType() != ConfigValueType.OBJECT) {
                throw new IllegalArgumentException(
                        "Compaction tier '%s' must be an object of tier settings, got %s"
                                .formatted(name, entry.getValue().valueType()));
            }
            Config tier = ((ConfigObject) entry.getValue()).toConfig();
            // A half-migrated config keeping `name` under a differently-named key would otherwise
            // run under one name while being tuned under the other.
            if (tier.hasPath("name") && !tier.getString("name").equals(name)) {
                throw new IllegalArgumentException(
                        ("Compaction tier keyed '%s' declares name '%s' — the key is the tier name,"
                                + " so drop the field").formatted(name, tier.getString("name")));
            }
            try {
                tiers.add(toTier(name, tier));
            } catch (ConfigException.Missing e) {
                // Reached most often by a config still on the LIST shape that has picked up a
                // tier override: the override is an object, so it REPLACES the list, leaving one
                // tier holding only the overridden field. Without this the failure surfaces as a
                // bare "No configuration setting found for key 'enabled'" pointing at nothing.
                throw new IllegalArgumentException(
                        ("Compaction tier '%s' is incomplete: %s. A tier must declare enabled,"
                                + " frequency, min_file_size, max_file_size and max_compacted_files."
                                + " If %s is still a LIST in your config, an override such as"
                                + " %s.%s.frequency replaces the whole list rather than merging into"
                                + " it — key the tiers by name first.")
                                .formatted(name, e.getMessage(), TIERS_KEY, TIERS_KEY, name), e);
            }
        }
        tiers.sort(Comparator.comparingLong(CompactionTier::minFileSize));
        return List.copyOf(tiers);
    }

    /** Declaration order is preserved, being the only ordering a list ever had. */
    private static List<CompactionTier> parseTierList(Config c) {
        return c.getConfigList(TIERS_KEY).stream()
                .map(t -> toTier(t.getString("name"), t))
                .toList();
    }

    /**
     * {@code connection_settings} defaults to empty rather than being required: it is the tier's
     * only list-valued field, and a key/value {@code config_provider} table can supply every
     * scalar but not a list. Optional, a tier can be declared entirely from such a table.
     */
    private static CompactionTier toTier(String name, Config t) {
        return new CompactionTier(
                name,
                t.getBoolean("enabled"),
                t.getDuration("frequency"),
                t.getBytes("min_file_size"),
                t.getBytes("max_file_size"),
                t.getLong("max_compacted_files"),
                t.hasPath("connection_settings") ? t.getStringList("connection_settings") : List.of());
    }

    /**
     * Tiers run concurrently with no lock between them, safe only because their file-size ranges
     * never overlap. Refuses to start rather than run silently on a config that could race — the
     * same "loud failure over silent misconfiguration" philosophy the rest of this config uses.
     */
    private static void validateTiers(List<CompactionTier> tiers) {
        Set<String> names = new HashSet<>();
        for (CompactionTier tier : tiers) {
            if (!names.add(tier.name())) {
                throw new IllegalArgumentException("Duplicate compaction tier name: " + tier.name());
            }
            if (tier.enabled() && tier.maxFileSize() <= tier.minFileSize()) {
                throw new IllegalArgumentException(
                        "Compaction tier '%s': max_file_size (%d) must be greater than min_file_size (%d)"
                                .formatted(tier.name(), tier.maxFileSize(), tier.minFileSize()));
            }
        }

        List<CompactionTier> enabledSorted = tiers.stream()
                .filter(CompactionTier::enabled)
                .sorted(Comparator.comparingLong(CompactionTier::minFileSize))
                .toList();
        for (int i = 1; i < enabledSorted.size(); i++) {
            CompactionTier previous = enabledSorted.get(i - 1);
            CompactionTier current = enabledSorted.get(i);
            if (current.minFileSize() < previous.maxFileSize()) {
                throw new IllegalArgumentException(
                        "Compaction tiers '%s' [%d, %d) and '%s' [%d, %d) overlap"
                                .formatted(previous.name(), previous.minFileSize(), previous.maxFileSize(),
                                        current.name(), current.minFileSize(), current.maxFileSize()));
            }
        }
    }
}
