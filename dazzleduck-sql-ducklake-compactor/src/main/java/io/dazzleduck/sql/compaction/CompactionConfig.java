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
        int runHistorySize,
        boolean rewriteDeletesEnabled,
        Double rewriteDeleteThreshold,
        DataRetentionConfig dataRetention
) {
    private static final String CONFIG_PATH = "dazzleduck_sql_compaction";

    /**
     * Delete-file cleanup off, for callers (mostly tests) that only care about compaction tiers.
     */
    public CompactionConfig(List<String> databases, List<CompactionTier> tiers, Duration housekeepingFrequency,
                            Duration snapshotRetention, List<String> housekeepingConnectionSettings,
                            int healthPort, int runHistorySize) {
        this(databases, tiers, housekeepingFrequency, snapshotRetention, housekeepingConnectionSettings,
                healthPort, runHistorySize, false, null);
    }

    /** Data retention off, for callers (mostly tests) written before it existed. */
    public CompactionConfig(List<String> databases, List<CompactionTier> tiers, Duration housekeepingFrequency,
                            Duration snapshotRetention, List<String> housekeepingConnectionSettings,
                            int healthPort, int runHistorySize, boolean rewriteDeletesEnabled,
                            Double rewriteDeleteThreshold) {
        this(databases, tiers, housekeepingFrequency, snapshotRetention, housekeepingConnectionSettings,
                healthPort, runHistorySize, rewriteDeletesEnabled, rewriteDeleteThreshold, DataRetentionConfig.DISABLED);
    }
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
        List<CompactionTier> tiers = resolveCpuShares(parseTiers(c));
        validateTiers(tiers);

        return new CompactionConfig(
                c.getStringList("databases"),
                tiers,
                c.getDuration("housekeeping_frequency"),
                c.getDuration("snapshot_retention"),
                c.getStringList("housekeeping_connection_settings"),
                c.getInt("health_port"),
                c.hasPath("run_history_size")
                        ? c.getInt("run_history_size") : CompactionRunLog.DEFAULT_CAPACITY,
                !c.hasPath("rewrite_deletes_enabled") || c.getBoolean("rewrite_deletes_enabled"),
                rewriteDeleteThreshold(c),
                DataRetentionConfig.from(c)
        );
    }

    /**
     * {@code null} when unset, so DuckLake falls back to the catalog's own
     * {@code rewrite_delete_threshold} option (0.95 unless set with {@code ducklake_set_option}).
     */
    private static Double rewriteDeleteThreshold(Config c) {
        if (!c.hasPath("rewrite_delete_threshold")) {
            return null;
        }
        double threshold = c.getDouble("rewrite_delete_threshold");
        if (threshold < 0 || threshold > 1) {
            throw new IllegalArgumentException(
                    "rewrite_delete_threshold must be between 0 and 1, got " + threshold);
        }
        return threshold;
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
                                + " min_file_size, max_file_size, target_file_size and max_compacted_files."
                                + " If %s is still a LIST in your config, an override such as"
                                + " %s.%s.max_compacted_files replaces the whole list rather than merging into"
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
     *
     * <p>{@code target_file_size} is read as optional here and required of an enabled tier by
     * {@link #validateTiers}, so a disabled tier can leave it out. {@code frequency}, the timer of
     * releases before per-table scheduling, is no longer read: a config that still sets it starts.
     * An unset {@code cpu_share} is {@code NaN} until {@link #resolveCpuShares} fills it in.
     */
    private static CompactionTier toTier(String name, Config t) {
        return new CompactionTier(
                name,
                t.getBoolean("enabled"),
                t.getBytes("min_file_size"),
                t.getBytes("max_file_size"),
                t.hasPath("target_file_size") ? t.getBytes("target_file_size") : 0,
                t.getLong("max_compacted_files"),
                t.hasPath("cpu_share") ? t.getDouble("cpu_share") : Double.NaN,
                t.hasPath("pass_time_limit") ? t.getDuration("pass_time_limit") : CompactionTier.DEFAULT_PASS_TIME_LIMIT,
                t.hasPath("idle_sleep_max") ? t.getDuration("idle_sleep_max") : CompactionTier.DEFAULT_IDLE_SLEEP,
                t.hasPath("max_files_per_call") ? t.getLong("max_files_per_call") : CompactionTier.DEFAULT_MAX_FILES_PER_CALL,
                t.hasPath("min_files") ? t.getInt("min_files") : CompactionTier.DEFAULT_MIN_FILES,
                t.hasPath("max_wait") ? t.getDuration("max_wait") : Duration.ZERO,
                t.hasPath("call_time_limit") ? t.getDuration("call_time_limit") : CompactionTier.DEFAULT_CALL_TIME_LIMIT,
                t.hasPath("connection_settings") ? t.getStringList("connection_settings") : List.of());
    }

    /**
     * Each enabled tier without a {@code cpu_share} gets an equal part of what the others leave, so
     * the shares always add up to at most 1 (spec: Threads and cores). Disabled tiers get none.
     */
    static List<CompactionTier> resolveCpuShares(List<CompactionTier> tiers) {
        double set = 0;
        int unset = 0;
        for (CompactionTier tier : tiers) {
            if (!tier.enabled()) {
                continue;
            }
            if (Double.isNaN(tier.cpuShare())) {
                unset++;
            } else {
                set += tier.cpuShare();
            }
        }
        double each = unset == 0 ? 0 : (1.0 - set) / unset;
        List<CompactionTier> resolved = new ArrayList<>(tiers.size());
        for (CompactionTier tier : tiers) {
            if (!tier.enabled()) {
                resolved.add(Double.isNaN(tier.cpuShare()) ? tier.withCpuShare(0) : tier);
            } else if (Double.isNaN(tier.cpuShare())) {
                if (each <= 0) {
                    throw new IllegalArgumentException(("Compaction tier '%s' has no cpu_share, and the other"
                            + " enabled tiers' shares already add up to %s").formatted(tier.name(), set));
                }
                resolved.add(tier.withCpuShare(each));
            } else {
                resolved.add(tier);
            }
        }
        return List.copyOf(resolved);
    }

    /**
     * The startup checks of DYNAMIC_COMPACTION_SPEC.md for one enabled tier. A target below the band's
     * maximum would never merge the in-band files at or above it: DuckLake skips a file at least as
     * large as the target.
     */
    private static void validateEnabledTier(CompactionTier tier) {
        String name = tier.name();
        if (tier.maxFileSize() <= tier.minFileSize()) {
            throw new IllegalArgumentException(
                    "Compaction tier '%s': max_file_size (%d) must be greater than min_file_size (%d)"
                            .formatted(name, tier.maxFileSize(), tier.minFileSize()));
        }
        if (tier.targetFileSize() <= 0) {
            throw new IllegalArgumentException(("Compaction tier '%s' has no target_file_size: without it a merge"
                    + " group grows to the catalog's target (DuckLake defaults to 512 MB)").formatted(name));
        }
        if (tier.targetFileSize() < tier.maxFileSize()) {
            throw new IllegalArgumentException(("Compaction tier '%s': target_file_size (%d) must be at least"
                    + " max_file_size (%d), or the files between them never merge")
                    .formatted(name, tier.targetFileSize(), tier.maxFileSize()));
        }
        if (tier.maxCompactedFiles() <= 0) {
            throw new IllegalArgumentException(("Compaction tier '%s': max_compacted_files is the most groups one"
                    + " call merges and must be positive, got %d").formatted(name, tier.maxCompactedFiles()));
        }
        if (tier.maxFilesPerCall() <= 0) {
            throw new IllegalArgumentException("Compaction tier '%s': max_files_per_call must be positive, got %d"
                    .formatted(name, tier.maxFilesPerCall()));
        }
        if (tier.minFiles() < 2) {
            throw new IllegalArgumentException(("Compaction tier '%s': min_files must be at least 2, since a"
                    + " merge needs two files, got %d").formatted(name, tier.minFiles()));
        }
        if (!(tier.cpuShare() > 0 && tier.cpuShare() <= 1)) {
            throw new IllegalArgumentException("Compaction tier '%s': cpu_share must be in (0, 1], got %s"
                    .formatted(name, tier.cpuShare()));
        }
        requirePositive(name, "pass_time_limit", tier.passTimeLimit());
        requirePositive(name, "idle_sleep_max", tier.idleSleep());
        requirePositive(name, "call_time_limit", tier.callTimeLimit());
        if (tier.maxWait().isNegative()) {
            throw new IllegalArgumentException("Compaction tier '%s': max_wait must not be negative".formatted(name));
        }
    }

    private static void requirePositive(String tier, String key, Duration value) {
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(
                    "Compaction tier '%s': %s must be positive, got %s".formatted(tier, key, value));
        }
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
            // a tier's name is also its metrics' type tag, which these jobs use
            if (tier.name().equalsIgnoreCase(CompactionState.HOUSEKEEPING_KIND)
                    || tier.name().equalsIgnoreCase(CompactionState.RETENTION_KIND)) {
                throw new IllegalArgumentException("Compaction tier name '%s' is reserved".formatted(tier.name()));
            }
            if (tier.enabled()) {
                validateEnabledTier(tier);
            }
        }
        double shares = tiers.stream().filter(CompactionTier::enabled).mapToDouble(CompactionTier::cpuShare).sum();
        if (shares > 1 + 1e-9) {
            // Each tier has its own DuckDB instance, so their threads add up.
            throw new IllegalArgumentException(
                    "The enabled compaction tiers' cpu_share values add up to %s, more than 1".formatted(shares));
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
