package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;

import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The {@code data_retention} block: how often to delete expired rows, and the policy table that
 * says which rows of which tables expire.
 */
public record DataRetentionConfig(boolean enabled, Duration frequency, String policyTable,
                                  List<String> connectionSettings, int maxFilesPerCycle) {

    /** The default cap on the data files one table's DELETE drops in a cycle. */
    public static final int DEFAULT_MAX_FILES_PER_CYCLE = 1000;

    public static final DataRetentionConfig DISABLED =
            new DataRetentionConfig(false, Duration.ofHours(1), null, List.of(), DEFAULT_MAX_FILES_PER_CYCLE);

    public DataRetentionConfig(boolean enabled, Duration frequency, String policyTable, List<String> connectionSettings) {
        this(enabled, frequency, policyTable, connectionSettings, DEFAULT_MAX_FILES_PER_CYCLE);
    }

    /** The policy table is interpolated into SQL, so it must be a plain, possibly qualified, name. */
    private static final Pattern RELATION =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*(\\.[A-Za-z_][A-Za-z0-9_$]*){0,2}");

    public DataRetentionConfig {
        if (enabled) {
            if (policyTable == null || !RELATION.matcher(policyTable).matches()) {
                throw new IllegalArgumentException(
                        "data_retention.policy_table must name a table or view such as lake.main.retention_policy, got "
                                + policyTable);
            }
            if (frequency.isNegative() || frequency.toSeconds() < 1) {
                throw new IllegalArgumentException("data_retention.frequency must be at least 1 second, got " + frequency);
            }
        }
        if (maxFilesPerCycle < 0) {
            throw new IllegalArgumentException("data_retention.max_files_per_cycle must be 0 (no cap) or more, got "
                    + maxFilesPerCycle);
        }
        connectionSettings = List.copyOf(connectionSettings);
    }

    static DataRetentionConfig from(Config c) {
        if (!c.hasPath("data_retention")) {
            return DISABLED;
        }
        Config r = c.getConfig("data_retention");
        boolean enabled = r.hasPath("enabled") && r.getBoolean("enabled");
        return new DataRetentionConfig(
                enabled,
                r.hasPath("frequency") ? r.getDuration("frequency") : DISABLED.frequency(),
                r.hasPath("policy_table") ? r.getString("policy_table") : null,
                r.hasPath("connection_settings") ? r.getStringList("connection_settings") : List.of(),
                r.hasPath("max_files_per_cycle") ? r.getInt("max_files_per_cycle") : DEFAULT_MAX_FILES_PER_CYCLE);
    }
}
