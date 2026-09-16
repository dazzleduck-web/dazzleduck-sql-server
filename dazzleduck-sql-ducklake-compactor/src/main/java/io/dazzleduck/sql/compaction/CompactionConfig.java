package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.util.CommandLineConfigUtil;

import java.time.Duration;
import java.util.List;

public record CompactionConfig(
        List<String> databases,
        Duration minorCompactionFrequency,
        Duration majorCompactionFrequency,
        Duration housekeepingFrequency,
        long minorCompactionMaxSize,
        long minorCompactionMaxFiles,
        long majorCompactionMaxSize,
        Duration snapshotRetention,
        int healthPort
) {
    private static final String CONFIG_PATH = "dazzleduck_sql_compaction";

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
        return new CompactionConfig(
                c.getStringList("databases"),
                c.getDuration("minor_compaction_frequency"),
                c.getDuration("major_compaction_frequency"),
                c.getDuration("housekeeping_frequency"),
                c.getBytes("minor_compaction_max_size"),
                c.getLong("minor_compaction_max_files"),
                c.getBytes("major_compaction_max_size"),
                c.getDuration("snapshot_retention"),
                c.getInt("health_port")
        );
    }
}
