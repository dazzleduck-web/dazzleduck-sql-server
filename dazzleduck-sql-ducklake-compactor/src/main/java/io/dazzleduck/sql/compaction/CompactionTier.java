package io.dazzleduck.sql.compaction;

import java.time.Duration;
import java.util.List;

/**
 * One compaction tier: a band of file sizes, the target its merges write, and the limits that size
 * and schedule its per-table calls (DYNAMIC_COMPACTION_SPEC.md). Any number of tiers can be
 * configured, as long as enabled tiers' {@code [minFileSize, maxFileSize)} bands never overlap —
 * validated at startup in {@link CompactionConfig#from}.
 *
 * @param targetFileSize     {@code ducklake_target_file_size}, set on the session before every call.
 *                           It bounds each merge group, so a call's peak memory and each output file.
 *                           Required for an enabled tier, and at least {@code maxFileSize}
 * @param maxCompactedFiles  the static cap: the most {@code max_compacted_files} (groups) one call is
 *                           ever given. Every sizing rule can only lower it
 * @param cpuShare           share of the process's CPUs this tier's DuckDB instance gets; its threads
 *                           are {@link #THREADS_PER_CPU} times its CPUs, at least 1
 * @param passTimeLimit      after this, a pass starts no new call; the tables not yet called carry
 *                           over to the front of the next pass
 * @param idleSleep          how long the loop sleeps after a refresh that found no work
 * @param maxFilesPerCall    the most input files a call is sized to merge
 * @param minFiles           a table is listed when one of its groups has this many eligible files ...
 * @param maxWait            ... or when a group's second-oldest eligible file is this old;
 *                           {@link Duration#ZERO} for no time trigger
 * @param callTimeLimit      the longest a call is sized to run, at the per-file rate
 * @param connectionSettings raw SQL run on the tier's connection after it opens
 */
public record CompactionTier(
        String name,
        boolean enabled,
        long minFileSize,
        long maxFileSize,
        long targetFileSize,
        long maxCompactedFiles,
        double cpuShare,
        Duration passTimeLimit,
        Duration idleSleep,
        long maxFilesPerCall,
        int minFiles,
        Duration maxWait,
        Duration callTimeLimit,
        List<String> connectionSettings
) {

    public static final Duration DEFAULT_PASS_TIME_LIMIT = Duration.ofMinutes(2);
    public static final Duration DEFAULT_IDLE_SLEEP = Duration.ofSeconds(10);
    public static final long DEFAULT_MAX_FILES_PER_CALL = 6_000;
    /** Any two files in a group merge, so 2 is DuckLake's own trigger. */
    public static final int DEFAULT_MIN_FILES = 2;
    public static final Duration DEFAULT_CALL_TIME_LIMIT = Duration.ofMinutes(10);
    /** DuckDB threads per CPU of a tier: they mostly wait on object-store reads. */
    public static final int THREADS_PER_CPU = 2;

    public CompactionTier {
        connectionSettings = List.copyOf(connectionSettings);
    }

    /**
     * A tier with the spec's defaults for everything but its band, target and static cap, and all of
     * the process's CPUs. For tests and simple configs.
     */
    public static CompactionTier of(String name, long minFileSize, long maxFileSize, long targetFileSize,
                                    long maxCompactedFiles) {
        return new CompactionTier(name, true, minFileSize, maxFileSize, targetFileSize, maxCompactedFiles, 1.0,
                DEFAULT_PASS_TIME_LIMIT, DEFAULT_IDLE_SLEEP, DEFAULT_MAX_FILES_PER_CALL, DEFAULT_MIN_FILES,
                Duration.ZERO, DEFAULT_CALL_TIME_LIMIT, List.of());
    }

    public CompactionTier withEnabled(boolean value) {
        return new CompactionTier(name, value, minFileSize, maxFileSize, targetFileSize, maxCompactedFiles, cpuShare,
                passTimeLimit, idleSleep, maxFilesPerCall, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withCpuShare(double value) {
        return new CompactionTier(name, enabled, minFileSize, maxFileSize, targetFileSize, maxCompactedFiles, value,
                passTimeLimit, idleSleep, maxFilesPerCall, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withAdmission(int files, Duration wait) {
        return new CompactionTier(name, enabled, minFileSize, maxFileSize, targetFileSize, maxCompactedFiles, cpuShare,
                passTimeLimit, idleSleep, maxFilesPerCall, files, wait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withPassTimeLimit(Duration value) {
        return new CompactionTier(name, enabled, minFileSize, maxFileSize, targetFileSize, maxCompactedFiles, cpuShare,
                value, idleSleep, maxFilesPerCall, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withIdleSleep(Duration value) {
        return new CompactionTier(name, enabled, minFileSize, maxFileSize, targetFileSize, maxCompactedFiles, cpuShare,
                passTimeLimit, value, maxFilesPerCall, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withMaxFilesPerCall(long value) {
        return new CompactionTier(name, enabled, minFileSize, maxFileSize, targetFileSize, maxCompactedFiles, cpuShare,
                passTimeLimit, idleSleep, value, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withCallTimeLimit(Duration value) {
        return new CompactionTier(name, enabled, minFileSize, maxFileSize, targetFileSize, maxCompactedFiles, cpuShare,
                passTimeLimit, idleSleep, maxFilesPerCall, minFiles, maxWait, value, connectionSettings);
    }

    public CompactionTier withConnectionSettings(List<String> value) {
        return new CompactionTier(name, enabled, minFileSize, maxFileSize, targetFileSize, maxCompactedFiles, cpuShare,
                passTimeLimit, idleSleep, maxFilesPerCall, minFiles, maxWait, callTimeLimit, value);
    }

    /**
     * DuckDB threads for one of this tier's instances: {@link #THREADS_PER_CPU} times its CPUs, at
     * least 1. Every database runs its own instance of every tier, so the CPUs a tier's share gives it
     * are divided among the databases: the shares budget the whole process.
     */
    public int threads(int availableProcessors, int databases) {
        return Math.max(1, (int) Math.round(THREADS_PER_CPU * cpuShare * availableProcessors / Math.max(1, databases)));
    }
}
