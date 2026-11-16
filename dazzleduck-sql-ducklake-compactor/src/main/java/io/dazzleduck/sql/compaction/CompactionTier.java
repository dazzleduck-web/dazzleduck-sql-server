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
 * @param cpuShare           share of the process's CPUs this tier's DuckDB instances get; their
 *                           threads are {@link #THREADS_PER_CPU} times their CPUs
 * @param memoryShare        share of the tiers' memory this tier's instances get (see
 *                           {@link MemoryBudget}). Separate from {@code cpuShare}: minor merges many
 *                           tiny files and needs threads, major and mega merge few large files and
 *                           need memory
 * @param minThreads         the fewest DuckDB threads an instance gets, whatever its CPUs: merging
 *                           small files from an object store waits on round trips, not on CPU, so a
 *                           small CPU share must still keep requests in flight
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
        double memoryShare,
        int minThreads,
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
    /**
     * Enough threads to keep requests in flight against an object store: with 2 on a 1.5-CPU
     * container and ~25 ms per request, a load test used 8-77% of its CPUs.
     */
    public static final int DEFAULT_MIN_THREADS = 4;

    public CompactionTier {
        connectionSettings = List.copyOf(connectionSettings);
    }

    /**
     * A tier with the spec's defaults for everything but its band, target and static cap, and all of
     * the process's CPUs and of the tiers' memory. For tests and simple configs.
     */
    public static CompactionTier of(String name, long minFileSize, long maxFileSize, long targetFileSize,
                                    long maxCompactedFiles) {
        return new CompactionTier(name, true, minFileSize, maxFileSize, targetFileSize, maxCompactedFiles, 1.0, 1.0,
                DEFAULT_MIN_THREADS, DEFAULT_PASS_TIME_LIMIT, DEFAULT_IDLE_SLEEP, DEFAULT_MAX_FILES_PER_CALL,
                DEFAULT_MIN_FILES, Duration.ZERO, DEFAULT_CALL_TIME_LIMIT, List.of());
    }

    /** A copy with the fields given; the rest are this tier's. */
    private CompactionTier copy(boolean enabled, long maxFileSize, long targetFileSize, double cpuShare,
                                double memoryShare, int minThreads, Duration passTimeLimit, Duration idleSleep,
                                long maxFilesPerCall, int minFiles, Duration maxWait, Duration callTimeLimit,
                                List<String> connectionSettings) {
        return new CompactionTier(name, enabled, minFileSize, maxFileSize, targetFileSize, maxCompactedFiles, cpuShare,
                memoryShare, minThreads, passTimeLimit, idleSleep, maxFilesPerCall, minFiles, maxWait, callTimeLimit,
                connectionSettings);
    }

    public CompactionTier withEnabled(boolean value) {
        return copy(value, maxFileSize, targetFileSize, cpuShare, memoryShare, minThreads, passTimeLimit, idleSleep,
                maxFilesPerCall, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withCpuShare(double value) {
        return copy(enabled, maxFileSize, targetFileSize, value, memoryShare, minThreads, passTimeLimit, idleSleep,
                maxFilesPerCall, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withMemoryShare(double value) {
        return copy(enabled, maxFileSize, targetFileSize, cpuShare, value, minThreads, passTimeLimit, idleSleep,
                maxFilesPerCall, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withMinThreads(int value) {
        return copy(enabled, maxFileSize, targetFileSize, cpuShare, memoryShare, value, passTimeLimit, idleSleep,
                maxFilesPerCall, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withAdmission(int files, Duration wait) {
        return copy(enabled, maxFileSize, targetFileSize, cpuShare, memoryShare, minThreads, passTimeLimit, idleSleep,
                maxFilesPerCall, files, wait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withPassTimeLimit(Duration value) {
        return copy(enabled, maxFileSize, targetFileSize, cpuShare, memoryShare, minThreads, value, idleSleep,
                maxFilesPerCall, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withIdleSleep(Duration value) {
        return copy(enabled, maxFileSize, targetFileSize, cpuShare, memoryShare, minThreads, passTimeLimit, value,
                maxFilesPerCall, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withMaxFilesPerCall(long value) {
        return copy(enabled, maxFileSize, targetFileSize, cpuShare, memoryShare, minThreads, passTimeLimit, idleSleep,
                value, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withCallTimeLimit(Duration value) {
        return copy(enabled, maxFileSize, targetFileSize, cpuShare, memoryShare, minThreads, passTimeLimit, idleSleep,
                maxFilesPerCall, minFiles, maxWait, value, connectionSettings);
    }

    public CompactionTier withMaxFileSize(long value) {
        return copy(enabled, value, targetFileSize, cpuShare, memoryShare, minThreads, passTimeLimit, idleSleep,
                maxFilesPerCall, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withTargetFileSize(long value) {
        return copy(enabled, maxFileSize, value, cpuShare, memoryShare, minThreads, passTimeLimit, idleSleep,
                maxFilesPerCall, minFiles, maxWait, callTimeLimit, connectionSettings);
    }

    public CompactionTier withConnectionSettings(List<String> value) {
        return copy(enabled, maxFileSize, targetFileSize, cpuShare, memoryShare, minThreads, passTimeLimit, idleSleep,
                maxFilesPerCall, minFiles, maxWait, callTimeLimit, value);
    }

    /**
     * DuckDB threads for one of this tier's instances: {@link #THREADS_PER_CPU} times its CPUs, and
     * at least {@code minThreads}. Every database runs its own instance of every tier, so the CPUs a
     * tier's share gives it are divided among the databases: the shares budget the whole process.
     */
    public int threads(int availableProcessors, int databases) {
        int fromCpus = (int) Math.round(THREADS_PER_CPU * cpuShare * availableProcessors / Math.max(1, databases));
        return Math.max(Math.max(1, minThreads), fromCpus);
    }
}
