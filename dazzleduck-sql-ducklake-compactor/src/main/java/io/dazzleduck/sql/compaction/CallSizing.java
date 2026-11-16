package io.dazzleduck.sql.compaction;

import java.time.Duration;

/**
 * The cap for one table call ({@code max_compacted_files}), as DYNAMIC_COMPACTION_SPEC.md's "Call
 * sizing" defines it:
 *
 * <pre>
 * files_per_group = min(ceil(target / average_file_size), files_waiting)
 * file_cap        = max(1, floor(MAX_FILES_PER_CALL / files_per_group))
 * time_limit      = min(CALL_TIME_LIMIT, 0.8 x IDLE_TXN_TIMEOUT)    (CALL_TIME_LIMIT without a timeout)
 * time_cap        = max(1, floor(time_limit / (files_per_group x sec_per_file)))
 * cap             = min(static_cap, time_cap, max(file_cap, busy_partitions + 1))
 * </pre>
 *
 * The cap is never above the tier's static cap: every rule here can only lower it.
 */
public final class CallSizing {

    /** Measured: about 22 files a second with a sort key, on 4 threads. */
    public static final double SEC_PER_FILE_SORTED = 0.045;
    /** A guess at twice the sorted rate; Phase 2 learns it per table. */
    public static final double SEC_PER_FILE_UNSORTED = 0.09;
    /** A partition with a file committed this recently is busy. */
    public static final Duration BUSY_WINDOW = Duration.ofMinutes(10);
    /** Share of the catalog's idle-in-transaction timeout a call is sized to use. */
    public static final double TIMEOUT_SHARE = 0.8;
    /**
     * The smallest file the startup check assumes: a full group of a tier whose band starts at 0 is
     * {@code target / TINY_FILE_BYTES} files (3,200 for a 16 MB target).
     */
    public static final long TINY_FILE_BYTES = 5_000;
    /** Measured peak memory per input file of a group, 1,000 small files: with and without a sort key. */
    public static final long PER_FILE_COST_SORTED = (long) (0.12 * 1024 * 1024);
    public static final long PER_FILE_COST_UNSORTED = (long) (0.85 * 1024 * 1024);
    /** Share of an instance's memory_limit one group is sized to use. */
    public static final double MEMORY_LIMIT_SHARE = 0.8;

    private CallSizing() {
    }

    /**
     * @param cap     what to pass as {@code max_compacted_files}
     * @param starved whether the busy-partition floor wanted more than the static or time cap allow,
     *                so the table's quieter partitions may not get a slot
     */
    public record Cap(int cap, boolean starved, long filesPerGroup) {
    }

    /**
     * @param idleTxnTimeoutMs the catalog's idle-in-transaction timeout, or 0 or less for none
     */
    public static Cap of(CompactionTier tier, TableWork work, long idleTxnTimeoutMs) {
        long staticCap = Math.max(1, tier.maxCompactedFiles());
        long average = Math.max(1, work.averageFileSize());
        long filesPerGroup = Math.max(1, Math.min(ceilDiv(tier.targetFileSize(), average), work.filesWaiting()));
        long fileCap = Math.max(1, tier.maxFilesPerCall() / filesPerGroup);
        long timeCap = Math.max(1, (long) Math.floor(timeLimitSeconds(tier, idleTxnTimeoutMs)
                / (filesPerGroup * secPerFile(work.sorted()))));
        long floor = (long) work.busyPartitions() + 1;
        long upper = Math.min(staticCap, timeCap);
        long cap = Math.min(upper, Math.max(fileCap, floor));
        return new Cap((int) Math.min(Integer.MAX_VALUE, cap), floor > upper, filesPerGroup);
    }

    /**
     * What one call does: its cap and the session target it runs with.
     *
     * @param target  {@code ducklake_target_file_size} for this call: the tier's target, or a lower one
     *                when a full group would not fit (see {@link #plan})
     * @param reduced whether {@code target} is lowered
     */
    public record Plan(Cap cap, long target, boolean reduced) {
    }

    /**
     * The most files one group of the table may have and still commit within 80% of the catalog's
     * idle-in-transaction timeout at the table's per-file rate, and fit in 80% of the instance's
     * {@code memory_limit} at its per-file cost. {@link Long#MAX_VALUE} when neither is known.
     */
    public static long maxFilesPerGroup(TableWork work, long idleTxnTimeoutMs, long memoryLimitBytes) {
        long max = Long.MAX_VALUE;
        if (idleTxnTimeoutMs > 0) {
            max = Math.min(max, (long) Math.floor(TIMEOUT_SHARE * idleTxnTimeoutMs / 1000.0 / secPerFile(work.sorted())));
        }
        if (memoryLimitBytes > 0) {
            long cost = work.sorted() ? PER_FILE_COST_SORTED : PER_FILE_COST_UNSORTED;
            max = Math.min(max, (long) Math.floor(MEMORY_LIMIT_SHARE * memoryLimitBytes / cost));
        }
        return max;
    }

    /**
     * The call for one table. DuckLake cannot cut a group by file count, so when a full group at the
     * tier's target would outrun the catalog's timeout or the instance's memory (an unsorted table of
     * tiny files: 3,200 files, 288 s and about 2.7 GiB), the call runs with a lower session target, the
     * files that fit times the average file size. Its outputs are smaller and stay in the band, to be
     * merged again later, but the table makes progress instead of being killed call after call.
     *
     * @param groupFilesLimit the most files per group this table has shown it can commit (see
     *                        {@code TierLoop}), or {@link Long#MAX_VALUE}
     * @return null when not even two files fit in a group: the table cannot be merged at all
     */
    public static Plan plan(CompactionTier tier, TableWork work, long idleTxnTimeoutMs, long memoryLimitBytes,
                            long groupFilesLimit) {
        long average = Math.max(1, work.averageFileSize());
        long fullGroup = Math.max(1, Math.min(ceilDiv(tier.targetFileSize(), average), work.filesWaiting()));
        long maxFiles = Math.min(maxFilesPerGroup(work, idleTxnTimeoutMs, memoryLimitBytes), groupFilesLimit);
        if (fullGroup <= maxFiles) {
            return new Plan(of(tier, work, idleTxnTimeoutMs), tier.targetFileSize(), false);
        }
        if (maxFiles < 2) {
            return null;
        }
        // a group adds files while its total is below the target: this many average files, no more
        long target = maxFiles * average;
        CompactionTier lowered = tier.withTargetFileSize(target);
        return new Plan(of(lowered, work, idleTxnTimeoutMs), target, true);
    }

    /** {@link #plan(CompactionTier, TableWork, long, long, long)} with no limit learnt for the table. */
    public static Plan plan(CompactionTier tier, TableWork work, long idleTxnTimeoutMs, long memoryLimitBytes) {
        return plan(tier, work, idleTxnTimeoutMs, memoryLimitBytes, Long.MAX_VALUE);
    }

    public static double secPerFile(boolean sorted) {
        return sorted ? SEC_PER_FILE_SORTED : SEC_PER_FILE_UNSORTED;
    }

    /** {@code CALL_TIME_LIMIT}, or 80% of the timeout if that is shorter. */
    public static double timeLimitSeconds(CompactionTier tier, long idleTxnTimeoutMs) {
        double limit = tier.callTimeLimit().toMillis() / 1000.0;
        if (idleTxnTimeoutMs > 0) {
            limit = Math.min(limit, TIMEOUT_SHARE * idleTxnTimeoutMs / 1000.0);
        }
        return limit;
    }

    /**
     * The startup check: whether one full group of the tier's smallest files fits in 80% of the
     * timeout at the sorted rate. Unsorted tables, twice as slow, are sized per table by
     * {@link #plan}. A tier whose single group cannot commit before the catalog kills
     * the transaction would fail every call, so it does not start. Always true without a timeout.
     */
    public static boolean fullGroupFits(CompactionTier tier, long idleTxnTimeoutMs) {
        return idleTxnTimeoutMs <= 0 || fullGroupSeconds(tier) <= TIMEOUT_SHARE * idleTxnTimeoutMs / 1000.0;
    }

    /** Seconds one full group of the tier's smallest files takes at the sorted rate. */
    public static double fullGroupSeconds(CompactionTier tier) {
        long smallest = Math.max(tier.minFileSize(), TINY_FILE_BYTES);
        long files = Math.min(tier.maxFilesPerCall(), ceilDiv(tier.targetFileSize(), smallest));
        return files * SEC_PER_FILE_SORTED;
    }

    private static long ceilDiv(long a, long b) {
        return -Math.floorDiv(-a, b);
    }
}
