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
     * Whether one group of the table, at its own per-file rate, commits within 80% of the catalog's
     * idle-in-transaction timeout. DuckLake cannot cut a group by file count, so a table whose single
     * group cannot (an unsorted table of tiny files under a short timeout) would only be killed by the
     * catalog, call after call: it is skipped and reported instead. Always true without a timeout.
     */
    public static boolean groupFits(CompactionTier tier, TableWork work, long idleTxnTimeoutMs) {
        if (idleTxnTimeoutMs <= 0) {
            return true;
        }
        long average = Math.max(1, work.averageFileSize());
        long filesPerGroup = Math.max(1, Math.min(ceilDiv(tier.targetFileSize(), average), work.filesWaiting()));
        return filesPerGroup * secPerFile(work.sorted()) <= TIMEOUT_SHARE * idleTxnTimeoutMs / 1000.0;
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
     * timeout at the sorted rate. Unsorted tables, twice as slow, are checked per table by
     * {@link #groupFits}. A tier whose single group cannot commit before the catalog kills
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
