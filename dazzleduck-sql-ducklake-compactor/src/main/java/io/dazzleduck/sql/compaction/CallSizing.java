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
    /**
     * Memory a merge needs per byte of input file, on top of the per-file cost: decompression, the
     * sort, and the writer's row-group buffer grow with a group's bytes, which dominates for files of
     * megabytes. A starting value; calibrate it with the per-call RSS readings
     * ({@code rss_growth_per_file}).
     */
    public static final double BYTES_EXPANSION = 4.0;
    /**
     * An allowance for what each DuckDB thread holds for its own reads (decoded pages, request
     * buffers). It is not taken off a group's memory: a load test measured about the same memory at 1,
     * 2 and 8 threads, and charging it left major merging 10 MB files two at a time at 2 GiB. It only
     * caps the threads ({@link #maxThreads}).
     */
    public static final long PER_THREAD_BYTES = 8L << 20;
    /**
     * The threads get no more than this share of a group's memory at {@link #PER_THREAD_BYTES} each.
     * Threads grow with CPUs and memory does not, and a container with a memory limit but no CPU limit
     * sees the node's CPUs: without a cap, a 64-CPU node would run 256 threads per tier, at a thread
     * count no measurement covers.
     */
    public static final double THREAD_MEMORY_SHARE = 0.5;

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
     * @param target      {@code ducklake_target_file_size} for this call: the tier's target, or a lower
     *                    one when a full group would not fit (see {@link #plan})
     * @param maxFileSize the {@code max_file_size} for this call: the band's, or half the lowered
     *                    target, so a lowered call leaves the outputs of earlier ones alone
     * @param reduced     whether {@code target} is lowered
     */
    public record Plan(Cap cap, long target, long maxFileSize, boolean reduced) {
    }

    /**
     * The memory one input file of {@code fileBytes} adds to a group: a per-file cost (measured on small
     * files: 0.12 MiB sorted, 0.85 MiB unsorted) plus {@link #BYTES_EXPANSION} times its bytes.
     */
    public static double memoryPerFile(long fileBytes, boolean sorted) {
        return (sorted ? PER_FILE_COST_SORTED : PER_FILE_COST_UNSORTED) + BYTES_EXPANSION * fileBytes;
    }

    /**
     * The most files one group of the table may have and still commit within 80% of the catalog's
     * idle-in-transaction timeout at the table's per-file rate, and fit in 80% of the instance's
     * {@code memory_limit} at {@link #memoryPerFile} for its average file. {@link Long#MAX_VALUE}
     * when neither is known.
     */
    public static long maxFilesPerGroup(TableWork work, long idleTxnTimeoutMs, long memoryLimitBytes) {
        long max = Long.MAX_VALUE;
        if (idleTxnTimeoutMs > 0) {
            max = Math.min(max, (long) Math.floor(TIMEOUT_SHARE * idleTxnTimeoutMs / 1000.0 / secPerFile(work.sorted())));
        }
        long groupMemory = groupMemoryBytes(memoryLimitBytes);
        if (groupMemory >= 0) {
            double perFile = memoryPerFile(Math.max(1, work.averageFileSize()), work.sorted());
            max = Math.min(max, (long) Math.floor(groupMemory / perFile));
        }
        return max;
    }

    /** The memory one group may use: 80% of the instance's {@code memory_limit}, or -1 when it is unknown. */
    public static long groupMemoryBytes(long memoryLimitBytes) {
        return memoryLimitBytes <= 0 ? -1 : (long) (MEMORY_LIMIT_SHARE * memoryLimitBytes);
    }

    /**
     * The most threads an instance with {@code memoryLimitBytes} gets: their {@link #PER_THREAD_BYTES}
     * take at most {@link #THREAD_MEMORY_SHARE} of 80% of it. At least 1; {@link Integer#MAX_VALUE}
     * when the limit is unknown.
     */
    public static int maxThreads(long memoryLimitBytes) {
        if (memoryLimitBytes <= 0) {
            return Integer.MAX_VALUE;
        }
        return (int) Math.max(1, Math.min(Integer.MAX_VALUE,
                (long) (THREAD_MEMORY_SHARE * MEMORY_LIMIT_SHARE * memoryLimitBytes / PER_THREAD_BYTES)));
    }

    /**
     * The call for one table. DuckLake cannot cut a group by file count, so when a full group at the
     * tier's target would outrun the catalog's timeout or the instance's memory (an unsorted table of
     * tiny files: 3,200 files, 288 s and about 2.7 GiB; or 10 MB files in a 256 MiB instance, which
     * holds 5), the call runs with a lower session target, the files that fit times the average file
     * size. Its outputs are smaller and stay in the band, to be
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
            return new Plan(of(tier, work, idleTxnTimeoutMs), tier.targetFileSize(), tier.maxFileSize(), false);
        }
        if (maxFiles < 2) {
            return null;
        }
        // a group adds files while its total is below the target: this many average files, no more
        long target = maxFiles * average;
        CompactionTier lowered = tier.withTargetFileSize(target);
        // Merged tiny files compress several-fold, so an output lands far below the target and would be
        // grouped with the next tiny files and rewritten on every pass. Files of half the lowered target
        // or more are left for when the table can be merged at the tier's own target, which bounds how
        // often one output is rewritten. DuckLake needs max_file_size above min_file_size.
        long maxFileSize = Math.max(tier.minFileSize() + 1, Math.min(tier.maxFileSize(), target / 2));
        return new Plan(of(lowered, work, idleTxnTimeoutMs), target, maxFileSize, true);
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
     * Whether one full group of the tier's smallest files fits in 80% of the timeout at the sorted
     * rate. When it does not, the tier still starts and {@link #plan} runs its tables with a lowered
     * target; see {@link #startupRefusal}. Always true without a timeout.
     */
    public static boolean fullGroupFits(CompactionTier tier, long idleTxnTimeoutMs) {
        return idleTxnTimeoutMs <= 0 || fullGroupSeconds(tier) <= TIMEOUT_SHARE * idleTxnTimeoutMs / 1000.0;
    }

    /**
     * The startup check, under the same condition as {@link #plan} returning null: why the tier must
     * not start, or {@code null}. A tier is refused only when not even two of its band's smallest files
     * fit in a group within 80% of the timeout (at the sorted rate) or 80% of the instance's
     * {@code memory_limit}: then no table of it could ever be merged. Otherwise full groups that do not
     * fit run with a lowered target.
     */
    public static String startupRefusal(CompactionTier tier, long idleTxnTimeoutMs, long memoryLimitBytes) {
        long smallest = Math.max(tier.minFileSize(), TINY_FILE_BYTES);
        if (idleTxnTimeoutMs > 0 && 2 * SEC_PER_FILE_SORTED > TIMEOUT_SHARE * idleTxnTimeoutMs / 1000.0) {
            return ("not even two files fit in one group within %.0f%% of the catalog's idle-in-transaction"
                    + " timeout (%d ms)").formatted(TIMEOUT_SHARE * 100, idleTxnTimeoutMs);
        }
        long groupMemory = groupMemoryBytes(memoryLimitBytes);
        if (groupMemory >= 0 && 2 * memoryPerFile(smallest, true) > groupMemory) {
            return ("not even two of its smallest files (%d bytes) fit in one group within %.0f%% of the instance's"
                    + " memory_limit (%d bytes): give the tier more memory (memory_limit in its connection_settings,"
                    + " or a larger memory_share or container) or disable it").formatted(smallest,
                    MEMORY_LIMIT_SHARE * 100, memoryLimitBytes);
        }
        return null;
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
