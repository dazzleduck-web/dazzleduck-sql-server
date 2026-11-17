package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CompactionState {

    private static final String DURATION_METRIC    = "ducklake.compaction.duration";
    private static final String CYCLE_COUNT_METRIC = "ducklake.compaction.cycles";
    private static final String FAILURE_COUNT_METRIC = "ducklake.compaction.failures";
    private static final String LAST_SUCCESS_AGE_METRIC = "ducklake.compaction.last_success_age";
    private static final String FILES_COMPACTED_METRIC = "ducklake.files.compacted";
    private static final String FILES_REWRITTEN_METRIC = "ducklake.files.rewritten";
    private static final String TIER_FILES_METRIC = "ducklake.files.by_tier";
    private static final String TOTAL_FILES_METRIC = "ducklake.files.total";
    private static final String FAILURE_BY_CLASS_METRIC = "ducklake.compaction.failures_by_class";
    private static final String CYCLES_BY_OUTCOME_METRIC = "ducklake.compaction.cycles_by_outcome";
    // The tier's static cap, and the cap its latest call was given (Call sizing).
    private static final String MAX_COMPACTED_FILES_METRIC = "ducklake.compaction.max_compacted_files";
    private static final String CAP_METRIC = "ducklake.compaction.cap";
    // Per tier: its loop's passes. An alert watches pass_age (DYNAMIC_COMPACTION_SPEC.md).
    private static final String PASSES_METRIC = "ducklake.compaction.passes";
    private static final String PASS_AGE_METRIC = "ducklake.compaction.pass_age";
    private static final String TIER_SUCCESS_AGE_METRIC = "ducklake.compaction.tier_last_success_age";
    private static final String QUEUE_METRIC = "ducklake.compaction.queue";
    private static final String CARRY_OVER_METRIC = "ducklake.compaction.carry_over";
    private static final String BACKOFFS_METRIC = "ducklake.compaction.backoffs";
    // Calls that committed a merge: each is one snapshot, so its rate is the commits per minute that
    // snapshot expiry has to keep up with.
    private static final String COMMITS_METRIC = "ducklake.compaction.commits";
    // Per table and tier, for the starvation, back-off and oldest-small-file alerts.
    private static final String TABLE_FILES_WAITING_METRIC = "ducklake.compaction.table.files_waiting";
    private static final String TABLE_OLDEST_WAIT_METRIC = "ducklake.compaction.table.oldest_wait";
    private static final String TABLE_STARVED_METRIC = "ducklake.compaction.table.starved";
    private static final String TABLE_BACKOFF_METRIC = "ducklake.compaction.table.backoff";
    private static final String UNEXPIRED_SNAPSHOTS_METRIC = "ducklake.snapshots.unexpired";
    private static final String RSS_METRIC = "ducklake.process.rss_bytes";
    private static final String MEMORY_LIMIT_METRIC = "ducklake.process.memory_limit_bytes";
    private static final String SPILL_PEAK_METRIC = "ducklake.compaction.spill_peak_bytes";
    private static final String RSS_GROWTH_METRIC = "ducklake.compaction.rss_growth_per_file";
    private static final String RSS_PEAK_METRIC = "ducklake.compaction.rss_peak_bytes";
    private static final String BOUNDARY_LAG_METRIC = "ducklake.retention.boundary_lag";
    private static final String ROWS_DELETED_METRIC = "ducklake.retention.rows_deleted";

    /** Failure kind used for housekeeping cycles, which aren't a tier. */
    static final String HOUSEKEEPING_KIND = "housekeeping";

    /** Failure kind used for data-retention cycles, which aren't a tier either. */
    static final String RETENTION_KIND = "retention";

    private final MeterRegistry registry;
    private final List<String> tierNames;
    private final Instant serviceStart = Instant.now();

    // Per-database, per-tier successful-cycle counters (also back Micrometer FunctionCounters)
    private final ConcurrentHashMap<String, Map<String, AtomicLong>> tierCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> filesCompacted = new ConcurrentHashMap<>();
    // Data files rewritten by housekeeping's delete-file rewrite (ducklake_rewrite_data_files).
    private final ConcurrentHashMap<String, AtomicLong> filesRewritten = new ConcurrentHashMap<>();
    // Per-database, per-failure-class cumulative counters, tagged so opposite failure modes stay distinct.
    private final ConcurrentHashMap<String, Map<CompactionRun.FailureClass, AtomicLong>> failureClassCounts = new ConcurrentHashMap<>();
    // Per-(database, tier) cumulative cycle counters, tagged by outcome (SUCCESS/EMPTY/FAILED).
    private final ConcurrentHashMap<String, Map<String, Map<CompactionRun.Outcome, AtomicLong>>> outcomeCounts = new ConcurrentHashMap<>();
    // Per-(database, tier) holder for the latest control inputs + derived per-cycle gauges.
    private final ConcurrentHashMap<String, Map<String, TierGauges>> tierGauges = new ConcurrentHashMap<>();
    // Process-wide peak RSS (VmHWM is monotonic), so a single series rather than one per tier.
    private final AtomicLong rssPeakBytes = new AtomicLong(0);

    /** Mutable backing for the per-(database, tier) gauges. */
    private static final class TierGauges {
        volatile long maxCompactedFiles;
        volatile long cap;
        volatile long spillPeakBytes;
        volatile double rssGrowthPerFile;
        volatile long queue;
        volatile long carryOver;
        volatile long backoffs;
        volatile Instant lastPass;
        volatile Instant lastSuccess;
        final AtomicLong passes = new AtomicLong();
        final AtomicLong commits = new AtomicLong();
    }

    /** Mutable backing for one table's gauges in one tier. */
    private static final class TableGauges {
        volatile long filesWaiting;
        volatile long oldestWaitSeconds;
        volatile double starved;
        volatile double backoff;
    }

    // "db|tier|table" -> gauges, registered the first time a table is seen
    private final ConcurrentHashMap<String, TableGauges> tableGauges = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> unexpiredSnapshots = new ConcurrentHashMap<>();
    private final AtomicLong rssBytes = new AtomicLong(-1);
    private final AtomicLong memoryLimitBytes = new AtomicLong(-1);

    // Failures are attributable to the tier that threw, or HOUSEKEEPING_KIND. Each inner map is
    // fully populated on creation, so only the counters are ever mutated — never the map structure.
    private final ConcurrentHashMap<String, Map<String, AtomicLong>> failureCounts = new ConcurrentHashMap<>();

    // Per-database, per-tier current-file-count gauges, plus one whole-catalog total per database.
    private final ConcurrentHashMap<String, Map<String, AtomicLong>> tierFiles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> totalFiles = new ConcurrentHashMap<>();

    // Set when a table call of any tier completes without an error
    private final ConcurrentHashMap<String, AtomicReference<Instant>> lastSuccessTimes = new ConcurrentHashMap<>();

    // Set at the end of every pass of any tier, regardless of outcome.
    private final ConcurrentHashMap<String, AtomicReference<Instant>> lastRunTimes = new ConcurrentHashMap<>();

    public CompactionState(MeterRegistry registry, List<String> databases, List<String> tierNames) {
        this.registry = registry;
        this.tierNames = tierNames;
        databases.forEach(this::registerDatabase);
        // Process-wide peak RSS (VmHWM) — one series, updated by whichever cycle sampled it last.
        Gauge.builder(RSS_PEAK_METRIC, rssPeakBytes, AtomicLong::get)
                .description("Peak process RSS observed at a cycle end (/proc VmHWM)")
                .baseUnit("bytes")
                .register(registry);
        Gauge.builder(RSS_METRIC, rssBytes, AtomicLong::get)
                .description("Process RSS at the last sample (/proc VmRSS); -1 when unknown")
                .baseUnit("bytes")
                .register(registry);
        Gauge.builder(MEMORY_LIMIT_METRIC, memoryLimitBytes, AtomicLong::get)
                .description("The container's memory limit (cgroup); -1 without one")
                .baseUnit("bytes")
                .register(registry);
    }

    /**
     * Registers the per-(database, tier) gauges and counters: static cap and latest cap, the loop's
     * passes, queue, carry-over and back-offs, commits, spill peak, and the outcome-tagged call
     * counters. All read from mutable holders.
     */
    private void registerTierGaugesAndOutcomes(String db, String tierName) {
        TierGauges g = tierGauges(db, tierName);
        Gauge.builder(MAX_COMPACTED_FILES_METRIC, g, h -> h.maxCompactedFiles)
                .description("This tier's static cap: the most groups one table call merges")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(CAP_METRIC, g, h -> h.cap)
                .description("The max_compacted_files this tier's latest table call was given")
                .tag("database", db).tag("tier", tierName).register(registry);
        FunctionCounter.builder(PASSES_METRIC, g.passes, AtomicLong::doubleValue)
                .description("Completed passes of this tier's loop, including passes that found no work")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(PASS_AGE_METRIC, g, h -> Duration.between(h.lastPass != null ? h.lastPass : serviceStart,
                        Instant.now()).toSeconds())
                .description("Seconds since this tier's loop last completed a pass")
                .baseUnit("seconds")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(TIER_SUCCESS_AGE_METRIC, g, h -> Duration.between(h.lastSuccess != null ? h.lastSuccess
                        : serviceStart, Instant.now()).toSeconds())
                .description("Seconds since one of this tier's table calls last completed without an error")
                .baseUnit("seconds")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(QUEUE_METRIC, g, h -> h.queue)
                .description("Tables this tier's latest pass listed")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(CARRY_OVER_METRIC, g, h -> h.carryOver)
                .description("Tables this tier's latest pass left for the next one at its time limit")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(BACKOFFS_METRIC, g, h -> h.backoffs)
                .description("Tables this tier is backing off after consecutive failures")
                .tag("database", db).tag("tier", tierName).register(registry);
        FunctionCounter.builder(COMMITS_METRIC, g.commits, AtomicLong::doubleValue)
                .description("Table calls that committed a merge; each commits one snapshot")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(RSS_GROWTH_METRIC, g, h -> h.rssGrowthPerFile)
                .description("Process RSS this tier's recent calls left behind per input file merged: memory kept"
                        + " outside memory_limit after the commit")
                .baseUnit("bytes")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(SPILL_PEAK_METRIC, g, h -> h.spillPeakBytes)
                .description("Temp-directory (spill) bytes at the end of the latest call")
                .baseUnit("bytes")
                .tag("database", db).tag("tier", tierName).register(registry);

        Map<CompactionRun.Outcome, AtomicLong> outcomes = outcomeCounters(db, tierName);
        outcomes.forEach((outcome, counter) ->
                FunctionCounter.builder(CYCLES_BY_OUTCOME_METRIC, counter, AtomicLong::doubleValue)
                        .description("Compaction cycles by outcome")
                        .tag("database", db).tag("tier", tierName).tag("outcome", outcome.name())
                        .register(registry));
    }

    private Map<CompactionRun.Outcome, AtomicLong> outcomeCounters(String db, String tierName) {
        return outcomeCounts.computeIfAbsent(db, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(tierName, k -> {
                    Map<CompactionRun.Outcome, AtomicLong> m = new java.util.EnumMap<>(CompactionRun.Outcome.class);
                    for (CompactionRun.Outcome o : CompactionRun.Outcome.values()) {
                        m.put(o, new AtomicLong(0));
                    }
                    return m;
                });
    }

    private void registerDatabase(String db) {
        Map<String, AtomicLong> counts = tierCounts.computeIfAbsent(db, k -> new ConcurrentHashMap<>());
        Map<String, AtomicLong> files = tierFiles.computeIfAbsent(db, k -> new ConcurrentHashMap<>());
        for (String tierName : tierNames) {
            AtomicLong count = counts.computeIfAbsent(tierName, k -> new AtomicLong(0));
            FunctionCounter.builder(CYCLE_COUNT_METRIC, count, AtomicLong::doubleValue)
                    .description("Successful compaction cycles for this tier")
                    .tag("database", db)
                    .tag("tier", tierName)
                    .register(registry);

            AtomicLong tierFileCount = files.computeIfAbsent(tierName, k -> new AtomicLong(0));
            Gauge.builder(TIER_FILES_METRIC, tierFileCount, AtomicLong::get)
                    .description("Active files in this tier's file-size range")
                    .tag("database", db)
                    .tag("tier", tierName)
                    .register(registry);

            registerTierGaugesAndOutcomes(db, tierName);
        }

        // Failure-by-class counters, one series per class (except NONE) so a zero is visible.
        Map<CompactionRun.FailureClass, AtomicLong> byClass = failureClassCounters(db);
        byClass.forEach((cls, counter) ->
                FunctionCounter.builder(FAILURE_BY_CLASS_METRIC, counter, AtomicLong::doubleValue)
                        .description("Compaction cycle failures by classified cause")
                        .tag("database", db)
                        .tag("class", cls.name())
                        .register(registry));

        AtomicLong total = totalFiles.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicLong filesCompactedTotal  = filesCompacted.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicLong unexpired = unexpiredSnapshots.computeIfAbsent(db, k -> new AtomicLong(-1));
        Gauge.builder(UNEXPIRED_SNAPSHOTS_METRIC, unexpired, AtomicLong::get)
                .description("Snapshots left in the catalog after housekeeping's latest expiry; -1 before one")
                .tag("database", db)
                .register(registry);
        AtomicLong filesRewrittenTotal  = filesRewritten.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicReference<Instant> lastSuccess =
                lastSuccessTimes.computeIfAbsent(db, k -> new AtomicReference<>());
        lastRunTimes.computeIfAbsent(db, k -> new AtomicReference<>());

        // Every kind (each tier, plus housekeeping) is registered up front so a zero is visible
        // rather than a missing series.
        failureCounters(db).forEach((kind, failures) ->
                FunctionCounter.builder(FAILURE_COUNT_METRIC, failures, AtomicLong::doubleValue)
                        .description("Cycles that ended in an exception")
                        .tag("database", db)
                        .tag("type", kind)
                        .register(registry));

        // Falls back to service start so a compactor that has never succeeded reports a climbing
        // age rather than a healthy-looking zero — this gauge is what an alert should watch.
        Gauge.builder(LAST_SUCCESS_AGE_METRIC, lastSuccess, ref -> {
                    Instant last = ref.get();
                    return Duration.between(last != null ? last : serviceStart, Instant.now()).toSeconds();
                })
                .description("Seconds since the last successful compaction cycle")
                .baseUnit("seconds")
                .tag("database", db)
                .register(registry);

        FunctionCounter.builder(FILES_COMPACTED_METRIC, filesCompactedTotal, AtomicLong::doubleValue)
                .description("Total Parquet files merged by compaction")
                .tag("database", db)
                .register(registry);

        FunctionCounter.builder(FILES_REWRITTEN_METRIC, filesRewrittenTotal, AtomicLong::doubleValue)
                .description("Total data files rewritten by housekeeping to drop their delete files")
                .tag("database", db)
                .register(registry);

        Gauge.builder(TOTAL_FILES_METRIC, total, AtomicLong::get)
                .description("Total active files")
                .tag("database", db)
                .register(registry);
    }

    private Map<String, AtomicLong> failureCounters(String db) {
        return failureCounts.computeIfAbsent(db, k -> {
            Map<String, AtomicLong> counters = new HashMap<>();
            List<String> kinds = new ArrayList<>(tierNames);
            kinds.add(HOUSEKEEPING_KIND);
            kinds.add(RETENTION_KIND);
            for (String kind : kinds) {
                counters.put(kind, new AtomicLong(0));
            }
            return counters;
        });
    }

    private Map<CompactionRun.FailureClass, AtomicLong> failureClassCounters(String db) {
        return failureClassCounts.computeIfAbsent(db, k -> {
            Map<CompactionRun.FailureClass, AtomicLong> counters = new java.util.EnumMap<>(CompactionRun.FailureClass.class);
            for (CompactionRun.FailureClass cls : CompactionRun.FailureClass.values()) {
                if (cls != CompactionRun.FailureClass.NONE) {
                    counters.put(cls, new AtomicLong(0));
                }
            }
            return counters;
        });
    }

    // ── Update methods ────────────────────────────────────────────────────────

    public void incrementTier(String db, String tierName) {
        tierCounts.computeIfAbsent(db, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(tierName, k -> new AtomicLong(0))
                .incrementAndGet();
    }

    public void addFilesCompacted(String db, long delta) {
        if (delta > 0) filesCompacted.computeIfAbsent(db, k -> new AtomicLong(0)).addAndGet(delta);
    }

    public void addFilesRewritten(String db, long delta) {
        if (delta > 0) filesRewritten.computeIfAbsent(db, k -> new AtomicLong(0)).addAndGet(delta);
    }

    /** Counts the rows a data-retention cycle deleted from one table, tagged by database and table. */
    // Per "db|table": how far retention's file boundary lags the cutoff, in seconds.
    private final ConcurrentHashMap<String, AtomicLong> boundaryLags = new ConcurrentHashMap<>();

    /**
     * How far behind its cutoff a table's retention is held by files straddling it: rows older than
     * the retention that are still kept. A gauge in seconds; 0 when nothing holds it back.
     */
    public void setBoundaryLag(String db, String table, java.time.Duration lag) {
        boundaryLags.computeIfAbsent(db + "|" + table, key -> {
            AtomicLong value = new AtomicLong();
            Gauge.builder(BOUNDARY_LAG_METRIC, value, AtomicLong::get)
                    .tags("database", db, "table", table)
                    .baseUnit("seconds")
                    .description("How far retention's file boundary lags its cutoff")
                    .register(registry);
            return value;
        }).set(lag.toSeconds());
    }

    public void addRowsDeleted(String db, String table, long delta) {
        if (delta > 0) {
            registry.counter(ROWS_DELETED_METRIC, "database", db, "table", table).increment(delta);
        }
    }

    public long getFilesRewritten(String db) {
        return filesRewritten.getOrDefault(db, new AtomicLong(0)).get();
    }

    /** Increments the per-class failure counter. {@link CompactionRun.FailureClass#NONE} is ignored. */
    public void recordFailureClass(String db, CompactionRun.FailureClass failureClass) {
        if (failureClass != CompactionRun.FailureClass.NONE) {
            failureClassCounters(db).get(failureClass).incrementAndGet();
        }
    }

    /** Increments the outcome-tagged cycle counter (SUCCESS/EMPTY/FAILED). */
    public void recordOutcome(String db, String tierName, CompactionRun.Outcome outcome) {
        outcomeCounters(db, tierName).get(outcome).incrementAndGet();
    }

    private TierGauges tierGauges(String db, String tierName) {
        return tierGauges.computeIfAbsent(db, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(tierName, k -> new TierGauges());
    }

    /** One table call's cap, and the tier's static cap and spill reading at its end. */
    public void recordCall(String db, String tierName, long staticCap, int cap, boolean committed, long spillPeakBytes,
                           double rssGrowthPerFile) {
        TierGauges g = tierGauges(db, tierName);
        g.rssGrowthPerFile = rssGrowthPerFile;
        g.maxCompactedFiles = staticCap;
        g.cap = cap;
        if (committed) {
            g.commits.incrementAndGet();
        }
        if (spillPeakBytes >= 0) {
            g.spillPeakBytes = spillPeakBytes;
        }
    }

    /** A completed pass of a tier's loop: what it listed, left over and is backing off. */
    public void recordPass(String db, String tierName, Instant at, int queue, int carryOver, int backoffs) {
        TierGauges g = tierGauges(db, tierName);
        g.lastPass = at;
        g.queue = queue;
        g.carryOver = carryOver;
        g.backoffs = backoffs;
        g.passes.incrementAndGet();
    }

    /** A table call of the tier completed without an error. */
    public void recordTierSuccess(String db, String tierName, Instant at) {
        tierGauges(db, tierName).lastSuccess = at;
    }

    /** When one of the tier's calls last completed without an error, or null. */
    public Instant getTierLastSuccess(String db, String tierName) {
        return tierGauges(db, tierName).lastSuccess;
    }

    /** When the tier's loop last completed a pass, or null before its first. */
    public Instant getLastPass(String db, String tierName) {
        return tierGauges(db, tierName).lastPass;
    }

    /**
     * One table's state in one tier. Registered the first time the table is seen; a table no longer
     * listed is reported with nothing waiting.
     */
    public void updateTable(String db, String tierName, String table, long filesWaiting, long oldestWaitSeconds,
                            boolean starved, boolean backoff) {
        TableGauges g = tableGauges.computeIfAbsent(db + "|" + tierName + "|" + table, key -> {
            TableGauges created = new TableGauges();
            Gauge.builder(TABLE_FILES_WAITING_METRIC, created, h -> h.filesWaiting)
                    .description("Eligible files waiting in the table's groups of two or more")
                    .tags("database", db, "tier", tierName, "table", table).register(registry);
            Gauge.builder(TABLE_OLDEST_WAIT_METRIC, created, h -> h.oldestWaitSeconds)
                    .description("Age of the table's oldest file that has something to merge with")
                    .baseUnit("seconds")
                    .tags("database", db, "tier", tierName, "table", table).register(registry);
            Gauge.builder(TABLE_STARVED_METRIC, created, h -> h.starved)
                    .description("1 when busy partitions + 1 is above the cap, so quieter partitions may starve")
                    .tags("database", db, "tier", tierName, "table", table).register(registry);
            Gauge.builder(TABLE_BACKOFF_METRIC, created, h -> h.backoff)
                    .description("1 while the table is backed off after consecutive failures")
                    .tags("database", db, "tier", tierName, "table", table).register(registry);
            return created;
        });
        g.filesWaiting = filesWaiting;
        g.oldestWaitSeconds = oldestWaitSeconds;
        g.starved = starved ? 1.0 : 0.0;
        g.backoff = backoff ? 1.0 : 0.0;
    }

    /** Housekeeping runs whose unexpired-snapshot counts {@link #snapshotsGrowing} compares. */
    static final int SNAPSHOT_GROWTH_RUNS = 3;

    // per database, the unexpired-snapshot counts of the latest housekeeping runs, oldest first
    private final ConcurrentHashMap<String, java.util.Deque<Long>> snapshotHistory = new ConcurrentHashMap<>();
    // per "db|tier", (epoch ms, file count) samples of the band's live files
    private final ConcurrentHashMap<String, java.util.Deque<long[]>> fileCountHistory = new ConcurrentHashMap<>();

    public void updateUnexpiredSnapshots(String db, long count) {
        unexpiredSnapshots.computeIfAbsent(db, k -> new AtomicLong(-1)).set(count);
        java.util.Deque<Long> history = snapshotHistory.computeIfAbsent(db, k -> new java.util.ArrayDeque<>());
        synchronized (history) {
            history.addLast(count);
            while (history.size() > SNAPSHOT_GROWTH_RUNS) {
                history.removeFirst();
            }
        }
    }

    /**
     * Whether unexpired snapshots grew across each of the latest housekeeping runs: expiry is not
     * keeping up with the commits (every table call commits one snapshot).
     */
    public boolean snapshotsGrowing(String db) {
        java.util.Deque<Long> history = snapshotHistory.get(db);
        if (history == null) {
            return false;
        }
        synchronized (history) {
            if (history.size() < SNAPSHOT_GROWTH_RUNS) {
                return false;
            }
            long previous = Long.MIN_VALUE;
            for (long count : history) {
                if (count <= previous) {
                    return false;
                }
                previous = count;
            }
            return true;
        }
    }

    /**
     * Whether the tier's band has held more files at every sample over the last {@code window}: its
     * file count rose and never fell. Needs samples spanning the whole window.
     */
    public boolean fileCountRising(String db, String tierName, Duration window, Instant now) {
        java.util.Deque<long[]> history = fileCountHistory.get(db + "|" + tierName);
        if (history == null) {
            return false;
        }
        long from = now.minus(window).toEpochMilli();
        synchronized (history) {
            long[] first = null;
            long previous = Long.MIN_VALUE;
            boolean spansWindow = false;
            for (long[] sample : history) {
                if (sample[0] <= from) {
                    spansWindow = true;
                    first = sample;
                    previous = sample[1];
                    continue;
                }
                if (first == null) {
                    first = sample;
                    previous = sample[1];
                    continue;
                }
                if (sample[1] < previous) {
                    return false;
                }
                previous = sample[1];
            }
            return spansWindow && first != null && previous > first[1];
        }
    }

    /** Current process RSS and the container's memory limit; -1 for unknown. */
    public void updateProcessMemory(long rss, long limit) {
        rssBytes.set(rss);
        memoryLimitBytes.set(limit);
    }

    /** Records a process RSS sample (VmHWM); keeps the max seen. Ignores unavailable (-1) readings. */
    public void updateRssPeak(long rssBytes) {
        if (rssBytes >= 0) {
            rssPeakBytes.accumulateAndGet(rssBytes, Math::max);
        }
    }

    public void updateTierFileCount(String db, String tierName, long count) {
        updateTierFileCount(db, tierName, count, Instant.now());
    }

    /** Also keeps the sample for {@link #fileCountRising}, for an hour. */
    public void updateTierFileCount(String db, String tierName, long count, Instant at) {
        tierFiles.computeIfAbsent(db, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(tierName, k -> new AtomicLong(0))
                .set(count);
        java.util.Deque<long[]> history = fileCountHistory.computeIfAbsent(db + "|" + tierName,
                k -> new java.util.ArrayDeque<>());
        long cutoff = at.minus(Duration.ofHours(1)).toEpochMilli();
        synchronized (history) {
            history.addLast(new long[]{at.toEpochMilli(), count});
            while (history.size() > 1 && history.peekFirst()[0] < cutoff
                    && history.stream().skip(1).findFirst().map(next -> next[0] <= cutoff).orElse(false)) {
                history.removeFirst();
            }
        }
    }

    public void updateTotalFiles(String db, long total) {
        totalFiles.computeIfAbsent(db, k -> new AtomicLong(0)).set(total);
    }

    /** Call only when the whole cycle completed without throwing. */
    public void recordSuccess(String db) {
        lastSuccessTimes.computeIfAbsent(db, k -> new AtomicReference<>()).set(Instant.now());
    }

    /**
     * {@code kind} is a tier's name, {@link #HOUSEKEEPING_KIND} for a housekeeping failure, or
     * {@link #RETENTION_KIND} for a data-retention failure.
     */
    public void recordFailure(String db, String kind) {
        failureCounters(db).get(kind).incrementAndGet();
    }

    /** Call at the end of every cycle, success or failure — it drives the next-run estimate. */
    public void recordRunCompleted(String db) {
        recordRunCompleted(db, Instant.now());
    }

    /**
     * Same as {@link #recordRunCompleted(String)}, but with a caller-supplied completion instant so
     * it can be reused consistently elsewhere — e.g. CompactionService also stamps its own
     * per-tier last-run tracking with the exact same instant.
     */
    public void recordRunCompleted(String db, Instant at) {
        lastRunTimes.computeIfAbsent(db, k -> new AtomicReference<>()).set(at);
    }

    // ── Timer helpers ─────────────────────────────────────────────────────────

    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    public void stopTimer(Timer.Sample sample, String type, String step, String db) {
        sample.stop(Timer.builder(DURATION_METRIC)
                .description("Duration of compaction step")
                .tag("type", type)
                .tag("step", step)
                .tag("database", db)
                .register(registry));
    }

    // ── Snapshot for health endpoint ──────────────────────────────────────────

    public CompactionStats getSnapshot(List<String> databases) {
        Map<String, CompactionStats.DatabaseStats> dbStats = new HashMap<>();
        for (String db : databases) {
            Map<String, Long> tierCompactionCounts = new HashMap<>();
            Map<String, Long> currentTierFileCounts = new HashMap<>();
            for (String tierName : tierNames) {
                tierCompactionCounts.put(tierName,
                        tierCounts.getOrDefault(db, Map.of()).getOrDefault(tierName, new AtomicLong(0)).get());
                currentTierFileCounts.put(tierName,
                        tierFiles.getOrDefault(db, Map.of()).getOrDefault(tierName, new AtomicLong(0)).get());
            }
            dbStats.put(db, new CompactionStats.DatabaseStats(
                    tierCompactionCounts,
                    getFailureCount(db),
                    filesCompacted.getOrDefault(db, new AtomicLong(0)).get(),
                    getFilesRewritten(db),
                    getLastSuccessTime(db),
                    Map.of(), // tier loops filled in by CompactionService
                    currentTierFileCounts,
                    totalFiles.getOrDefault(db, new AtomicLong(0)).get(),
                    unexpiredSnapshots.getOrDefault(db, new AtomicLong(-1)).get()));
        }
        return new CompactionStats(serviceStart, dbStats);
    }

    public Instant getLastSuccessTime(String db) {
        AtomicReference<Instant> ref = lastSuccessTimes.get(db);
        return ref != null ? ref.get() : null;
    }

    public Instant getLastRunTime(String db) {
        AtomicReference<Instant> ref = lastRunTimes.get(db);
        return ref != null ? ref.get() : null;
    }

    public long getFailureCount(String db) {
        long total = 0;
        for (AtomicLong count : failureCounts.getOrDefault(db, Map.of()).values()) {
            total += count.get();
        }
        return total;
    }
}
