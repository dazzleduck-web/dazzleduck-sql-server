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
    private static final String BYTES_COMPACTED_METRIC = "ducklake.bytes.compacted";
    private static final String TIER_FILES_METRIC = "ducklake.files.by_tier";
    private static final String TOTAL_FILES_METRIC = "ducklake.files.total";
    private static final String FAILURE_BY_CLASS_METRIC = "ducklake.compaction.failures_by_class";
    private static final String CYCLES_BY_OUTCOME_METRIC = "ducklake.compaction.cycles_by_outcome";
    // Control inputs the future adaptive controller will make dynamic — emitted now so the change is
    // observable when it lands. Holder-backed, so whatever value a cycle actually used is reflected.
    private static final String MAX_COMPACTED_FILES_METRIC = "ducklake.compaction.max_compacted_files";
    private static final String FREQUENCY_METRIC = "ducklake.compaction.tier_frequency";
    // Derived, per-cycle. arrival/drain are NOT reconstructable from cumulative counters, so they are
    // emitted rather than left for the backend to rate.
    private static final String DRAIN_RATE_METRIC = "ducklake.compaction.drain_rate";
    private static final String ARRIVAL_RATE_METRIC = "ducklake.compaction.arrival_rate";
    private static final String SATURATED_METRIC = "ducklake.compaction.saturated";
    private static final String SPILL_PEAK_METRIC = "ducklake.compaction.spill_peak_bytes";
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
    // Per-database, per-tier cumulative bytes retired (bytes are the real cost driver, per spec).
    private final ConcurrentHashMap<String, Map<String, AtomicLong>> bytesCompacted = new ConcurrentHashMap<>();
    // Per-database, per-failure-class cumulative counters, tagged so opposite failure modes stay distinct.
    private final ConcurrentHashMap<String, Map<CompactionRun.FailureClass, AtomicLong>> failureClassCounts = new ConcurrentHashMap<>();
    // Per-(database, tier) cumulative cycle counters, tagged by outcome (SUCCESS/EMPTY/FAILED).
    private final ConcurrentHashMap<String, Map<String, Map<CompactionRun.Outcome, AtomicLong>>> outcomeCounts = new ConcurrentHashMap<>();
    // Per-(database, tier) holder for the latest control inputs + derived per-cycle gauges.
    private final ConcurrentHashMap<String, Map<String, TierGauges>> tierGauges = new ConcurrentHashMap<>();
    // Process-wide peak RSS (VmHWM is monotonic), so a single series rather than one per tier.
    private final AtomicLong rssPeakBytes = new AtomicLong(0);

    /** Mutable backing for the per-(database, tier) gauges, updated once per cycle. */
    private static final class TierGauges {
        volatile long maxCompactedFiles;
        volatile long frequencyMs;
        volatile long spillPeakBytes;
        volatile double drainRatePerSec;
        volatile double arrivalRatePerSec;
        volatile double saturated; // 0.0 / 1.0
    }

    // Failures are attributable to the tier that threw, or HOUSEKEEPING_KIND. Each inner map is
    // fully populated on creation, so only the counters are ever mutated — never the map structure.
    private final ConcurrentHashMap<String, Map<String, AtomicLong>> failureCounts = new ConcurrentHashMap<>();

    // Per-database, per-tier current-file-count gauges, plus one whole-catalog total per database.
    private final ConcurrentHashMap<String, Map<String, AtomicLong>> tierFiles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> totalFiles = new ConcurrentHashMap<>();

    // Set only when a cycle completes without throwing
    private final ConcurrentHashMap<String, AtomicReference<Instant>> lastSuccessTimes = new ConcurrentHashMap<>();

    // Set at the end of every cycle regardless of outcome. The fixed-delay scheduler re-arms from
    // completion, so this — not the last success — is what predicts the next scheduled run: a
    // database that keeps failing is still due again one interval after its last attempt.
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
    }

    /**
     * Registers the per-(database, tier) control-input gauges (max_compacted_files, tier_frequency),
     * the derived per-cycle gauges (drain/arrival rate, saturated, spill peak), and the
     * outcome-tagged cycle counters. All read from mutable holders updated once per cycle.
     */
    private void registerTierGaugesAndOutcomes(String db, String tierName) {
        TierGauges g = tierGauges.computeIfAbsent(db, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(tierName, k -> new TierGauges());
        Gauge.builder(MAX_COMPACTED_FILES_METRIC, g, h -> h.maxCompactedFiles)
                .description("max_compacted_files configured for this tier (will become dynamic)")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(FREQUENCY_METRIC, g, h -> h.frequencyMs)
                .description("Configured cycle interval for this tier (will become dynamic)")
                .baseUnit("milliseconds")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(DRAIN_RATE_METRIC, g, h -> h.drainRatePerSec)
                .description("Files retired per second minus files arriving per second (< 0 = losing ground)")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(ARRIVAL_RATE_METRIC, g, h -> h.arrivalRatePerSec)
                .description("Files arriving into this tier's file-size range per second (from consecutive cycles)")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(SATURATED_METRIC, g, h -> h.saturated)
                .description("1 when cycles run back-to-back (cadence no longer a control variable)")
                .tag("database", db).tag("tier", tierName).register(registry);
        Gauge.builder(SPILL_PEAK_METRIC, g, h -> h.spillPeakBytes)
                .description("Temp-directory (spill) bytes at the last cycle end")
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
        Map<String, AtomicLong> bytes = bytesCompacted.computeIfAbsent(db, k -> new ConcurrentHashMap<>());
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

            AtomicLong tierBytes = bytes.computeIfAbsent(tierName, k -> new AtomicLong(0));
            FunctionCounter.builder(BYTES_COMPACTED_METRIC, tierBytes, AtomicLong::doubleValue)
                    .description("Cumulative bytes retired by compaction for this tier")
                    .baseUnit("bytes")
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

    public void addBytesCompacted(String db, String tierName, long delta) {
        if (delta > 0) {
            bytesCompacted.computeIfAbsent(db, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(tierName, k -> new AtomicLong(0))
                    .addAndGet(delta);
        }
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

    /**
     * Pushes one cycle's control inputs and derived quantities into the per-(database, tier) gauge
     * holders. Called once per cycle so the gauges never recompute on scrape.
     */
    public void updateTierGauges(String db, String tierName, long maxCompactedFiles, long frequencyMs,
                                 double drainRatePerSec, double arrivalRatePerSec, boolean saturated,
                                 long spillPeakBytes) {
        TierGauges g = tierGauges.computeIfAbsent(db, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(tierName, k -> new TierGauges());
        g.maxCompactedFiles = maxCompactedFiles;
        g.frequencyMs = frequencyMs;
        g.drainRatePerSec = drainRatePerSec;
        g.arrivalRatePerSec = arrivalRatePerSec;
        g.saturated = saturated ? 1.0 : 0.0;
        if (spillPeakBytes >= 0) {
            g.spillPeakBytes = spillPeakBytes;
        }
    }

    /** Records a process RSS sample (VmHWM); keeps the max seen. Ignores unavailable (-1) readings. */
    public void updateRssPeak(long rssBytes) {
        if (rssBytes >= 0) {
            rssPeakBytes.accumulateAndGet(rssBytes, Math::max);
        }
    }

    public void updateTierFileCount(String db, String tierName, long count) {
        tierFiles.computeIfAbsent(db, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(tierName, k -> new AtomicLong(0))
                .set(count);
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
                    Map.of(), // nextExecutionTimeByTier injected by CompactionService
                    currentTierFileCounts,
                    totalFiles.getOrDefault(db, new AtomicLong(0)).get()));
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
