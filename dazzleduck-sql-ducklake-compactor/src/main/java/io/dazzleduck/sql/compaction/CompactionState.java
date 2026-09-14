package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CompactionState {

    private static final String DURATION_METRIC    = "ducklake.compaction.duration";
    private static final String MINOR_COUNT_METRIC = "ducklake.compaction.minor";
    private static final String MAJOR_COUNT_METRIC = "ducklake.compaction.major";
    private static final String FAILURE_COUNT_METRIC = "ducklake.compaction.failures";
    private static final String LAST_SUCCESS_AGE_METRIC = "ducklake.compaction.last_success_age";
    private static final String FILES_COMPACTED_METRIC = "ducklake.files.compacted";
    private static final String SMALL_FILES_METRIC  = "ducklake.files.small";
    private static final String MEDIUM_FILES_METRIC = "ducklake.files.medium";
    private static final String TOTAL_FILES_METRIC  = "ducklake.files.total";

    private final MeterRegistry registry;
    private final Instant serviceStart = Instant.now();

    // Per-database counters (also back Micrometer FunctionCounters)
    private final ConcurrentHashMap<String, AtomicLong> minorCounts    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> majorCounts    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> filesCompacted = new ConcurrentHashMap<>();

    // Failures are attributable to the cycle kind that threw. Each inner map is fully populated on
    // creation, so only the counters are ever mutated — never the map structure.
    private final ConcurrentHashMap<String, Map<CycleKind, AtomicLong>> failureCounts = new ConcurrentHashMap<>();

    // Per-database gauges
    private final ConcurrentHashMap<String, AtomicLong> smallFiles  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> mediumFiles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> totalFiles  = new ConcurrentHashMap<>();

    // Set only when a cycle completes without throwing
    private final ConcurrentHashMap<String, AtomicReference<Instant>> lastSuccessTimes = new ConcurrentHashMap<>();

    // Set at the end of every cycle regardless of outcome. The fixed-delay scheduler re-arms from
    // completion, so this — not the last success — is what predicts the next scheduled run: a
    // database that keeps failing is still due again one interval after its last attempt.
    private final ConcurrentHashMap<String, AtomicReference<Instant>> lastRunTimes = new ConcurrentHashMap<>();

    public CompactionState(MeterRegistry registry, List<String> databases) {
        this.registry = registry;
        databases.forEach(this::registerDatabase);
    }

    private void registerDatabase(String db) {
        AtomicLong minor  = minorCounts.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicLong major  = majorCounts.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicLong files  = filesCompacted.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicLong small  = smallFiles.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicLong medium = mediumFiles.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicLong total  = totalFiles.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicReference<Instant> lastSuccess =
                lastSuccessTimes.computeIfAbsent(db, k -> new AtomicReference<>());
        lastRunTimes.computeIfAbsent(db, k -> new AtomicReference<>());

        FunctionCounter.builder(MINOR_COUNT_METRIC, minor, AtomicLong::doubleValue)
                .description("Successful minor compaction cycles")
                .tag("database", db)
                .register(registry);

        FunctionCounter.builder(MAJOR_COUNT_METRIC, major, AtomicLong::doubleValue)
                .description("Successful major compaction cycles")
                .tag("database", db)
                .register(registry);

        // Every kind is registered up front so a zero is visible rather than a missing series.
        failureCounters(db).forEach((kind, failures) ->
                FunctionCounter.builder(FAILURE_COUNT_METRIC, failures, AtomicLong::doubleValue)
                        .description("Cycles that ended in an exception")
                        .tag("database", db)
                        .tag("type", kind.tag())
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

        FunctionCounter.builder(FILES_COMPACTED_METRIC, files, AtomicLong::doubleValue)
                .description("Total Parquet files merged by compaction")
                .tag("database", db)
                .register(registry);

        Gauge.builder(SMALL_FILES_METRIC, small, AtomicLong::get)
                .description("Active files smaller than minor_compaction_max_size")
                .tag("database", db)
                .register(registry);

        Gauge.builder(MEDIUM_FILES_METRIC, medium, AtomicLong::get)
                .description("Active files between minor and major compaction max size")
                .tag("database", db)
                .register(registry);

        Gauge.builder(TOTAL_FILES_METRIC, total, AtomicLong::get)
                .description("Total active files")
                .tag("database", db)
                .register(registry);
    }

    private Map<CycleKind, AtomicLong> failureCounters(String db) {
        return failureCounts.computeIfAbsent(db, k -> {
            Map<CycleKind, AtomicLong> counters = new EnumMap<>(CycleKind.class);
            for (CycleKind kind : CycleKind.values()) {
                counters.put(kind, new AtomicLong(0));
            }
            return counters;
        });
    }

    // ── Update methods ────────────────────────────────────────────────────────

    public void incrementMinor(String db) {
        minorCounts.computeIfAbsent(db, k -> new AtomicLong(0)).incrementAndGet();
    }

    public void incrementMajor(String db) {
        majorCounts.computeIfAbsent(db, k -> new AtomicLong(0)).incrementAndGet();
    }

    public void addFilesCompacted(String db, long delta) {
        if (delta > 0) filesCompacted.computeIfAbsent(db, k -> new AtomicLong(0)).addAndGet(delta);
    }

    public void updateFileCounts(String db, long small, long medium, long total) {
        smallFiles.computeIfAbsent(db, k -> new AtomicLong(0)).set(small);
        mediumFiles.computeIfAbsent(db, k -> new AtomicLong(0)).set(medium);
        totalFiles.computeIfAbsent(db, k -> new AtomicLong(0)).set(total);
    }

    /** Call only when the whole cycle completed without throwing. */
    public void recordSuccess(String db) {
        lastSuccessTimes.computeIfAbsent(db, k -> new AtomicReference<>()).set(Instant.now());
    }

    public void recordFailure(String db, CycleKind kind) {
        failureCounters(db).get(kind).incrementAndGet();
    }

    /** Call at the end of every cycle, success or failure — it drives the next-run estimate. */
    public void recordRunCompleted(String db) {
        lastRunTimes.computeIfAbsent(db, k -> new AtomicReference<>()).set(Instant.now());
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
            dbStats.put(db, new CompactionStats.DatabaseStats(
                    minorCounts.getOrDefault(db, new AtomicLong(0)).get(),
                    majorCounts.getOrDefault(db, new AtomicLong(0)).get(),
                    getFailureCount(db),
                    filesCompacted.getOrDefault(db, new AtomicLong(0)).get(),
                    getLastSuccessTime(db),
                    null, // nextExecutionTime injected by CompactionService
                    smallFiles.getOrDefault(db, new AtomicLong(0)).get(),
                    mediumFiles.getOrDefault(db, new AtomicLong(0)).get(),
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
