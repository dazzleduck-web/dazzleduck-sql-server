package io.dazzleduck.sql.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One tier's loop for one database (DYNAMIC_COMPACTION_SPEC.md, Phase 1). Each pass:
 *
 * <ol>
 *   <li><b>Refresh:</b> one discovery query lists the tables with work.</li>
 *   <li><b>Rank</b> by files waiting, most first; ties go to the older oldest file. Tables carried
 *       over from the previous pass go first.</li>
 *   <li><b>Call</b> each table in turn under its lease, with the cap of {@link CallSizing}. A table
 *       whose lease is held elsewhere is skipped this pass.</li>
 *   <li><b>End</b> when every table has been called or the pass time limit has passed: a running
 *       call finishes, and the tables not yet called carry over.</li>
 * </ol>
 *
 * After a pass that merged nothing — no table listed, or every listed table backed off, held by
 * another job, already merged or failing — the loop sleeps {@code idle_sleep_max}; otherwise the
 * next pass starts at once. A failure costs one table's call: the table is retried on the next pass, and after
 * {@link #FAILURES_BEFORE_BACKOFF} in a row it backs off for {@link #BACKOFF}.
 *
 * <p>All state but the published {@link #status()} belongs to the loop's thread (or to the test that
 * calls {@link #runPass()} instead of starting it).
 */
final class TierLoop {

    private static final Logger logger = LoggerFactory.getLogger(TierLoop.class);

    static final int FAILURES_BEFORE_BACKOFF = 3;
    static final Duration BACKOFF = Duration.ofMinutes(15);
    /** A table whose oldest file with a partner waits longer than this is reported stale. */
    static final Duration STALE_AFTER = Duration.ofMinutes(30);

    private final String database;
    private final CompactionTier tier;
    private final TierCompactor compactor;
    private final TableLeases leases;
    private final CompactionState state;
    private final CompactionRunLog runLog;
    private final Clock clock;
    private final AtomicLong runIds = new AtomicLong();
    private final Instant created;

    private static final class TableHealth {
        int failures;
        Instant backoffUntil;
        boolean starved;
        TableWork lastWork;
    }

    private final Map<String, TableHealth> tables = new HashMap<>();
    private List<String> carryOver = List.of();
    private int queue;
    private List<String> backoffs = List.of();
    private List<String> starved = List.of();
    private List<String> stale = List.of();
    private TierCompactor.Readiness readiness;
    private Instant lastCallEnded;
    private long passes;

    private volatile CompactionStats.TierStatus status;
    private volatile boolean stopping;
    private volatile Thread thread;

    /**
     * @param refused  a startup check failed: the loop has stopped for good
     * @param listed   tables the refresh listed, before back-offs; -1 when the refresh failed or the
     *                 connection is not ready
     */
    record PassResult(boolean refused, int listed, int called, int merged, int failed, int carriedOver) {
        /**
         * Whether the next pass should start at once: this one merged something, so more may be
         * waiting. Otherwise the loop sleeps, rather than refresh again and again while every table
         * it lists is backed off, held by another job, or has nothing DuckLake would merge.
         */
        boolean progressed() {
            return merged > 0;
        }
    }

    TierLoop(String database, CompactionTier tier, TierCompactor compactor, TableLeases leases,
             CompactionState state, CompactionRunLog runLog, Clock clock) {
        this.database = database;
        this.tier = tier;
        this.compactor = compactor;
        this.leases = leases;
        this.state = state;
        this.runLog = runLog;
        this.clock = clock;
        this.created = clock.instant();
        this.status = new CompactionStats.TierStatus("STARTING", null, null, 0, 0, 0, List.of(), List.of(), List.of(),
                -1, 0, -1);
    }

    CompactionTier tier() {
        return tier;
    }

    /** When the loop was created, which stands in for its last pass before its first. */
    Instant created() {
        return created;
    }

    CompactionStats.TierStatus status() {
        return status;
    }

    void start() {
        Thread t = new Thread(this::run, "compaction-" + tier.name() + "-" + database);
        t.setDaemon(false);
        thread = t;
        t.start();
    }

    /** Asks the loop to stop after its running call, and wakes it from a sleep. */
    void stop() {
        stopping = true;
        Thread t = thread;
        if (t != null) {
            t.interrupt();
        }
    }

    /** @return whether the loop's thread ended within the timeout */
    boolean join(Duration timeout) throws InterruptedException {
        Thread t = thread;
        if (t == null) {
            return true;
        }
        t.join(timeout.toMillis());
        return !t.isAlive();
    }

    private void run() {
        while (!stopping) {
            PassResult result;
            try {
                result = runPass();
            } catch (Throwable t) {
                // runPass contains its failures; this is a bug, and must not end the loop
                logger.error("Tier '{}' loop for {} failed unexpectedly; it continues", tier.name(), database, t);
                result = new PassResult(false, -1, 0, 0, 0, 0);
            }
            if (result.refused()) {
                return;
            }
            if (!result.progressed() && !sleep(tier.idleSleep())) {
                return;
            }
        }
        publish("STOPPED", null);
    }

    private boolean sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
            return !stopping;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** One pass. Never throws. */
    PassResult runPass() {
        if (readiness == null && !prepare()) {
            return new PassResult(readiness != null && readiness.isRefused(), -1, 0, 0, 0, 0);
        }
        Instant passStart = clock.instant();
        List<TableWork> listed;
        try {
            listed = compactor.discover(database, tier);
        } catch (Exception e) {
            CompactionRun.FailureClass failureClass = CompactionRun.classify(e);
            state.recordFailure(database, tier.name());
            state.recordFailureClass(database, failureClass);
            logger.error("Tier '{}' could not list the tables with work in {} [{}]; it reconnects and retries",
                    tier.name(), database, failureClass, e);
            compactor.reset(database, tier);
            readiness = null;
            publish("STARTING", null);
            return new PassResult(false, -1, 0, 0, 0, 0);
        }

        Instant now = clock.instant();
        List<TableWork> due = new ArrayList<>();
        for (TableWork work : listed) {
            TableHealth health = tables.computeIfAbsent(work.name(), k -> new TableHealth());
            health.lastWork = work;
            if (health.backoffUntil == null || !now.isBefore(health.backoffUntil)) {
                due.add(work);
            }
        }
        List<TableWork> order = rank(due, carryOver);
        carryOver = List.of();

        int called = 0, merged = 0, failed = 0;
        for (int i = 0; i < order.size(); i++) {
            // At least one call per pass, so a refresh slower than the limit cannot stall the tier
            if (stopping || i > 0 && Duration.between(passStart, clock.instant()).compareTo(tier.passTimeLimit()) >= 0) {
                carryOver = order.subList(i, order.size()).stream().map(TableWork::name).toList();
                break;
            }
            TableWork work = order.get(i);
            String key = work.leaseKey(database);
            if (!leases.tryAcquire(key)) {
                logger.debug("Tier '{}' skips {}.{} this pass: another job holds it", tier.name(), database, work.name());
                continue;
            }
            CallResult result;
            try {
                result = call(work);
            } finally {
                leases.release(key);
            }
            called++;
            if (result == CallResult.MERGED) {
                merged++;
            } else if (result == CallResult.FAILED || result == CallResult.CONNECTION_LOST) {
                failed++;
            }
            if (result == CallResult.CONNECTION_LOST) {
                // The connection is gone: the rest wait for the next pass, which reconnects.
                carryOver = order.subList(i + 1, order.size()).stream().map(TableWork::name).toList();
                break;
            }
        }

        Instant end = clock.instant();
        passes++;
        queue = listed.size();
        reportTables(listed, end);
        state.recordPass(database, tier.name(), end, queue, carryOver.size(), backoffs.size());
        state.recordRunCompleted(database, end);
        if (failed == 0 || merged > 0) {
            state.recordSuccess(database);
        }
        state.updateProcessMemory(ResourceSampler.rssBytes(), ResourceSampler.containerMemoryLimitBytes());
        publish("RUNNING", null);
        if (!listed.isEmpty()) {
            logger.info("Tier '{}' pass for {}: {} table(s) listed, {} called ({} merged, {} failed), {} carried over, in {} ms",
                    tier.name(), database, listed.size(), called, merged, failed, carryOver.size(),
                    Duration.between(passStart, end).toMillis());
        }
        return new PassResult(false, listed.size(), called, merged, failed, carryOver.size());
    }

    private boolean prepare() {
        try {
            readiness = compactor.prepare(database, tier);
        } catch (Exception e) {
            readiness = null;
            state.recordFailure(database, tier.name());
            state.recordFailureClass(database, CompactionRun.classify(e));
            logger.error("Tier '{}' could not open its connection for {}; it retries", tier.name(), database, e);
            publish("STARTING", null);
            return false;
        }
        if (readiness.isRefused()) {
            logger.error("Tier '{}' does not start for {}: {}", tier.name(), database, readiness.refused());
            publish("REFUSED", readiness.refused());
            return false;
        }
        logger.info("Tier '{}' ready for {}: {} DuckDB thread(s), target {} bytes, catalog idle-in-transaction timeout {}",
                tier.name(), database, readiness.threads(), tier.targetFileSize(),
                readiness.idleTxnTimeoutMs() > 0 ? readiness.idleTxnTimeoutMs() + " ms" : "none");
        return true;
    }

    /** Carried-over tables first, in their order, then the most files waiting, then the oldest. */
    static List<TableWork> rank(List<TableWork> due, List<String> carryOver) {
        Map<String, TableWork> byName = new HashMap<>();
        due.forEach(work -> byName.put(work.name(), work));
        Set<TableWork> order = new LinkedHashSet<>();
        for (String name : carryOver) {
            TableWork work = byName.get(name);
            if (work != null) {
                order.add(work);
            }
        }
        due.stream()
                .sorted(Comparator.comparingLong(TableWork::filesWaiting).reversed()
                        .thenComparing(TableWork::oldestWaiting))
                .forEach(order::add);
        return List.copyOf(order);
    }

    private enum CallResult { MERGED, EMPTY, FAILED, CONNECTION_LOST }

    private CallResult call(TableWork work) {
        TableHealth health = tables.computeIfAbsent(work.name(), k -> new TableHealth());
        long timeoutMs = readiness != null ? readiness.idleTxnTimeoutMs() : 0;
        CallSizing.Cap cap = CallSizing.of(tier, work, timeoutMs);
        if (cap.starved() && !health.starved) {
            logger.warn(("Tier '{}' may starve partitions of {}.{}: {} busy partition(s) + 1 is above its cap of {}"
                            + " (static cap {}, then the time limit), so its quieter partitions can wait behind them"),
                    tier.name(), database, work.name(), work.busyPartitions(), cap.cap(), tier.maxCompactedFiles());
        }
        health.starved = cap.starved();

        Instant startedAt = clock.instant();
        long gapMs = lastCallEnded != null ? Duration.between(lastCallEnded, startedAt).toMillis() : -1;
        TierCompactor.MergeOutcome merge = null;
        CompactionRun.Outcome outcome;
        CompactionRun.FailureClass failureClass = CompactionRun.FailureClass.NONE;
        String error = null;
        CallResult result;
        try {
            merge = compactor.compact(database, tier, work, cap.cap());
            boolean mergedAny = merge.filesProcessed() != null && merge.filesProcessed() > 0;
            outcome = mergedAny ? CompactionRun.Outcome.SUCCESS : CompactionRun.Outcome.EMPTY;
            result = mergedAny ? CallResult.MERGED : CallResult.EMPTY;
            health.failures = 0;
            health.backoffUntil = null;
            state.incrementTier(database, tier.name());
            if (mergedAny && merge.filesCreated() != null) {
                state.addFilesCompacted(database, merge.filesProcessed() - merge.filesCreated());
            }
        } catch (Exception e) {
            outcome = CompactionRun.Outcome.FAILED;
            failureClass = CompactionRun.classify(e);
            error = CompactionRun.truncateError(e);
            health.failures++;
            state.recordFailure(database, tier.name());
            state.recordFailureClass(database, failureClass);
            boolean connectionLost = failureClass == CompactionRun.FailureClass.CATALOG_UNAVAILABLE
                    || failureClass == CompactionRun.FailureClass.COMMIT_TIMEOUT;
            result = connectionLost ? CallResult.CONNECTION_LOST : CallResult.FAILED;
            String conflictNote = failureClass == CompactionRun.FailureClass.TRANSACTION_CONFLICT
                    ? " (the compactor's own jobs hold the table's lease, so the other side is another writer)" : "";
            if (health.failures >= FAILURES_BEFORE_BACKOFF) {
                health.backoffUntil = clock.instant().plus(BACKOFF);
                logger.error("Tier '{}' failed {} time(s) in a row on {}.{} [{}]{}; it backs off until {}",
                        tier.name(), health.failures, database, work.name(), failureClass, conflictNote,
                        health.backoffUntil, e);
            } else {
                logger.warn("Tier '{}' failed on {}.{} [{}]{}; it is retried next pass",
                        tier.name(), database, work.name(), failureClass, conflictNote, e);
            }
            if (connectionLost) {
                compactor.reset(database, tier);
                readiness = null;
            }
        }
        Instant endedAt = clock.instant();
        lastCallEnded = endedAt;
        record(work, cap, startedAt, endedAt, gapMs, merge, outcome, failureClass, error, timeoutMs);
        return result;
    }

    private void record(TableWork work, CallSizing.Cap cap, Instant startedAt, Instant endedAt, long gapMs,
                        TierCompactor.MergeOutcome merge, CompactionRun.Outcome outcome,
                        CompactionRun.FailureClass failureClass, String error, long timeoutMs) {
        try {
            String tempDir = ResourceSampler.tempDirectory(tier.connectionSettings());
            CompactionRun run = new CompactionRun(
                    runIds.incrementAndGet(), database, tier.name(), work.name(), startedAt, endedAt, gapMs,
                    work.filesWaiting(), work.bytesWaiting(), cap.cap(), tier.maxCompactedFiles(),
                    work.busyPartitions(),
                    merge != null ? merge.filesProcessed() : null,
                    merge != null ? merge.filesCreated() : null,
                    Duration.between(startedAt, endedAt).toMillis(),
                    merge != null ? merge.durationMergeMs() : -1,
                    outcome, failureClass, error,
                    ResourceSampler.rssPeakBytes(),
                    ResourceSampler.dirSizeBytes(tempDir),
                    ResourceSampler.memoryLimitBytes(tier.connectionSettings()),
                    timeoutMs);
            runLog.record(run);
            state.recordOutcome(database, tier.name(), outcome);
            state.recordCall(database, tier.name(), tier.maxCompactedFiles(), cap.cap(),
                    outcome == CompactionRun.Outcome.SUCCESS, run.spillPeakBytes());
            state.updateRssPeak(run.rssPeakBytes());
        } catch (Exception e) {
            // Telemetry must never take down a call.
            logger.warn("Failed to record compaction telemetry for {} tier '{}'", database, tier.name(), e);
        }
    }

    /**
     * Updates every known table's gauges: what the pass listed, and nothing waiting for a table it
     * no longer lists. Forgets a table that is neither listed nor failing. Collects the back-offs, and
     * the starved and stale tables among those listed.
     */
    private void reportTables(List<TableWork> listed, Instant now) {
        Set<String> seen = new HashSet<>();
        listed.forEach(work -> seen.add(work.name()));
        List<String> backedOffNow = new ArrayList<>();
        List<String> starvedNow = new ArrayList<>();
        List<String> staleNow = new ArrayList<>();
        var it = tables.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            String name = entry.getKey();
            TableHealth health = entry.getValue();
            boolean backedOff = health.backoffUntil != null && now.isBefore(health.backoffUntil);
            if (backedOff) {
                backedOffNow.add(name);
            }
            if (seen.contains(name)) {
                TableWork work = health.lastWork;
                Duration age = Duration.between(work.oldestWaiting(), now);
                if (health.starved) {
                    starvedNow.add(name);
                }
                if (age.compareTo(STALE_AFTER) > 0) {
                    staleNow.add(name);
                }
                state.updateTable(database, tier.name(), name, work.filesWaiting(), Math.max(0, age.toSeconds()),
                        health.starved, backedOff);
            } else {
                health.starved = false;
                state.updateTable(database, tier.name(), name, 0, 0, false, backedOff);
                if (!backedOff && health.failures == 0) {
                    it.remove();
                }
            }
        }
        backoffs = List.copyOf(backedOffNow);
        starved = List.copyOf(starvedNow);
        stale = List.copyOf(staleNow);
    }

    private void publish(String loopState, String refused) {
        long memory = readiness != null && !readiness.isRefused() ? compactor.memoryUsageBytes(database, tier) : -1;
        status = new CompactionStats.TierStatus(loopState, refused, state.getLastPass(database, tier.name()), passes,
                queue, carryOver.size(), backoffs, starved, stale,
                readiness != null ? readiness.idleTxnTimeoutMs() : -1,
                readiness != null ? readiness.threads() : 0, memory);
    }
}
