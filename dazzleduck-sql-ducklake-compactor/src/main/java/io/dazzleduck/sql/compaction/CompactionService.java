package io.dazzleduck.sql.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CompactionService implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionService.class);

    private final CompactionConfig config;
    private final TierCompactor tierCompactor;
    private final Housekeeper housekeeper;
    // null when data retention is off
    private final DataRetention dataRetention;
    private final CompactionState state;
    private final CompactionRunLog runLog;

    // Shared schedulers
    private final ScheduledExecutorService compactionScheduler;
    private final ScheduledExecutorService housekeepingScheduler;

    // database -> tier name -> last-run instant. Stamped with the exact same Instant passed to
    // CompactionState.recordRunCompleted(db, at) in the same call, so the two never disagree.
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Instant>> lastTierRun = new ConcurrentHashMap<>();

    // Monotonic per-(database, tier) run id counter for CompactionRun.
    private final ConcurrentHashMap<String, AtomicLong> runIds = new ConcurrentHashMap<>();

    // One raw connection per (database, tier) for this service's own file-count queries —
    // deliberately not io.dazzleduck.sql.commons.ConnectionPool (see RawConnections). Keyed by both,
    // not database alone: different tiers for the same database run concurrently by design, and a
    // plain JDBC Connection is not safe for concurrent use by more than one thread — the same reason
    // DuckDbTierCompactor/DuckLakeHousekeeper key their own connections this way.
    private final RawConnectionCache countingConnections;

    /** Synthetic "tier" key for the off-control-path whole-catalog file-count refresh connection. */
    private static final String FILE_COUNT_REFRESH_KEY = "__filecount_refresh__";

    /**
     * Cadence for the whole-catalog file-count gauge refresh, off the compaction control path. Fixed
     * rather than configurable — it is an internal gauge-maintenance interval, not a control
     * parameter. Set to 2 minutes: these gauges are a coarse backlog indicator, so a slow cadence
     * keeps the COUNT(*) FILTER aggregate off the shared catalog's back. If a future need arises to
     * tune it, reintroduce a config key here.
     */
    private static final long FILE_COUNT_REFRESH_SECONDS = 120;

    public CompactionService(CompactionConfig config, String startupScript, TierCompactor tierCompactor,
                             Housekeeper housekeeper, CompactionState state, CompactionRunLog runLog) {
        this(config, startupScript, tierCompactor, housekeeper, null, state, runLog);
    }

    /**
     * @param dataRetention deletes expired rows on {@code config.dataRetention()}'s schedule, or
     *                      {@code null} to run no retention
     */
    public CompactionService(CompactionConfig config, String startupScript, TierCompactor tierCompactor,
                             Housekeeper housekeeper, DataRetention dataRetention, CompactionState state,
                             CompactionRunLog runLog) {
        if (dataRetention != null && !config.dataRetention().enabled()) {
            throw new IllegalArgumentException("a DataRetention was given but data_retention is not enabled");
        }
        if (dataRetention == null && config.dataRetention().enabled()) {
            logger.warn("data_retention is enabled but no DataRetention was given, so retention will not run");
        }
        this.config = config;
        this.countingConnections = new RawConnectionCache(startupScript, List.of(), "counting");
        this.tierCompactor = tierCompactor;
        this.housekeeper = housekeeper;
        this.dataRetention = dataRetention;
        this.state = state;
        this.runLog = runLog;
        long enabledTiers = config.tiers().stream().filter(CompactionTier::enabled).count();
        int compactionThreads = Math.max(1, (int) (config.databases().size() * Math.max(enabledTiers, 1)));
        int housekeepingThreads = Math.max(1, config.databases().size());
        this.compactionScheduler = Executors.newScheduledThreadPool(compactionThreads, r -> {
            Thread t = new Thread(r, "compaction");
            t.setDaemon(false);
            return t;
        });
        // Housekeeping, the whole-catalog file-count refresh and data retention share this pool. One
        // thread per job per database, so a database's housekeeping and its (now separate) file-count
        // refresh don't contend for a single thread — the file-count refresh is exactly the traffic
        // we moved off the per-cycle control path, so it must not be starved by it — and a long
        // retention delete does not hold up either.
        int jobsPerDatabase = dataRetention != null ? 3 : 2;
        this.housekeepingScheduler = Executors.newScheduledThreadPool(Math.max(1, housekeepingThreads * jobsPerDatabase), r -> {
            Thread t = new Thread(r, "housekeeping");
            t.setDaemon(false);
            return t;
        });
    }

    public void start() {
        if (config.databases().isEmpty()) {
            logger.warn("No databases configured — compaction service idle");
            return;
        }
        long housekeepingSeconds = config.housekeepingFrequency().toSeconds();
        long fileCountSeconds = FILE_COUNT_REFRESH_SECONDS;

        for (String db : config.databases()) {
            for (CompactionTier tier : config.tiers()) {
                if (tier.enabled()) {
                    compactionScheduler.scheduleAtFixedRate(
                            () -> runTier(db, tier), 0, tier.frequency().toSeconds(), TimeUnit.SECONDS);
                }
            }
            housekeepingScheduler.scheduleWithFixedDelay(
                    () -> runHousekeeping(db), housekeepingSeconds, housekeepingSeconds, TimeUnit.SECONDS);
            // Whole-catalog per-tier gauges refreshed on their own slow cadence, OFF the compaction
            // control path (spec §"One change that is not capture"). Previously this ran every cycle,
            // adding a full COUNT(*) FILTER aggregate per tier at cycle rate against a shared catalog.
            housekeepingScheduler.scheduleWithFixedDelay(
                    () -> refreshFileCounts(db), 0, fileCountSeconds, TimeUnit.SECONDS);
            if (dataRetention != null) {
                // First run at startup, so a compactor that restarts more often than the frequency
                // still enforces retention.
                housekeepingScheduler.scheduleWithFixedDelay(
                        () -> runRetention(db), 0, config.dataRetention().frequency().toSeconds(), TimeUnit.SECONDS);
            }
        }

        logger.info("Compaction service started for {} database(s), {} tier(s) ({}), housekeeping every {}s, "
                        + "file-count refresh every {}s, data retention {}",
                config.databases().size(), config.tiers().size(),
                config.tiers().stream()
                        .map(t -> t.name() + (t.enabled() ? "" : " (disabled)"))
                        .reduce((a, b) -> a + ", " + b).orElse(""),
                housekeepingSeconds, fileCountSeconds,
                dataRetention != null
                        ? "every %ds from %s".formatted(config.dataRetention().frequency().toSeconds(),
                                config.dataRetention().policyTable())
                        : "off");
    }

    public CompactionStats getStats() {
        Map<String, CompactionStats.DatabaseStats> dbStats = new HashMap<>();
        CompactionStats base = state.getSnapshot(config.databases());
        base.databases().forEach((db, ds) -> dbStats.put(db, ds.withNextExecutionTimeByTier(nextExecutionTimeByTier(db))));
        return new CompactionStats(base.serviceStart(), dbStats);
    }

    private Map<String, Instant> nextExecutionTimeByTier(String database) {
        Map<String, Instant> next = new HashMap<>();
        Map<String, Instant> lastRuns = lastTierRun.getOrDefault(database, new ConcurrentHashMap<>());
        for (CompactionTier tier : config.tiers()) {
            if (!tier.enabled()) {
                continue;
            }
            Instant lastRun = lastRuns.get(tier.name());
            if (lastRun != null) {
                next.put(tier.name(), lastRun.plus(tier.frequency()));
            }
        }
        return next;
    }

    /** Before/after counts and total bytes for a tier's file-size range; null components on read failure. */
    private record ActiveFiles(Long files, Long bytes) {
        static final ActiveFiles UNKNOWN = new ActiveFiles(null, null);
    }

    void runTier(String database, CompactionTier tier) {
        Instant scheduledAt = Instant.now();
        Instant startedAt = scheduledAt; // the scheduled task IS runTier; queueing shows up as actualGap
        Instant prevEnded = lastTierRun.getOrDefault(database, new ConcurrentHashMap<>()).get(tier.name());
        long actualGapMs = prevEnded != null ? Duration.between(prevEnded, startedAt).toMillis() : -1;
        long runId = runIds.computeIfAbsent(runKey(database, tier.name()), k -> new AtomicLong()).incrementAndGet();

        ActiveFiles before = ActiveFiles.UNKNOWN;
        ActiveFiles after = ActiveFiles.UNKNOWN;
        TierCompactor.MergeOutcome merge = null;
        CompactionRun.Outcome outcome = CompactionRun.Outcome.FAILED; // overwritten below; keeps `finally` definitely-assigned
        CompactionRun.FailureClass failureClass = CompactionRun.FailureClass.NONE;
        String errorMessage = null;

        // Keeping the whole body inside the try is what prevents a stray throwable from escaping the
        // scheduled task — a Runnable that throws would cancel this tier's schedule forever.
        try {
            before = queryActiveFiles(database, tier.name(), tier.minFileSize(), tier.maxFileSize());
            merge = tierCompactor.compact(database, tier);
            after = queryActiveFiles(database, tier.name(), tier.minFileSize(), tier.maxFileSize());
            state.incrementTier(database, tier.name());

            Long filesRetired = (before.files() != null && after.files() != null) ? before.files() - after.files() : null;
            if (filesRetired != null && filesRetired > 0) {
                state.addFilesCompacted(database, filesRetired);
            }
            Long bytesRetired = (before.bytes() != null && after.bytes() != null) ? before.bytes() - after.bytes() : null;
            if (bytesRetired != null && bytesRetired > 0) {
                state.addBytesCompacted(database, tier.name(), bytesRetired);
            }
            outcome = (filesRetired != null && filesRetired == 0) ? CompactionRun.Outcome.EMPTY : CompactionRun.Outcome.SUCCESS;
            state.recordSuccess(database);
            logger.info("Tier '{}' compaction completed for {} (files {} -> {})",
                    tier.name(), database, before.files(), after.files());
        } catch (Throwable t) {
            outcome = CompactionRun.Outcome.FAILED;
            failureClass = CompactionRun.classify(t);
            errorMessage = CompactionRun.truncateError(t);
            state.recordFailure(database, tier.name());
            state.recordFailureClass(database, failureClass);
            logger.error("Tier '{}' compaction cycle failed for {} [{}] — scheduler will continue",
                    tier.name(), database, failureClass, t);
        } finally {
            Instant endedAt = Instant.now();
            lastTierRun.computeIfAbsent(database, k -> new ConcurrentHashMap<>()).put(tier.name(), endedAt);
            state.recordRunCompleted(database, endedAt);
            recordRun(database, tier, runId, scheduledAt, startedAt, endedAt, actualGapMs,
                    before, after, merge, outcome, failureClass, errorMessage);
        }
    }

    private void recordRun(String database, CompactionTier tier, long runId, Instant scheduledAt, Instant startedAt,
                           Instant endedAt, long actualGapMs, ActiveFiles before, ActiveFiles after,
                           TierCompactor.MergeOutcome merge, CompactionRun.Outcome outcome,
                           CompactionRun.FailureClass failureClass, String errorMessage) {
        try {
            Long filesRetired = (before.files() != null && after.files() != null) ? before.files() - after.files() : null;
            String tempDir = ResourceSampler.tempDirectory(tier.connectionSettings());
            // The commit timeout the cycle races comes from the tier's connection settings; fall back
            // to the Postgres server default when the tier does not set one (spec Q3).
            long tierCommitTimeoutMs = ResourceSampler.idleInTransactionTimeoutMs(tier.connectionSettings());
            if (tierCommitTimeoutMs <= 0) {
                tierCommitTimeoutMs = CompactionRun.DEFAULT_COMMIT_TIMEOUT_MS;
            }
            CompactionRun run = new CompactionRun(
                    runId, database, tier.name(), scheduledAt, startedAt, endedAt,
                    tier.frequency().toMillis(), actualGapMs,
                    before.files(), after.files(), filesRetired, before.bytes(), after.bytes(),
                    tier.maxCompactedFiles(),
                    merge != null ? merge.filesProcessed() : null,
                    merge != null ? merge.filesCreated() : null,
                    Duration.between(startedAt, endedAt).toMillis(),
                    merge != null ? merge.durationMergeMs() : -1,
                    outcome, failureClass, errorMessage,
                    ResourceSampler.rssPeakBytes(),
                    ResourceSampler.dirSizeBytes(tempDir),
                    ResourceSampler.memoryLimitBytes(tier.connectionSettings()),
                    tierCommitTimeoutMs);
            runLog.record(run);

            // Push this cycle's control inputs + derived quantities to the OTLP-exported gauges/counters.
            // max_compacted_files and tier_frequency are emitted now so making them dynamic later is visible.
            CompactionRunLog.DerivedAggregates agg =
                    runLog.aggregates(new CompactionRunLog.Key(database, tier.name()));
            state.recordOutcome(database, tier.name(), outcome);
            state.updateTierGauges(database, tier.name(), tier.maxCompactedFiles(), tier.frequency().toMillis(),
                    agg.drainFilesPerSec(), agg.arrivalFilesPerSec(), agg.saturated(), run.spillPeakBytes());
            state.updateRssPeak(run.rssPeakBytes());
        } catch (Exception e) {
            // Telemetry assembly must never take down a compaction cycle.
            logger.warn("Failed to record compaction telemetry for {} tier '{}'", database, tier.name(), e);
        }
    }

    void runHousekeeping(String database) {
        try {
            housekeeper.housekeep(database);
            logger.info("Housekeeping completed for {}", database);
        } catch (Throwable t) {
            state.recordFailure(database, CompactionState.HOUSEKEEPING_KIND);
            logger.error("Unexpected error in housekeeping cycle for {} — scheduler will continue", database, t);
        }
    }

    void runRetention(String database) {
        try {
            DataRetention.Outcome outcome = dataRetention.enforce(database);
            if (outcome.failedTables() > 0) {
                state.recordFailure(database, CompactionState.RETENTION_KIND);
            }
            logger.info("Data retention completed for {}: {} row(s) deleted from {} table(s), {} failed",
                    database, outcome.rowsDeleted(), outcome.tables(), outcome.failedTables());
        } catch (Throwable t) {
            // Never let it escape: a Runnable that throws cancels its schedule for good.
            state.recordFailure(database, CompactionState.RETENTION_KIND);
            logger.error("Data retention cycle failed for {} — scheduler will continue", database, t);
        }
    }

    /**
     * Counts live files and sums their bytes in {@code [minSizeInclusive, maxSizeExclusive)} in one
     * query — the COUNT the compactor already ran, now with a same-round-trip {@code SUM(file_size_bytes)}
     * (spec: bytes, not file counts, are the real cost driver). Scoped to the tier's own range so
     * concurrently-running tiers each measure only their own file-size range.
     */
    private ActiveFiles queryActiveFiles(String database, String tierName, long minSizeInclusive, long maxSizeExclusive) {
        String mdDatabase = "\"__ducklake_metadata_" + database + "\"";
        String sql = ("SELECT COUNT(*) AS total, COALESCE(SUM(file_size_bytes), 0) AS total_bytes "
                + "FROM %s.ducklake_data_file WHERE end_snapshot IS NULL "
                + "AND file_size_bytes >= %d AND file_size_bytes < %d")
                .formatted(mdDatabase, minSizeInclusive, maxSizeExclusive);
        try (var statement = countingConnectionFor(database, tierName).createStatement()) {
            statement.execute(sql);
            try (ResultSet rs = statement.getResultSet()) {
                return rs.next() ? new ActiveFiles(rs.getLong("total"), rs.getLong("total_bytes")) : ActiveFiles.UNKNOWN;
            }
        } catch (Exception e) {
            logger.warn("Could not read active files for {}", database, e);
            return ActiveFiles.UNKNOWN;
        }
    }

    private static String runKey(String database, String tierName) {
        return database.length() + ":" + database + "|" + tierName;
    }

    private Connection countingConnectionFor(String database, String tierName) throws SQLException {
        return countingConnections.get(runKey(database, tierName));
    }

    /**
     * Refreshes every configured tier's current-file-count gauge plus the whole-catalog total, on its
     * own slow schedule using a dedicated connection — never the per-tier counting connections, which
     * a concurrently-running {@link #runTier} may be using (a JDBC Connection is not concurrency-safe).
     */
    private void refreshFileCounts(String database) {
        String mdDatabase = "\"__ducklake_metadata_" + database + "\"";
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) AS total_files");
        for (CompactionTier tier : config.tiers()) {
            sql.append(", COUNT(*) FILTER (WHERE file_size_bytes >= %d AND file_size_bytes < %d) AS \"%s\""
                    .formatted(tier.minFileSize(), tier.maxFileSize(), tier.name()));
        }
        sql.append(" FROM %s.ducklake_data_file WHERE end_snapshot IS NULL".formatted(mdDatabase));

        try (var statement = countingConnectionFor(database, FILE_COUNT_REFRESH_KEY).createStatement()) {
            statement.execute(sql.toString());
            try (ResultSet rs = statement.getResultSet()) {
                if (rs.next()) {
                    for (CompactionTier tier : config.tiers()) {
                        state.updateTierFileCount(database, tier.name(), rs.getLong(tier.name()));
                    }
                    state.updateTotalFiles(database, rs.getLong("total_files"));
                }
            }
        } catch (Exception e) {
            logger.error("Failed to refresh file counts for {}", database, e);
        }
    }

    @Override
    public void close() {
        compactionScheduler.shutdown();
        housekeepingScheduler.shutdown();
        try {
            if (!compactionScheduler.awaitTermination(30, TimeUnit.SECONDS))
                compactionScheduler.shutdownNow();
            if (!housekeepingScheduler.awaitTermination(30, TimeUnit.SECONDS))
                housekeepingScheduler.shutdownNow();
        } catch (InterruptedException e) {
            compactionScheduler.shutdownNow();
            housekeepingScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        closeQuietly(tierCompactor);
        closeQuietly(housekeeper);
        if (dataRetention != null) {
            closeQuietly(dataRetention);
        }
        countingConnections.close();
        logger.info("Compaction service stopped");
    }

    private void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            logger.warn("Failed to close {}", closeable.getClass().getSimpleName(), e);
        }
    }
}
