package io.dazzleduck.sql.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs the compactor: one {@link TierLoop} thread per enabled {@code (database, tier)}
 * (DYNAMIC_COMPACTION_SPEC.md, Phase 1), plus housekeeping, data retention and the file-count gauges
 * on their own timers. Merges, retention and the delete-file rewrite share one {@link TableLeases}, so
 * a table runs at most one of them at a time.
 */
public class CompactionService implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionService.class);

    /** A tier loop that has not completed a pass for this long raises an alert. */
    static final Duration PASS_ALERT_AFTER = Duration.ofMinutes(15);
    /** A band whose file count rose at every sample for this long raises an alert. */
    static final Duration RISING_ALERT_AFTER = Duration.ofMinutes(30);
    /** RSS above this share of the container's memory limit raises an alert. */
    static final double RSS_ALERT_SHARE = 0.75;

    private final CompactionConfig config;
    private final String startupScript;
    private final TierCompactor tierCompactor;
    private final Housekeeper housekeeper;
    // null when data retention is off
    private final DataRetention dataRetention;
    private final CompactionState state;
    // Where its file-count instances spill, and what /health reports; set by useSpill before start
    private volatile SpillBudget spill = SpillBudget.NONE;
    private final CompactionRunLog runLog;
    private final TableLeases leases;
    private final Clock clock;

    // "db|tier" -> loop, for every enabled tier; started by start()
    private final Map<String, TierLoop> loops = new ConcurrentHashMap<>();
    private final ScheduledExecutorService housekeepingScheduler;
    private volatile boolean started;

    // One raw connection for the whole-catalog file-count gauges — deliberately not
    // io.dazzleduck.sql.commons.ConnectionPool (see RawConnections).
    private final ConcurrentHashMap<String, Connection> countingConnections = new ConcurrentHashMap<>();

    /**
     * Cadence for the whole-catalog file-count gauges, off the compaction path. These gauges are a
     * coarse backlog indicator, so a slow cadence keeps the COUNT(*) FILTER aggregate off the shared
     * catalog's back.
     */
    private static final long FILE_COUNT_REFRESH_SECONDS = 120;

    public CompactionService(CompactionConfig config, String startupScript, TierCompactor tierCompactor,
                             Housekeeper housekeeper, CompactionState state, CompactionRunLog runLog) {
        this(config, startupScript, tierCompactor, housekeeper, null, state, runLog);
    }

    public CompactionService(CompactionConfig config, String startupScript, TierCompactor tierCompactor,
                             Housekeeper housekeeper, DataRetention dataRetention, CompactionState state,
                             CompactionRunLog runLog) {
        this(config, startupScript, tierCompactor, housekeeper, dataRetention, state, runLog, new TableLeases(),
                Clock.systemUTC());
    }

    /**
     * @param dataRetention deletes expired rows on {@code config.dataRetention()}'s schedule, or
     *                      {@code null} to run no retention
     * @param leases        shared with the housekeeper's delete-file rewrite and with data retention
     */
    public CompactionService(CompactionConfig config, String startupScript, TierCompactor tierCompactor,
                             Housekeeper housekeeper, DataRetention dataRetention, CompactionState state,
                             CompactionRunLog runLog, TableLeases leases, Clock clock) {
        if (dataRetention != null && !config.dataRetention().enabled()) {
            throw new IllegalArgumentException("a DataRetention was given but data_retention is not enabled");
        }
        if (dataRetention == null && config.dataRetention().enabled()) {
            logger.warn("data_retention is enabled but no DataRetention was given, so retention will not run");
        }
        this.config = config;
        this.startupScript = startupScript;
        this.tierCompactor = tierCompactor;
        this.housekeeper = housekeeper;
        this.dataRetention = dataRetention;
        this.state = state;
        this.runLog = runLog;
        this.leases = leases;
        this.clock = clock;
        for (String db : config.databases()) {
            for (CompactionTier tier : config.tiers()) {
                if (tier.enabled()) {
                    loops.put(loopKey(db, tier.name()), newLoop(db, tier));
                }
            }
        }
        // Housekeeping, the file-count refresh and data retention share this pool: one thread per job
        // per database, so a long retention delete or a rewrite waiting on a lease holds up neither.
        int jobsPerDatabase = dataRetention != null ? 3 : 2;
        this.housekeepingScheduler = Executors.newScheduledThreadPool(
                Math.max(1, config.databases().size() * jobsPerDatabase), r -> {
                    Thread t = new Thread(r, "housekeeping");
                    t.setDaemon(false);
                    return t;
                });
    }

    private TierLoop newLoop(String db, CompactionTier tier) {
        return new TierLoop(db, tier, tierCompactor, leases, state, runLog, clock);
    }

    public void start() {
        if (config.databases().isEmpty()) {
            logger.warn("No databases configured — compaction service idle");
            return;
        }
        started = true;
        loops.values().forEach(TierLoop::start);
        long housekeepingSeconds = config.housekeepingFrequency().toSeconds();
        for (String db : config.databases()) {
            housekeepingScheduler.scheduleWithFixedDelay(
                    () -> runHousekeeping(db), housekeepingSeconds, housekeepingSeconds, TimeUnit.SECONDS);
            housekeepingScheduler.scheduleWithFixedDelay(
                    () -> refreshFileCounts(db), 0, FILE_COUNT_REFRESH_SECONDS, TimeUnit.SECONDS);
            if (dataRetention != null) {
                // First run at startup, so a compactor that restarts more often than the frequency
                // still enforces retention.
                housekeepingScheduler.scheduleWithFixedDelay(
                        () -> runRetention(db), 0, config.dataRetention().frequency().toSeconds(), TimeUnit.SECONDS);
            }
        }

        logger.info("Compaction service started for {} database(s), {} tier(s) ({}), housekeeping every {}s, "
                        + "data retention {}",
                config.databases().size(), config.tiers().size(),
                config.tiers().stream()
                        .map(t -> t.enabled()
                                ? "%s [%d, %d) target %d, cap %d, cpu_share %.2f".formatted(t.name(), t.minFileSize(),
                                        t.maxFileSize(), t.targetFileSize(), t.maxCompactedFiles(), t.cpuShare())
                                : t.name() + " (disabled)")
                        .reduce((a, b) -> a + "; " + b).orElse(""),
                housekeepingSeconds,
                dataRetention != null
                        ? "every %ds from %s".formatted(config.dataRetention().frequency().toSeconds(),
                                config.dataRetention().policyTable())
                        : "off");
    }

    /**
     * Runs one pass of a tier's loop on the calling thread, for tests and tools. Must not be used
     * once {@link #start()} has started the loops: a loop's state belongs to one thread.
     */
    TierLoop.PassResult runPass(String database, CompactionTier tier) {
        if (started) {
            throw new IllegalStateException("the tier loops are running");
        }
        TierLoop loop = loops.compute(loopKey(database, tier.name()),
                (k, existing) -> existing != null && existing.tier().equals(tier) ? existing : newLoop(database, tier));
        return loop.runPass();
    }

    /** Where the file-count instances spill, and the spill /health reports. Call before {@link #start()}. */
    public void useSpill(SpillBudget value) {
        this.spill = value;
    }

    public CompactionStats getStats() {
        Instant now = clock.instant();
        CompactionStats base = state.getSnapshot(config.databases());
        Map<String, CompactionStats.DatabaseStats> dbStats = new HashMap<>();
        List<String> alerts = new ArrayList<>();
        for (Map.Entry<String, CompactionStats.DatabaseStats> entry : base.databases().entrySet()) {
            String db = entry.getKey();
            Map<String, CompactionStats.TierStatus> tiers = new LinkedHashMap<>();
            for (CompactionTier tier : config.tiers()) {
                TierLoop loop = loops.get(loopKey(db, tier.name()));
                if (loop == null) {
                    continue;
                }
                CompactionStats.TierStatus status = loop.status();
                tiers.put(tier.name(), status);
                alerts.addAll(tierAlerts(db, tier, loop, status, now));
            }
            if (state.snapshotsGrowing(db)) {
                alerts.add("%s: unexpired snapshots grew across the last %d housekeeping runs"
                        .formatted(db, CompactionState.SNAPSHOT_GROWTH_RUNS));
            }
            dbStats.put(db, entry.getValue().withTiers(tiers));
        }
        long rss = ResourceSampler.rssBytes();
        long limit = ResourceSampler.containerMemoryLimitBytes();
        if (rss > 0 && limit > 0 && rss > RSS_ALERT_SHARE * limit) {
            alerts.add("RSS %d bytes is above %.0f%% of the container's memory limit %d bytes"
                    .formatted(rss, RSS_ALERT_SHARE * 100, limit));
        }
        CompactionStats.SpillStatus spillStatus = spill.status();
        alerts.addAll(SpillBudget.alerts(spillStatus));
        return new CompactionStats(base.serviceStart(), dbStats, List.copyOf(alerts), spillStatus);
    }

    private List<String> tierAlerts(String db, CompactionTier tier, TierLoop loop, CompactionStats.TierStatus status,
                                    Instant now) {
        List<String> alerts = new ArrayList<>();
        String prefix = "%s tier '%s'".formatted(db, tier.name());
        if ("REFUSED".equals(status.state())) {
            alerts.add(prefix + " did not start: " + status.refused());
            return alerts;
        }
        Instant lastPass = status.lastPass() != null ? status.lastPass() : loop.started();
        if (lastPass != null && Duration.between(lastPass, now).compareTo(PASS_ALERT_AFTER) > 0) {
            alerts.add(prefix + " has not completed a pass since " + lastPass);
        }
        status.backoffs().forEach(t -> alerts.add("%s backs off %s after %d consecutive failures"
                .formatted(prefix, t, TierLoop.FAILURES_BEFORE_BACKOFF)));
        status.starved().forEach(t -> alerts.add(prefix + " may starve partitions of " + t
                + ": its busy partitions want more slots than its cap"));
        status.oversized().forEach(t -> alerts.add(prefix + " skips " + t
                + ": not even two of its files fit in one group within the catalog's idle-in-transaction timeout"
                + " and the instance's memory_limit"));
        status.stale().forEach(t -> alerts.add("%s: %s has had a file waiting over %d minutes"
                .formatted(prefix, t, TierLoop.STALE_AFTER.toMinutes())));
        if (state.fileCountRising(db, tier.name(), RISING_ALERT_AFTER, now)) {
            alerts.add("%s: its band's file count rose for %d minutes".formatted(prefix, RISING_ALERT_AFTER.toMinutes()));
        }
        return alerts;
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

    private static String loopKey(String database, String tierName) {
        return database.length() + ":" + database + "|" + tierName;
    }

    private Connection countingConnectionFor(String database) throws SQLException {
        Connection existing = countingConnections.get(database);
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = countingConnections.get(database);
            if (existing != null) {
                return existing;
            }
            int databases = config.databases().size();
            Connection opened = RawConnections.open(startupScript, spill.apply(database, "file-counts",
                    MemoryBudget.FILE_COUNTS, databases, MemoryBudget.withDefaultLimit(List.of(),
                            MemoryBudget.FILE_COUNTS, databases, ResourceSampler.totalMemoryBytes())));
            countingConnections.put(database, opened);
            return opened;
        }
    }

    /**
     * Refreshes every configured tier's current-file-count gauge plus the whole-catalog total, on its
     * own slow schedule and connection, and samples the process's memory.
     */
    void refreshFileCounts(String database) {
        state.updateProcessMemory(ResourceSampler.rssBytes(), ResourceSampler.containerMemoryLimitBytes());
        String mdDatabase = Sql.metadataCatalog(database);
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) AS total_files");
        for (CompactionTier tier : config.tiers()) {
            sql.append(", COUNT(*) FILTER (WHERE file_size_bytes >= %d AND file_size_bytes < %d) AS \"%s\""
                    .formatted(tier.minFileSize(), tier.maxFileSize(), tier.name()));
        }
        sql.append(" FROM %s.ducklake_data_file WHERE end_snapshot IS NULL".formatted(mdDatabase));

        try (var statement = countingConnectionFor(database).createStatement()) {
            statement.execute(sql.toString());
            try (ResultSet rs = statement.getResultSet()) {
                if (rs.next()) {
                    Instant now = clock.instant();
                    for (CompactionTier tier : config.tiers()) {
                        state.updateTierFileCount(database, tier.name(), rs.getLong(tier.name()), now);
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
        loops.values().forEach(TierLoop::stop);
        housekeepingScheduler.shutdown();
        try {
            for (TierLoop loop : loops.values()) {
                if (!loop.join(Duration.ofSeconds(30))) {
                    logger.warn("Tier '{}' is still in a call after 30 s; closing its connection aborts it",
                            loop.tier().name());
                }
            }
            if (!housekeepingScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                housekeepingScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            housekeepingScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        closeQuietly(tierCompactor);
        closeQuietly(housekeeper);
        if (dataRetention != null) {
            closeQuietly(dataRetention);
        }
        countingConnections.values().forEach(connection -> {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("Failed to close a counting connection", e);
            }
        });
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
