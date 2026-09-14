package io.dazzleduck.sql.compaction;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CompactionService implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionService.class);

    private final CompactionConfig config;
    private final MajorCompactor majorCompactor;
    private final CompactionState state;

    // Shared schedulers — one task submitted per database so each runs independently
    private final ScheduledExecutorService compactionScheduler;
    private final ScheduledExecutorService housekeepingScheduler;

    // Tracks when major compaction was last attempted per database
    private final ConcurrentHashMap<String, AtomicLong> lastMajorAttempt = new ConcurrentHashMap<>();

    public CompactionService(CompactionConfig config, MajorCompactor majorCompactor, CompactionState state) {
        this.config = config;
        this.majorCompactor = majorCompactor;
        this.state = state;
        int dbCount = Math.max(1, config.databases().size());
        this.compactionScheduler = Executors.newScheduledThreadPool(dbCount, r -> {
            Thread t = new Thread(r, "compaction");
            t.setDaemon(false);
            return t;
        });
        this.housekeepingScheduler = Executors.newScheduledThreadPool(dbCount, r -> {
            Thread t = new Thread(r, "housekeeping");
            t.setDaemon(false);
            return t;
        });
        config.databases().forEach(db -> lastMajorAttempt.put(db, new AtomicLong(System.currentTimeMillis())));
    }

    public void start() {
        if (config.databases().isEmpty()) {
            logger.warn("No databases configured — compaction service idle");
            return;
        }
        long minorSeconds = config.minorCompactionFrequency().toSeconds();
        long housekeepingSeconds = config.housekeepingFrequency().toSeconds();

        for (String db : config.databases()) {
            compactionScheduler.scheduleWithFixedDelay(
                    () -> runCompaction(db), 0, minorSeconds, TimeUnit.SECONDS);
            housekeepingScheduler.scheduleWithFixedDelay(
                    () -> runHousekeeping(db), housekeepingSeconds, housekeepingSeconds, TimeUnit.SECONDS);
        }

        logger.info("Compaction service started for {} database(s) — minor every {}s, major every {}s, housekeeping every {}s",
                config.databases().size(), minorSeconds, config.majorCompactionFrequency().toSeconds(), housekeepingSeconds);
    }

    public CompactionStats getStats() {
        Map<String, CompactionStats.DatabaseStats> dbStats = new HashMap<>();
        CompactionStats base = state.getSnapshot(config.databases());
        base.databases().forEach((db, ds) -> {
            Instant last = ds.lastSuccessTime();
            dbStats.put(db, ds.withNextExecutionTime(
                    last != null ? last.plus(config.minorCompactionFrequency()) : null));
        });
        return new CompactionStats(base.serviceStart(), dbStats);
    }

    void runCompaction(String database) {
        // Default so the catch below can always attribute a failure to a kind, even if determining
        // eligibility itself throws. Keeping the whole body inside the try is what prevents a stray
        // throwable from escaping the scheduled task — scheduleWithFixedDelay silently cancels a task
        // whose Runnable throws, which would stop this database's compaction forever.
        CycleKind kind = CycleKind.MINOR;
        try {
            boolean major = System.currentTimeMillis() - lastMajorAttempt.get(database).get()
                    >= config.majorCompactionFrequency().toMillis();
            kind = major ? CycleKind.MAJOR : CycleKind.MINOR;

            // Stamp the attempt before running it: a major that throws must not become eligible again
            // on the next minor tick, or a persistent failure turns into a retry storm.
            if (major) {
                lastMajorAttempt.get(database).set(System.currentTimeMillis());
            }

            OptionalLong filesBefore = queryTotalFiles(database);
            if (major) {
                majorCompactor.compact(database);
                logger.info("Major compaction completed for {}", database);
                state.incrementMajor(database);
            } else {
                runMinor(database);
                state.incrementMinor(database);
            }
            OptionalLong filesAfter = updateFileCounts(database);
            // Only record a delta when both reads succeeded; a failed metadata read must not be
            // treated as "zero files" or the cumulative counter is permanently inflated.
            if (filesBefore.isPresent() && filesAfter.isPresent()) {
                state.addFilesCompacted(database, filesBefore.getAsLong() - filesAfter.getAsLong());
            }
            state.recordSuccess(database);
        } catch (Throwable t) {
            state.recordFailure(database, kind);
            logger.error("{} compaction cycle failed for {} — scheduler will continue",
                    kind.tag(), database, t);
        }
    }

    void runHousekeeping(String database) {
        try {
            majorCompactor.housekeep(database);
            logger.info("Housekeeping completed for {}", database);
        } catch (Throwable t) {
            state.recordFailure(database, CycleKind.HOUSEKEEPING);
            logger.error("Unexpected error in housekeeping cycle for {} — scheduler will continue", database, t);
        }
    }

    private void runMinor(String database) throws Exception {
        Timer.Sample sample = state.startTimer();
        try (var connection = ConnectionPool.getConnection()) {
            String sql = "CALL ducklake_merge_adjacent_files('%s', max_file_size := %d)"
                    .formatted(database, config.minorCompactionMaxSize());
            ConnectionPool.execute(connection, sql);
            logger.info("Minor compaction completed for {}", database);
        } finally {
            state.stopTimer(sample, "minor", "merge", database);
        }
    }

    private OptionalLong queryTotalFiles(String database) {
        String mdDatabase = "\"__ducklake_metadata_" + database + "\"";
        String sql = "SELECT COUNT(*) AS total FROM %s.ducklake_data_file WHERE end_snapshot IS NULL"
                .formatted(mdDatabase);
        try (var connection = ConnectionPool.getConnection();
             var statement = connection.createStatement()) {
            statement.execute(sql);
            try (ResultSet rs = statement.getResultSet()) {
                return rs.next() ? OptionalLong.of(rs.getLong("total")) : OptionalLong.empty();
            }
        } catch (Exception e) {
            logger.warn("Could not query file count for {}", database, e);
            return OptionalLong.empty();
        }
    }

    /**
     * Refreshes the file-count gauges and returns the current active file total, which is the same
     * number a separate count would report — so this doubles as the post-cycle measurement.
     * Returns an empty {@link OptionalLong} when the metadata cannot be read, so a failed read is
     * never mistaken for a genuine zero, matching {@link #queryTotalFiles}.
     */
    private OptionalLong updateFileCounts(String database) {
        String mdDatabase = "\"__ducklake_metadata_" + database + "\"";
        String sql = """
                SELECT
                  COUNT(*) FILTER (WHERE file_size_bytes < %d) AS small_files,
                  COUNT(*) FILTER (WHERE file_size_bytes >= %d AND file_size_bytes < %d) AS medium_files,
                  COUNT(*) AS total_files
                FROM %s.ducklake_data_file
                WHERE end_snapshot IS NULL
                """.formatted(
                config.minorCompactionMaxSize(),
                config.minorCompactionMaxSize(),
                config.majorCompactionMaxSize(),
                mdDatabase);

        try (var connection = ConnectionPool.getConnection();
             var statement = connection.createStatement()) {
            statement.execute(sql);
            try (ResultSet rs = statement.getResultSet()) {
                if (rs.next()) {
                    long total = rs.getLong("total_files");
                    state.updateFileCounts(database,
                            rs.getLong("small_files"),
                            rs.getLong("medium_files"),
                            total);
                    return OptionalLong.of(total);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to update file counts for {}", database, e);
        }
        return OptionalLong.empty();
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
        logger.info("Compaction service stopped");
    }
}
