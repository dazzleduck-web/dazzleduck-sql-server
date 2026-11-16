package io.dazzleduck.sql.compaction;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * What {@code /health} reports.
 *
 * @param alerts the alert conditions of DYNAMIC_COMPACTION_SPEC.md that hold now, one line each
 */
public record CompactionStats(
        Instant serviceStart,
        Map<String, DatabaseStats> databases,
        List<String> alerts,
        SpillStatus spill) {

    public CompactionStats(Instant serviceStart, Map<String, DatabaseStats> databases) {
        this(serviceStart, databases, List.of(), null);
    }

    public CompactionStats(Instant serviceStart, Map<String, DatabaseStats> databases, List<String> alerts) {
        this(serviceStart, databases, alerts, null);
    }

    /**
     * Where the DuckDB instances spill (see {@code SpillBudget}).
     *
     * @param budgetBytes what {@code max_temp_directory_size} is split from, -1 unknown
     * @param usedBytes   bytes spilled under the directory now, -1 unknown
     * @param freeBytes   free space under it, -1 unknown
     */
    public record SpillStatus(String directory, long budgetBytes, long usedBytes, long freeBytes) {
    }

    /**
     * Compaction counts are successful table calls; {@code lastSuccessTime} is null until a pass
     * succeeds.
     *
     * @param tiers              each enabled tier's loop, or empty before the service starts
     * @param unexpiredSnapshots snapshots left after housekeeping's latest expiry; -1 before one
     */
    public record DatabaseStats(
            Map<String, Long> tierCompactionCounts,
            long totalFailedCycles,
            long totalFilesCompacted,
            long totalFilesRewritten,
            Instant lastSuccessTime,
            Map<String, TierStatus> tiers,
            Map<String, Long> currentTierFileCounts,
            long currentTotalFiles,
            long unexpiredSnapshots) {

        /** Fills in what {@link CompactionState} cannot know: the tier loops. */
        public DatabaseStats withTiers(Map<String, TierStatus> value) {
            return new DatabaseStats(tierCompactionCounts, totalFailedCycles, totalFilesCompacted,
                    totalFilesRewritten, lastSuccessTime, value, currentTierFileCounts, currentTotalFiles,
                    unexpiredSnapshots);
        }
    }

    /**
     * One tier's loop for one database.
     *
     * @param state            STARTING until its connection is ready, RUNNING, REFUSED when a startup
     *                         check failed (see {@code refused}), or STOPPED
     * @param lastPass         when it last completed a pass, or null
     * @param queue            tables its latest pass listed
     * @param carryOver        tables its latest pass left for the next at the pass time limit
     * @param backoffs         tables it is backing off after consecutive failures
     * @param starved          tables whose busy partitions want more slots than the cap allows
     * @param stale            tables whose oldest file with a partner has waited over 30 minutes
     * @param oversized        tables skipped because not even two files fit in a group within the
     *                         catalog's timeout and the instance's memory
     * @param reduced          tables whose calls run with a lowered target because a full group does not fit
     * @param tables           every table the tier tracks: listed by its latest refresh, or backed off
     * @param idleTxnTimeoutMs the catalog's idle-in-transaction timeout, 0 for none, -1 unknown
     * @param duckdbMemoryBytes {@code duckdb_memory()} of its instance at its latest pass, -1 unknown
     */
    public record TierStatus(
            String state,
            String refused,
            Instant lastPass,
            long passes,
            int queue,
            int carryOver,
            List<String> backoffs,
            List<String> starved,
            List<String> stale,
            List<String> oversized,
            List<String> reduced,
            long idleTxnTimeoutMs,
            int threads,
            long duckdbMemoryBytes,
            List<TableStatus> tables) {
    }

    /**
     * One table in one tier.
     *
     * @param filesWaiting        eligible files in its groups of two or more, at the latest refresh;
     *                            0 for a table no longer listed (backed off and caught up)
     * @param oldestWaitSeconds   age of its oldest file with something to merge
     * @param lastCall            when its latest call ended, or null
     * @param lastOutcome         SUCCESS (merged), EMPTY (merged nothing), FAILED, or null
     * @param lastSuccess         when a call of it last completed without an error, or null
     * @param backoffUntil        the end of its back-off, or null
     * @param capLimit            the cap learnt from a timeout, or null for none
     * @param groupFilesLimit     the files per group learnt from a timeout, or null for none
     * @param flags               stale, starved, oversized, reduced, backoff
     */
    public record TableStatus(
            String table,
            long filesWaiting,
            long bytesWaiting,
            long oldestWaitSeconds,
            int busyPartitions,
            boolean sorted,
            Instant lastCall,
            String lastOutcome,
            Instant lastSuccess,
            int consecutiveFailures,
            Instant backoffUntil,
            Integer capLimit,
            Long groupFilesLimit,
            List<String> flags) {
    }

    public Duration uptime() {
        return Duration.between(serviceStart, Instant.now());
    }
}
