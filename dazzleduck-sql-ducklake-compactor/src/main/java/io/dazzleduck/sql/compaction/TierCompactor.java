package io.dazzleduck.sql.compaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * The catalog side of one tier's loop (DYNAMIC_COMPACTION_SPEC.md, Phase 1). Every method for a
 * given {@code (database, tier)} is called from that pair's loop thread only, so an implementation
 * may keep one connection per pair without locking.
 */
public interface TierCompactor extends Closeable {

    /**
     * Opens the pair's connection if it is not open, and checks it: the session target can be set,
     * the catalog's idle-in-transaction timeout is read, and one full group fits in it.
     *
     * @return the pair's readiness; {@link Readiness#refused()} is permanent: the tier does not start
     * @throws Exception when the connection cannot be opened now (the loop retries)
     */
    Readiness prepare(String database, CompactionTier tier) throws Exception;

    /** The tables with work for the tier: one round trip over the catalog metadata. */
    List<TableWork> discover(String database, CompactionTier tier) throws Exception;

    /**
     * One table call: sets the tier's target on the session, then merges at most {@code cap} groups
     * of the table in one transaction and one commit.
     */
    MergeOutcome compact(String database, CompactionTier tier, TableWork table, int cap) throws Exception;

    /** Drops the pair's connection after a failure that may have broken it; prepare reopens it. */
    default void reset(String database, CompactionTier tier) {
    }

    /** Memory DuckDB reports in use by the pair's instance ({@code duckdb_memory()}), or -1. */
    default long memoryUsageBytes(String database, CompactionTier tier) {
        return -1;
    }

    /**
     * @param refused          why the tier must not start, or {@code null} when it is ready
     * @param idleTxnTimeoutMs the catalog's idle-in-transaction timeout; 0 for none
     * @param threads          the DuckDB threads of the pair's instance
     */
    record Readiness(String refused, long idleTxnTimeoutMs, int threads) {
        static Readiness ready(long idleTxnTimeoutMs, int threads) {
            return new Readiness(null, idleTxnTimeoutMs, threads);
        }

        boolean isRefused() {
            return refused != null;
        }
    }

    /**
     * Timing of one call. DuckLake merges and commits inside the single CALL, so
     * {@code durationMergeMs} covers both.
     *
     * @param filesProcessed input files the merge retired, or {@code null} if nothing was merged
     * @param filesCreated   output files it wrote, or {@code null} if nothing was merged
     */
    record MergeOutcome(long durationMergeMs, Long filesProcessed, Long filesCreated) {
    }

    @Override
    default void close() throws IOException {
    }
}
