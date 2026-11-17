package io.dazzleduck.sql.compaction;

import java.time.Instant;

/**
 * One telemetry record per table call (DYNAMIC_COMPACTION_SPEC.md, "Telemetry and alerts"): the
 * table, the cap it was given, what the discovery query saw waiting, and what the merge's result row
 * reported. Capture only: it costs no catalog round trip, since the band-wide counts the compactor
 * used to run around every cycle are gone and files come from the merge's own result row (which has
 * no byte counts).
 *
 * <p>Nullable ({@link Long}) fields are values that can be genuinely unknown — a merge that returned
 * no row — and must stay distinct from a real zero. {@code -1} on the {@code long} resource fields
 * means "not measurable here" (e.g. {@code /proc} is Linux-only; a tier with no {@code memory_limit}).
 *
 * <p>DuckLake merges and commits inside the one {@code CALL}, so {@code durationMergeMs} is the whole
 * call; there is no separate commit time to capture.
 *
 * @param actualGapMs     ms since the tier's previous call ended, or -1 for its first
 * @param filesWaiting    the table's eligible files when the pass listed it
 * @param bytesWaiting    their total size
 * @param cap             the {@code max_compacted_files} the call was given
 * @param staticCap       the tier's static cap, the most any call is given
 * @param busyPartitions  the table's groups with a file committed in the last 10 minutes
 * @param rssBeforeBytes  process RSS just before the call, or -1 (not Linux)
 * @param rssAfterBytes   process RSS just after it: the growth over its input files calibrates the
 *                        per-file memory the call kept, outside DuckDB's {@code memory_limit}
 * @param commitTimeoutMs the catalog's idle-in-transaction timeout, or 0 for none
 */
public record CompactionRun(
        long runId,
        String database,
        String tierName,
        String tableName,
        Instant startedAt,
        Instant endedAt,
        long actualGapMs,
        Long filesWaiting,
        Long bytesWaiting,
        int cap,
        long staticCap,
        int busyPartitions,
        Long filesProcessed,
        Long filesCreated,
        long durationTotalMs,
        long durationMergeMs,
        Outcome outcome,
        FailureClass failureClass,
        String errorMessage,
        long rssPeakBytes,
        long rssBeforeBytes,
        long rssAfterBytes,
        long spillPeakBytes,
        long memoryLimitBytes,
        long commitTimeoutMs) {

    public enum Outcome { SUCCESS, EMPTY, FAILED }

    /**
     * Failure classes from the telemetry spec's taxonomy, plus {@link #PLANNING}: an error binding or
     * planning the merge itself (a table that cannot be read, a bad path), which isolating failures per
     * table is meant to contain. Kept apart because they call for different responses. {@link #NONE}
     * is the non-failure sentinel.
     */
    public enum FailureClass { NONE, COMMIT_TIMEOUT, TRANSACTION_CONFLICT, OUT_OF_MEMORY, SPILL_EXCEEDED, CATALOG_UNAVAILABLE, PLANNING, OTHER }

    private static final int MAX_ERROR_LEN = 200;

    /** Classifies an exception (and its cause chain) into the spec's taxonomy by message shape. */
    public static FailureClass classify(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            String m = c.getMessage();
            if (m == null) {
                continue;
            }
            String lower = m.toLowerCase(java.util.Locale.ROOT);
            // COMMIT_TIMEOUT: cycle outran idle_in_transaction_session_timeout — commit dies on a dead
            // connection, surfacing as a failed ROLLBACK. Batch too large.
            if (m.contains("Failed to execute query \"ROLLBACK\"") || lower.contains("idle_in_transaction")) {
                return FailureClass.COMMIT_TIMEOUT;
            }
            if (lower.contains("transaction conflict")) {
                return FailureClass.TRANSACTION_CONFLICT;
            }
            if (lower.contains("out of memory")) {
                return FailureClass.OUT_OF_MEMORY;
            }
            if (lower.contains("failed to offload data block") || lower.contains("max_temp_directory_size")
                    || lower.contains("temp directory")) {
                return FailureClass.SPILL_EXCEEDED;
            }
            if (lower.contains("connection refused") || lower.contains("connection reset")
                    || lower.contains("could not connect") || lower.contains("network")) {
                return FailureClass.CATALOG_UNAVAILABLE;
            }
            if (lower.contains("binder error") || lower.contains("catalog error")
                    || lower.contains("failed to get compaction file list") || lower.contains("internal error")) {
                return FailureClass.PLANNING;
            }
        }
        return FailureClass.OTHER;
    }

    /** Root-cause message, truncated to ~200 chars for storage. */
    public static String truncateError(Throwable t) {
        Throwable root = t;
        for (Throwable c = t; c != null; c = c.getCause()) {
            root = c;
        }
        String m = root.getMessage() != null ? root.getMessage() : root.getClass().getName();
        return m.length() > MAX_ERROR_LEN ? m.substring(0, MAX_ERROR_LEN) + "…" : m;
    }

    /** Files the call retired net of what it wrote, or null when the merge returned no row. */
    public Long filesRetired() {
        return (filesProcessed != null && filesCreated != null) ? filesProcessed - filesCreated : null;
    }
}
