package io.dazzleduck.sql.compaction;

import java.time.Instant;

/** Shared factory for {@link CompactionRun} fixtures in the compactor tests. */
final class TestRuns {
    private TestRuns() {}

    static final long TIMEOUT_MS = 600_000;

    static CompactionRun run(long id, long startSec, long endSec, long gapMs, Long processed, Long created,
                             CompactionRun.Outcome outcome, CompactionRun.FailureClass failureClass) {
        return run(id, startSec, endSec, gapMs, processed, created, outcome, failureClass, -1, -1);
    }

    static CompactionRun run(long id, long startSec, long endSec, long gapMs, Long processed, Long created,
                             CompactionRun.Outcome outcome, CompactionRun.FailureClass failureClass,
                             long rssBefore, long rssAfter) {
        return new CompactionRun(
                id, "db", "tier", "main.events",
                Instant.ofEpochSecond(startSec), Instant.ofEpochSecond(endSec), gapMs,
                100L, 1_000L, 4, 12, 1,
                processed, created,
                (endSec - startSec) * 1000, (endSec - startSec) * 1000,
                outcome, failureClass, failureClass == CompactionRun.FailureClass.NONE ? null : "err",
                -1, rssBefore, rssAfter, -1, -1, TIMEOUT_MS);
    }
}
