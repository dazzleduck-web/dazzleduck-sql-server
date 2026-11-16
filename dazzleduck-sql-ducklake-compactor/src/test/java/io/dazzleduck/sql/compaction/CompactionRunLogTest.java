package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompactionRunLogTest {

    private static final CompactionRunLog.Key KEY = new CompactionRunLog.Key("db", "tier");

    @Test
    void ringEvictsOldestBeyondCapacity() {
        CompactionRunLog log = new CompactionRunLog(3);
        for (int i = 1; i <= 5; i++) {
            log.record(TestRuns.run(i, i, i, 0, 10L, 1L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        }
        var recent = log.recent(KEY);
        assertEquals(3, recent.size(), "only the last 3 retained");
        assertEquals(3, recent.get(0).runId(), "oldest retained is run 3");
        assertEquals(5, log.latest(KEY).runId());
    }

    @Test
    void keysAreListedAndIsolatedPerDatabaseTier() {
        CompactionRunLog log = new CompactionRunLog(10);
        log.record(TestRuns.run(1, 0, 1, 0, 10L, 1L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        assertEquals(1, log.keys().size());
        assertEquals(KEY, log.keys().get(0));
    }

    @Test
    void derivedAggregatesMatchTheSpecFormulas() {
        CompactionRunLog log = new CompactionRunLog(50);
        // call 1: 2 s, merged 81 files into 1 -> 80 retired
        log.record(TestRuns.run(1, 0, 2, 2000, 81L, 1L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        // call 2: 3 s, merged 42 into 2 -> 40 retired
        log.record(TestRuns.run(2, 10, 13, 8000, 42L, 2L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        // call 3: 1 s, nothing merged
        log.record(TestRuns.run(3, 20, 21, 7000, null, null, CompactionRun.Outcome.EMPTY, CompactionRun.FailureClass.NONE));

        var agg = log.aggregates(KEY);
        assertEquals(3, agg.windowSize());
        // throughput = (80 + 40) files over the 5 s of the calls that reported files = 24 f/s
        assertEquals(24.0, agg.throughputFilesPerSec(), 0.001);
        assertEquals(1.0 / 3, agg.idleRatio(), 0.001);
        assertEquals(3000, agg.p95DurationMs());
        assertEquals(3000.0 / TestRuns.TIMEOUT_MS, agg.durationHeadroom(), 0.0001);
    }

    @Test
    void headroomIsZeroWithoutACatalogTimeout() {
        CompactionRunLog log = new CompactionRunLog(10);
        CompactionRun run = TestRuns.run(1, 0, 5, 0, 10L, 1L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE);
        log.record(new CompactionRun(run.runId(), run.database(), run.tierName(), run.tableName(), run.startedAt(),
                run.endedAt(), run.actualGapMs(), run.filesWaiting(), run.bytesWaiting(), run.cap(), run.staticCap(),
                run.busyPartitions(), run.filesProcessed(), run.filesCreated(), run.durationTotalMs(),
                run.durationMergeMs(), run.outcome(), run.failureClass(), run.errorMessage(), -1, -1, -1, 0));
        assertEquals(0.0, log.aggregates(KEY).durationHeadroom());
    }

    @Test
    void failuresAreCountedByClass() {
        CompactionRunLog log = new CompactionRunLog(10);
        log.record(TestRuns.run(1, 0, 1, 100, 10L, 1L, CompactionRun.Outcome.FAILED, CompactionRun.FailureClass.OUT_OF_MEMORY));
        log.record(TestRuns.run(2, 2, 3, 100, 10L, 1L, CompactionRun.Outcome.FAILED, CompactionRun.FailureClass.OUT_OF_MEMORY));
        log.record(TestRuns.run(3, 4, 5, 100, 10L, 1L, CompactionRun.Outcome.FAILED, CompactionRun.FailureClass.COMMIT_TIMEOUT));
        var byClass = log.aggregates(KEY).failuresByClass();
        assertEquals(2L, byClass.get(CompactionRun.FailureClass.OUT_OF_MEMORY));
        assertEquals(1L, byClass.get(CompactionRun.FailureClass.COMMIT_TIMEOUT));
    }

    @Test
    void emptyKeyYieldsEmptyAggregates() {
        CompactionRunLog log = new CompactionRunLog(10);
        assertEquals(0, log.aggregates(KEY).windowSize());
        assertTrue(log.recent(KEY).isEmpty());
        assertNull(log.latest(KEY));
    }
}
