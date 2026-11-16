package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers the distinction the compaction counters used to miss: a call that throws must not count as
 * a success, and must not refresh the last-success timestamp an alert watches.
 */
class CompactionOutcomeTest {

    private static final String DB = "outcome_test";

    private static final CompactionTier MINOR = CompactionTier.of("minor", 0, 512 * 1024L, 1024 * 1024L, 12);
    private static final CompactionTier MAJOR = CompactionTier.of("major", 512 * 1024L, 10 * 1024 * 1024L,
            10 * 1024 * 1024L, 16);

    private static final CompactionConfig CONFIG = new CompactionConfig(
            List.of(DB),
            List.of(MINOR, MAJOR),
            Duration.ofSeconds(60),
            Duration.ofSeconds(5),
            List.of(),
            0,
            CompactionRunLog.DEFAULT_CAPACITY);

    // JUnit builds a fresh test instance per method, so these are per-test state.
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final CompactionState state = new CompactionState(registry, List.of(DB), List.of("minor", "major"));
    private final CompactionRunLog runLog = new CompactionRunLog(10);
    private final FakeTierCompactor compactor = new FakeTierCompactor();

    private static final TableWork EVENTS = FakeTierCompactor.table("main.events", 40, 40_000, Instant.now(), 0, true);

    private FakeTierCompactor withWork(boolean fail) {
        compactor.work = List.of(EVENTS);
        if (fail) {
            compactor.failing.put("main.events", "tier compaction blew up");
        }
        return compactor;
    }

    private static Housekeeper housekeeper(boolean fail) {
        return database -> {
            if (fail) throw new IllegalStateException("housekeeping blew up");
        };
    }

    private CompactionService service(TierCompactor tierCompactor, Housekeeper housekeeper) {
        return new CompactionService(CONFIG, null, tierCompactor, housekeeper, state, runLog);
    }

    private double failures(String kind) {
        return registry.get("ducklake.compaction.failures")
                .tag("database", DB).tag("type", kind).functionCounter().count();
    }

    private double successes(String tierName) {
        return registry.get("ducklake.compaction.cycles")
                .tag("database", DB).tag("tier", tierName).functionCounter().count();
    }

    @Test
    void aFailedCallCountsAsFailureAndLeavesLastSuccessUnset() {
        try (CompactionService service = service(withWork(true), housekeeper(false))) {
            TierLoop.PassResult result = service.runPass(DB, MAJOR);

            assertEquals(1, result.failed());
            assertEquals(1, failures("major"), "the failed call should be counted");
            assertEquals(0, successes("major"), "a call that threw must not count as a successful compaction");
            assertEquals(0, successes("minor"));
            assertNull(state.getLastSuccessTime(DB),
                    "an alert on last success must not be satisfied by a pass whose only call failed");
            assertEquals(1, state.getFailureCount(DB));
        }
    }

    @Test
    void differentTiersFailIndependently() {
        try (CompactionService service = service(withWork(true), housekeeper(false))) {
            service.runPass(DB, MINOR);

            assertEquals(1, failures("minor"), "the failed call should be counted");
            assertEquals(0, failures("major"), "a different tier's failure must not be blamed on this one");
            assertNull(state.getLastSuccessTime(DB));
            assertEquals(1, state.getFailureCount(DB));
        }
    }

    @Test
    void failedHousekeepingIsAttributedToItsOwnKind() {
        try (CompactionService service = service(withWork(false), housekeeper(true))) {
            service.runHousekeeping(DB);

            assertEquals(1, failures(CompactionState.HOUSEKEEPING_KIND));
            assertEquals(0, failures("major"), "housekeeping must not be blamed on a tier");
            assertNull(state.getLastSuccessTime(DB),
                    "housekeeping is not a compaction pass and does not mark success");
        }
    }

    @Test
    void aSuccessfulCallCountsAndStampsLastSuccess() {
        try (CompactionService service = service(withWork(false), housekeeper(false))) {
            service.runPass(DB, MAJOR);

            assertEquals(1, successes("major"));
            assertEquals(0, state.getFailureCount(DB));
            assertNotNull(state.getLastSuccessTime(DB));
            assertEquals(3.0, registry.get("ducklake.files.compacted").tag("database", DB).functionCounter().count(),
                    "4 files merged into 1 retire 3");
        }
    }

    @Test
    void aPassThatFindsNoWorkCountsAsAPassAndASuccess() {
        // An idle tier must not look stuck: the no-completed-pass alert counts a pass with no work.
        try (CompactionService service = service(compactor, housekeeper(false))) {
            TierLoop.PassResult result = service.runPass(DB, MINOR);

            assertFalse(result.progressed());
            assertEquals(1.0, registry.get("ducklake.compaction.passes").tag("database", DB).tag("tier", "minor")
                    .functionCounter().count());
            assertNotNull(state.getLastPass(DB, "minor"));
            assertNotNull(state.getLastSuccessTime(DB));
            assertTrue(compactor.calls.isEmpty());
        }
    }

    @Test
    void aCallUpdatesTheOutcomeCounterCapGaugesAndCommits() {
        try (CompactionService service = service(withWork(false), housekeeper(false))) {
            service.runPass(DB, MAJOR);

            assertEquals(1, registry.get("ducklake.compaction.cycles_by_outcome")
                    .tag("database", DB).tag("tier", "major").tag("outcome", "SUCCESS").functionCounter().count());
            assertEquals(MAJOR.maxCompactedFiles(), registry.get("ducklake.compaction.max_compacted_files")
                    .tag("database", DB).tag("tier", "major").gauge().value());
            assertEquals(compactor.calls.get(0).cap(), registry.get("ducklake.compaction.cap")
                    .tag("database", DB).tag("tier", "major").gauge().value());
            assertEquals(1, registry.get("ducklake.compaction.commits")
                    .tag("database", DB).tag("tier", "major").functionCounter().count());
            assertEquals(40, registry.get("ducklake.compaction.table.files_waiting")
                    .tag("database", DB).tag("tier", "major").tag("table", "main.events").gauge().value());
        }
    }

    @Test
    void theMergeOutcomeAndTheCallsSizingReachTheRecord() {
        try (CompactionService service = service(withWork(false), housekeeper(false))) {
            service.runPass(DB, MAJOR);
            CompactionRun latest = runLog.latest(new CompactionRunLog.Key(DB, "major"));
            assertNotNull(latest);
            assertEquals("main.events", latest.tableName());
            assertEquals(4L, latest.filesProcessed());
            assertEquals(1L, latest.filesCreated());
            assertEquals(3, latest.durationMergeMs());
            assertEquals(MAJOR.maxCompactedFiles(), latest.staticCap());
            assertEquals(compactor.calls.get(0).cap(), latest.cap());
            assertEquals(40L, latest.filesWaiting());
            assertEquals(40_000L, latest.bytesWaiting());
        }
    }

    @Test
    void aFailedCallIsCountedByOutcomeToo() {
        try (CompactionService service = service(withWork(true), housekeeper(false))) {
            service.runPass(DB, MAJOR);
            assertEquals(1, registry.get("ducklake.compaction.cycles_by_outcome")
                    .tag("database", DB).tag("tier", "major").tag("outcome", "FAILED").functionCounter().count());
        }
    }

    @Test
    void lastSuccessAgeIsRegisteredBeforeAnySuccessHasHappened() {
        // Registered up front rather than on first success, so a compactor that has never
        // succeeded still reports an age instead of no series at all.
        assertTrue(registry.get("ducklake.compaction.last_success_age")
                .tag("database", DB).gauge().value() >= 0);
    }

    @Test
    void statsReportEachEnabledTiersLoop() {
        try (CompactionService service = service(withWork(false), housekeeper(false))) {
            service.runPass(DB, MAJOR);
            CompactionStats.DatabaseStats ds = service.getStats().databases().get(DB);
            assertEquals("RUNNING", ds.tiers().get("major").state());
            assertEquals(1, ds.tiers().get("major").passes());
            assertEquals(1, ds.tiers().get("major").queue());
            assertEquals("STARTING", ds.tiers().get("minor").state(), "minor has not run a pass");
        }
    }

    @Test
    void aRefusedTierReportsWhyAndRaisesAnAlert() {
        compactor.readiness = new TierCompactor.Readiness("no ducklake_target_file_size", 0, 1);
        try (CompactionService service = service(compactor, housekeeper(false))) {
            TierLoop.PassResult result = service.runPass(DB, MINOR);
            assertTrue(result.refused());
            CompactionStats stats = service.getStats();
            assertEquals("REFUSED", stats.databases().get(DB).tiers().get("minor").state());
            assertTrue(stats.alerts().stream().anyMatch(a -> a.contains("did not start")
                    && a.contains("ducklake_target_file_size")), stats.alerts().toString());
        }
    }

    @Test
    void theServiceRefusesARunPassOnceItsLoopsRun() {
        try (CompactionService service = service(compactor, housekeeper(false))) {
            service.start();
            assertThrows(IllegalStateException.class, () -> service.runPass(DB, MINOR));
        }
    }
}
