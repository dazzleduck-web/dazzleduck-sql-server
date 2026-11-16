package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static io.dazzleduck.sql.compaction.FakeTierCompactor.table;
import static org.junit.jupiter.api.Assertions.*;

/** The Phase 1 loop of DYNAMIC_COMPACTION_SPEC.md, against a scripted compactor and clock. */
class TierLoopTest {

    private static final String DB = "lake";
    private static final Instant T0 = Instant.parse("2026-11-16T00:00:00Z");
    private static final CompactionTier MINOR = CompactionTier.of("minor", 0, 8_000_000, 16_000_000, 12)
            .withPassTimeLimit(Duration.ofMinutes(2));

    private final MutableTestClock clock = new MutableTestClock(T0);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final CompactionState state = new CompactionState(registry, List.of(DB), List.of("minor"));
    private final CompactionRunLog runLog = new CompactionRunLog(50);
    private final FakeTierCompactor compactor = new FakeTierCompactor();
    private final TableLeases leases = new TableLeases();
    private final TierLoop loop = new TierLoop(DB, MINOR, compactor, leases, state, runLog, clock);

    private static TableWork work(String name, long files, Instant oldest) {
        return table(name, files, files * 1_000, oldest, 0, true);
    }

    @Test
    void tablesAreCalledMostFilesFirstThenOldestFirst() {
        compactor.work = List.of(
                work("main.small", 3, T0.minusSeconds(10)),
                work("main.big", 50, T0.minusSeconds(5)),
                work("main.tie_new", 10, T0.minusSeconds(1)),
                work("main.tie_old", 10, T0.minusSeconds(100)));
        TierLoop.PassResult result = loop.runPass();
        assertEquals(List.of("main.big", "main.tie_old", "main.tie_new", "main.small"), compactor.calledTables());
        assertEquals(4, result.called());
        assertEquals(4, result.merged());
        assertEquals(0, result.carriedOver());
    }

    @Test
    void thePassTimeLimitCarriesTheRestOverToTheFrontOfTheNextPass() {
        compactor.work = List.of(
                work("main.a", 40, T0), work("main.b", 30, T0), work("main.c", 20, T0), work("main.d", 10, T0));
        // each call takes a minute: two fit before the 2-minute limit
        compactor.duringCall = t -> clock.advance(Duration.ofMinutes(1));

        TierLoop.PassResult first = loop.runPass();
        assertEquals(List.of("main.a", "main.b"), compactor.calledTables(), "a running call finishes, then the pass ends");
        assertEquals(2, first.carriedOver());
        assertEquals(2, loop.status().carryOver());

        // main.e now has more files than the carried-over tables, but they go first
        compactor.work = List.of(
                work("main.a", 40, T0), work("main.c", 20, T0), work("main.d", 10, T0), work("main.e", 99, T0));
        compactor.calls.clear();
        loop.runPass();
        assertEquals(List.of("main.c", "main.d"), compactor.calledTables());
    }

    @Test
    void aPassAlwaysMakesOneCallEvenWhenTheRefreshOutlastsTheLimit() {
        compactor.work = List.of(work("main.a", 40, T0), work("main.b", 30, T0));
        TierCompactor slowRefresh = new TierCompactor() {
            @Override
            public Readiness prepare(String database, CompactionTier tier) {
                return compactor.prepare(database, tier);
            }

            @Override
            public List<TableWork> discover(String database, CompactionTier tier) {
                clock.advance(Duration.ofMinutes(3));
                return compactor.discover(database, tier);
            }

            @Override
            public MergeOutcome compact(String database, CompactionTier tier, TableWork table, int cap, long target) {
                return compactor.compact(database, tier, table, cap, target);
            }
        };
        TierLoop slow = new TierLoop(DB, MINOR, slowRefresh, leases, state, runLog, clock);
        TierLoop.PassResult result = slow.runPass();
        assertEquals(List.of("main.a"), compactor.calledTables());
        assertEquals(1, result.carriedOver());
    }

    @Test
    void aPassThatMergesNothingLetsTheLoopSleep() {
        compactor.work = List.of(work("main.a", 40, T0));
        String key = TableLeases.key(DB, "main", "a");
        assertTrue(leases.tryAcquire(key));
        TierLoop.PassResult held = loop.runPass();
        assertEquals(1, held.listed());
        assertFalse(held.progressed(), "every listed table is held elsewhere: sleep rather than refresh again");
        leases.release(key);

        compactor.failing.put("main.a", "boom");
        assertFalse(loop.runPass().progressed(), "a failing table is retried after the sleep, not at once");
        compactor.failing.clear();
        assertTrue(loop.runPass().progressed());
    }

    @Test
    void aCarriedOverTableThatIsNoLongerListedIsDropped() {
        compactor.work = List.of(work("main.a", 40, T0), work("main.b", 30, T0), work("main.c", 20, T0));
        compactor.duringCall = t -> clock.advance(Duration.ofMinutes(2));
        loop.runPass();
        assertEquals(List.of("main.a"), compactor.calledTables());

        compactor.work = List.of(work("main.c", 20, T0));
        compactor.calls.clear();
        loop.runPass();
        assertEquals(List.of("main.c"), compactor.calledTables());
    }

    @Test
    void aTableWhoseLeaseIsHeldElsewhereIsSkippedThisPass() {
        compactor.work = List.of(work("main.a", 40, T0), work("main.b", 30, T0));
        String key = TableLeases.key(DB, "main", "a");
        assertTrue(leases.tryAcquire(key)); // retention runs on main.a
        TierLoop.PassResult result = loop.runPass();
        assertEquals(List.of("main.b"), compactor.calledTables());
        assertEquals(0, result.carriedOver(), "a skipped table is not carried over; the next refresh lists it again");
        assertFalse(leases.isBusy(TableLeases.key(DB, "main", "b")), "the loop released its own lease");

        leases.release(key);
        compactor.calls.clear();
        loop.runPass();
        assertTrue(compactor.calledTables().contains("main.a"));
    }

    @Test
    void aCallsLeaseIsReleasedWhenItFails() {
        compactor.work = List.of(work("main.a", 40, T0));
        compactor.failing.put("main.a", "boom");
        loop.runPass();
        assertFalse(leases.isBusy(TableLeases.key(DB, "main", "a")));
    }

    @Test
    void aFailingTableIsRetriedThenBacksOffAfterThreeFailuresInARow() {
        compactor.work = List.of(work("main.bad", 40, T0), work("main.good", 30, T0));
        compactor.failing.put("main.bad", "Out of Memory Error: could not allocate");
        for (int pass = 1; pass <= TierLoop.FAILURES_BEFORE_BACKOFF; pass++) {
            loop.runPass();
        }
        assertEquals(3, compactor.calledTables().stream().filter("main.bad"::equals).count(),
                "retried on each pass, with the same cap");
        assertEquals(List.of("main.bad"), loop.status().backoffs());
        assertEquals(1, registry.get("ducklake.compaction.table.backoff").tag("table", "main.bad").gauge().value());

        compactor.calls.clear();
        clock.advance(TierLoop.BACKOFF.minusMinutes(1));
        loop.runPass();
        assertEquals(List.of("main.good"), compactor.calledTables(), "backed off: the other table still runs");

        compactor.failing.clear();
        compactor.calls.clear();
        clock.advance(Duration.ofMinutes(2));
        loop.runPass();
        assertEquals(List.of("main.bad", "main.good"), compactor.calledTables(), "the back-off has ended");
        assertEquals(List.of(), loop.status().backoffs(), "a success clears it");
    }

    @Test
    void aFailureCostsOneTablesCallAndNothingElse() {
        compactor.work = List.of(work("main.a", 40, T0), work("main.b", 30, T0), work("main.c", 20, T0));
        compactor.failing.put("main.b", "Binder Error: no such column");
        TierLoop.PassResult result = loop.runPass();
        assertEquals(List.of("main.a", "main.b", "main.c"), compactor.calledTables());
        assertEquals(2, result.merged());
        assertEquals(1, result.failed());
        assertEquals(1.0, registry.get("ducklake.compaction.failures_by_class").tag("class", "PLANNING")
                .functionCounter().count());
    }

    @Test
    void aLostConnectionEndsThePassAndTheNextOneReconnects() {
        compactor.work = List.of(work("main.a", 40, T0), work("main.b", 30, T0), work("main.c", 20, T0));
        compactor.failing.put("main.a", "IO Error: connection refused");
        TierLoop.PassResult result = loop.runPass();
        assertEquals(List.of("main.a"), compactor.calledTables());
        assertEquals(2, result.carriedOver());
        assertEquals(1, compactor.resets);
        assertEquals(1, compactor.prepares);

        compactor.failing.clear();
        compactor.calls.clear();
        loop.runPass();
        assertEquals(2, compactor.prepares, "the next pass opens a new connection");
        assertEquals(List.of("main.b", "main.c", "main.a"), compactor.calledTables());
    }

    @Test
    void anInternalErrorReopensTheInstance() {
        compactor.work = List.of(work("main.a", 40, T0), work("main.b", 30, T0));
        compactor.failing.put("main.a", "INTERNAL Error: Attempted to access index 3 within vector of size 3");
        TierLoop.PassResult result = loop.runPass();
        assertEquals(List.of("main.a"), compactor.calledTables(), "the rest wait for a new instance");
        assertEquals(1, compactor.resets);
        assertEquals(1, result.carriedOver());
        assertTrue(TierLoop.invalidatesInstance(new RuntimeException("wrapped",
                new RuntimeException("FATAL Error: database has been invalidated"))));
        assertFalse(TierLoop.invalidatesInstance(new RuntimeException("Binder Error: no such column")));
    }

    @Test
    void aFailingTableThatIsNoLongerListedIsForgotten() {
        compactor.work = List.of(work("main.dropped", 40, T0));
        compactor.failing.put("main.dropped", "Catalog Error: Table with name dropped does not exist");
        loop.runPass();
        loop.runPass();
        compactor.work = List.of();
        loop.runPass();
        compactor.work = List.of(work("main.dropped", 40, T0)); // recreated under the same name
        loop.runPass();
        assertEquals(List.of(), loop.status().backoffs(),
                "the failures before it left the list do not count towards a back-off");
    }

    @Test
    void aFailedRefreshResetsTheConnectionAndCountsAsAFailure() {
        compactor.discoverFailure = new IllegalStateException("IO Error: could not connect to server");
        TierLoop.PassResult result = loop.runPass();
        assertEquals(-1, result.listed());
        assertFalse(result.progressed(), "the loop sleeps before retrying");
        assertEquals(1, compactor.resets);
        assertEquals(1, state.getFailureCount(DB));
        assertEquals("STARTING", loop.status().state());
        assertNull(state.getLastPass(DB, "minor"), "a failed refresh is not a completed pass");

        compactor.discoverFailure = null;
        loop.runPass();
        assertEquals(2, compactor.prepares);
        assertEquals("RUNNING", loop.status().state());
    }

    @Test
    void aRefusedTierStopsForGood() {
        compactor.readiness = new TierCompactor.Readiness("one full group does not fit", 60_000, 1);
        TierLoop.PassResult result = loop.runPass();
        assertTrue(result.refused());
        assertEquals("REFUSED", loop.status().state());
        assertEquals("one full group does not fit", loop.status().refused());
        assertTrue(compactor.calls.isEmpty());
    }

    @Test
    void theCatalogTimeoutSizesTheCall() {
        // 5 KB files: 3,200 per full group, 144 s each sorted. 80% of a 5-minute timeout fits one.
        compactor.readiness = TierCompactor.Readiness.ready(Duration.ofMinutes(5).toMillis(), 2);
        compactor.work = List.of(table("main.backlog", 98_000, 98_000L * 5_000, T0, 3, true));
        loop.runPass();
        assertEquals(1, compactor.calls.get(0).cap());
        assertEquals(List.of("main.backlog"), loop.status().starved(), "3 busy partitions want 4 slots");
        assertEquals(1.0, registry.get("ducklake.compaction.table.starved").tag("table", "main.backlog").gauge().value());
    }

    @Test
    void aTableWaitingOverThirtyMinutesIsReportedStale() {
        compactor.work = List.of(work("main.slow", 40, T0.minus(Duration.ofMinutes(31))), work("main.fresh", 30, T0));
        loop.runPass();
        assertEquals(List.of("main.slow"), loop.status().stale());
        assertEquals(31 * 60, registry.get("ducklake.compaction.table.oldest_wait").tag("table", "main.slow")
                .gauge().value());
    }

    @Test
    void aTableNoLongerListedReportsNothingWaiting() {
        compactor.work = List.of(work("main.a", 40, T0));
        loop.runPass();
        assertEquals(40, registry.get("ducklake.compaction.table.files_waiting").tag("table", "main.a").gauge().value());
        compactor.work = List.of();
        loop.runPass();
        assertEquals(0, registry.get("ducklake.compaction.table.files_waiting").tag("table", "main.a").gauge().value());
        assertEquals(0, loop.status().queue());
    }

    @Test
    void eachCallIsRecordedWithItsTableAndTheGapSinceThePreviousCall() {
        compactor.work = List.of(work("main.a", 40, T0), work("main.b", 30, T0));
        compactor.duringCall = t -> clock.advance(Duration.ofSeconds(3));
        loop.runPass();
        List<CompactionRun> runs = runLog.recent(new CompactionRunLog.Key(DB, "minor"));
        assertEquals(List.of("main.a", "main.b"), runs.stream().map(CompactionRun::tableName).toList());
        assertEquals(-1, runs.get(0).actualGapMs(), "the tier's first call");
        assertEquals(0, runs.get(1).actualGapMs(), "the second call starts as the first ends");
        assertEquals(3_000, runs.get(0).durationTotalMs());
    }

    @Test
    void anUnsortedTableWhoseFullGroupCannotBeatTheTimeoutRunsWithALowerTarget() {
        // 3,200 unsorted 5 KB files take 288 s; 80% of 180 s is 144 s, so 1,600 files fit in a group
        compactor.readiness = TierCompactor.Readiness.ready(Duration.ofSeconds(180).toMillis(), 2);
        compactor.work = List.of(table("main.unsorted", 98_000, 98_000L * 5_000, T0, 0, false),
                table("main.sorted", 98_000, 98_000L * 5_000, T0, 0, true));
        loop.runPass();
        assertEquals(List.of("main.unsorted", "main.sorted"), compactor.calledTables(), "both make progress");
        assertEquals(1_600L * 5_000, compactor.calls.get(0).target(), "the files that fit x the average file");
        assertEquals(1_600L * 5_000 / 2, compactor.calls.get(0).maxFileSize(),
                "earlier lowered outputs, at half the lowered target or more, are left alone");
        assertEquals(16_000_000L, compactor.calls.get(1).target(), "a sorted group fits at the tier's own target");
        assertEquals(8_000_000L, compactor.calls.get(1).maxFileSize(), "the band's own maximum");
        assertEquals(List.of(), loop.status().oversized());
        assertEquals(List.of("main.unsorted"), loop.status().reduced(), "reported in /health");

        compactor.work = List.of(table("main.unsorted", 30, 30L * 2_000_000, T0, 0, false)); // now 2 MB files
        loop.runPass();
        assertEquals(List.of(), loop.status().reduced(), "a full group fits again");
    }

    @Test
    void aTableOfWhichNotTwoFilesFitIsSkippedAndReported() {
        // 0.85 MiB per unsorted file against 80% of a 1 MiB memory_limit: not even one file
        compactor.readiness = TierCompactor.Readiness.ready(0, 2, 1L << 20);
        compactor.work = List.of(table("main.unsorted", 98_000, 98_000L * 5_000, T0, 0, false));
        TierLoop.PassResult result = loop.runPass();
        assertTrue(compactor.calls.isEmpty(), "never handed to DuckLake to fail");
        assertEquals(0, result.failed());
        assertEquals(List.of("main.unsorted"), loop.status().oversized());
        assertEquals(List.of(), loop.status().backoffs());
    }

    @Test
    void aCallTheCatalogTimeoutKilledIsRetriedWithHalfTheCapAndNeverAsLargeAgain() {
        compactor.work = List.of(table("main.slow", 500, 500L * 1_000_000, T0, 0, true)); // 16 files a group
        compactor.failing.put("main.slow", "Failed to execute query \"ROLLBACK\": server closed the connection");
        loop.runPass();
        loop.runPass();
        assertEquals(List.of(12, 6), compactor.calls.stream().map(FakeTierCompactor.Call::cap).toList(),
                "the static cap, then half of it");

        compactor.failing.clear();
        loop.runPass();
        loop.runPass();
        loop.runPass();
        assertEquals(List.of(12, 6, 3, 5, 5), compactor.calls.stream().map(FakeTierCompactor.Call::cap).toList(),
                "commits double the limit, but only up to below the smallest cap the timeout killed (6)");

        compactor.work = List.of(); // caught up: the table leaves the list, and its limit with it
        loop.runPass();
        compactor.work = List.of(table("main.slow", 500, 500L * 1_000_000, T0, 0, true));
        loop.runPass();
        assertEquals(12, compactor.calls.get(compactor.calls.size() - 1).cap());
    }

    @Test
    void aTimeoutAtACapOfOneHalvesTheFilesPerGroup() {
        // One group of 3,200 tiny files already outruns the timeout: only a smaller group can commit
        compactor.work = List.of(table("main.tiny", 98_000, 98_000L * 5_000, T0, 0, true));
        compactor.failing.put("main.tiny", "Failed to execute query \"ROLLBACK\": server closed the connection");
        loop.runPass();
        loop.runPass();
        compactor.failing.clear();
        loop.runPass();
        // 1 group of 3,200 is killed: groups of 1,600, of which 3 fit in 6,000 files. 3 groups are
        // killed: 1 group of 1,600, which commits.
        assertEquals(List.of(16_000_000L, 1_600L * 5_000, 1_600L * 5_000),
                compactor.calls.stream().map(FakeTierCompactor.Call::target).toList());
        assertEquals(List.of(1, 3, 1), compactor.calls.stream().map(FakeTierCompactor.Call::cap).toList());
        assertEquals(List.of(), loop.status().backoffs(), "it commits before a third failure");
    }

    @Test
    void limitsLearntFromATimeoutExpireAnHourAfterTheLastOne() {
        // Review on #542: a busy table never leaves discovery, so the limits must expire on their own
        compactor.work = List.of(table("main.hot", 500, 500L * 1_000_000, T0, 0, true));
        compactor.failing.put("main.hot", "Failed to execute query \"ROLLBACK\": server closed the connection");
        loop.runPass(); // 12 killed
        compactor.failing.clear();
        loop.runPass(); // 6
        clock.advance(Duration.ofMinutes(59));
        loop.runPass(); // still limited: 11, below the ceiling of 12
        clock.advance(Duration.ofMinutes(2));
        loop.runPass(); // over an hour since the timeout: the full cap again
        assertEquals(List.of(12, 6, 11, 12), compactor.calls.stream().map(FakeTierCompactor.Call::cap).toList());
    }

    @Test
    void aConflictDoesNotChangeTheCap() {
        compactor.work = List.of(table("main.busy", 500, 500L * 1_000_000, T0, 0, true));
        compactor.failing.put("main.busy", "TransactionContext Error: Transaction conflict");
        loop.runPass();
        loop.runPass();
        assertEquals(List.of(12, 12), compactor.calls.stream().map(FakeTierCompactor.Call::cap).toList());
    }

    @Test
    void aStoppedLoopReportsStoppedEvenWhenStoppedInItsSleep() throws Exception {
        compactor.work = List.of();
        TierLoop running = new TierLoop(DB, MINOR.withIdleSleep(Duration.ofMinutes(10)), compactor, leases, state,
                runLog, java.time.Clock.systemUTC());
        running.start();
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (running.status().passes() == 0) {
            assertTrue(System.nanoTime() < deadline);
            Thread.sleep(5);
        }
        running.stop();
        assertTrue(running.join(Duration.ofSeconds(5)));
        assertEquals("STOPPED", running.status().state());
    }

    @Test
    void stopEndsTheLoopAfterItsRunningCall() throws Exception {
        compactor.work = List.of();
        TierLoop running = new TierLoop(DB, MINOR.withIdleSleep(Duration.ofMinutes(10)), compactor, leases, state,
                runLog, java.time.Clock.systemUTC());
        running.start();
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (running.status().passes() == 0) {
            assertTrue(System.nanoTime() < deadline);
            Thread.sleep(5);
        }
        running.stop(); // wakes it from its idle sleep
        assertTrue(running.join(Duration.ofSeconds(5)));
    }
}
