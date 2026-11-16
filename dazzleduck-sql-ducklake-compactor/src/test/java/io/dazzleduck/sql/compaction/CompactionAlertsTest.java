package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** The trend alerts of DYNAMIC_COMPACTION_SPEC.md, "Telemetry and alerts". */
class CompactionAlertsTest {

    private static final String DB = "lake";
    private static final Instant T0 = Instant.parse("2026-11-16T00:00:00Z");
    private static final Duration WINDOW = CompactionService.RISING_ALERT_AFTER;

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final CompactionState state = new CompactionState(registry, List.of(DB), List.of("minor"));

    @Test
    void snapshotsGrowWhenEachOfTheLastThreeHousekeepingRunsLeftMore() {
        assertFalse(state.snapshotsGrowing(DB));
        state.updateUnexpiredSnapshots(DB, 100);
        state.updateUnexpiredSnapshots(DB, 120);
        assertFalse(state.snapshotsGrowing(DB), "two runs are not a trend");
        state.updateUnexpiredSnapshots(DB, 150);
        assertTrue(state.snapshotsGrowing(DB));
        assertEquals(150, registry.get("ducklake.snapshots.unexpired").tag("database", DB).gauge().value());
        state.updateUnexpiredSnapshots(DB, 90);
        assertFalse(state.snapshotsGrowing(DB), "expiry caught up");
    }

    @Test
    void theFileCountRisesWhenItNeverFellOverTheWholeWindow() {
        for (int minute = 0; minute <= 30; minute += 2) {
            state.updateTierFileCount(DB, "minor", 100 + minute, T0.plus(Duration.ofMinutes(minute)));
        }
        assertTrue(state.fileCountRising(DB, "minor", WINDOW, T0.plus(Duration.ofMinutes(30))));
    }

    @Test
    void aDipInTheWindowIsNotRising() {
        for (int minute = 0; minute <= 30; minute += 2) {
            state.updateTierFileCount(DB, "minor", minute == 16 ? 50 : 100 + minute, T0.plus(Duration.ofMinutes(minute)));
        }
        assertFalse(state.fileCountRising(DB, "minor", WINDOW, T0.plus(Duration.ofMinutes(30))));
    }

    @Test
    void samplesThatDoNotSpanTheWindowAreNotATrend() {
        for (int minute = 10; minute <= 30; minute += 2) {
            state.updateTierFileCount(DB, "minor", 100 + minute, T0.plus(Duration.ofMinutes(minute)));
        }
        assertFalse(state.fileCountRising(DB, "minor", WINDOW, T0.plus(Duration.ofMinutes(30))));
    }

    @Test
    void aFlatCountIsNotRising() {
        for (int minute = 0; minute <= 30; minute += 2) {
            state.updateTierFileCount(DB, "minor", 100, T0.plus(Duration.ofMinutes(minute)));
        }
        assertFalse(state.fileCountRising(DB, "minor", WINDOW, T0.plus(Duration.ofMinutes(30))));
    }

    @Test
    void oldSamplesAreDroppedButTheWindowStaysJudgeable() {
        for (int minute = 0; minute <= 120; minute += 2) {
            state.updateTierFileCount(DB, "minor", minute < 80 ? 500 : 100 + minute, T0.plus(Duration.ofMinutes(minute)));
        }
        assertTrue(state.fileCountRising(DB, "minor", WINDOW, T0.plus(Duration.ofMinutes(120))),
                "rising since minute 80, and the samples before the window do not count");
    }

    @Test
    void aTierLoopWithoutAPassForFifteenMinutesRaisesAnAlert() throws Exception {
        MutableTestClock clock = new MutableTestClock(T0);
        CompactionTier minor = CompactionTier.of("minor", 0, 8_000_000, 16_000_000, 12)
                .withIdleSleep(Duration.ofMinutes(10));
        CompactionConfig config = new CompactionConfig(List.of(DB), List.of(minor), Duration.ofHours(1),
                Duration.ofMinutes(5), List.of(), 0, 10);
        try (CompactionService service = new CompactionService(config, null, new FakeTierCompactor(),
                database -> { }, null, state, new CompactionRunLog(10), new TableLeases(), clock)) {
            assertTrue(service.getStats().alerts().isEmpty());
            service.start();
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (service.getStats().databases().get(DB).tiers().get("minor").passes() == 0) {
                assertTrue(System.nanoTime() < deadline, "the loop should complete a pass");
                Thread.sleep(5);
            }
            assertTrue(service.getStats().alerts().stream().noneMatch(a -> a.contains("pass")),
                    "a pass that found no work counts");

            clock.advance(Duration.ofMinutes(16)); // the loop sleeps in real time, so no pass completes
            List<String> alerts = service.getStats().alerts();
            assertTrue(alerts.stream().anyMatch(a -> a.contains("tier 'minor' has not completed a pass")), alerts.toString());
        }
    }
}
