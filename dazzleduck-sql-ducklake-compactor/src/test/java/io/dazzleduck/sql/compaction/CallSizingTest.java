package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/** The cap formula of DYNAMIC_COMPACTION_SPEC.md, "Call sizing", with the spec's own numbers. */
class CallSizingTest {

    /** The spec's minor tier: [0, 8 MB), target 16 MB, static cap 12, 6,000 files per call. */
    private static final CompactionTier MINOR = CompactionTier.of("minor", 0, 8_000_000, 16_000_000, 12);
    private static final long NO_TIMEOUT = 0;

    private static TableWork work(long files, long averageBytes, int busy, boolean sorted) {
        return new TableWork("main", "t", files, files * averageBytes, Instant.EPOCH, busy, sorted);
    }

    @Test
    void inSteadyStateGroupsAreAFewFilesAndTheCapIsTheStaticCap() {
        // 2 MB files: 8 per 16 MB group
        CallSizing.Cap cap = CallSizing.of(MINOR, work(30, 2_000_000, 0, true), NO_TIMEOUT);
        assertEquals(8, cap.filesPerGroup());
        assertEquals(12, cap.cap());
        assertFalse(cap.starved());
    }

    @Test
    void aGroupIsNeverLargerThanTheFilesWaiting() {
        CallSizing.Cap cap = CallSizing.of(MINOR, work(3, 1_000, 0, true), NO_TIMEOUT);
        assertEquals(3, cap.filesPerGroup());
        assertEquals(12, cap.cap());
    }

    @Test
    void aBacklogOfTinyFilesIsLimitedByFilesPerCall() {
        // 5 KB files: 3,200 per group, so 6,000 files per call allow 1 full group
        CallSizing.Cap cap = CallSizing.of(MINOR, work(98_000, 5_000, 0, true), NO_TIMEOUT);
        assertEquals(3_200, cap.filesPerGroup());
        assertEquals(1, cap.cap());
        assertFalse(cap.starved());
    }

    @Test
    void busyPartitionsRaiseTheCapPastTheFileLimitWithinTheTimeCap() {
        // file cap 1, but 2 busy partitions want 3 slots; the time cap of a sorted table is
        // floor(600 s / (3,200 x 0.045 s)) = 4, so 3 fits
        CallSizing.Cap cap = CallSizing.of(MINOR, work(98_000, 5_000, 2, true), NO_TIMEOUT);
        assertEquals(3, cap.cap());
        assertFalse(cap.starved());
    }

    @Test
    void theTimeCapBoundsTheBusyPartitionFloorAndFlagsStarvation() {
        // 5 busy partitions want 6 slots; 10 minutes fit 4 full groups sorted, 2 unsorted
        CallSizing.Cap sorted = CallSizing.of(MINOR, work(98_000, 5_000, 5, true), NO_TIMEOUT);
        assertEquals(4, sorted.cap());
        assertTrue(sorted.starved());
        CallSizing.Cap unsorted = CallSizing.of(MINOR, work(98_000, 5_000, 5, false), NO_TIMEOUT);
        assertEquals(2, unsorted.cap());
        assertTrue(unsorted.starved());
    }

    @Test
    void theStaticCapBoundsTheBusyPartitionFloorToo() {
        CallSizing.Cap cap = CallSizing.of(MINOR, work(500, 1_000_000, 20, true), NO_TIMEOUT);
        assertEquals(12, cap.cap());
        assertTrue(cap.starved());
    }

    @Test
    void aShortCatalogTimeoutLowersTheTimeLimit() {
        // 80% of a 5-minute timeout is 240 s: one 3,200-file group (144 s) fits, two do not
        CallSizing.Cap cap = CallSizing.of(MINOR, work(98_000, 5_000, 3, true), Duration.ofMinutes(5).toMillis());
        assertEquals(1, cap.cap());
        assertTrue(cap.starved());
        assertEquals(240.0, CallSizing.timeLimitSeconds(MINOR, Duration.ofMinutes(5).toMillis()), 1e-9);
    }

    @Test
    void aLongCatalogTimeoutDoesNotRaiseTheCallTimeLimit() {
        assertEquals(600.0, CallSizing.timeLimitSeconds(MINOR, Duration.ofHours(1).toMillis()), 1e-9);
        assertEquals(600.0, CallSizing.timeLimitSeconds(MINOR, NO_TIMEOUT), 1e-9, "no timeout: CALL_TIME_LIMIT alone");
    }

    @Test
    void theCapIsAtLeastOneEvenWhenOneGroupOutrunsTheTimeLimit() {
        CompactionTier tight = MINOR.withCallTimeLimit(Duration.ofSeconds(10));
        assertEquals(1, CallSizing.of(tight, work(98_000, 5_000, 0, true), NO_TIMEOUT).cap());
    }

    @Test
    void anUnsortedTableWhoseOneGroupOutrunsTheTimeoutDoesNotFit() {
        // Review on #542: 180 s passes the startup check (144 s sorted) but an unsorted group takes 288 s.
        long timeout = Duration.ofSeconds(180).toMillis();
        assertTrue(CallSizing.fullGroupFits(MINOR, timeout));
        assertTrue(CallSizing.groupFits(MINOR, work(98_000, 5_000, 0, true), timeout));
        assertFalse(CallSizing.groupFits(MINOR, work(98_000, 5_000, 0, false), timeout));
        assertTrue(CallSizing.groupFits(MINOR, work(98_000, 5_000, 0, false), 0), "no timeout: it fits");
        assertTrue(CallSizing.groupFits(MINOR, work(30, 2_000_000, 0, false), timeout), "8 files of 2 MB fit");
    }

    @Test
    void theStartupCheckRefusesATimeoutTooShortForOneFullGroup() {
        // a full minor group of 5 KB files is 3,200 files, 144 s sorted: it needs a timeout of 180 s
        assertEquals(144.0, CallSizing.fullGroupSeconds(MINOR), 1e-9);
        assertTrue(CallSizing.fullGroupFits(MINOR, NO_TIMEOUT));
        assertTrue(CallSizing.fullGroupFits(MINOR, 180_000));
        assertFalse(CallSizing.fullGroupFits(MINOR, 120_000), "Postgres's old 2-minute fallback is too short");
        // major's smallest files are 8 MB, so its full group is 8 files
        CompactionTier major = CompactionTier.of("major", 8_000_000, 64_000_000, 64_000_000, 16);
        assertTrue(CallSizing.fullGroupFits(major, 1_000));
    }
}
