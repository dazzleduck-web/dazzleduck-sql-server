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
    void anUnsortedTableWhoseFullGroupOutrunsTheTimeoutGetsALowerTarget() {
        // Review on #542: 180 s passes the startup check (144 s sorted) but an unsorted group takes 288 s.
        long timeout = Duration.ofSeconds(180).toMillis();
        assertTrue(CallSizing.fullGroupFits(MINOR, timeout));
        CallSizing.Plan sorted = CallSizing.plan(MINOR, work(98_000, 5_000, 0, true), timeout, -1);
        assertFalse(sorted.reduced());
        assertEquals(16_000_000L, sorted.target());

        CallSizing.Plan unsorted = CallSizing.plan(MINOR, work(98_000, 5_000, 0, false), timeout, -1);
        assertTrue(unsorted.reduced());
        assertEquals(1_600L * 5_000, unsorted.target(), "144 s / 0.09 s = 1,600 files of 5 KB");
        assertEquals(1_600L * 5_000 / 2, unsorted.maxFileSize(), "half the lowered target");
        assertEquals(8_000_000L, sorted.maxFileSize(), "the band's own");
        assertEquals(1_600, unsorted.cap().filesPerGroup());
        assertEquals(1, unsorted.cap().cap(), "1,600 files at 0.09 s fill the 144 s");

        assertFalse(CallSizing.plan(MINOR, work(98_000, 5_000, 0, false), 0, -1).reduced(), "no timeout: it fits");
        assertFalse(CallSizing.plan(MINOR, work(30, 2_000_000, 0, false), timeout, -1).reduced(), "8 files of 2 MB fit");
    }

    @Test
    void theInstancesMemoryBoundsAGroupToo() {
        // A full unsorted group of 5 KB files needs 3,200 x (0.85 MiB + 4 x 5 KB), about 2.8 GiB
        long limit = 1L << 30; // 1 GiB: 80% of it holds 942 unsorted files, 5,890 sorted
        CallSizing.Plan unsorted = CallSizing.plan(MINOR, work(98_000, 5_000, 0, false), 0, limit);
        assertTrue(unsorted.reduced());
        assertEquals(942L * 5_000, unsorted.target());
        assertFalse(CallSizing.plan(MINOR, work(98_000, 5_000, 0, true), 0, limit).reduced(),
                "3,200 sorted files need about 445 MiB");
    }

    @Test
    void aGroupOfLargeFilesIsBoundedByItsBytes() {
        // Review on #542: a 2 GiB container gives major 50% x 2 GiB x 0.25 = 256 MiB, and every call on
        // 10 MB files failed with Out of Memory when only the file count bounded a group.
        long limit = 256L << 20;
        CompactionTier major = CompactionTier.of("major", 8_000_000, 64_000_000, 64_000_000, 16);
        CallSizing.Plan plan = CallSizing.plan(major, work(40, 10_000_000, 0, true), 0, limit);
        assertTrue(plan.reduced());
        assertEquals(50_000_000L, plan.target(), "5 files of 10 MB fit, so a 50 MB target instead of 64 MB");
        assertEquals(5, plan.cap().filesPerGroup());

        CompactionTier mega = CompactionTier.of("mega", 64_000_000, 256_000_000, 256_000_000, 5);
        assertNull(CallSizing.plan(mega, work(6, 70_000_000, 0, true), 0, limit), "not even two 70 MB files fit");
    }

    @Test
    void notEvenTwoFilesFitMeansNoPlan() {
        assertNull(CallSizing.plan(MINOR, work(98_000, 5_000, 0, false), 0, 1L << 20));
        assertNull(CallSizing.plan(MINOR, work(98_000, 5_000, 0, false), 100, -1), "80 ms is under one file");
    }

    @Test
    void theStartupCheckRefusesATierOnlyWhenNotEvenTwoFilesFit() {
        // Review on #542: a 120 s timeout refused minor though plan would lower its target
        assertEquals(144.0, CallSizing.fullGroupSeconds(MINOR), 1e-9);
        assertFalse(CallSizing.fullGroupFits(MINOR, 120_000), "a full group does not fit ...");
        assertNull(CallSizing.startupRefusal(MINOR, 120_000, -1), "... but the tier starts, with lowered targets");
        assertNull(CallSizing.startupRefusal(MINOR, 0, -1));
        assertNotNull(CallSizing.startupRefusal(MINOR, 100, -1), "two files take 90 ms, more than 80% of 100 ms");

        long limit = 256L << 20;
        CompactionTier major = CompactionTier.of("major", 8_000_000, 64_000_000, 64_000_000, 16);
        CompactionTier mega = CompactionTier.of("mega", 64_000_000, 256_000_000, 256_000_000, 5);
        assertNull(CallSizing.startupRefusal(major, 0, limit), "two 8 MB files need about 64 MB");
        String refused = CallSizing.startupRefusal(mega, 0, limit);
        assertNotNull(refused, "two 64 MB files need about 512 MB");
        assertTrue(refused.contains("memory_limit"), refused);
    }
}
