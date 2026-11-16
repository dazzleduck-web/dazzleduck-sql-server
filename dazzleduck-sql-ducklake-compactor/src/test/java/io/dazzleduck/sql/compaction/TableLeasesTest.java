package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class TableLeasesTest {

    private static final String KEY = TableLeases.key("Lake", "Main", "Events");

    @Test
    void keysIgnoreCase() {
        assertEquals("lake.main.events", KEY);
    }

    @Test
    void aTableRunsOneJobAtATime() {
        TableLeases leases = new TableLeases();
        assertTrue(leases.tryAcquire(KEY));
        assertFalse(leases.tryAcquire(KEY), "a second job on the same table");
        assertTrue(leases.tryAcquire(TableLeases.key("lake", "main", "other")), "other tables are free");
        leases.release(KEY);
        assertTrue(leases.tryAcquire(KEY));
    }

    @Test
    void aWaitingJobTakesTheLeaseWhenItIsReleasedAndLoopsSkipTheTableMeanwhile() throws Exception {
        TableLeases leases = new TableLeases();
        assertTrue(leases.tryAcquire(KEY)); // a merge runs
        CountDownLatch acquired = new CountDownLatch(1);
        Thread retention = new Thread(() -> {
            try {
                leases.acquire(KEY);
                acquired.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        retention.start();
        waitFor(() -> leases.hasWaiter(KEY));
        assertFalse(acquired.await(100, TimeUnit.MILLISECONDS), "retention waits for the merge");

        leases.release(KEY); // the merge ends
        assertTrue(acquired.await(5, TimeUnit.SECONDS), "retention takes the lease");
        assertTrue(leases.isBusy(KEY));
        assertFalse(leases.tryAcquire(KEY), "a loop skips the table while retention holds it");
        retention.join();
    }

    @Test
    void whileAJobWaitsTheLoopsCannotTakeTheTableEvenWhenItIsFree() throws Exception {
        // So a busy table cannot keep the lease from retention by one merge after another.
        TableLeases leases = new TableLeases();
        assertTrue(leases.tryAcquire(KEY));
        AtomicBoolean done = new AtomicBoolean();
        Thread waiter = new Thread(() -> {
            try {
                leases.acquire(KEY);
                done.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();
        waitFor(() -> leases.hasWaiter(KEY));
        // Right after the merge's release a loop asks again: the table is either still awaited or
        // already the waiter's, never the loop's.
        leases.release(KEY);
        assertFalse(leases.tryAcquire(KEY), "a loop must not take the table from a waiting job");
        waiter.join(5_000);
        assertTrue(done.get());
    }

    @Test
    void aWaitingJobCanBeInterrupted() throws Exception {
        TableLeases leases = new TableLeases();
        assertTrue(leases.tryAcquire(KEY));
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread waiter = new Thread(() -> {
            try {
                leases.acquire(KEY);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        waiter.start();
        waitFor(() -> leases.hasWaiter(KEY));
        waiter.interrupt();
        waiter.join(5_000);
        assertTrue(interrupted.get());
        assertFalse(leases.hasWaiter(KEY), "an interrupted waiter is no longer counted");
        leases.release(KEY);
        assertTrue(leases.tryAcquire(KEY));
    }

    private static void waitFor(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "timed out");
            Thread.sleep(5);
        }
    }
}
