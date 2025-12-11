package io.dazzleduck.sql.common.ingestion;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FlightSenderTest {
    // Use  on demand Sender to test it

    @Test
    void testStoreStatusMemoryAndDisk() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        OnDemandSender sender = new OnDemandSender(
                2 * 1024 * 1024,
                10 * 1024 * 1024,
                latch
        );

        sender.start();
        assertEquals(FlightSender.StoreStatus.IN_MEMORY, sender.getStoreStatus(1024 * 1024));
        assertEquals(FlightSender.StoreStatus.ON_DISK, sender.getStoreStatus(7 * 1024 * 1024));
        latch.countDown();
    }
    @Test
    void testDiskFull() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        OnDemandSender sender = new OnDemandSender(
                1024 * 1024,
                5 * 1024 * 1024,
                latch
        );

        sender.start();
        sender.enqueue(new byte[1024 * 1024]);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                sender.enqueue(new byte[5 * 1024 * 1024])
        );
        assertEquals("queue is full", ex.getMessage());
        latch.countDown();
    }

    @Test
    void testQueueDrainsAfterLatch() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        OnDemandSender sender = new OnDemandSender(
                10 * 1024 * 1024,
                10 * 1024 * 1024,
                latch
        );
        sender.start();
        sender.enqueue(new byte[5 * 1024 * 1024]);
        sender.enqueue(new byte[5 * 1024 * 1024]);
        Thread.sleep(10);
        latch.countDown();
        Thread.sleep(10);
    }

}

class OnDemandSender extends FlightSender.AbstractFlightSender {

    private final long maxOnDiskSize;
    private final long maxInMemorySize;
    private final CountDownLatch latch;

    public OnDemandSender(long maxInMemorySize, long maxOnDiskSize, CountDownLatch latch) {
        this.maxInMemorySize = maxInMemorySize;
        this.maxOnDiskSize = maxOnDiskSize;
        this.latch = latch;
    }

    @Override
    public long getMaxInMemorySize() {
        return maxInMemorySize;
    }

    @Override
    public long getMaxOnDiskSize() {
        return maxOnDiskSize;
    }

    @Override
    protected void doSend(SendElement element) throws InterruptedException {
        latch.await();
    }
}
