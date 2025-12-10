package io.dazzleduck.sql.common.ingestion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class FlightSenderTest {

    private OnDemandSender sender;

    @AfterEach
    void cleanup() {
        sender = null;
    }

    @Test
    void testStoreStatusMemoryAndDisk() {
        sender = createSender(2 * MB, 10 * MB);

        assertEquals(FlightSender.StoreStatus.IN_MEMORY, sender.getStoreStatus((int) MB));
        assertEquals(FlightSender.StoreStatus.ON_DISK, sender.getStoreStatus((int) (7 * MB)));
        assertEquals(MB + 7 * MB, sender.getCurrentOnDiskSize() + sender.getCurrentInMemorySize());
    }

    @Test
    void testStoreStatusFull() {
        sender = createSender(MB, 5 * MB);

        assertEquals(FlightSender.StoreStatus.IN_MEMORY, sender.getStoreStatus((int) (1 * MB)));
        assertEquals(FlightSender.StoreStatus.ON_DISK, sender.getStoreStatus((int) (5 * MB)));
        assertEquals(FlightSender.StoreStatus.FULL, sender.getStoreStatus(1));
    }

    @Test
    void testEnqueueInMemory() {
        sender = createSender(10 * MB, 10 * MB);
        sender.start();
        assertDoesNotThrow(() -> sender.enqueue(new byte[1024]));
    }

    @Test
    void testEnqueueOnDisk() {
        sender = createSender(100, 10 * MB);
        sender.start();
        assertDoesNotThrow(() -> sender.enqueue(new byte[1024]));
    }

    @Test
    void testEnqueueThrowsWhenFull()  {
        sender = createSender(MB, 5 * MB);
        sender.start();
        sender.enqueue(new byte[(int) MB]);
        sender.release();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> sender.enqueue(new byte[(int) (6 * MB)]));
        assertEquals("queue is full", ex.getMessage());
    }

    @Test
    void testProcessingClearsCounters() throws Exception {
        sender = createSender(10 * MB, 10 * MB);
        sender.start();
        sender.enqueue(new byte[1024]);
        sender.enqueue(new byte[2048]);
        assertEquals(1024 + 2048, sender.getCurrentInMemorySize());
        sender.release();
        Thread.sleep(50);
        assertEquals(0, sender.getCurrentInMemorySize());
    }

    @Test
    void testQueueDrains() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger processed = new AtomicInteger();
        sender = new CountingSender(10 * MB, 10 * MB, latch, processed);
        sender.start();
        sender.enqueue(new byte[100]);
        sender.enqueue(new byte[200]);
        latch.countDown();   // allow processing
        Thread.sleep(50);
        assertEquals(2, processed.get());
    }

    @Test
    void testFileCleanupAfterProcessing() throws Exception {
        TrackingSender ts = new TrackingSender(100, 10 * MB);
        sender = ts;
        sender.start();
        sender.enqueue(new byte[1024]);
        assertEquals(1, ts.filesCreated.get());
        sender.release();
        Thread.sleep(50);
        assertEquals(1, ts.filesDeleted.get());
    }

    @Test
    void testFileCleanupOnEnqueueFailure() {
        TrackingSender ts = new TrackingSender(100, 1024);
        sender = ts;
        sender.enqueue(new byte[500]);
        assertEquals(1, ts.filesCreated.get());
        assertThrows(IllegalStateException.class, () -> sender.enqueue(new byte[555]));
        assertEquals(2, ts.filesCreated.get());
        assertEquals(1, ts.filesDeleted.get());
    }

    private final long KB = 1024;
    private final long MB = 1024 * KB;

    private OnDemandSender createSender(long mem, long disk) {
        return new OnDemandSender(mem, disk, new CountDownLatch(1));
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

    public long getInMemorySizeValue() {
        return super.getCurrentInMemorySize();
    }

    public long getOnDiskSizeValue() {
        return super.getCurrentOnDiskSize();
    }

        public void release() {
            latch.countDown();
        }
    }

    class CountingSender extends OnDemandSender {
        private final AtomicInteger processed;

        public CountingSender(long mem, long disk, CountDownLatch latch, AtomicInteger processed) {
            super(mem, disk, latch);
            this.processed = processed;
        }

        @Override
        protected void doSend(SendElement element) throws InterruptedException {
            super.doSend(element);
            processed.incrementAndGet();
        }
    }

    class TrackingSender extends OnDemandSender {
        public final AtomicInteger filesCreated = new AtomicInteger();
        public final AtomicInteger filesDeleted = new AtomicInteger();

        public TrackingSender(long mem, long disk) {
            super(mem, disk, new CountDownLatch(1));
        }

        @Override
        public void enqueue(byte[] input) {
            filesCreated.incrementAndGet();
            try {
                super.enqueue(input);
            } catch (Exception e) {
                filesDeleted.incrementAndGet();
                throw e;
            }
        }

        @Override
        protected void doSend(SendElement element) throws InterruptedException {
            super.doSend(element);
            filesDeleted.incrementAndGet();
        }
    }
}