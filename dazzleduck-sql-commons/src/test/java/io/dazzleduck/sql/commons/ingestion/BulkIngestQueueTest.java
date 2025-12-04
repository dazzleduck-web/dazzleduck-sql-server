package io.dazzleduck.sql.commons.ingestion;


import io.dazzleduck.sql.commons.util.MutableClock;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jmock.lib.concurrent.DeterministicScheduler;

import static org.junit.jupiter.api.Assertions.*;

public class BulkIngestQueueTest {
    public static final int DEFAULT_SMALL_BATCH_SIZE = 1024;
    private static final long DEFAULT_MIN_BATCH_SIZE = 10 * DEFAULT_SMALL_BATCH_SIZE;
    private static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(5);
    @Test
    public void testSingleLargeBatchBiggerThanBucket() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            var res = queue.addToQueue(mockBatch("123",
                    0, DEFAULT_MIN_BATCH_SIZE + 1));
            service.tick(1, TimeUnit.MILLISECONDS);
            assertTrue(res.isDone());
            assertEquals(new MockWriteResult(0, DEFAULT_MIN_BATCH_SIZE + 1), res.get());
        });
    }

    @Test
    public void testSmallBatchToFillTheBucket() throws Exception {
        var list = new ArrayList<Future<MockWriteResult>>();
        var numBatches = 10;
        withServiceAndQueue((service, queue, clock) -> {
            for (int i = 0; i < numBatches; i++) {
                var res = queue.addToQueue(mockBatch("123",
                        i, DEFAULT_SMALL_BATCH_SIZE));
                list.add(res);
            }
            service.tick(1, TimeUnit.MILLISECONDS);
            var stat = queue.getStats();
            // test schedule write
            assertEquals(0 , stat.scheduledWrite());
            for (int i = 0; i < numBatches; i++) {
                var f = list.get(i);
                assertTrue(f.isDone());
                assertEquals(new MockWriteResult(0, numBatches * DEFAULT_SMALL_BATCH_SIZE), f.get());
            }
        });
    }

    @Test
    public void testSmallBatchesToFillTheBucketAndSomeSpace() throws Exception {
        var list = new ArrayList<Future<MockWriteResult>>();
        var numBatches = 25;
        withServiceAndQueue((service, queue, clock) -> {
            for (int i = 0; i < numBatches; i++) {
                var res = queue.addToQueue(mockBatch("123",
                        i, DEFAULT_SMALL_BATCH_SIZE));
                list.add(res);
            }
            service.tick(1, TimeUnit.MILLISECONDS);
            var stat = queue.getStats();
            clock.advanceBy(DEFAULT_MAX_DELAY.plusMillis(10));
            service.tick(DEFAULT_MAX_DELAY.toMillis() + 10, TimeUnit.MILLISECONDS);
            var newStat = queue.getStats();

            // test schedule write
            assertEquals(0, stat.scheduledWrite());
            assertEquals(2, stat.totalWriteBatches());

            assertEquals(1, newStat.scheduledWrite());
            assertEquals(3, newStat.totalWriteBatches());
            for (int i = 0; i < numBatches; i++) {
                var f = list.get(i);
                assertTrue(f.isDone());
            }
        });
    }

    @Test
    public void testSingleFewBatchWithSpaceInTheBucket() throws Exception {
        var list = new ArrayList<Future<MockWriteResult>>();
        var numBatches = 5;
        withServiceAndQueue((service, queue, clock) -> {
            for (int i = 0; i < numBatches; i++) {
                var res = queue.addToQueue(mockBatch("123",
                        i, DEFAULT_SMALL_BATCH_SIZE));
                list.add(res);
            }

            //service.tick(1, TimeUnit.MILLISECONDS);
            for (int i = 0; i < numBatches; i++) {
                assertFalse(list.get(i).isDone());
            }
            clock.advanceBy(DEFAULT_MAX_DELAY.plusMillis(10));
            service.tick(DEFAULT_MAX_DELAY.toMillis() + 10, TimeUnit.MILLISECONDS);
            var stat = queue.getStats();
            // schedule write
            assertEquals(1, stat.scheduledWrite());
            for (int i = 0; i < numBatches; i++) {
                var f = list.get(i);
                assertTrue(f.isDone());
                //assertEquals(new MockWriteResult(0, numBatches * DEFAULT_SMALL_BATCH_SIZE), f.get());
            }
        });
    }

    @Test
    public void testSmallLargeBatchToFillTheBucket() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            var smallBatch = queue.addToQueue(mockBatch("123", 0, DEFAULT_SMALL_BATCH_SIZE));
            var largeBatch = queue.addToQueue(mockBatch("124", 1, 11 * DEFAULT_SMALL_BATCH_SIZE));
            service.tick(1, TimeUnit.MILLISECONDS);
            assertTrue(smallBatch.isDone());
            assertEquals(new MockWriteResult(0, 12 * DEFAULT_SMALL_BATCH_SIZE), smallBatch.get());
            assertEquals(new MockWriteResult(0, 12 * DEFAULT_SMALL_BATCH_SIZE), largeBatch.get());
        });
    }

    private MockBulkIngestQueue createMockQueue(ScheduledExecutorService executorService, Clock clock) {
        return new MockBulkIngestQueue("", DEFAULT_MIN_BATCH_SIZE, DEFAULT_MAX_DELAY,
                executorService,
                clock);
    }

    private void withServiceAndQueue(ServiceAndQueue serviceAndQueue) throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var queue = createMockQueue(service, clock);
        serviceAndQueue.apply(service, queue, clock);
    }

    private Batch<String> mockBatch(String producerId, long producerBatchId, long totalSize) {
        return new Batch<String>(new String[0], new String[0], new String[0], "", producerId, producerBatchId, totalSize, "parquet",Instant.now());
    }

    interface ServiceAndQueue {
        void apply(DeterministicScheduler service, MockBulkIngestQueue queue, MutableClock clock) throws Exception;
    }
}
