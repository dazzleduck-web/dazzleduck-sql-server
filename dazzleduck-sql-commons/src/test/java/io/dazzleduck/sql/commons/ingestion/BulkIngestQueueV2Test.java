package io.dazzleduck.sql.commons.ingestion;


import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.MutableClock;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class BulkIngestQueueV2Test {
    public static final int DEFAULT_SMALL_BATCH_SIZE = 1024;
    private static final long DEFAULT_MIN_BATCH_SIZE = 10 * DEFAULT_SMALL_BATCH_SIZE;
    private static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(5);

    private final static String LONG_RUNNING_QUERY = "with t as " +
            "(select len(split(concat('abcdefghijklmnopqrstuvwxyz:', generate_series), ':')) as len  from generate_series(1, 1000000000) )" +
            " select count(*) from t where len = 10";

    @Test
    public void testOutOfSequenceBatch() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            var firstBatch = queue.add(mockBatch("producer1", 1, DEFAULT_SMALL_BATCH_SIZE));
            var secondBatch = queue.add(mockBatch("producer1", 2, DEFAULT_SMALL_BATCH_SIZE));

            // Try to add an out-of-sequence batch
            var outOfSequenceBatch = queue.add(mockBatch("producer1", 1, DEFAULT_SMALL_BATCH_SIZE));

            assertTrue(outOfSequenceBatch.isDone());
            assertThrows(Exception.class, outOfSequenceBatch::get);
        });
    }

    @Test
    public void testCloseWithPendingBatches() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            var batch1 = queue.add(mockBatch("producer1", 0, DEFAULT_SMALL_BATCH_SIZE));
            var batch2 = queue.add(mockBatch("producer2", 0, DEFAULT_SMALL_BATCH_SIZE));

            // Close immediately without allowing write to complete
            queue.close();

            // Both futures should be completed exceptionally
            assertTrue(batch1.isDone());
            assertTrue(batch2.isDone());
            assertThrows(Exception.class, batch1::get);
            assertThrows(Exception.class, batch2::get);
        });
    }

    @Test
    public void testCloseRejectsNewBatches() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            queue.close();

            // Trying to add a batch after close should throw
            assertThrows(IllegalStateException.class,
                () -> queue.add(mockBatch("producer1", 0, DEFAULT_SMALL_BATCH_SIZE)));
        });
    }

    @Test
    public void testStatsTracking() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            var initialStats = queue.getStats();
            assertEquals(0, initialStats.totalWriteBatches());
            assertEquals(0, initialStats.totalWriteBuckets());

            // Add batches to fill bucket
            for (int i = 0; i < 10; i++) {
                queue.add(mockBatch("producer1", i, DEFAULT_SMALL_BATCH_SIZE));
            }

            service.tick(1, TimeUnit.MILLISECONDS);
            Thread.sleep(5);

            var afterStats = queue.getStats();
            assertEquals(10, afterStats.totalWriteBatches());
            assertEquals(1, afterStats.totalWriteBuckets());
        });
    }


    @Test
    public void testExceptionHandlingInWrite() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var queue = new MockBulkIngestQueueWithException("test", DEFAULT_MIN_BATCH_SIZE,
                DEFAULT_MAX_DELAY, service, clock);

        var batch = queue.add(mockBatch("producer1", 0, DEFAULT_MIN_BATCH_SIZE + 1));

        service.tick(1, TimeUnit.MILLISECONDS);
        Thread.sleep(5);

        assertTrue(batch.isDone());
        assertThrows(Exception.class, batch::get);

        queue.close();
    }

    @Test
    public void testMultipleProducersWithSequenceTracking() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            // Add batches from multiple producers
            var p1b1 = queue.add(mockBatch("producer1", 0, DEFAULT_SMALL_BATCH_SIZE));
            var p2b1 = queue.add(mockBatch("producer2", 0, DEFAULT_SMALL_BATCH_SIZE));
            var p1b2 = queue.add(mockBatch("producer1", 1, DEFAULT_SMALL_BATCH_SIZE));
            var p2b2 = queue.add(mockBatch("producer2", 1, DEFAULT_SMALL_BATCH_SIZE));

            // Out of sequence for producer1
            var p1OutOfSeq = queue.add(mockBatch("producer1", 0, DEFAULT_SMALL_BATCH_SIZE));

            assertTrue(p1OutOfSeq.isDone());
            assertThrows(Exception.class, p1OutOfSeq::get);

            // producer2 should still accept in-sequence batches
            var p2b3 = queue.add(mockBatch("producer2", 2, DEFAULT_SMALL_BATCH_SIZE));
            assertFalse(p2b3.isDone());
        });
    }

    @Test
    public void testWriteThreadNameAndDaemonStatus() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var queue = new MockBulkIngestQueue("test-queue", DEFAULT_MIN_BATCH_SIZE,
                DEFAULT_MAX_DELAY, service, clock);

        // Give the thread a moment to start
        Thread.sleep(10);

        // Get all threads and find the write thread
        var writeThread = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().equals("BulkIngestQueue-test-queue-writer"))
                .findFirst();

        assertTrue(writeThread.isPresent());
        assertTrue(writeThread.get().isDaemon());

        queue.close();
    }

    @Test
    public void testBatchIdSequenceWithGaps() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            var batch1 = queue.add(mockBatch("producer1", 0, DEFAULT_SMALL_BATCH_SIZE));
            var batch3 = queue.add(mockBatch("producer1", 2, DEFAULT_SMALL_BATCH_SIZE));

            // Skip batch 1, try to go back
            var batch1Again = queue.add(mockBatch("producer1", 1, DEFAULT_SMALL_BATCH_SIZE));

            assertTrue(batch1Again.isDone());
            assertThrows(Exception.class, batch1Again::get);
        });
    }

    @Test
    public void testSchedulingStopsOnTermination() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            var batch = queue.add(mockBatch("producer1", 0, DEFAULT_SMALL_BATCH_SIZE));

            queue.close();

            // Advance time - no new tasks should be scheduled after termination
            clock.advanceBy(DEFAULT_MAX_DELAY.plusSeconds(100));

            // Verify queue is closed
            assertThrows(IllegalStateException.class,
                () -> queue.add(mockBatch("producer1", 1, DEFAULT_SMALL_BATCH_SIZE)));
        });
    }

    @Test
    public void testNullProducerIdHandling() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            // Batches with null producer ID should not participate in sequence tracking
            var batch1 = queue.add(mockBatch(null, 0, DEFAULT_SMALL_BATCH_SIZE));
            var batch2 = queue.add(mockBatch(null, 0, DEFAULT_SMALL_BATCH_SIZE));

            // Both should be accepted
            assertFalse(batch1.isDone());
            assertFalse(batch2.isDone());
        });
    }


    @Test
    public void testLRUCacheEviction() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            // Add batches from many different producers to test LRU eviction
            // Use 100 batches to keep test fast while still testing functionality
            for (int i = 0; i < 100; i++) {
                var batch = queue.add(mockBatch("producer" + i, 0, DEFAULT_SMALL_BATCH_SIZE));
            }

            service.tick(1, TimeUnit.MILLISECONDS);
            Thread.sleep(5);

            // All batches should be accepted (cache should handle eviction)
            var stats = queue.getStats();
            assertTrue(stats.totalWriteBatches() > 0);
        });
    }

    @Test
    public void testExceptionDoesNotCompleteAlreadyCompletedFutures() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var queue = new MockBulkIngestQueueWithPartialException("test", DEFAULT_MIN_BATCH_SIZE,
                DEFAULT_MAX_DELAY, service, clock);

        var batch = queue.add(mockBatch("producer1", 0, DEFAULT_MIN_BATCH_SIZE + 1));

        service.tick(1, TimeUnit.MILLISECONDS);
        Thread.sleep(10);

        assertTrue(batch.isDone());

        queue.close();
    }

    @Test
    @Disabled("Flaky test")
    public void testWriteQueueSize() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            var initialStats = queue.getStats();
            assertEquals(0, initialStats.scheduledWriteBuckets());

            // Add enough batches to trigger multiple writes without processing them
            for (int i = 0; i < 50; i++) {
                queue.add(mockBatch("producer1", i, DEFAULT_SMALL_BATCH_SIZE));
            }

            // Don't tick the service to process writes
            var stats = queue.getStats();
            assertTrue(stats.scheduledWriteBuckets() > 0);
        });
    }

    @Test
    public void testCloseLongRunningTask() throws Exception {
        ConnectionPool.execute("select 1");
        var queue = createMockQueueWithLongRunningDuckDBWrite(new DeterministicScheduler(),
                new MutableClock(Clock.systemUTC().instant(), Clock.systemUTC().getZone()));
        var res = queue.add(mockBatch("producer1", 0, DEFAULT_SMALL_BATCH_SIZE * 10));
        var waitTime = Duration.ofSeconds(2);
        Thread.sleep(500);
        queue.close();
        Thread.sleep(1000);
        assertTrue(res.isCompletedExceptionally());
    }
    private MockBulkIngestQueue createMockQueue(ScheduledExecutorService executorService, Clock clock) {
        return new MockBulkIngestQueue("test", DEFAULT_MIN_BATCH_SIZE, DEFAULT_MAX_DELAY,
                executorService, clock);
    }

    private BulkIngestQueue<String, MockWriteResult> createMockQueueWithLongRunningDuckDBWrite(ScheduledExecutorService executorService, Clock clock) {

        return new BulkIngestQueue<String, MockWriteResult>("test", DEFAULT_MIN_BATCH_SIZE, DEFAULT_MAX_DELAY,
                executorService, clock) {
            @Override
            public void write(WriteTask<String, MockWriteResult> writeTask) {
                try {
                    cancellableWrite(writeTask);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    record TestCancellableHook(Statement statement)  implements Runnable {
        @Override
        public void run() {
            try {
                statement.cancel();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    private static void cancellableWrite(WriteTask<String, MockWriteResult> writeTask) throws SQLException {
        ConnectionPool.execute("select 1");
           var connection = ConnectionPool.getConnection();
            var statement = connection.createStatement();
            var cancelHook = writeTask.setCancelHook(new TestCancellableHook(statement));
            if (cancelHook) {
                try {
                    statement.execute(LONG_RUNNING_QUERY);
                    var rs = statement.getResultSet();
                    while (rs.next()) {
                        // do nothing
                    }
                }  catch (Exception e){
                    writeTask.bucket().futures().forEach(f -> f.completeExceptionally(e));
                }
            }

    }



    private void withServiceAndQueue(ServiceAndQueue serviceAndQueue) throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var queue = createMockQueue(service, clock);
        try {
            serviceAndQueue.apply(service, queue, clock);
        } finally {
            queue.close();
        }
    }

    private Batch<String> mockBatch(String producerId, long producerBatchId, long totalSize) {
        return new Batch<String>(new String[0], new String[0], new String[0], "",
                producerId, producerBatchId, totalSize, "parquet", Instant.now());
    }

    interface ServiceAndQueue {
        void apply(DeterministicScheduler service, MockBulkIngestQueue queue, MutableClock clock) throws Exception;
    }

    // Helper class to test exception handling
    static class MockBulkIngestQueueWithException extends BulkIngestQueue<String, MockWriteResult> {
        public MockBulkIngestQueueWithException(String identifier,
                                               long minBatchSize,
                                               Duration maxDelay,
                                               ScheduledExecutorService executorService,
                                               Clock clock) {
            super(identifier, minBatchSize, maxDelay, executorService, clock);
        }

        @Override
        public void write(WriteTask<String, MockWriteResult> writeTask) {
            throw new RuntimeException("Test exception");
        }
    }

    // Helper class to test partial exception handling (future already completed)
    static class MockBulkIngestQueueWithPartialException extends BulkIngestQueue<String, MockWriteResult> {
        public MockBulkIngestQueueWithPartialException(String identifier,
                                                       long minBatchSize,
                                                       Duration maxDelay,
                                                       ScheduledExecutorService executorService,
                                                       Clock clock) {
            super(identifier, minBatchSize, maxDelay, executorService, clock);
        }

        @Override
        public void write(WriteTask<String, MockWriteResult> writeTask) {
            // Complete the first future, then throw
            var futures = writeTask.bucket().futures();
            if (!futures.isEmpty()) {
                futures.get(0).complete(new MockWriteResult(writeTask.taskId(), writeTask.size()));
            }
            throw new RuntimeException("Test exception after partial completion");
        }
    }
}
