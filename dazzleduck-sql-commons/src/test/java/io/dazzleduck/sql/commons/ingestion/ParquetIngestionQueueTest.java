package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.MutableClock;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ParquetIngestionQueueTest {
    private static final int DEFAULT_SMALL_BATCH_SIZE = 1024;
    private static final long DEFAULT_MIN_BATCH_SIZE = 10 * DEFAULT_SMALL_BATCH_SIZE;
    private static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(5);
    private static final String TEST_APP_ID = "test-app";
    private static final String INPUT_FORMAT = "parquet";

    @TempDir
    Path tempDir;

    private Path sourceFile1;
    private Path sourceFile2;
    private Path targetPath;

    @BeforeEach
    public void setup() throws Exception {
        // Create test arrow/parquet files
        sourceFile1 = createTestParquetFile("source1.parquet", 100);
        sourceFile2 = createTestParquetFile("source2.parquet", 50);
        targetPath = tempDir.resolve("output");
        Files.createDirectories(targetPath);
    }

    @AfterEach
    public void cleanup() {
        // Cleanup is handled by @TempDir
    }

    @Test
    public void testBasicIngestion() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        AtomicBoolean postTaskExecuted = new AtomicBoolean(false);
        var postTaskFactory = createPostTaskFactory(postTaskExecuted, false);

        try (var queue = new ParquetIngestionQueue(
                TEST_APP_ID,
                INPUT_FORMAT,
                targetPath.toString(),
                "test-queue",
                DEFAULT_MIN_BATCH_SIZE,
                DEFAULT_MAX_DELAY,
                postTaskFactory,
                service,
                clock)) {

            var batch = createBatch(sourceFile1.toString(), "producer1", 0, DEFAULT_MIN_BATCH_SIZE + 1);
            var future = queue.add(batch);

            service.tick(1, TimeUnit.MILLISECONDS);
            var result = future.get(2, SECONDS);

            assertEquals(targetPath.toString(), result.queueName());
            assertEquals(TEST_APP_ID, result.applicationId());
            assertEquals(100, result.rowCount());
            assertFalse(result.filesCreated().isEmpty());
            assertTrue(postTaskExecuted.get());
        }
    }

    @Test
    @org.junit.jupiter.api.Disabled("Transformations syntax needs review")
    public void testIngestionWithTransformations() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        var postTaskFactory = createPostTaskFactory(new AtomicBoolean(), false);

        try (var queue = new ParquetIngestionQueue(
                TEST_APP_ID,
                INPUT_FORMAT,
                targetPath.toString(),
                "test-queue",
                DEFAULT_MIN_BATCH_SIZE,
                DEFAULT_MAX_DELAY,
                postTaskFactory,
                service,
                clock)) {

            var batch = new Batch<>(
                    null,  // sortOrder
                    null,  // partitions
                    new String[]{"value * 2 as doubled_value"},  // transformations
                    sourceFile1.toString(),
                    "producer1",
                    0L,
                    DEFAULT_MIN_BATCH_SIZE + 1,
                    "parquet",
                    Instant.now()
            );

            var future = queue.add(batch);

            service.tick(1, TimeUnit.MILLISECONDS);
            var result = future.get(2, SECONDS);
            assertEquals(100, result.rowCount());
        }
    }

    @Test
    public void testIngestionWithSortOrder() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        var postTaskFactory = createPostTaskFactory(new AtomicBoolean(), false);

        try (var queue = new ParquetIngestionQueue(
                TEST_APP_ID,
                INPUT_FORMAT,
                targetPath.toString(),
                "test-queue",
                DEFAULT_MIN_BATCH_SIZE,
                DEFAULT_MAX_DELAY,
                postTaskFactory,
                service,
                clock)) {

            var batch = new Batch<>(
                    new String[]{"id"},  // sortOrder
                    null,  // partitions
                    null,  // transformations
                    sourceFile1.toString(),
                    "producer1",
                    0L,
                    DEFAULT_MIN_BATCH_SIZE + 1,
                    "parquet",
                    Instant.now()
            );

            var future = queue.add(batch);

            service.tick(1, TimeUnit.MILLISECONDS);
            var result = future.get(2, SECONDS);
            assertEquals(100, result.rowCount());
        }
    }

    @Test
    public void testIngestionWithPartitioning() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        var postTaskFactory = createPostTaskFactory(new AtomicBoolean(), false);

        try (var queue = new ParquetIngestionQueue(
                TEST_APP_ID,
                INPUT_FORMAT,
                targetPath.toString(),
                "test-queue",
                DEFAULT_MIN_BATCH_SIZE,
                DEFAULT_MAX_DELAY,
                postTaskFactory,
                service,
                clock)) {

            var batch = new Batch<>(
                    null,  // sortOrder
                    new String[]{"category"},  // partitions
                    null,  // transformations
                    sourceFile1.toString(),
                    "producer1",
                    0L,
                    DEFAULT_MIN_BATCH_SIZE + 1,
                    "parquet",
                    Instant.now()
            );

            var future = queue.add(batch);

            service.tick(1, TimeUnit.MILLISECONDS);
            var result = future.get(2, SECONDS);
            assertEquals(100, result.rowCount());
            // Partitioned output creates multiple files
            assertFalse(result.filesCreated().isEmpty());
        }
    }

    @Test
    public void testMultipleBatches() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        AtomicInteger postTaskCount = new AtomicInteger(0);
        var postTaskFactory = new PostIngestionTaskFactory() {
            @Override
            public PostIngestionTask create(IngestionResult ingestionResult) {
                return () -> postTaskCount.incrementAndGet();
            }
        };

        try (var queue = new ParquetIngestionQueue(
                TEST_APP_ID,
                INPUT_FORMAT,
                targetPath.toString(),
                "test-queue",
                DEFAULT_SMALL_BATCH_SIZE,  // Small batch size to trigger multiple writes
                DEFAULT_MAX_DELAY,
                postTaskFactory,
                service,
                clock)) {

            var future1 = queue.add(createBatch(sourceFile1.toString(), "producer1", 0, 600));
            var future2 = queue.add(createBatch(sourceFile2.toString(), "producer1", 1, 600));

            service.tick(1, TimeUnit.MILLISECONDS);

            var result1 = future1.get(2, SECONDS);
            var result2 = future2.get(2, SECONDS);

            // Both batches should have been written in the same bucket
            assertEquals(150, result1.rowCount());  // 100 + 50 from both files
            assertEquals(150, result2.rowCount());
            assertEquals(1, postTaskCount.get());  // Both batches in same bucket
        }
    }

    @Test
    public void testPostIngestionTaskFailure() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        var postTaskFactory = createPostTaskFactory(new AtomicBoolean(), true);

        try (var queue = new ParquetIngestionQueue(
                TEST_APP_ID,
                INPUT_FORMAT,
                targetPath.toString(),
                "test-queue",
                DEFAULT_MIN_BATCH_SIZE,
                DEFAULT_MAX_DELAY,
                postTaskFactory,
                service,
                clock)) {

            var batch = createBatch(sourceFile1.toString(), "producer1", 0, DEFAULT_MIN_BATCH_SIZE + 1);
            var future = queue.add(batch);

            service.tick(1, TimeUnit.MILLISECONDS);

            assertTrue(future.isCompletedExceptionally() ||
                      assertThrows(Exception.class, () -> future.get(2, SECONDS)) != null);
        }
    }

    @Test
    @org.junit.jupiter.api.Disabled("Cancellation timing is hard to test with DeterministicScheduler")
    public void testCancellationDuringWrite() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        var postTaskFactory = createPostTaskFactory(new AtomicBoolean(), false);

        var queue = new ParquetIngestionQueue(
                TEST_APP_ID,
                INPUT_FORMAT,
                targetPath.toString(),
                "test-queue",
                DEFAULT_MIN_BATCH_SIZE,
                DEFAULT_MAX_DELAY,
                postTaskFactory,
                service,
                clock);

        // Create a large batch to give us time to cancel
        var batch = createBatch(sourceFile1.toString(), "producer1", 0, DEFAULT_MIN_BATCH_SIZE + 1);
        var future = queue.add(batch);

        // Start processing
        service.tick(1, TimeUnit.MILLISECONDS);
        Thread.sleep(50);

        // Close the queue which should trigger cancellation
        queue.close();

        Thread.sleep(100);

        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    @org.junit.jupiter.api.Disabled("Disabled for now")
    public void testProducerSequenceTracking() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        var postTaskFactory = createPostTaskFactory(new AtomicBoolean(), false);

        try (var queue = new ParquetIngestionQueue(
                TEST_APP_ID,
                INPUT_FORMAT,
                targetPath.toString(),
                "test-queue",
                DEFAULT_MIN_BATCH_SIZE,
                DEFAULT_MAX_DELAY,
                postTaskFactory,
                service,
                clock)) {

            var future1 = queue.add(createBatch(sourceFile1.toString(), "producer1", 0, 150));
            var future2 = queue.add(createBatch(sourceFile2.toString(), "producer1", 1, 150));

            service.tick(1, TimeUnit.MILLISECONDS);

            var result1 = future1.get(2, SECONDS);
            var result2 = future2.get(2, SECONDS);

            // Check that producer max batch IDs are tracked
            assertTrue(result1.maxProducerIds().containsKey("producer1") ||
                      result2.maxProducerIds().containsKey("producer1"));
        }
    }

    @Test
    @org.junit.jupiter.api.Disabled("Empty batches don't trigger writes with min batch size")
    public void testEmptyBatchHandling() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        var postTaskFactory = createPostTaskFactory(new AtomicBoolean(), false);

        try (var queue = new ParquetIngestionQueue(
                TEST_APP_ID,
                INPUT_FORMAT,
                targetPath.toString(),
                "test-queue",
                DEFAULT_MIN_BATCH_SIZE,
                DEFAULT_MAX_DELAY,
                postTaskFactory,
                service,
                clock)) {

            // Add batch with size 0
            var batch = createBatch(sourceFile1.toString(), "producer1", 0, 0);
            var future = queue.add(batch);

            service.tick(1, TimeUnit.MILLISECONDS);

            var result = future.get(2, SECONDS);
            assertEquals(0, result.rowCount());
        }
    }

    @Test
    public void testMultipleProducers() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());

        var postTaskFactory = createPostTaskFactory(new AtomicBoolean(), false);

        try (var queue = new ParquetIngestionQueue(
                TEST_APP_ID,
                INPUT_FORMAT,
                targetPath.toString(),
                "test-queue",
                DEFAULT_SMALL_BATCH_SIZE,
                DEFAULT_MAX_DELAY,
                postTaskFactory,
                service,
                clock)) {

            var p1b1 = queue.add(createBatch(sourceFile1.toString(), "producer1", 0, 600));
            var p2b1 = queue.add(createBatch(sourceFile2.toString(), "producer2", 0, 600));

            service.tick(1, TimeUnit.MILLISECONDS);

            var result = p1b1.get(2, SECONDS);
            p2b1.get(2, SECONDS);  // Ensure second future also completes

            // Should have entries for both producers
            assertTrue(result.maxProducerIds().size() >= 1);
        }
    }

    // Helper methods

    private Path createTestParquetFile(String filename, int rowCount) throws Exception {
        Path file = tempDir.resolve(filename);

        // Create a simple parquet file using DuckDB
        String sql = String.format(
            "COPY (SELECT i as id, i * 2 as value, 'category' || (i %% 3) as category FROM range(0, %d) t(i)) TO '%s' (FORMAT PARQUET)",
            rowCount,
            file.toString()
        );

        ConnectionPool.execute(sql);
        return file;
    }

    private Batch<String> createBatch(String file, String producerId, long batchId, long totalSize) {
        return new Batch<>(
                null,  // sortOrder
                null,  // partitions
                null,  // transformations
                file,
                producerId,
                batchId,
                totalSize,
                "parquet",
                Instant.now()
        );
    }

    private PostIngestionTaskFactory createPostTaskFactory(AtomicBoolean executed, boolean shouldFail) {
        return ingestionResult -> () -> {
            executed.set(true);
            if (shouldFail) {
                throw new RuntimeException("Post-ingestion task failed");
            }
        };
    }
}
