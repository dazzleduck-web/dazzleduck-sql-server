package io.dazzleduck.sql.commons.ingestion;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Load testing utility for BulkIngestQueue.
 *
 * Tests various scenarios to measure:
 * - Throughput (batches/sec, MB/sec)
 * - Latency percentiles (p50, p95, p99)
 * - Bucket combining efficiency
 * - Back pressure handling
 */
public class BulkIngestQueueLoadTest {

    /**
     * Configuration for a load test scenario.
     */
    record LoadTestConfig(
            String name,
            int numProducers,
            int batchesPerProducer,
            long batchSize,
            Duration writeLatency,
            long minBucketSize,
            long maxBucketSize,
            int maxBatches,
            long maxPendingWrite,
            Duration maxDelay,
            Duration producerDelay  // Delay between batch submissions per producer
    ) {
        static LoadTestConfig defaults() {
            return new LoadTestConfig(
                    "default",
                    4,                          // numProducers
                    1000,                       // batchesPerProducer
                    1024,                       // batchSize (1KB)
                    Duration.ofMillis(10),      // writeLatency
                    10 * 1024,                  // minBucketSize (10KB)
                    100 * 1024,                 // maxBucketSize (100KB)
                    100,                        // maxBatches
                    Long.MAX_VALUE,             // maxPendingWrite
                    Duration.ofSeconds(1),      // maxDelay
                    Duration.ZERO               // producerDelay
            );
        }

        LoadTestConfig withName(String name) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, batchSize,
                    writeLatency, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, producerDelay);
        }

        LoadTestConfig withNumProducers(int n) {
            return new LoadTestConfig(name, n, batchesPerProducer, batchSize,
                    writeLatency, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, producerDelay);
        }

        LoadTestConfig withBatchesPerProducer(int n) {
            return new LoadTestConfig(name, numProducers, n, batchSize,
                    writeLatency, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, producerDelay);
        }

        LoadTestConfig withBatchSize(long size) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, size,
                    writeLatency, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, producerDelay);
        }

        LoadTestConfig withWriteLatency(Duration latency) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, batchSize,
                    latency, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, producerDelay);
        }

        LoadTestConfig withMinBucketSize(long size) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, batchSize,
                    writeLatency, size, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, producerDelay);
        }

        LoadTestConfig withMaxBucketSize(long size) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, batchSize,
                    writeLatency, minBucketSize, size, maxBatches, maxPendingWrite, maxDelay, producerDelay);
        }

        LoadTestConfig withMaxBatches(int n) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, batchSize,
                    writeLatency, minBucketSize, maxBucketSize, n, maxPendingWrite, maxDelay, producerDelay);
        }

        LoadTestConfig withMaxPendingWrite(long size) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, batchSize,
                    writeLatency, minBucketSize, maxBucketSize, maxBatches, size, maxDelay, producerDelay);
        }

        LoadTestConfig withMaxDelay(Duration delay) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, batchSize,
                    writeLatency, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, delay, producerDelay);
        }

        LoadTestConfig withProducerDelay(Duration delay) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, batchSize,
                    writeLatency, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, delay);
        }
    }

    /**
     * Results from a load test run.
     */
    record LoadTestResult(
            String scenarioName,
            long totalBatches,
            long totalBytes,
            Duration totalDuration,
            long writeOperations,
            long bucketsCreated,
            double throughputBatchesPerSec,
            double throughputMBPerSec,
            long latencyP50Micros,
            long latencyP95Micros,
            long latencyP99Micros,
            long latencyMaxMicros,
            long backPressureEvents,
            double combiningRatio  // bucketsCreated / writeOperations
    ) {
        void print() {
            System.out.println("\n" + "=".repeat(60));
            System.out.println("Load Test Results: " + scenarioName);
            System.out.println("=".repeat(60));
            System.out.printf("Total batches:        %,d%n", totalBatches);
            System.out.printf("Total data:           %,.2f MB%n", totalBytes / (1024.0 * 1024.0));
            System.out.printf("Total duration:       %,d ms%n", totalDuration.toMillis());
            System.out.println("-".repeat(60));
            System.out.printf("Throughput:           %,.0f batches/sec%n", throughputBatchesPerSec);
            System.out.printf("Throughput:           %,.2f MB/sec%n", throughputMBPerSec);
            System.out.println("-".repeat(60));
            System.out.printf("Latency p50:          %,d µs%n", latencyP50Micros);
            System.out.printf("Latency p95:          %,d µs%n", latencyP95Micros);
            System.out.printf("Latency p99:          %,d µs%n", latencyP99Micros);
            System.out.printf("Latency max:          %,d µs%n", latencyMaxMicros);
            System.out.println("-".repeat(60));
            System.out.printf("Write operations:     %,d%n", writeOperations);
            System.out.printf("Buckets created:      %,d%n", bucketsCreated);
            System.out.printf("Combining ratio:      %.2fx%n", combiningRatio);
            System.out.printf("Back pressure events: %,d%n", backPressureEvents);
            System.out.println("=".repeat(60));
        }
    }

    /**
     * Runs a load test with the given configuration.
     */
    private LoadTestResult runLoadTest(LoadTestConfig config) throws Exception {
        return runLoadTest(config, (c, executor) -> new LoadTestBulkIngestQueue(
                c.name(),
                c.minBucketSize(),
                c.maxBucketSize(),
                c.maxBatches(),
                c.maxPendingWrite(),
                c.maxDelay(),
                c.writeLatency(),
                executor,
                Clock.systemUTC()));
    }

    /**
     * Runs a load test with the given configuration and a custom queue (e.g. a queue whose
     * write() models a specific commit-latency profile).
     */
    private LoadTestResult runLoadTest(LoadTestConfig config,
                                       BiFunction<LoadTestConfig, ScheduledExecutorService, LoadTestBulkIngestQueue> queueFactory)
            throws Exception {
        var executor = Executors.newScheduledThreadPool(2);
        var queue = queueFactory.apply(config, executor);

        var producerPool = Executors.newFixedThreadPool(config.numProducers());
        var latencies = new ConcurrentLinkedQueue<Long>();
        var backPressureCount = new LongAdder();
        var completedBatches = new LongAdder();

        var startTime = Instant.now();

        // Submit producer tasks
        var futures = new ArrayList<Future<?>>();
        for (int p = 0; p < config.numProducers(); p++) {
            final int producerId = p;
            futures.add(producerPool.submit(() -> {
                int b = 0;
                while (b < config.batchesPerProducer()) {
                    var batch = createBatch("producer-" + producerId, b, config.batchSize());
                    var submitTime = System.nanoTime();

                    var future = queue.add(batch);

                    // Check if back pressure occurred (failed future)
                    if (future.isCompletedExceptionally()) {
                        backPressureCount.increment();
                        // Wait and retry
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        continue; // Retry same batch
                    }

                    // Track completion
                    future.whenComplete((result, error) -> {
                        if (error == null) {
                            var latencyMicros = (System.nanoTime() - submitTime) / 1000;
                            latencies.add(latencyMicros);
                            completedBatches.increment();
                        } else if (error instanceof PendingWriteExceededException) {
                            backPressureCount.increment();
                        }
                    });

                    b++; // Move to next batch

                    // Optional delay between submissions
                    if (!config.producerDelay().isZero()) {
                        try {
                            Thread.sleep(config.producerDelay().toMillis());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }));
        }

        // Wait for all producers to finish submitting
        for (var future : futures) {
            future.get();
        }

        // Wait for all batches to complete (with timeout)
        long expectedBatches = (long) config.numProducers() * config.batchesPerProducer();
        var deadline = Instant.now().plusSeconds(60);
        while (completedBatches.sum() < expectedBatches && Instant.now().isBefore(deadline)) {
            Thread.sleep(10);
        }

        var endTime = Instant.now();
        var duration = Duration.between(startTime, endTime);

        // Calculate latency percentiles
        var sortedLatencies = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
        var p50 = percentile(sortedLatencies, 50);
        var p95 = percentile(sortedLatencies, 95);
        var p99 = percentile(sortedLatencies, 99);
        var max = sortedLatencies.length > 0 ? sortedLatencies[sortedLatencies.length - 1] : 0;

        // Get queue stats
        var stats = queue.getStats();
        var writeOps = queue.getWriteCount();
        var bucketsCreated = stats.totalWriteBuckets();

        // Cleanup
        queue.close();
        producerPool.shutdown();
        executor.shutdown();

        long totalBatches = completedBatches.sum();
        long totalBytes = totalBatches * config.batchSize();
        double throughputBatches = totalBatches / (duration.toMillis() / 1000.0);
        double throughputMB = (totalBytes / (1024.0 * 1024.0)) / (duration.toMillis() / 1000.0);
        double combiningRatio = writeOps > 0 ? (double) bucketsCreated / writeOps : 1.0;

        return new LoadTestResult(
                config.name(),
                totalBatches,
                totalBytes,
                duration,
                writeOps,
                bucketsCreated,
                throughputBatches,
                throughputMB,
                p50, p95, p99, max,
                backPressureCount.sum(),
                combiningRatio
        );
    }

    private long percentile(long[] sortedValues, int percentile) {
        if (sortedValues.length == 0) return 0;
        int index = (int) Math.ceil(percentile / 100.0 * sortedValues.length) - 1;
        return sortedValues[Math.max(0, Math.min(index, sortedValues.length - 1))];
    }

    private Batch<String> createBatch(String producerId, long batchId, long size) {
        return new Batch<>(
                new String[0], new String[0],
                "", producerId, batchId, size, "parquet", Instant.now()
        );
    }

    // ============================================================
    // Test Scenarios
    // ============================================================

    @Test
    public void testHighThroughputSmallBatches() throws Exception {
        var config = LoadTestConfig.defaults()
                .withName("High Throughput - Small Batches")
                .withNumProducers(8)
                .withBatchesPerProducer(500)
                .withBatchSize(1024)           // 1KB batches
                .withWriteLatency(Duration.ofMillis(5))
                .withMinBucketSize(10 * 1024); // 10KB bucket threshold

        var result = runLoadTest(config);
        result.print();

        // Verify combining is effective (ratio > 1 means buckets were combined)
        assertTrue(result.totalBatches() > 0, "Should complete batches");
        assertTrue(result.combiningRatio() >= 1.0, "Should combine buckets");
    }

    @Test
    public void testSlowWriterBackPressure() throws Exception {
        var config = LoadTestConfig.defaults()
                .withName("Slow Writer - Back Pressure")
                .withNumProducers(4)
                .withBatchesPerProducer(50)
                .withBatchSize(2 * 1024)       // 2KB batches
                .withWriteLatency(Duration.ofMillis(20))  // Moderate write latency
                .withMinBucketSize(10 * 1024)  // 10KB bucket threshold
                .withMaxPendingWrite(200 * 1024);  // 200KB limit

        var result = runLoadTest(config);
        result.print();

        assertTrue(result.totalBatches() > 0, "Should complete batches");
    }

    @Test
    public void testManyConcurrentProducers() throws Exception {
        var config = LoadTestConfig.defaults()
                .withName("Many Concurrent Producers")
                .withNumProducers(16)
                .withBatchesPerProducer(200)
                .withBatchSize(2048)           // 2KB batches
                .withWriteLatency(Duration.ofMillis(10));

        var result = runLoadTest(config);
        result.print();

        assertEquals(16 * 200, result.totalBatches(), "All batches should complete");
    }

    @Test
    public void testLargeBatches() throws Exception {
        var config = LoadTestConfig.defaults()
                .withName("Large Batches")
                .withNumProducers(4)
                .withBatchesPerProducer(50)
                .withBatchSize(100 * 1024)     // 100KB batches
                .withWriteLatency(Duration.ofMillis(20))
                .withMinBucketSize(50 * 1024)  // 50KB threshold
                .withMaxBucketSize(500 * 1024); // 500KB max

        var result = runLoadTest(config);
        result.print();

        assertTrue(result.totalBatches() > 0, "Should complete batches");
        assertTrue(result.throughputMBPerSec() > 0, "Should have positive throughput");
    }

    @Test
    public void testBucketCombiningEfficiency() throws Exception {
        // Configure to maximize bucket combining opportunity
        var config = LoadTestConfig.defaults()
                .withName("Bucket Combining Efficiency")
                .withNumProducers(4)
                .withBatchesPerProducer(250)
                .withBatchSize(1024)           // 1KB batches
                .withWriteLatency(Duration.ofMillis(20))  // Slow enough to accumulate
                .withMinBucketSize(5 * 1024)   // 5KB bucket threshold (5 batches)
                .withMaxBucketSize(50 * 1024)  // 50KB max combined size
                .withMaxBatches(50);           // Allow many batches per bucket

        var result = runLoadTest(config);
        result.print();

        // With slow writes and small bucket threshold, we should see combining
        assertTrue(result.combiningRatio() >= 1.0,
                "Expected bucket combining, ratio: " + result.combiningRatio());
    }

    @Test
    public void testZeroLatencyBaseline() throws Exception {
        // Baseline test with no write latency to measure queue overhead
        var config = LoadTestConfig.defaults()
                .withName("Zero Latency Baseline")
                .withNumProducers(4)
                .withBatchesPerProducer(1000)
                .withBatchSize(1024)
                .withWriteLatency(Duration.ZERO);

        var result = runLoadTest(config);
        result.print();

        assertEquals(4 * 1000, result.totalBatches(), "All batches should complete");
        // With zero latency, throughput should be high
        assertTrue(result.throughputBatchesPerSec() > 1000,
                "Expected high throughput with zero latency");
    }

    /**
     * Baseline for finding #7 (parallel commit lanes), modelling the DuckLake commit path as two
     * phases with distinct parallelism characteristics:
     *
     * - data phase: DuckDB COPY of the bucket to Parquet. Internally multi-threaded within one
     *   COPY, and with multiple commit lanes several COPYs could also overlap (in the real system
     *   this mostly overlaps object-store/network waits, not CPU).
     * - catalog phase: the DuckLake metadata update. Single-writer by design, so it stays
     *   serialized globally no matter how many lanes exist.
     *
     * Today the queue has one commit thread ("scheduling" is single-threaded), so the two phases
     * serialize end to end and the throughput ceiling is
     * maxBucketSize / (dataLatency + catalogLatency). Parallel lanes can only overlap the data
     * phase, so lane scaling is Amdahl-bounded at (data + catalog) / catalog — with the profile
     * below at most 4x regardless of lane count. This test measures the single-lane plateau and
     * prints both bounds; a lane implementation should be evaluated against the same profile by
     * passing a shared catalogLock to N queues/lanes.
     */
    @Test
    public void testDuckLakeCommitProfileBaseline() throws Exception {
        var dataLatency = Duration.ofMillis(150);     // COPY to Parquet (object store PUT etc.)
        var catalogLatency = Duration.ofMillis(50);   // DuckLake catalog transaction

        var config = LoadTestConfig.defaults()
                .withName("DuckLake Commit Profile (single lane)")
                .withNumProducers(8)
                .withBatchesPerProducer(100)
                .withBatchSize(8 * 1024)              // 8KB batches
                .withMinBucketSize(64 * 1024)         // flush by size, not by timer
                .withMaxBucketSize(512 * 1024)        // combining cap per commit
                .withMaxBatches(64);

        var result = runLoadTest(config, (c, executor) ->
                new DuckLakeProfileQueue(c, dataLatency, catalogLatency, new Object(), executor));
        result.print();

        double commitSeconds = (dataLatency.toMillis() + catalogLatency.toMillis()) / 1000.0;
        double ceilingMBPerSec = (config.maxBucketSize() / (1024.0 * 1024.0)) / commitSeconds;
        double amdahlBound = (dataLatency.toMillis() + catalogLatency.toMillis())
                / (double) catalogLatency.toMillis();
        double writerBusySeconds = result.writeOperations() * commitSeconds;
        double writerUtilization = writerBusySeconds / (result.totalDuration().toMillis() / 1000.0);
        System.out.printf("Single-lane ceiling:  %.2f MB/sec (maxBucketSize / commit latency)%n", ceilingMBPerSec);
        System.out.printf("Writer utilization:   %.0f%%%n", writerUtilization * 100);
        System.out.printf("Lane-scaling bound:   %.1fx (Amdahl: catalog phase stays serialized)%n", amdahlBound);

        assertEquals(8 * 100, result.totalBatches(), "all batches should complete");
        // The point of the scenario: under a DuckLake-like commit latency the single commit
        // thread is the bottleneck (busy nearly the whole run) while throughput plateaus at the
        // ceiling. Loose bounds keep this stable on slow CI machines.
        assertTrue(writerUtilization > 0.6,
                "expected a writer-bound plateau, utilization=" + writerUtilization);
        assertTrue(result.throughputMBPerSec() <= ceilingMBPerSec * 1.15,
                "throughput cannot exceed the single-lane ceiling; measured="
                        + result.throughputMBPerSec() + " MB/s, ceiling=" + ceilingMBPerSec);
    }

    /**
     * Two-phase commit queue for the DuckLake profile: a parallelizable data phase followed by a
     * globally serialized catalog phase. The catalog lock is passed in so a future lane
     * implementation can share it across N queues and measure realistic lane scaling.
     */
    static final class DuckLakeProfileQueue extends LoadTestBulkIngestQueue {
        private final Duration dataLatency;
        private final Duration catalogLatency;
        private final Object catalogLock;

        DuckLakeProfileQueue(LoadTestConfig config, Duration dataLatency, Duration catalogLatency,
                             Object catalogLock, ScheduledExecutorService executor) {
            super(config.name(), config.minBucketSize(), config.maxBucketSize(), config.maxBatches(),
                    config.maxPendingWrite(), config.maxDelay(), Duration.ZERO, executor, Clock.systemUTC());
            this.dataLatency = dataLatency;
            this.catalogLatency = catalogLatency;
            this.catalogLock = catalogLock;
        }

        @Override
        public void write(WriteTask<String, MockWriteResult> writeTask) {
            sleepQuietly(dataLatency);          // COPY: overlappable across lanes
            synchronized (catalogLock) {        // catalog update: single-writer, always serialized
                sleepQuietly(catalogLatency);
            }
            super.write(writeTask);             // writeLatency is ZERO: just count + complete futures
        }

        private static void sleepQuietly(Duration d) {
            try {
                Thread.sleep(d.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Write interrupted", e);
            }
        }
    }

    /**
     * Run all scenarios and print a summary comparison.
     * This is not a JUnit test but can be run directly.
     */
    public static void main(String[] args) throws Exception {
        var loadTest = new BulkIngestQueueLoadTest();
        var results = new ArrayList<LoadTestResult>();

        System.out.println("\n" + "=".repeat(60));
        System.out.println("BulkIngestQueue Load Test Suite");
        System.out.println("=".repeat(60));

        // Run all scenarios
        var scenarios = List.of(
                LoadTestConfig.defaults()
                        .withName("Baseline (Zero Latency)")
                        .withWriteLatency(Duration.ZERO),
                LoadTestConfig.defaults()
                        .withName("Small Batches, Fast Write")
                        .withBatchSize(1024)
                        .withWriteLatency(Duration.ofMillis(5)),
                LoadTestConfig.defaults()
                        .withName("Small Batches, Slow Write")
                        .withBatchSize(1024)
                        .withWriteLatency(Duration.ofMillis(50)),
                LoadTestConfig.defaults()
                        .withName("Large Batches")
                        .withBatchSize(100 * 1024)
                        .withWriteLatency(Duration.ofMillis(20)),
                LoadTestConfig.defaults()
                        .withName("Many Producers")
                        .withNumProducers(16)
                        .withBatchesPerProducer(250),
                LoadTestConfig.defaults()
                        .withName("Back Pressure Test")
                        .withMaxPendingWrite(200 * 1024)
                        .withWriteLatency(Duration.ofMillis(20))
        );

        for (var scenario : scenarios) {
            try {
                var result = loadTest.runLoadTest(scenario);
                results.add(result);
                result.print();
            } catch (Exception e) {
                System.err.println("Scenario failed: " + scenario.name());
                e.printStackTrace();
            }
        }

        // Print summary table
        System.out.println("\n" + "=".repeat(80));
        System.out.println("SUMMARY");
        System.out.println("=".repeat(80));
        System.out.printf("%-30s %12s %12s %10s %10s%n",
                "Scenario", "Throughput", "p99 Latency", "Combining", "BackPress");
        System.out.printf("%-30s %12s %12s %10s %10s%n",
                "", "(batch/s)", "(µs)", "Ratio", "Events");
        System.out.println("-".repeat(80));
        for (var r : results) {
            System.out.printf("%-30s %,12.0f %,12d %10.2f %,10d%n",
                    truncate(r.scenarioName(), 30),
                    r.throughputBatchesPerSec(),
                    r.latencyP99Micros(),
                    r.combiningRatio(),
                    r.backPressureEvents());
        }
        System.out.println("=".repeat(80));
    }

    private static String truncate(String s, int maxLen) {
        return s.length() <= maxLen ? s : s.substring(0, maxLen - 3) + "...";
    }
}
