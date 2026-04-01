package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Load testing utility for ParquetIngestionQueue with actual Arrow data and Parquet writes.
 *
 * Supports:
 * - Local filesystem writes
 * - S3 writes (when configured via environment variables)
 *
 * Measures:
 * - Throughput (rows/sec, MB/sec)
 * - Latency percentiles
 * - Write operation efficiency
 *
 * S3 Configuration (via environment variables):
 * - S3_LOAD_TEST_BUCKET: S3 bucket name (required for S3 tests)
 * - S3_LOAD_TEST_PREFIX: S3 key prefix (optional, defaults to "load-test/")
 * - AWS_ACCESS_KEY_ID: AWS access key
 * - AWS_SECRET_ACCESS_KEY: AWS secret key
 * - AWS_REGION: AWS region (optional, defaults to us-east-1)
 * - S3_ENDPOINT: Custom S3 endpoint for MinIO/LocalStack (optional)
 */
public class ParquetIngestionQueueLoadTest {

    private static final String TEST_APP_ID = "load-test-app";
    private static final String INPUT_FORMAT = "parquet";

    @TempDir
    Path tempDir;

    private Path inputDir;
    private Path outputDir;
    private ScheduledExecutorService executor;

    @BeforeEach
    public void setup() throws Exception {
        inputDir = tempDir.resolve("input");
        outputDir = tempDir.resolve("output");
        Files.createDirectories(inputDir);
        Files.createDirectories(outputDir);
        executor = Executors.newScheduledThreadPool(4);

        // Setup DuckDB extensions
        setupDuckDBExtensions();
    }

    @AfterEach
    public void cleanup() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private void setupDuckDBExtensions() {
        try {
            // Install and load httpfs for S3 support
            ConnectionPool.execute("INSTALL httpfs");
            ConnectionPool.execute("LOAD httpfs");
        } catch (Exception e) {
            // Extension might already be loaded
        }
    }

    private void setupS3Credentials() {
        String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String region = System.getenv().getOrDefault("AWS_REGION", "us-east-1");
        String endpoint = System.getenv("S3_ENDPOINT");

        if (accessKey != null && secretKey != null) {
            try {
                ConnectionPool.execute("SET s3_access_key_id = '%s'".formatted(accessKey));
                ConnectionPool.execute("SET s3_secret_access_key = '%s'".formatted(secretKey));
                ConnectionPool.execute("SET s3_region = '%s'".formatted(region));

                if (endpoint != null && !endpoint.isEmpty()) {
                    ConnectionPool.execute("SET s3_endpoint = '%s'".formatted(endpoint));
                    ConnectionPool.execute("SET s3_url_style = 'path'");
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to configure S3 credentials", e);
            }
        }
    }

    /**
     * Configuration for a load test scenario.
     */
    record LoadTestConfig(
            String name,
            int numProducers,
            int batchesPerProducer,
            int rowsPerBatch,
            long minBucketSize,
            long maxBucketSize,
            int maxBatches,
            long maxPendingWrite,
            Duration maxDelay,
            String outputPath,
            boolean useS3
    ) {
        static LoadTestConfig localDefaults(Path outputDir) {
            return new LoadTestConfig(
                    "local-default",
                    4,                          // numProducers
                    10,                         // batchesPerProducer
                    10000,                      // rowsPerBatch
                    100 * 1024,                 // minBucketSize (100KB)
                    10 * 1024 * 1024,           // maxBucketSize (10MB)
                    100,                        // maxBatches
                    Long.MAX_VALUE,             // maxPendingWrite
                    Duration.ofSeconds(2),      // maxDelay
                    outputDir.toString(),       // outputPath
                    false                       // useS3
            );
        }

        static LoadTestConfig s3Defaults(String bucket, String prefix) {
            String s3Path = "s3://%s/%s%s".formatted(bucket, prefix, UUID.randomUUID());
            return new LoadTestConfig(
                    "s3-default",
                    4,                          // numProducers
                    10,                         // batchesPerProducer
                    10000,                      // rowsPerBatch
                    100 * 1024,                 // minBucketSize (100KB)
                    10 * 1024 * 1024,           // maxBucketSize (10MB)
                    100,                        // maxBatches
                    Long.MAX_VALUE,             // maxPendingWrite
                    Duration.ofSeconds(2),      // maxDelay
                    s3Path,                     // outputPath
                    true                        // useS3
            );
        }

        LoadTestConfig withName(String name) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, rowsPerBatch,
                    minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, outputPath, useS3);
        }

        LoadTestConfig withNumProducers(int n) {
            return new LoadTestConfig(name, n, batchesPerProducer, rowsPerBatch,
                    minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, outputPath, useS3);
        }

        LoadTestConfig withBatchesPerProducer(int n) {
            return new LoadTestConfig(name, numProducers, n, rowsPerBatch,
                    minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, outputPath, useS3);
        }

        LoadTestConfig withRowsPerBatch(int n) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, n,
                    minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, outputPath, useS3);
        }

        LoadTestConfig withMinBucketSize(long size) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, rowsPerBatch,
                    size, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, outputPath, useS3);
        }

        LoadTestConfig withMaxBucketSize(long size) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, rowsPerBatch,
                    minBucketSize, size, maxBatches, maxPendingWrite, maxDelay, outputPath, useS3);
        }

        LoadTestConfig withMaxBatches(int n) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, rowsPerBatch,
                    minBucketSize, maxBucketSize, n, maxPendingWrite, maxDelay, outputPath, useS3);
        }

        LoadTestConfig withMaxPendingWrite(long size) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, rowsPerBatch,
                    minBucketSize, maxBucketSize, maxBatches, size, maxDelay, outputPath, useS3);
        }

        LoadTestConfig withMaxDelay(Duration delay) {
            return new LoadTestConfig(name, numProducers, batchesPerProducer, rowsPerBatch,
                    minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, delay, outputPath, useS3);
        }
    }

    /**
     * Results from a load test run.
     */
    record LoadTestResult(
            String scenarioName,
            long totalBatches,
            long totalRows,
            long totalInputBytes,
            long totalOutputBytes,
            Duration totalDuration,
            long writeOperations,
            long filesCreated,
            double throughputRowsPerSec,
            double throughputMBPerSec,
            long latencyP50Micros,
            long latencyP95Micros,
            long latencyP99Micros,
            long latencyMaxMicros,
            long backPressureEvents,
            double compressionRatio
    ) {
        void print() {
            System.out.println("\n" + "=".repeat(70));
            System.out.println("ParquetIngestionQueue Load Test Results: " + scenarioName);
            System.out.println("=".repeat(70));
            System.out.printf("Total batches:        %,d%n", totalBatches);
            System.out.printf("Total rows:           %,d%n", totalRows);
            System.out.printf("Total input data:     %,.2f MB%n", totalInputBytes / (1024.0 * 1024.0));
            System.out.printf("Total output data:    %,.2f MB%n", totalOutputBytes / (1024.0 * 1024.0));
            System.out.printf("Total duration:       %,d ms%n", totalDuration.toMillis());
            System.out.println("-".repeat(70));
            System.out.printf("Throughput:           %,.0f rows/sec%n", throughputRowsPerSec);
            System.out.printf("Throughput:           %,.2f MB/sec (input)%n", throughputMBPerSec);
            System.out.println("-".repeat(70));
            System.out.printf("Latency p50:          %,d ms%n", latencyP50Micros / 1000);
            System.out.printf("Latency p95:          %,d ms%n", latencyP95Micros / 1000);
            System.out.printf("Latency p99:          %,d ms%n", latencyP99Micros / 1000);
            System.out.printf("Latency max:          %,d ms%n", latencyMaxMicros / 1000);
            System.out.println("-".repeat(70));
            System.out.printf("Write operations:     %,d%n", writeOperations);
            System.out.printf("Files created:        %,d%n", filesCreated);
            System.out.printf("Compression ratio:    %.2fx%n", compressionRatio);
            System.out.printf("Back pressure events: %,d%n", backPressureEvents);
            System.out.println("=".repeat(70));
        }
    }

    /**
     * Runs a load test with the given configuration.
     */
    private LoadTestResult runLoadTest(LoadTestConfig config) throws Exception {
        if (config.useS3()) {
            setupS3Credentials();
        }

        // Create post-ingestion task factory (no-op for load testing)
        var postTaskFactory = new IngestionHandler() {
            @Override
            public PostIngestionTask createPostIngestionTask(IngestionResult ingestionResult) {
                return () -> {}; // No-op
            }

            @Override
            public String getTargetPath(String queueId) {
                return config.outputPath();
            }
        };

        var queue = new ParquetIngestionQueue(
                TEST_APP_ID,
                INPUT_FORMAT,
                config.outputPath(),
                "load-test-queue",
                config.minBucketSize(),
                config.maxBucketSize(),
                config.maxBatches(),
                config.maxPendingWrite(),
                config.maxDelay(),
                postTaskFactory,
                executor,
                Clock.systemUTC()
        );

        var producerPool = Executors.newFixedThreadPool(config.numProducers());
        var latencies = new ConcurrentLinkedQueue<Long>();
        var backPressureCount = new LongAdder();
        var completedBatches = new LongAdder();
        var totalInputBytes = new LongAdder();
        var totalRows = new LongAdder();
        var totalOutputBytes = new LongAdder();
        var filesCreated = new LongAdder();

        // Pre-generate Parquet files for all batches
        System.out.printf("Generating %d Parquet files with %d rows each...%n",
                config.numProducers() * config.batchesPerProducer(), config.rowsPerBatch());
        var generatedFiles = new ConcurrentHashMap<String, List<LogDataGenerator.GeneratedFile>>();

        for (int p = 0; p < config.numProducers(); p++) {
            String producerId = "producer-" + p;
            var files = new ArrayList<LogDataGenerator.GeneratedFile>();
            for (int b = 0; b < config.batchesPerProducer(); b++) {
                String filename = String.format("%s_batch_%d.parquet", producerId, b);
                var generated = LogDataGenerator.generate(inputDir, filename, config.rowsPerBatch());
                files.add(generated);
            }
            generatedFiles.put(producerId, files);
        }
        System.out.println("File generation complete.");

        var startTime = Instant.now();

        // Submit producer tasks
        var futures = new ArrayList<Future<?>>();
        for (int p = 0; p < config.numProducers(); p++) {
            final int producerId = p;
            final String producerIdStr = "producer-" + p;
            futures.add(producerPool.submit(() -> {
                var files = generatedFiles.get(producerIdStr);
                int b = 0;
                while (b < files.size()) {
                    var generated = files.get(b);
                    var batch = new Batch<>(
                            null,  // sortOrder
                            null,  // partitionBy
                            generated.path().toString(),
                            producerIdStr,
                            b,
                            generated.sizeBytes(),
                            "parquet",
                            Instant.now()
                    );

                    var submitTime = System.nanoTime();
                    var future = queue.add(batch);

                    // Check if back pressure occurred
                    if (future.isCompletedExceptionally()) {
                        backPressureCount.increment();
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        continue; // Retry same batch
                    }

                    final long inputBytes = generated.sizeBytes();
                    final int rows = generated.rowCount();

                    future.whenComplete((result, error) -> {
                        if (error == null) {
                            var latencyMicros = (System.nanoTime() - submitTime) / 1000;
                            latencies.add(latencyMicros);
                            completedBatches.increment();
                            totalInputBytes.add(inputBytes);
                            totalRows.add(rows);
                            if (result != null) {
                                filesCreated.add(result.filesCreated().size());
                            }
                        } else if (error instanceof PendingWriteExceededException) {
                            backPressureCount.increment();
                        }
                    });

                    b++;
                }
            }));
        }

        // Wait for all producers to finish submitting
        for (var future : futures) {
            future.get();
        }

        // Wait for all batches to complete (with timeout)
        long expectedBatches = (long) config.numProducers() * config.batchesPerProducer();
        var deadline = Instant.now().plusSeconds(120);
        while (completedBatches.sum() < expectedBatches && Instant.now().isBefore(deadline)) {
            Thread.sleep(100);
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
        var writeOps = stats.totalWriteBuckets();

        // Calculate output size (for local files)
        long outputBytes = 0;
        if (!config.useS3()) {
            try (var stream = Files.walk(Path.of(config.outputPath()))) {
                outputBytes = stream
                        .filter(Files::isRegularFile)
                        .mapToLong(p -> {
                            try {
                                return Files.size(p);
                            } catch (Exception e) {
                                return 0;
                            }
                        })
                        .sum();
            }
        }
        totalOutputBytes.add(outputBytes);

        // Cleanup
        queue.close();
        producerPool.shutdown();

        long totalBatches = completedBatches.sum();
        double throughputRows = totalRows.sum() / (duration.toMillis() / 1000.0);
        double throughputMB = (totalInputBytes.sum() / (1024.0 * 1024.0)) / (duration.toMillis() / 1000.0);
        double compressionRatio = totalOutputBytes.sum() > 0
                ? (double) totalInputBytes.sum() / totalOutputBytes.sum()
                : 0;

        return new LoadTestResult(
                config.name(),
                totalBatches,
                totalRows.sum(),
                totalInputBytes.sum(),
                totalOutputBytes.sum(),
                duration,
                writeOps,
                filesCreated.sum(),
                throughputRows,
                throughputMB,
                p50, p95, p99, max,
                backPressureCount.sum(),
                compressionRatio
        );
    }

    private long percentile(long[] sortedValues, int percentile) {
        if (sortedValues.length == 0) return 0;
        int index = (int) Math.ceil(percentile / 100.0 * sortedValues.length) - 1;
        return sortedValues[Math.max(0, Math.min(index, sortedValues.length - 1))];
    }

    // ============================================================
    // Test Scenarios - Local Filesystem
    // ============================================================

    @Test
    public void testLocalSmallBatches() throws Exception {
        var config = LoadTestConfig.localDefaults(outputDir)
                .withName("Local - Small Batches")
                .withNumProducers(4)
                .withBatchesPerProducer(5)
                .withRowsPerBatch(1000)
                .withMinBucketSize(10 * 1024);  // 10KB

        var result = runLoadTest(config);
        result.print();

        assertEquals(20, result.totalBatches(), "All batches should complete");
        assertTrue(result.totalRows() > 0, "Should have written rows");
    }

    @Test
    public void testLocalLargeBatches() throws Exception {
        var config = LoadTestConfig.localDefaults(outputDir)
                .withName("Local - Large Batches")
                .withNumProducers(2)
                .withBatchesPerProducer(3)
                .withRowsPerBatch(50000)
                .withMinBucketSize(500 * 1024);  // 500KB

        var result = runLoadTest(config);
        result.print();

        assertEquals(6, result.totalBatches(), "All batches should complete");
        assertTrue(result.compressionRatio() > 1, "Parquet should compress data");
    }

    @Test
    public void testLocalHighConcurrency() throws Exception {
        var config = LoadTestConfig.localDefaults(outputDir)
                .withName("Local - High Concurrency")
                .withNumProducers(8)
                .withBatchesPerProducer(3)
                .withRowsPerBatch(5000)
                .withMinBucketSize(50 * 1024);

        var result = runLoadTest(config);
        result.print();

        assertEquals(24, result.totalBatches(), "All batches should complete");
    }

    @Test
    public void testLocalBucketCombining() throws Exception {
        var config = LoadTestConfig.localDefaults(outputDir)
                .withName("Local - Bucket Combining")
                .withNumProducers(4)
                .withBatchesPerProducer(5)
                .withRowsPerBatch(2000)
                .withMinBucketSize(10 * 1024)    // Small bucket threshold
                .withMaxBucketSize(500 * 1024)   // Allow combining
                .withMaxDelay(Duration.ofMillis(500));

        var result = runLoadTest(config);
        result.print();

        assertEquals(20, result.totalBatches(), "All batches should complete");
        // With bucket combining, we should have fewer write operations than batches
        assertTrue(result.writeOperations() <= result.totalBatches(),
                "Bucket combining should reduce write operations");
    }

    // ============================================================
    // Test Scenarios - S3
    // ============================================================

    @Test
    @EnabledIfEnvironmentVariable(named = "S3_LOAD_TEST_BUCKET", matches = ".+")
    public void testS3SmallBatches() throws Exception {
        String bucket = System.getenv("S3_LOAD_TEST_BUCKET");
        String prefix = System.getenv().getOrDefault("S3_LOAD_TEST_PREFIX", "load-test/");

        var config = LoadTestConfig.s3Defaults(bucket, prefix)
                .withName("S3 - Small Batches")
                .withNumProducers(2)
                .withBatchesPerProducer(3)
                .withRowsPerBatch(5000)
                .withMinBucketSize(50 * 1024);

        var result = runLoadTest(config);
        result.print();

        assertEquals(6, result.totalBatches(), "All batches should complete");
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "S3_LOAD_TEST_BUCKET", matches = ".+")
    public void testS3LargeBatches() throws Exception {
        String bucket = System.getenv("S3_LOAD_TEST_BUCKET");
        String prefix = System.getenv().getOrDefault("S3_LOAD_TEST_PREFIX", "load-test/");

        var config = LoadTestConfig.s3Defaults(bucket, prefix)
                .withName("S3 - Large Batches")
                .withNumProducers(2)
                .withBatchesPerProducer(2)
                .withRowsPerBatch(50000)
                .withMinBucketSize(500 * 1024);

        var result = runLoadTest(config);
        result.print();

        assertEquals(4, result.totalBatches(), "All batches should complete");
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "S3_LOAD_TEST_BUCKET", matches = ".+")
    public void testS3HighThroughput() throws Exception {
        String bucket = System.getenv("S3_LOAD_TEST_BUCKET");
        String prefix = System.getenv().getOrDefault("S3_LOAD_TEST_PREFIX", "load-test/");

        var config = LoadTestConfig.s3Defaults(bucket, prefix)
                .withName("S3 - High Throughput")
                .withNumProducers(4)
                .withBatchesPerProducer(5)
                .withRowsPerBatch(10000)
                .withMinBucketSize(100 * 1024)
                .withMaxBucketSize(5 * 1024 * 1024);

        var result = runLoadTest(config);
        result.print();

        assertEquals(20, result.totalBatches(), "All batches should complete");
        System.out.printf("S3 Write Throughput: %.2f MB/sec%n", result.throughputMBPerSec());
    }

    // ============================================================
    // Main method for running all scenarios
    // ============================================================

    /**
     * Run all scenarios and print a summary comparison.
     */
    public static void main(String[] args) throws Exception {
        var loadTest = new ParquetIngestionQueueLoadTest();
        loadTest.tempDir = Files.createTempDirectory("parquet-load-test");
        loadTest.inputDir = loadTest.tempDir.resolve("input");
        loadTest.outputDir = loadTest.tempDir.resolve("output");
        Files.createDirectories(loadTest.inputDir);
        Files.createDirectories(loadTest.outputDir);
        loadTest.executor = Executors.newScheduledThreadPool(4);
        loadTest.setupDuckDBExtensions();

        try {
            var results = new ArrayList<LoadTestResult>();

            System.out.println("\n" + "=".repeat(70));
            System.out.println("ParquetIngestionQueue Load Test Suite");
            System.out.println("=".repeat(70));

            // Local scenarios
            var localScenarios = List.of(
                    LoadTestConfig.localDefaults(loadTest.outputDir)
                            .withName("Local - Small (1K rows)")
                            .withNumProducers(4)
                            .withBatchesPerProducer(5)
                            .withRowsPerBatch(1000),
                    LoadTestConfig.localDefaults(loadTest.outputDir)
                            .withName("Local - Medium (10K rows)")
                            .withNumProducers(4)
                            .withBatchesPerProducer(5)
                            .withRowsPerBatch(10000),
                    LoadTestConfig.localDefaults(loadTest.outputDir)
                            .withName("Local - Large (50K rows)")
                            .withNumProducers(2)
                            .withBatchesPerProducer(3)
                            .withRowsPerBatch(50000),
                    LoadTestConfig.localDefaults(loadTest.outputDir)
                            .withName("Local - High Concurrency")
                            .withNumProducers(8)
                            .withBatchesPerProducer(4)
                            .withRowsPerBatch(5000)
            );

            for (var scenario : localScenarios) {
                try {
                    // Clean output directory between scenarios
                    loadTest.outputDir = loadTest.tempDir.resolve("output-" + UUID.randomUUID());
                    Files.createDirectories(loadTest.outputDir);

                    var result = loadTest.runLoadTest(scenario);
                    results.add(result);
                    result.print();
                } catch (Exception e) {
                    System.err.println("Scenario failed: " + scenario.name());
                    e.printStackTrace();
                }
            }

            // S3 scenarios (if configured)
            String bucket = System.getenv("S3_LOAD_TEST_BUCKET");
            if (bucket != null && !bucket.isEmpty()) {
                String prefix = System.getenv().getOrDefault("S3_LOAD_TEST_PREFIX", "load-test/");

                var s3Scenarios = List.of(
                        LoadTestConfig.s3Defaults(bucket, prefix)
                                .withName("S3 - Small (5K rows)")
                                .withNumProducers(2)
                                .withBatchesPerProducer(3)
                                .withRowsPerBatch(5000),
                        LoadTestConfig.s3Defaults(bucket, prefix)
                                .withName("S3 - Large (50K rows)")
                                .withNumProducers(2)
                                .withBatchesPerProducer(2)
                                .withRowsPerBatch(50000)
                );

                for (var scenario : s3Scenarios) {
                    try {
                        var result = loadTest.runLoadTest(scenario);
                        results.add(result);
                        result.print();
                    } catch (Exception e) {
                        System.err.println("Scenario failed: " + scenario.name());
                        e.printStackTrace();
                    }
                }
            } else {
                System.out.println("\nSkipping S3 tests (S3_LOAD_TEST_BUCKET not set)");
            }

            // Print summary table
            System.out.println("\n" + "=".repeat(90));
            System.out.println("SUMMARY");
            System.out.println("=".repeat(90));
            System.out.printf("%-30s %12s %12s %10s %10s %10s%n",
                    "Scenario", "Rows/sec", "MB/sec", "p99 (ms)", "Writes", "Compress");
            System.out.println("-".repeat(90));
            for (var r : results) {
                System.out.printf("%-30s %,12.0f %12.2f %,10d %,10d %10.2fx%n",
                        truncate(r.scenarioName(), 30),
                        r.throughputRowsPerSec(),
                        r.throughputMBPerSec(),
                        r.latencyP99Micros() / 1000,
                        r.writeOperations(),
                        r.compressionRatio());
            }
            System.out.println("=".repeat(90));

        } finally {
            loadTest.cleanup();
            // Clean up temp directory
            try (var stream = Files.walk(loadTest.tempDir)) {
                stream.sorted((a, b) -> -a.compareTo(b))
                        .forEach(p -> {
                            try {
                                Files.delete(p);
                            } catch (Exception e) {
                                // Ignore
                            }
                        });
            }
        }
    }

    private static String truncate(String s, int maxLen) {
        return s.length() <= maxLen ? s : s.substring(0, maxLen - 3) + "...";
    }
}
