package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.hive.HivePartitionPruning;
import io.dazzleduck.sql.commons.util.MutableClock;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link PartitionedIngestionQueue}: single-partition batches route to the correct child,
 * every child writes straight into the shared output path (no per-partition sub-directory), and
 * batches whose rows span more than one partition are rejected and their input file deleted.
 */
public class PartitionedIngestionQueueTest {

    private static final String TEST_APP_ID = "test-app";
    private static final String INPUT_FORMAT = "parquet";
    private static final String PARTITION_EXPRESSION = "pkey";
    private static final int NUM_PARTITIONS = 4;
    private static final long MIN_BATCH_SIZE = 1024;
    private static final Duration MAX_DELAY = Duration.ofSeconds(5);

    @TempDir
    Path tempDir;

    private Path targetPath;

    @BeforeEach
    public void setup() throws Exception {
        targetPath = tempDir.resolve("output");
        Files.createDirectories(targetPath);
    }

    /** A parquet file whose every row shares the same partition key value. */
    private Path singleKeyFile(String name, int pkey, int rows) throws Exception {
        Path file = tempDir.resolve(name);
        ConnectionPool.execute(
                "COPY (SELECT %d AS pkey, i AS id FROM range(0, %d) t(i)) TO '%s' (FORMAT PARQUET)"
                        .formatted(pkey, rows, file));
        return file;
    }

    /** A parquet file with many distinct partition key values (spans multiple partitions). */
    private Path multiKeyFile(String name, int rows) throws Exception {
        Path file = tempDir.resolve(name);
        ConnectionPool.execute(
                "COPY (SELECT i AS pkey, i AS id FROM range(0, %d) t(i)) TO '%s' (FORMAT PARQUET)"
                        .formatted(rows, file));
        return file;
    }

    private int expectedPartition(int pkey) throws Exception {
        return ConnectionPool.collectFirst(
                "SELECT (hash(%d) %% %d)::INTEGER".formatted(pkey, NUM_PARTITIONS), Integer.class);
    }

    private Batch<String> batch(Path file, String producerId, long batchId, long size) {
        return new Batch<>(null, null, file.toString(), producerId, batchId, size, "parquet", Instant.now());
    }

    private IngestionHandler noopHandler() {
        return new IngestionHandler() {
            @Override public PostIngestionTask createPostIngestionTask(IngestionResult r) { return PostIngestionTask.NOOP; }
            @Override public String getTargetPath(String queueId) { return targetPath.toString(); }
            @Override public String[] getPartitionBy(String queueId) { return new String[0]; }
            @Override public int getNumPartitions(String queueId) { return NUM_PARTITIONS; }
            @Override public String getPartitionExpression(String queueId) { return PARTITION_EXPRESSION; }
        };
    }

    private PartitionedIngestionQueue newQueue(ScheduledExecutorService scheduler, MutableClock clock) {
        return newQueue(scheduler, clock, PARTITION_EXPRESSION);
    }

    private PartitionedIngestionQueue newQueue(ScheduledExecutorService scheduler, MutableClock clock, String expression) {
        return new PartitionedIngestionQueue(
                TEST_APP_ID, INPUT_FORMAT, targetPath.toString(), "test-queue",
                MIN_BATCH_SIZE, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                MAX_DELAY, null, noopHandler(), scheduler, clock, NUM_PARTITIONS, expression);
    }

    /** True when {@code file} sits directly inside the queue's output path, with no sub-directory. */
    private boolean isDirectlyUnderOutput(String file) {
        return Path.of(file).toAbsolutePath().getParent().equals(targetPath.toAbsolutePath());
    }

    @Test
    public void singlePartitionBatchIsWrittenByItsChildIntoSharedOutputPath() throws Exception {
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        int pkey = 7;
        Path source = singleKeyFile("single.parquet", pkey, 100);

        try (var queue = newQueue(scheduler, clock)) {
            assertEquals(NUM_PARTITIONS, queue.children().size());

            var future = queue.add(batch(source, "producer1", 0, MIN_BATCH_SIZE + 1));
            scheduler.tick(1, TimeUnit.MILLISECONDS);
            var result = future.get(5, SECONDS);
            queue.drain(); // write counters are bumped after the futures complete

            assertEquals(100, result.rowCount());
            assertFalse(result.filesCreated().isEmpty());
            String out = result.filesCreated().get(0);
            assertTrue(isDirectlyUnderOutput(out), "expected output directly under " + targetPath + " but was " + out);
            assertFalse(out.matches(".*[/\\\\]p\\d+[/\\\\].*"), "no p<index> sub-directory expected: " + out);
            // Only the routed child did the write.
            int expected = expectedPartition(pkey);
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                assertEquals(i == expected ? 1 : 0, queue.children().get(i).getTotalWriteBatches(), "child " + i);
            }
        }
    }

    @Test
    public void batchesRouteToDistinctChildrenByPartition() throws Exception {
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        // Two keys that hash to different partitions.
        int keyA = 0, keyB = -1;
        for (int candidate = 1; candidate < 1000 && expectedPartition(keyB) == expectedPartition(keyA); candidate++) {
            keyB = candidate;
        }
        assertNotEquals(expectedPartition(keyA), expectedPartition(keyB), "test needs two keys in different partitions");

        Path fileA = singleKeyFile("a.parquet", keyA, 20);
        Path fileB = singleKeyFile("b.parquet", keyB, 20);

        try (var queue = newQueue(scheduler, clock)) {
            var fa = queue.add(batch(fileA, "pa", 0, MIN_BATCH_SIZE + 1));
            var fb = queue.add(batch(fileB, "pb", 0, MIN_BATCH_SIZE + 1));
            scheduler.tick(1, TimeUnit.MILLISECONDS);

            String outA = fa.get(5, SECONDS).filesCreated().get(0);
            String outB = fb.get(5, SECONDS).filesCreated().get(0);
            queue.drain(); // write counters are bumped after the futures complete

            assertTrue(isDirectlyUnderOutput(outA), outA);
            assertTrue(isDirectlyUnderOutput(outB), outB);
            assertNotEquals(outA, outB);
            assertEquals(1, queue.children().get(expectedPartition(keyA)).getTotalWriteBatches());
            assertEquals(1, queue.children().get(expectedPartition(keyB)).getTotalWriteBatches());
        }
    }

    /**
     * Regression for the reported symptom: files written through a partitioned queue must be visible
     * to the plain-directory file listing that Hive pruning uses (it globs {@code base/*.parquet}),
     * which a per-partition sub-directory used to hide.
     */
    @Test
    public void partitionedOutputIsVisibleToHivePruning() throws Exception {
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        int keyA = 0, keyB = -1;
        for (int candidate = 1; candidate < 1000 && expectedPartition(keyB) == expectedPartition(keyA); candidate++) {
            keyB = candidate;
        }
        Path fileA = singleKeyFile("ha.parquet", keyA, 20);
        Path fileB = singleKeyFile("hb.parquet", keyB, 20);

        try (var queue = newQueue(scheduler, clock)) {
            var fa = queue.add(batch(fileA, "pa", 0, MIN_BATCH_SIZE + 1));
            var fb = queue.add(batch(fileB, "pb", 0, MIN_BATCH_SIZE + 1));
            scheduler.tick(1, TimeUnit.MILLISECONDS);
            fa.get(5, SECONDS);
            fb.get(5, SECONDS);
        }

        var files = HivePartitionPruning.pruneFiles(targetPath.toString().replace('\\', '/'), "true", null);
        assertEquals(2, files.size(), "both partitions' files must be listed: " + files);
    }

    /**
     * Regression for the {@code partition_by} symptom: with Hive {@code key=value} output, pruning
     * reads partition values by directory position, so a {@code p<index>} level made it take
     * {@code p1} as the first partition value and glob one level too shallow. The two batches are
     * routed to different children and carry different days; pruning must select each day's file
     * by its real value and list both with a match-all filter.
     */
    @Test
    public void partitionByOutputIsPrunedByItsRealPartitionValues() throws Exception {
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        int keyA = 0, keyB = -1;
        for (int candidate = 1; candidate < 1000 && expectedPartition(keyB) == expectedPartition(keyA); candidate++) {
            keyB = candidate;
        }
        assertNotEquals(expectedPartition(keyA), expectedPartition(keyB), "test needs two keys in different partitions");
        Path fileA = singleKeyDayFile("da.parquet", keyA, "2026-09-01", 20);
        Path fileB = singleKeyDayFile("db.parquet", keyB, "2026-09-02", 20);

        IngestionHandler byDay = new IngestionHandler() {
            @Override public PostIngestionTask createPostIngestionTask(IngestionResult r) { return PostIngestionTask.NOOP; }
            @Override public String getTargetPath(String queueId) { return targetPath.toString(); }
            @Override public String[] getPartitionBy(String queueId) { return new String[]{"day"}; }
            @Override public int getNumPartitions(String queueId) { return NUM_PARTITIONS; }
            @Override public String getPartitionExpression(String queueId) { return PARTITION_EXPRESSION; }
        };
        try (var queue = new PartitionedIngestionQueue(
                TEST_APP_ID, INPUT_FORMAT, targetPath.toString(), "test-queue",
                MIN_BATCH_SIZE, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                MAX_DELAY, null, byDay, scheduler, clock, NUM_PARTITIONS, PARTITION_EXPRESSION)) {
            var fa = queue.add(batch(fileA, "pa", 0, MIN_BATCH_SIZE + 1));
            var fb = queue.add(batch(fileB, "pb", 0, MIN_BATCH_SIZE + 1));
            scheduler.tick(1, TimeUnit.MILLISECONDS);
            fa.get(5, SECONDS);
            fb.get(5, SECONDS);
            queue.drain(); // write counters are bumped after the futures complete
            assertEquals(1, queue.children().get(expectedPartition(keyA)).getTotalWriteBatches());
            assertEquals(1, queue.children().get(expectedPartition(keyB)).getTotalWriteBatches());
        }

        String base = targetPath.toString().replace('\\', '/');
        String[][] partitionTypes = {{"day", "date"}};
        var dayA = HivePartitionPruning.pruneFiles(base, "day = DATE '2026-09-01'", partitionTypes);
        assertEquals(1, dayA.size(), "only 2026-09-01's file: " + dayA);
        assertEquals(targetPath.resolve("day=2026-09-01").toAbsolutePath(),
                Path.of(dayA.get(0).fileName()).toAbsolutePath().getParent(),
                "file must sit directly in its day= directory, with no p<index> level");
        var dayB = HivePartitionPruning.pruneFiles(base, "day = DATE '2026-09-02'", partitionTypes);
        assertEquals(1, dayB.size(), "only 2026-09-02's file: " + dayB);
        assertEquals(2, HivePartitionPruning.pruneFiles(base, "true", partitionTypes).size(),
                "a match-all filter must list both days' files");
    }

    /** A parquet file whose rows share one partition key and one {@code day}. */
    private Path singleKeyDayFile(String name, int pkey, String day, int rows) throws Exception {
        Path file = tempDir.resolve(name);
        ConnectionPool.execute(
                "COPY (SELECT %d AS pkey, i AS id, DATE '%s' AS day FROM range(0, %d) t(i)) TO '%s' (FORMAT PARQUET)"
                        .formatted(pkey, day, rows, file));
        return file;
    }

    @Test
    public void multiPartitionBatchIsRejectedAndInputDeleted() throws Exception {
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        Path source = multiKeyFile("multi.parquet", 100);

        try (var queue = newQueue(scheduler, clock)) {
            var future = queue.add(batch(source, "producer1", 0, MIN_BATCH_SIZE + 1));
            // Rejection is synchronous — no scheduler tick required.
            var ex = assertThrows(ExecutionException.class, () -> future.get(5, SECONDS));
            assertInstanceOf(IllegalArgumentException.class, ex.getCause());
            assertTrue(ex.getCause().getMessage().contains("more than one partition"),
                    ex.getCause().getMessage());
            assertFalse(Files.exists(source), "rejected batch's input file should be deleted");
        }
    }

    @Test
    public void statsAggregateChildrenAndExposePartitionRows() throws Exception {
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        Path source = singleKeyFile("s.parquet", 7, 100);

        try (var queue = newQueue(scheduler, clock)) {
            queue.add(batch(source, "p", 0, MIN_BATCH_SIZE + 1));
            scheduler.tick(1, TimeUnit.MILLISECONDS);
            queue.drain();

            var stats = queue.getStats();
            assertEquals("test-queue", stats.identifier());
            assertEquals(NUM_PARTITIONS, stats.partitions().size(), "one child row per partition");
            assertEquals(100, stats.rowsWritten(), "rows aggregated across children");
            assertTrue(stats.totalWriteBytes() > 0);
            // Rolling per-minute series is aggregated across children; all writes here land in the
            // fixed clock's minute, so the whole 100 rows show up in the series total.
            assertEquals(BulkIngestQueue.HISTORY_MINUTES, stats.rowsWrittenPerMinute().length);
            assertEquals(100, java.util.Arrays.stream(stats.rowsWrittenPerMinute()).sum());
            assertEquals(1, java.util.Arrays.stream(stats.batchesReceivedPerMinute()).sum(), "one batch received");
            // Exactly one child (the routed partition) did the write.
            long childrenWithRows = stats.partitions().stream().filter(p -> p.rowsWritten() > 0).count();
            assertEquals(1, childrenWithRows);
        }
    }

    @Test
    public void multiPartitionRejectIsCounted() throws Exception {
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        Path source = multiKeyFile("m.parquet", 100);

        try (var queue = newQueue(scheduler, clock)) {
            var future = queue.add(batch(source, "p", 0, MIN_BATCH_SIZE + 1));
            assertThrows(ExecutionException.class, () -> future.get(5, SECONDS));
            assertEquals(1, queue.getRejectedMultiPartition());
            assertEquals(1, queue.getStats().rejectedMultiPartition());
        }
    }

    @Test
    public void unevaluablePartitionExpressionIsRetryableNotRejected() throws Exception {
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        Path source = singleKeyFile("ok.parquet", 7, 10);

        // Expression references a column that does not exist -> evaluation fails.
        try (var queue = newQueue(scheduler, clock, "no_such_column")) {
            var future = queue.add(batch(source, "p", 0, MIN_BATCH_SIZE + 1));
            var ex = assertThrows(ExecutionException.class, () -> future.get(5, SECONDS));
            // Retryable server-side failure, NOT a multi-partition (IllegalArgumentException) reject.
            assertInstanceOf(PartitionEvaluationException.class, ex.getCause());
            assertEquals(0, queue.getRejectedMultiPartition(), "eval failure must not count as a multi-partition reject");
            assertFalse(Files.exists(source), "the staged input is cleaned up");
        }
    }

    @Test
    public void constructorRejectsInvalidPartitioning() {
        var scheduler = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        IngestionHandler handler = noopHandler();

        // Blank expression with >1 partitions.
        assertThrows(IllegalArgumentException.class, () -> new PartitionedIngestionQueue(
                TEST_APP_ID, INPUT_FORMAT, targetPath.toString(), "q",
                MIN_BATCH_SIZE, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                MAX_DELAY, null, handler, scheduler, clock, NUM_PARTITIONS, "  "));

        // numPartitions <= 1 is not a partitioned queue.
        assertThrows(IllegalArgumentException.class, () -> new PartitionedIngestionQueue(
                TEST_APP_ID, INPUT_FORMAT, targetPath.toString(), "q",
                MIN_BATCH_SIZE, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                MAX_DELAY, null, handler, scheduler, clock, 1, PARTITION_EXPRESSION));
    }
}
