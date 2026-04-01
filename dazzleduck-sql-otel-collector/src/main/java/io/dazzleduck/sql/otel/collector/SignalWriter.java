package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ingestion.Batch;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.ParquetIngestionQueue;
import io.dazzleduck.sql.commons.ingestion.Stats;
import io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Accepts Arrow batches from OTLP services, spills each batch to a temp Arrow
 * file on disk, and delegates batching + Parquet writing to
 * {@link ParquetIngestionQueue}.
 *
 * <p>DuckDB reads all accumulated Arrow files in one
 * {@code COPY (SELECT * FROM read_arrow([...])) TO outputPath} statement,
 * which is more efficient than writing one Parquet file per batch.
 *
 * <p>Flush is triggered when accumulated file bytes reach {@code minBucketSizeBytes}
 * or after {@code maxDelayMs} elapses since the first batch in the current bucket.
 *
 * <p>The transformation is refreshed every 2 minutes from
 * {@link IngestionHandler#getTransformation(String)} using the signal's queue ID.
 */
public class SignalWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SignalWriter.class);

    private static final long MAX_BUCKET_SIZE   = 100L * 1024 * 1024; // 100 MB
    private static final long MAX_PENDING_WRITE = 500L * 1024 * 1024; // 500 MB
    private static final long TRANSFORMATION_REFRESH_MINUTES = 2;

    private final String queueId;
    private final String outputPath;
    private final ParquetIngestionQueue queue;
    private final String[] partitions;
    private final ScheduledExecutorService scheduler;

    public SignalWriter(String queueId, SignalIngestionConfig config,
                        IngestionHandler ingestionHandler) throws IOException {
        this.queueId = queueId;
        this.outputPath = config.outputPath();
        Files.createDirectories(Path.of(outputPath));
        this.partitions = config.partitionBy().toArray(new String[0]);

        String queueTransformation = toQueueTransformation(config.transformation());

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "otel-signal-scheduler[" + queueId + "]");
            t.setDaemon(true);
            return t;
        });

        this.queue = new ParquetIngestionQueue(
                "otel-collector",
                "arrow",
                outputPath,
                queueId,
                config.minBucketSizeBytes(),
                MAX_BUCKET_SIZE,
                Integer.MAX_VALUE,
                MAX_PENDING_WRITE,
                Duration.ofMillis(config.maxDelayMs()),
                ingestionHandler,
                scheduler,
                Clock.systemUTC(),
                queueTransformation
        );

        // Refresh transformation from the handler every 2 minutes.
        // Allows DuckLake or ServiceIngestionHandler to push updates at runtime.
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                String updated = ingestionHandler.getTransformation(queueId);
                queue.setTransformation(toQueueTransformation(updated));
            } catch (Exception e) {
                log.warn("Failed to refresh transformation for signal '{}': {}", queueId, e.getMessage());
            }
        }, TRANSFORMATION_REFRESH_MINUTES, TRANSFORMATION_REFRESH_MINUTES, TimeUnit.MINUTES);
    }

    /**
     * Submits an Arrow file to the ingestion queue.
     * Returns a future that completes when the file has been written to Parquet.
     */
    public CompletableFuture<Void> addBatch(Path arrowFile) {
        try {
            long fileSize = Files.size(arrowFile);
            Batch<String> batch = new Batch<>(
                    new String[0],
                    partitions,
                    arrowFile.toString(),
                    null,
                    0,
                    fileSize,
                    "parquet",
                    Instant.now()
            );
            return queue.add(batch).thenApply(ignored -> null);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public Stats getStats() {
        return queue.getStats();
    }

    @Override
    public void close() {
        try {
            queue.close();
        } catch (Exception e) {
            log.warn("Error closing ingestion queue for '{}'", queueId, e);
        }
        scheduler.shutdown();
        log.info("SignalWriter[{}] closed", queueId);
    }

    /**
     * Converts a user-supplied transformation expression (comma-separated SQL expressions)
     * into the full CTE-compatible SQL expected by ParquetIngestionQueue.
     * Returns null if the transformation is blank.
     */
    private static String toQueueTransformation(String transformation) {
        if (transformation == null || transformation.isBlank()) return null;
        return "SELECT *, " + transformation + " FROM __this";
    }
}
