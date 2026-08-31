package io.dazzleduck.sql.otel.collector;

import com.google.protobuf.Any;
import com.google.rpc.RetryInfo;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ingestion.Batch;
import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.ParquetIngestionQueue;
import io.dazzleduck.sql.commons.ingestion.PendingWriteExceededException;
import io.dazzleduck.sql.otel.collector.auth.JwtServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Timer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;

/**
 * Shared resources and common logic for OTLP signal services.
 *
 * <p>Each signal service holds one instance as a field and delegates to these methods. Queue
 * resolution goes straight through the {@link IngestionHandler} — it is the single registry:
 * {@link IngestionHandler#getOrCreateQueue} lazily creates a queue for a known ID (via our
 * {@code creator}), caches it, and evicts/closes it when its target path disappears (firing
 * {@code onDeleted}). There is no separate writer cache. Metrics are registered when a queue is
 * created and unregistered via the {@code onDeleted} lifecycle callback.
 */
class OtelServiceBase implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(OtelServiceBase.class);

    final RootAllocator allocator;
    final Path tempDir;

    private final IngestionHandler handler;
    private final OtelCollectorMetrics metrics;
    private final IngestionHandler.QueueCreator creator;
    private final IngestionHandler.QueueEventListener listener;

    OtelServiceBase(String tempDirPrefix,
                    IngestionHandler handler,
                    IngestionConfig ingestionConfig,
                    ScheduledExecutorService flushScheduler,
                    OtelCollectorMetrics metrics) throws IOException {
        this.allocator = new RootAllocator();
        this.tempDir = Files.createTempDirectory(tempDirPrefix);
        this.handler = handler;
        this.metrics = metrics;
        // Build the queue (sharing the collector-wide flush scheduler) and register its metrics.
        this.creator = (id, targetPath) -> {
            ParquetIngestionQueue queue = new ParquetIngestionQueue(
                    "otel-collector", "arrow", targetPath, id,
                    ingestionConfig.minBucketSize(),
                    ingestionConfig.maxBucketSize(),
                    ingestionConfig.maxBatches(),
                    ingestionConfig.maxPendingWrite(),
                    ingestionConfig.maxDelay(),
                    handler,
                    flushScheduler, Clock.systemUTC());
            metrics.registerQueue(id, queue);
            return queue;
        };
        this.listener = new IngestionHandler.QueueEventListener() {
            @Override public void onCreated(String id)   { log.info("Ingestion queue '{}' created", id); }
            @Override public void onRefreshed(String id) { log.debug("Ingestion queue '{}' refreshed", id); }
            @Override public void onDeleted(String id)   {
                metrics.unregisterQueue(id); // drop per-queue meters so an evicted queue can be GC'd
                log.warn("Ingestion queue '{}' deleted", id);
            }
        };
    }

    /**
     * Reads the {@value Headers#CLAIM_INGESTION_QUEUE} JWT claim from the gRPC context and resolves
     * it to a live {@link ParquetIngestionQueue} via the handler. Returns {@code null} after
     * sending an error to {@code responseObserver}:
     * <ul>
     *   <li>{@code INVALID_ARGUMENT} if the claim is absent or names an unknown queue;</li>
     *   <li>{@code FAILED_PRECONDITION} if the queue is registered but cannot be created yet
     *       (e.g. its output location is not provisioned) — distinct from an unknown queue.</li>
     * </ul>
     */
    ParquetIngestionQueue resolveQueue(StreamObserver<?> responseObserver) {
        String queueId = JwtServerInterceptor.QUEUE_CONTEXT_KEY.get();
        if (queueId == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Missing required JWT claim: '" + Headers.CLAIM_INGESTION_QUEUE + "'")
                    .asRuntimeException());
            return null;
        }
        ParquetIngestionQueue queue;
        try {
            queue = handler.getOrCreateQueue(queueId, creator, listener);
        } catch (RuntimeException e) {
            // Known queue, but building it failed (e.g. output not provisioned) — retryable.
            // Log with the cause so a genuine handler/creator bug isn't silently masked as "not ready".
            log.error("Failed to resolve ingestion queue '{}'", queueId, e);
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("Ingestion queue '" + queueId + "' is registered but not ready: " + e.getMessage())
                    .asRuntimeException());
            return null;
        }
        if (queue == null) {
            // null means no target path: distinguish "registered but not ready" from "unknown".
            boolean known = handler.getKnownQueues().contains(queueId);
            responseObserver.onError((known ? Status.FAILED_PRECONDITION : Status.INVALID_ARGUMENT)
                    .withDescription(known
                            ? "Ingestion queue '" + queueId + "' is registered but not ready (no output path)"
                            : "Unknown ingestion queue: '" + queueId + "'")
                    .asRuntimeException());
            return null;
        }
        return queue;
    }

    /** Submits an Arrow file to the queue as a single batch. */
    CompletableFuture<Void> addBatch(ParquetIngestionQueue queue, Path arrowFile) throws IOException {
        long fileSize = Files.size(arrowFile);
        Batch<String> batch = new Batch<>(new String[0], new String[0],
                arrowFile.toString(), null, 0, fileSize, "parquet", Instant.now());
        return queue.add(batch).thenApply(ignored -> null);
    }

    /** Verified JWT claims for the queue, or {@code null} when it does not extract claims. */
    private Map<String, String> claimsFor(String queueId) {
        if (!handler.extractClaims(queueId)) {
            return null;
        }
        Map<String, String> claims = JwtServerInterceptor.CLAIMS_CONTEXT_KEY.get();
        return claims != null ? claims : Map.of();
    }

    /**
     * Serialises {@code entries} to a temporary Arrow IPC file using the provided schema
     * and batch writer. Deletes the partial file before rethrowing on any failure.
     * Queues with {@code extract_claims} get the claims-column schema variant, filled from
     * the caller's verified JWT claims after the batch writer runs.
     */
    <E> Path writeArrowFile(String queueId, List<E> entries, Schema schema,
                             BiConsumer<List<E>, VectorSchemaRoot> batchWriter) throws IOException {
        Map<String, String> claims = claimsFor(queueId);
        Schema effective = claims == null ? schema : OtelSchemaFields.withClaimsColumn(schema);
        Path tempFile = tempDir.resolve("batch_" + UUID.randomUUID() + ".arrow");
        try (VectorSchemaRoot root = VectorSchemaRoot.create(effective, allocator)) {
            batchWriter.accept(entries, root);
            if (claims != null) {
                ClaimsColumnWriter.write(root, claims);
            }
            try (FileOutputStream fos = new FileOutputStream(tempFile.toFile());
                 ArrowStreamWriter writer = io.dazzleduck.sql.commons.io.ResultStreams.newArrowStreamWriter(
                         root, null, fos,
                         org.apache.arrow.vector.compression.CompressionUtil.CodecType.NO_COMPRESSION, null)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        } catch (Exception e) {
            Files.deleteIfExists(tempFile);
            if (e instanceof IOException ioe) throw ioe;
            throw new IOException(e);
        }
        return tempFile;
    }

    /**
     * Returns a {@code whenComplete} handler for an {@link #addBatch} future. On success records
     * the export metric and notifies the client; on failure deletes the temp file, records the
     * error metric, and propagates the error to the client.
     */
    static <R> BiConsumer<Void, Throwable> batchCompleteHandler(
            Path arrowFile,
            int count,
            String queueId,
            Timer.Sample sample,
            OtelCollectorMetrics metrics,
            StreamObserver<R> responseObserver,
            R successResponse) {
        return (v, ex) -> {
            if (ex != null) {
                try { Files.deleteIfExists(arrowFile); } catch (IOException ignored) {}
                metrics.recordError(queueId, sample);
                log.error("Failed to persist {} records for queue '{}'", count, queueId, ex);
                responseObserver.onError(toStatusError(ex));
            } else {
                metrics.recordExport(queueId, count, sample);
                responseObserver.onNext(successResponse);
                responseObserver.onCompleted();
            }
        };
    }

    /**
     * Maps an ingestion failure to a meaningful gRPC status. Forwarding the raw exception would
     * make gRPC surface it as {@code UNKNOWN} with an empty message, so clients could not tell
     * overload from a server bug. Backpressure ({@link PendingWriteExceededException} anywhere in
     * the cause chain — it arrives wrapped in {@code CompletionException}) becomes
     * {@code RESOURCE_EXHAUSTED} with {@code RetryInfo} carrying the queue's suggested retry
     * delay, which OTLP exporters honor for throttling; an already-built {@code
     * StatusRuntimeException} passes through; anything else becomes {@code INTERNAL} with the
     * root cause's message.
     */
    static StatusRuntimeException toStatusError(Throwable t) {
        Throwable root = t;
        int depth = 0;
        // depth-bounded so a pathological cause cycle cannot loop forever
        for (Throwable current = t; current != null && depth < 20; current = current.getCause(), depth++) {
            if (current instanceof PendingWriteExceededException e) {
                var status = com.google.rpc.Status.newBuilder()
                        .setCode(Status.RESOURCE_EXHAUSTED.getCode().value())
                        .setMessage(e.getMessage())
                        .addDetails(Any.pack(RetryInfo.newBuilder()
                                .setRetryDelay(com.google.protobuf.Duration.newBuilder()
                                        .setSeconds(e.getRetryAfterSeconds()))
                                .build()))
                        .build();
                return StatusProto.toStatusRuntimeException(status);
            }
            if (current instanceof StatusRuntimeException sre) {
                return sre;
            }
            root = current;
        }
        return Status.INTERNAL
                .withDescription(root.getMessage() != null ? root.getMessage() : root.getClass().getName())
                .withCause(root)
                .asRuntimeException();
    }

    @Override
    public void close() {
        try {
            allocator.close();
        } catch (Exception e) {
            log.warn("Error closing Arrow allocator (prefix={})", tempDir.getFileName(), e);
        }
        try (var stream = Files.walk(tempDir)) {
            stream.sorted(Comparator.reverseOrder())
                  .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
        } catch (IOException ignored) {}
    }
}
