package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.IngestionResult;
import io.dazzleduck.sql.commons.ingestion.ParquetIngestionQueue;
import io.dazzleduck.sql.commons.ingestion.PendingWriteExceededException;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTask;
import io.dazzleduck.sql.otel.collector.auth.JwtServerInterceptor;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link OtelServiceBase#resolveQueue} — the collector's queue-resolution +
 * metric-lifecycle wiring now that there is no {@code WriterRegistry}. Restores the coverage the
 * deleted {@code WriterRegistryTest} provided: lazy creation registers metrics, eviction
 * unregisters them, an unknown queue is rejected with {@code INVALID_ARGUMENT}, and a known but
 * unprovisioned queue is rejected with {@code FAILED_PRECONDITION}.
 *
 * <p>No DuckLake/DuckDB is needed: {@link ParquetIngestionQueue} only writes (via {@code COPY}) on
 * a batch, which these tests never submit — they just resolve.
 */
class OtelServiceBaseTest {

    /** Values are irrelevant; no batch is ever written. */
    private static final IngestionConfig CONFIG = new IngestionConfig(
            1L, IngestionConfig.DEFAULT_MAX_BUCKET_SIZE, IngestionConfig.DEFAULT_MAX_BATCHES,
            IngestionConfig.DEFAULT_MAX_PENDING_WRITE, Duration.ofSeconds(10),
            IngestionConfig.DEFAULT_CONFIG_REFRESH);

    @TempDir
    Path tempDir;

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final OtelCollectorMetrics metrics = new OtelCollectorMetrics(meterRegistry);
    private ScheduledExecutorService scheduler;
    private FakeHandler handler;
    private OtelServiceBase base;

    @BeforeEach
    void setUp() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        handler = new FakeHandler(tempDir);
        base = new OtelServiceBase("otel-base-test-", handler, CONFIG, scheduler, metrics);
    }

    @AfterEach
    void tearDown() {
        handler.closeAll();
        if (base != null) base.close();
        if (scheduler != null) scheduler.shutdownNow();
        metrics.close();
    }

    @Test
    void missingClaim_returnsInvalidArgument() throws Exception {
        CapturingObserver obs = new CapturingObserver();
        ParquetIngestionQueue queue = resolve(null, obs); // no x-dd-ingestion-queue claim
        assertNull(queue);
        assertEquals(Status.Code.INVALID_ARGUMENT, statusOf(obs));
    }

    @Test
    void knownQueue_resolvesAndRegistersMetrics() throws Exception {
        handler.known.add("a");
        CapturingObserver obs = new CapturingObserver();
        ParquetIngestionQueue queue = resolve("a", obs);

        assertNotNull(queue, "a known queue resolves");
        assertNull(obs.error, "no error sent for a resolvable queue");
        assertEquals(10, meterCount("a"), "writer gauges/counters registered on creation");
        assertSame(queue, resolve("a", new CapturingObserver()), "second resolve returns the cached queue");
    }

    @Test
    void unknownQueue_returnsInvalidArgument() throws Exception {
        CapturingObserver obs = new CapturingObserver();
        ParquetIngestionQueue queue = resolve("nope", obs);
        assertNull(queue);
        assertEquals(Status.Code.INVALID_ARGUMENT, statusOf(obs));
        assertEquals(0, meterCount("nope"), "no metrics for a queue that was never created");
    }

    @Test
    void knownButNoTargetPath_returnsFailedPrecondition() throws Exception {
        handler.known.add("a");
        handler.targetPathNull = true; // registered, but not yet provisioned
        CapturingObserver obs = new CapturingObserver();
        ParquetIngestionQueue queue = resolve("a", obs);
        assertNull(queue);
        assertEquals(Status.Code.FAILED_PRECONDITION, statusOf(obs),
                "a known-but-unprovisioned queue is distinct from an unknown one");
    }

    @Test
    void evictedQueue_unregistersMetricsAndRejects() throws Exception {
        handler.known.add("a");
        assertNotNull(resolve("a", new CapturingObserver()));
        assertEquals(10, meterCount("a"), "precondition: writer metrics present");

        handler.known.remove("a"); // tombstone

        CapturingObserver obs = new CapturingObserver();
        ParquetIngestionQueue queue = resolve("a", obs);
        assertNull(queue, "evicted queue is rejected");
        assertEquals(Status.Code.INVALID_ARGUMENT, statusOf(obs));
        assertEquals(0, meterCount("a"),
                "eviction (onDeleted) must unregister the queue's metrics so it can be GC'd");
    }

    @Test
    void backpressure_mapsToResourceExhaustedWithRetryInfo() throws Exception {
        var ex = new java.util.concurrent.CompletionException(
                new PendingWriteExceededException(100, 50, 7));
        var sre = OtelServiceBase.toStatusError(ex);

        assertEquals(Status.Code.RESOURCE_EXHAUSTED, sre.getStatus().getCode());
        assertNotNull(sre.getStatus().getDescription());
        assertTrue(sre.getStatus().getDescription().contains("Pending write limit exceeded"),
                "the original backpressure message must survive");

        com.google.rpc.Status proto = io.grpc.protobuf.StatusProto.fromThrowable(sre);
        assertNotNull(proto);
        assertEquals(1, proto.getDetailsCount(), "RetryInfo detail attached");
        var retryInfo = proto.getDetails(0).unpack(com.google.rpc.RetryInfo.class);
        assertEquals(7, retryInfo.getRetryDelay().getSeconds(),
                "retry-after from the queue is propagated to the client");
    }

    @Test
    void writeFailure_mapsToUnavailableWithMessage() {
        var ex = new java.util.concurrent.CompletionException(
                new java.io.IOException("disk full"));
        var sre = OtelServiceBase.toStatusError(ex);
        // UNAVAILABLE, not INTERNAL: a batch that failed after being accepted is dropped by the
        // write loop, and the OTel C++ exporter only retries the SDK's fixed status set — INTERNAL
        // would have the producer discard the batch without retrying once.
        assertEquals(Status.Code.UNAVAILABLE, sre.getStatus().getCode());
        assertEquals("disk full", sre.getStatus().getDescription());
    }

    @Test
    void backpressure_isNotMappedToARetryableCode() {
        var ex = new java.util.concurrent.CompletionException(
                new PendingWriteExceededException(100, 50, 7));
        var code = OtelServiceBase.toStatusError(ex).getStatus().getCode();
        // Overload must not come back as a code gRPC retries on: clients would re-send at once and
        // amplify the overload. RetryInfo (asserted above) is the throttling signal instead.
        assertNotEquals(Status.Code.UNAVAILABLE, code);
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, code);
    }

    @Test
    void existingStatusException_passesThrough() {
        var original = Status.FAILED_PRECONDITION.withDescription("not ready").asRuntimeException();
        var sre = OtelServiceBase.toStatusError(
                new java.util.concurrent.CompletionException(original));
        assertSame(original, sre);
    }

    // ----- helpers ---------------------------------------------------------

    private ParquetIngestionQueue resolve(String queueId, CapturingObserver obs) throws Exception {
        Context ctx = queueId == null
                ? Context.current()
                : Context.current().withValue(JwtServerInterceptor.QUEUE_CONTEXT_KEY, queueId);
        Callable<ParquetIngestionQueue> call = () -> base.resolveQueue(obs);
        return ctx.call(call);
    }

    private long meterCount(String queueId) {
        return meterRegistry.getMeters().stream()
                .filter(m -> queueId.equals(m.getId().getTag("queue")))
                .count();
    }

    private static Status.Code statusOf(CapturingObserver obs) {
        assertNotNull(obs.error, "expected an error to be sent");
        return Status.fromThrowable(obs.error).getCode();
    }

    private static final class CapturingObserver implements StreamObserver<Object> {
        volatile Throwable error;
        @Override public void onNext(Object value) {}
        @Override public void onError(Throwable t) { this.error = t; }
        @Override public void onCompleted() {}
    }

    /**
     * Mirrors the real handlers' {@code getOrCreateQueue} semantics (create-once cache; evict +
     * {@code onDeleted} when the target path disappears) so the test exercises {@link OtelServiceBase}'s
     * creator/listener wiring against a realistic handler. {@code getTargetPath} is non-null iff the
     * queue is known (unless {@link #targetPathNull} forces "registered but not provisioned").
     */
    private static final class FakeHandler implements IngestionHandler {
        final Set<String> known = ConcurrentHashMap.newKeySet();
        final Map<String, ParquetIngestionQueue> cache = new ConcurrentHashMap<>();
        private final Path base;
        volatile boolean targetPathNull = false;

        FakeHandler(Path base) { this.base = base; }

        @Override public Set<String> getKnownQueues() { return known; }

        @Override public String getTargetPath(String id) {
            if (targetPathNull || !known.contains(id)) return null;
            return base.resolve(id).toString();
        }

        @Override public String[] getPartitionBy(String id) { return new String[0]; }

        @Override public PostIngestionTask createPostIngestionTask(IngestionResult r) {
            return PostIngestionTask.NOOP;
        }

        @Override
        public ParquetIngestionQueue getOrCreateQueue(String id, QueueCreator creator, QueueEventListener listener) {
            String path = getTargetPath(id);
            if (path == null) {
                ParquetIngestionQueue removed = cache.remove(id);
                if (removed != null) {
                    listener.onDeleted(id);
                    try { removed.close(); } catch (Exception ignored) {}
                }
                return null;
            }
            return cache.computeIfAbsent(id, k -> {
                ParquetIngestionQueue q = creator.create(k, path);
                listener.onCreated(k);
                return q;
            });
        }

        void closeAll() {
            cache.values().forEach(q -> { try { q.close(); } catch (Exception ignored) {} });
            cache.clear();
        }
    }
}
