package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ingestion.ParquetIngestionQueue;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;

/**
 * Micrometer metrics for the OTLP collector. All meters are keyed by queue ID so
 * each configured ingestion queue gets its own counters and timers.
 *
 * <p>Meters are created on first use ({@link #recordExport}/{@link #recordError}) and on
 * {@link #registerQueue} — no pre-allocation at startup. This means a queue that never
 * receives traffic produces no meters. Every meter is also tracked per queue ID so that
 * {@link #unregisterQueue} can remove them all when a dynamic queue is evicted — important
 * because the writer gauges/counters hold a strong reference to the {@code ParquetIngestionQueue},
 * so leaving them registered would pin an evicted queue in memory.
 *
 * <p>All meters carry a {@code queue} tag with the queue ID as the value, plus the common
 * tags ({@code service.name}, {@code host.name}, {@code container.id}) set once at startup
 * via {@code registry.config().commonTags()} in {@link OtelCollectorServer}.
 *
 * <pre>
 *   dazzleduck.otel.export.requests        {queue=&lt;id&gt;}  – RPC calls received
 *   dazzleduck.otel.export.records         {queue=&lt;id&gt;}  – individual records exported
 *   dazzleduck.otel.export.errors          {queue=&lt;id&gt;}  – failed exports
 *   dazzleduck.otel.export.latency         {queue=&lt;id&gt;}  – end-to-end RPC latency (p50/p95/p99)
 *   dazzleduck.otel.writer.bytes_written   {queue=&lt;id&gt;}  – cumulative bytes written to Parquet
 *   dazzleduck.otel.writer.batches_written {queue=&lt;id&gt;}  – batches flushed to Parquet
 *   dazzleduck.otel.writer.pending_batches {queue=&lt;id&gt;}  – queue depth (batches not yet written)
 *   dazzleduck.otel.writer.pending_buckets {queue=&lt;id&gt;}  – bucket queue depth
 * </pre>
 */
public class OtelCollectorMetrics implements Closeable {

    private final MeterRegistry registry;
    private final List<Meter> registeredMeters = new CopyOnWriteArrayList<>();

    private final ConcurrentHashMap<String, LongAdder> requestsPerQueue = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> recordsPerQueue  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> errorsPerQueue   = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer>     timersPerQueue   = new ConcurrentHashMap<>();

    // Not per-queue and never removed by unregisterQueue/close — unlike the writer FunctionCounters
    // above (which are bound to a live ParquetIngestionQueue and intentionally dropped on eviction
    // so the queue can be GC'd), this must keep counting across queue eviction/recreation and
    // shutdown drain, since it backs the /health batchesProcessed field queried right up until the
    // collector goes DOWN.
    private final LongAdder totalBatchesProcessed = new LongAdder();

    /** All meters created for a given queue ID, so {@link #unregisterQueue} can remove them. */
    private final ConcurrentHashMap<String, List<Meter>> metersByQueue = new ConcurrentHashMap<>();

    public OtelCollectorMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** Records a meter in both the global list (for {@link #close}) and the per-queue index. */
    private <M extends Meter> M track(String queueId, M meter) {
        registeredMeters.add(meter);
        metersByQueue.computeIfAbsent(queueId, k -> new CopyOnWriteArrayList<>()).add(meter);
        return meter;
    }

    // -----------------------------------------------------------------------
    // Sample factory — called at the start of each export() RPC handler
    // -----------------------------------------------------------------------

    public Timer.Sample startSample() {
        return Timer.start(registry);
    }

    // -----------------------------------------------------------------------
    // Recording methods — called by the three OTLP services
    // -----------------------------------------------------------------------

    public void recordExport(String queueId, int count, Timer.Sample sample) {
        getOrCreateRequests(queueId).increment();
        getOrCreateRecords(queueId).add(count);
        sample.stop(getOrCreateTimer(queueId));
        totalBatchesProcessed.increment();
    }

    public void recordError(String queueId, Timer.Sample sample) {
        getOrCreateErrors(queueId).increment();
        sample.stop(getOrCreateTimer(queueId));
    }

    // -----------------------------------------------------------------------
    // Writer metrics — registered when a queue is created, removed on eviction
    // -----------------------------------------------------------------------

    public void registerQueue(String queueId, ParquetIngestionQueue writer) {
        track(queueId, FunctionCounter.builder("dazzleduck.otel.writer.bytes_written", writer,
                        w -> w.getStats().totalWriteBytes())
                .tag("queue", queueId)
                .description("Cumulative bytes written to Parquet")
                .register(registry));
        track(queueId, FunctionCounter.builder("dazzleduck.otel.writer.batches_written", writer,
                        w -> w.getStats().totalWriteBatches())
                .tag("queue", queueId)
                .description("Cumulative number of Arrow batches flushed to Parquet")
                .register(registry));
        track(queueId, FunctionCounter.builder("dazzleduck.otel.writer.bytes_failed", writer,
                        w -> w.getStats().failedWriteBytes())
                .tag("queue", queueId)
                .description("Cumulative bytes in buckets whose Parquet write failed")
                .register(registry));
        track(queueId, FunctionCounter.builder("dazzleduck.otel.writer.batches_failed", writer,
                        w -> w.getStats().failedWriteBatches())
                .tag("queue", queueId)
                .description("Cumulative number of Arrow batches whose Parquet write failed")
                .register(registry));
        track(queueId, FunctionCounter.builder("dazzleduck.otel.writer.write_failures", writer,
                        w -> w.getStats().failedWriteBuckets())
                .tag("queue", queueId)
                .description("Cumulative number of failed bucket write attempts")
                .register(registry));
        track(queueId, FunctionCounter.builder("dazzleduck.otel.writer.producer_id_evictions", writer,
                        w -> w.getStats().producerIdEvictions())
                .tag("queue", queueId)
                .description("Cumulative producer ids evicted from the duplicate-protection cache")
                .register(registry));
        track(queueId, FunctionCounter.builder("dazzleduck.otel.writer.data_phase_ms", writer,
                        w -> w.getDataPhaseNanos() / 1_000_000.0)
                .tag("queue", queueId)
                .description("Cumulative ms in the commit data phase (DuckDB COPY to Parquet, parallelizable)")
                .register(registry));
        track(queueId, FunctionCounter.builder("dazzleduck.otel.writer.post_ingest_phase_ms", writer,
                        w -> w.getPostIngestPhaseNanos() / 1_000_000.0)
                .tag("queue", queueId)
                .description("Cumulative ms in the post-ingestion phase (e.g. DuckLake catalog commit, serialized)")
                .register(registry));
        track(queueId, Gauge.builder("dazzleduck.otel.writer.pending_batches", writer,
                        w -> w.getStats().pendingBatches())
                .tag("queue", queueId)
                .description("Current number of batches queued but not yet written")
                .register(registry));
        track(queueId, Gauge.builder("dazzleduck.otel.writer.pending_buckets", writer,
                        w -> w.getStats().pendingBuckets())
                .tag("queue", queueId)
                .description("Current number of buckets queued but not yet written")
                .register(registry));
    }

    /**
     * Removes every meter registered for {@code queueId} (writer gauges/counters plus any
     * lazily-created export counters/timer) and drops the per-queue accumulators. Called when a
     * dynamic queue is evicted so a deleted queue leaves no stale series behind and its
     * {@code ParquetIngestionQueue} (referenced by the writer gauges) can be garbage-collected.
     */
    public void unregisterQueue(String queueId) {
        List<Meter> meters = metersByQueue.remove(queueId);
        if (meters != null) {
            meters.forEach(m -> {
                registry.remove(m);
                registeredMeters.remove(m);
            });
        }
        requestsPerQueue.remove(queueId);
        recordsPerQueue.remove(queueId);
        errorsPerQueue.remove(queueId);
        timersPerQueue.remove(queueId);
    }

    // -----------------------------------------------------------------------
    // Lazy meter creation — one set of counters + one timer per queue ID
    // -----------------------------------------------------------------------

    private LongAdder getOrCreateRequests(String queueId) {
        return requestsPerQueue.computeIfAbsent(queueId, id -> {
            var adder = new LongAdder();
            track(id, FunctionCounter.builder("dazzleduck.otel.export.requests", adder, LongAdder::sum)
                    .tag("queue", id)
                    .description("Number of OTLP export RPC calls received")
                    .register(registry));
            return adder;
        });
    }

    private LongAdder getOrCreateRecords(String queueId) {
        return recordsPerQueue.computeIfAbsent(queueId, id -> {
            var adder = new LongAdder();
            track(id, FunctionCounter.builder("dazzleduck.otel.export.records", adder, LongAdder::sum)
                    .tag("queue", id)
                    .description("Number of individual records exported")
                    .register(registry));
            return adder;
        });
    }

    private LongAdder getOrCreateErrors(String queueId) {
        return errorsPerQueue.computeIfAbsent(queueId, id -> {
            var adder = new LongAdder();
            track(id, FunctionCounter.builder("dazzleduck.otel.export.errors", adder, LongAdder::sum)
                    .tag("queue", id)
                    .description("Number of failed export requests")
                    .register(registry));
            return adder;
        });
    }

    private Timer getOrCreateTimer(String queueId) {
        return timersPerQueue.computeIfAbsent(queueId, id ->
                track(id, Timer.builder("dazzleduck.otel.export.latency")
                        .tag("queue", id)
                        .description("End-to-end export RPC duration — from request received to onCompleted/onError")
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .register(registry)));
    }

    // -----------------------------------------------------------------------
    // Real-time accessors (for testing)
    // -----------------------------------------------------------------------

    public long getRequests(String queueId) {
        var a = requestsPerQueue.get(queueId);
        return a != null ? a.sum() : 0L;
    }

    public long getRecords(String queueId) {
        var a = recordsPerQueue.get(queueId);
        return a != null ? a.sum() : 0L;
    }

    public long getErrors(String queueId) {
        var a = errorsPerQueue.get(queueId);
        return a != null ? a.sum() : 0L;
    }

    /** Cumulative count of batches successfully written, across all queues, past and present. */
    public long getTotalBatchesProcessed() {
        return totalBatchesProcessed.sum();
    }

    public MeterRegistry getRegistry() { return registry; }

    /** Removes all registered meters from the registry so queue references can be GC'd. */
    @Override
    public void close() {
        registeredMeters.forEach(registry::remove);
        registeredMeters.clear();
        metersByQueue.clear();
    }
}
