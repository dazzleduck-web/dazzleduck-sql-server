package io.dazzleduck.sql.otel.collector;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.atomic.LongAdder;

/**
 * Micrometer metrics for the OTLP collector, following the dual-stream pattern:
 * <ul>
 *   <li>LongAdders — real-time source of truth, readable instantly via getXxx() accessors</li>
 *   <li>FunctionCounters/Gauges — bridge to Micrometer for Prometheus, Grafana, etc.</li>
 * </ul>
 *
 * All metrics are tagged with {@code signal=logs|traces|metrics}.
 * Common tags ({@code service.name}, {@code host.name}, {@code container.id}) are set once
 * at startup via {@code registry.config().commonTags()} in {@code OtelCollectorServer}.
 *
 * Metric names:
 * <pre>
 *   dazzleduck.otel.export.latency         – end-to-end RPC latency (p50/p95/p99)
 *   dazzleduck.otel.export.requests        – number of export RPC calls received
 *   dazzleduck.otel.export.records         – number of individual records (log records / spans / data points)
 *   dazzleduck.otel.export.errors          – number of failed exports
 *   dazzleduck.otel.writer.bytes_written   – cumulative bytes written to Parquet
 *   dazzleduck.otel.writer.batches_written – cumulative number of batches flushed to Parquet
 *   dazzleduck.otel.writer.pending_batches – current queue depth (batches not yet written)
 *   dazzleduck.otel.writer.pending_buckets – current bucket queue depth
 * </pre>
 */
public class OtelCollectorMetrics {

    private final MeterRegistry registry;

    // Logs
    private final LongAdder logRequests   = new LongAdder();
    private final LongAdder logRecords    = new LongAdder();
    private final LongAdder logErrors     = new LongAdder();
    private final Timer     logExportTimer;

    // Traces
    private final LongAdder traceRequests = new LongAdder();
    private final LongAdder traceSpans    = new LongAdder();
    private final LongAdder traceErrors   = new LongAdder();
    private final Timer     traceExportTimer;

    // Metrics
    private final LongAdder metricRequests   = new LongAdder();
    private final LongAdder metricDataPoints = new LongAdder();
    private final LongAdder metricErrors     = new LongAdder();
    private final Timer     metricExportTimer;

    public OtelCollectorMetrics(MeterRegistry registry,
                                long logMaxDelayMs, long traceMaxDelayMs, long metricMaxDelayMs) {
        this.registry = registry;
        registerExportCounters("logs",    logRequests,    logRecords,      logErrors);
        registerExportCounters("traces",  traceRequests,  traceSpans,      traceErrors);
        registerExportCounters("metrics", metricRequests, metricDataPoints, metricErrors);
        logExportTimer    = buildExportTimer("logs",    logMaxDelayMs);
        traceExportTimer  = buildExportTimer("traces",  traceMaxDelayMs);
        metricExportTimer = buildExportTimer("metrics", metricMaxDelayMs);
    }

    private Timer buildExportTimer(String signal, long maxDelayMs) {
        return Timer.builder("dazzleduck.otel.export.latency")
                .tag("signal", signal)
                .tag("max.delay.ms", String.valueOf(maxDelayMs))
                .description("End-to-end export RPC duration — from request received to onCompleted/onError")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    private void registerExportCounters(String signal,
                                        LongAdder requests,
                                        LongAdder records,
                                        LongAdder errors) {
        FunctionCounter.builder("dazzleduck.otel.export.requests", requests, LongAdder::sum)
                .tag("signal", signal)
                .description("Number of OTLP export RPC calls received")
                .register(registry);
        FunctionCounter.builder("dazzleduck.otel.export.records", records, LongAdder::sum)
                .tag("signal", signal)
                .description("Number of individual records exported (log records / spans / data points)")
                .register(registry);
        FunctionCounter.builder("dazzleduck.otel.export.errors", errors, LongAdder::sum)
                .tag("signal", signal)
                .description("Number of failed export requests")
                .register(registry);
    }

    /**
     * Registers writer stats for one signal type as Gauges and FunctionCounters.
     * Call this after the SignalWriter has been created.
     *
     * @param signal one of "logs", "traces", "metrics"
     * @param writer the corresponding SignalWriter
     */
    public void registerWriter(String signal, SignalWriter writer) {
        FunctionCounter.builder("dazzleduck.otel.writer.bytes_written", writer,
                        w -> w.getStats().totalWriteBytes())
                .tag("signal", signal)
                .description("Cumulative bytes written to Parquet")
                .register(registry);
        FunctionCounter.builder("dazzleduck.otel.writer.batches_written", writer,
                        w -> w.getStats().totalWriteBatches())
                .tag("signal", signal)
                .description("Cumulative number of Arrow batches flushed to Parquet")
                .register(registry);
        Gauge.builder("dazzleduck.otel.writer.pending_batches", writer,
                        w -> w.getStats().pendingBatches())
                .tag("signal", signal)
                .description("Current number of batches queued but not yet written")
                .register(registry);
        Gauge.builder("dazzleduck.otel.writer.pending_buckets", writer,
                        w -> w.getStats().pendingBuckets())
                .tag("signal", signal)
                .description("Current number of buckets queued but not yet written")
                .register(registry);
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

    public void recordLogExport(int recordCount, Timer.Sample sample) {
        logRequests.increment();
        logRecords.add(recordCount);
        sample.stop(logExportTimer);
    }

    public void recordLogError(Timer.Sample sample) {
        logErrors.increment();
        sample.stop(logExportTimer);
    }

    public void recordTraceExport(int spanCount, Timer.Sample sample) {
        traceRequests.increment();
        traceSpans.add(spanCount);
        sample.stop(traceExportTimer);
    }

    public void recordTraceError(Timer.Sample sample) {
        traceErrors.increment();
        sample.stop(traceExportTimer);
    }

    public void recordMetricExport(int dataPointCount, Timer.Sample sample) {
        metricRequests.increment();
        metricDataPoints.add(dataPointCount);
        sample.stop(metricExportTimer);
    }

    public void recordMetricError(Timer.Sample sample) {
        metricErrors.increment();
        sample.stop(metricExportTimer);
    }

    // -----------------------------------------------------------------------
    // Real-time accessors — return current values without Micrometer buffering
    // -----------------------------------------------------------------------

    public long getLogRequests()      { return logRequests.sum(); }
    public long getLogRecords()       { return logRecords.sum(); }
    public long getLogErrors()        { return logErrors.sum(); }

    public long getTraceRequests()    { return traceRequests.sum(); }
    public long getTraceSpans()       { return traceSpans.sum(); }
    public long getTraceErrors()      { return traceErrors.sum(); }

    public long getMetricRequests()   { return metricRequests.sum(); }
    public long getMetricDataPoints() { return metricDataPoints.sum(); }
    public long getMetricErrors()     { return metricErrors.sum(); }

    public MeterRegistry getRegistry() { return registry; }
}
