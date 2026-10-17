package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.ParquetIngestionQueue;
import io.dazzleduck.sql.otel.collector.auth.JwtServerInterceptor;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * gRPC service that receives OTLP metric exports and writes them to Parquet.
 * Queue routing is driven by the {@code x-dd-ingestion-queue} JWT claim.
 */
public class OtelMetricsService extends MetricsServiceGrpc.MetricsServiceImplBase implements Closeable {

    /** Prefix for this service's scratch directory, created by {@link OtelCollectorServer}. */
    static final String SCRATCH_PREFIX = "otel-metrics-arrow-";

    private static final Logger log = LoggerFactory.getLogger(OtelMetricsService.class);

    private final OtelCollectorMetrics metrics;
    private final OtelServiceBase base;
    private final boolean writeDescription;
    private final boolean attributesStringCopy;
    private final AttributeTypeRegistry attributeTypes;

    public OtelMetricsService(Path scratchDir, IngestionHandler handler, IngestionConfig ingestionConfig,
                              ScheduledExecutorService flushScheduler, OtelCollectorMetrics metrics) {
        this(scratchDir, handler, ingestionConfig, flushScheduler, metrics, false);
    }

    /** @param writeDescription write each metric's OTLP description; otherwise the column is NULL */
    public OtelMetricsService(Path scratchDir, IngestionHandler handler, IngestionConfig ingestionConfig,
                              ScheduledExecutorService flushScheduler, OtelCollectorMetrics metrics,
                              boolean writeDescription) {
        this(scratchDir, handler, ingestionConfig, flushScheduler, metrics, writeDescription, true);
    }

    /** @param attributesStringCopy see {@code attributes.string_copy} in reference.conf */
    public OtelMetricsService(Path scratchDir, IngestionHandler handler, IngestionConfig ingestionConfig,
                              ScheduledExecutorService flushScheduler, OtelCollectorMetrics metrics,
                              boolean writeDescription, boolean attributesStringCopy) {
        this(scratchDir, handler, ingestionConfig, flushScheduler, metrics, writeDescription,
                attributesStringCopy, null);
    }

    /** @param attributeTypes records the attribute name/type mappings seen; null records none */
    public OtelMetricsService(Path scratchDir, IngestionHandler handler, IngestionConfig ingestionConfig,
                              ScheduledExecutorService flushScheduler, OtelCollectorMetrics metrics,
                              boolean writeDescription, boolean attributesStringCopy,
                              AttributeTypeRegistry attributeTypes) {
        this.metrics = metrics;
        this.writeDescription = writeDescription;
        this.attributesStringCopy = attributesStringCopy;
        this.attributeTypes = attributeTypes;
        this.base = new OtelServiceBase(scratchDir, handler, ingestionConfig, flushScheduler, metrics);
    }

    @Override
    public void export(ExportMetricsServiceRequest request,
                       StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        ParquetIngestionQueue queue = base.resolveQueue(responseObserver);
        if (queue == null) return;

        List<MetricEntry> entries = new ArrayList<>();
        for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
            var resource = resourceMetrics.hasResource() ? resourceMetrics.getResource() : null;
            for (ScopeMetrics scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                var scope = scopeMetrics.hasScope() ? scopeMetrics.getScope() : null;
                for (var metric : scopeMetrics.getMetricsList()) {
                    entries.add(new MetricEntry(metric, resource, scope));
                }
            }
        }
        int metricCount = entries.size();
        String queueId = JwtServerInterceptor.QUEUE_CONTEXT_KEY.get();
        log.debug("Received {} metrics → queue '{}'", metricCount, queueId);
        var sample = metrics.startSample();

        try {
            String typesTable = base.attributeTypesTable(queueId);
            AttributeTypeScan scan = AttributeTypeRegistry.scanFor(attributeTypes, queueId, typesTable);
            Path arrowFile = base.writeArrowFile(queueId, entries, OtelMetricSchema.SCHEMA,
                    (batch, root) -> MetricBatchWriter.write(batch, root, writeDescription, attributesStringCopy, scan));
            // The mappings this batch is the first to carry ride its registration transaction, so
            // they are settled by its own outcome rather than written on a timer.
            // Null rather than an empty claim when the queue records nothing: a claim is a thing
            // to be settled exactly once, and there is nothing to settle here.
            AttributeTypeRegistry.Claim claimed = scan == null ? null
                    : attributeTypes.claim(queueId, Signal.METRICS, scan);
            // addBatch can throw before it returns a future — a rejected queue, or sizing the
            // file — and then nothing would ever settle the claim, so it would stay in flight for
            // the life of the process. Only that call is guarded: the handler below settles on
            // completion, and once it is attached a second settle here would decrement the gauge
            // twice, since settle is not idempotent.
            java.util.concurrent.CompletableFuture<Void> added;
            try {
                added = base.addBatch(queue, arrowFile,
                        scan == null ? null : attributeTypes.sidecar(claimed, typesTable));
            } catch (Throwable t) {
                if (scan != null) {
                    attributeTypes.settle(claimed, false);
                }
                throw t;
            }
            added.whenComplete((v, ex) -> {
                        if (scan != null) {
                            attributeTypes.settle(claimed, ex == null);
                        }
                    })
                    .whenComplete(OtelServiceBase.batchCompleteHandler(arrowFile, metricCount, queueId,
                            sample, metrics,
                            responseObserver, ExportMetricsServiceResponse.getDefaultInstance()));
        } catch (IOException e) {
            metrics.recordError(queueId, sample);
            log.error("Failed to write Arrow file for {} metrics", metricCount, e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void close() {
        base.close();
    }
}
