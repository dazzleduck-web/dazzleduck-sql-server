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

    private static final Logger log = LoggerFactory.getLogger(OtelMetricsService.class);

    private final OtelCollectorMetrics metrics;
    private final OtelServiceBase base;

    public OtelMetricsService(IngestionHandler handler, IngestionConfig ingestionConfig,
                              ScheduledExecutorService flushScheduler, OtelCollectorMetrics metrics) throws IOException {
        this.metrics = metrics;
        this.base = new OtelServiceBase("otel-metrics-arrow-", handler, ingestionConfig, flushScheduler, metrics);
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
            Path arrowFile = base.writeArrowFile(queueId, entries, OtelMetricSchema.SCHEMA, MetricBatchWriter::write);
            base.addBatch(queue, arrowFile).whenComplete(
                    OtelServiceBase.batchCompleteHandler(arrowFile, metricCount, queueId,
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
