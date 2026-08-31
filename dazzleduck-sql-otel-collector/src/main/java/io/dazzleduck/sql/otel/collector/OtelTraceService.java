package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.ParquetIngestionQueue;
import io.dazzleduck.sql.otel.collector.auth.JwtServerInterceptor;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * gRPC service that receives OTLP trace exports and writes them to Parquet.
 * Queue routing is driven by the {@code x-dd-ingestion-queue} JWT claim.
 */
public class OtelTraceService extends TraceServiceGrpc.TraceServiceImplBase implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(OtelTraceService.class);

    private final OtelCollectorMetrics metrics;
    private final OtelServiceBase base;

    public OtelTraceService(IngestionHandler handler, IngestionConfig ingestionConfig,
                            ScheduledExecutorService flushScheduler, OtelCollectorMetrics metrics) throws IOException {
        this.metrics = metrics;
        this.base = new OtelServiceBase("otel-traces-arrow-", handler, ingestionConfig, flushScheduler, metrics);
    }

    @Override
    public void export(ExportTraceServiceRequest request,
                       StreamObserver<ExportTraceServiceResponse> responseObserver) {
        ParquetIngestionQueue queue = base.resolveQueue(responseObserver);
        if (queue == null) return;

        List<SpanEntry> entries = new ArrayList<>();
        for (ResourceSpans resourceSpans : request.getResourceSpansList()) {
            var resource = resourceSpans.hasResource() ? resourceSpans.getResource() : null;
            for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
                var scope = scopeSpans.hasScope() ? scopeSpans.getScope() : null;
                for (var span : scopeSpans.getSpansList()) {
                    entries.add(new SpanEntry(span, resource, scope));
                }
            }
        }
        int spanCount = entries.size();
        String queueId = JwtServerInterceptor.QUEUE_CONTEXT_KEY.get();
        log.debug("Received {} spans → queue '{}'", spanCount, queueId);
        var sample = metrics.startSample();

        try {
            Path arrowFile = base.writeArrowFile(queueId, entries, OtelTraceSchema.SCHEMA, SpanBatchWriter::write);
            base.addBatch(queue, arrowFile).whenComplete(
                    OtelServiceBase.batchCompleteHandler(arrowFile, spanCount, queueId,
                            sample, metrics,
                            responseObserver, ExportTraceServiceResponse.getDefaultInstance()));
        } catch (IOException e) {
            metrics.recordError(queueId, sample);
            log.error("Failed to write Arrow file for {} spans", spanCount, e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void close() {
        base.close();
    }
}
