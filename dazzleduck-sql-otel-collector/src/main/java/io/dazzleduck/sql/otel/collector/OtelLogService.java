package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.ParquetIngestionQueue;
import io.dazzleduck.sql.otel.collector.auth.JwtServerInterceptor;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * gRPC service that receives OTLP log exports and writes them to Parquet.
 * Queue routing is driven by the {@code x-dd-ingestion-queue} JWT claim.
 */
public class OtelLogService extends LogsServiceGrpc.LogsServiceImplBase implements Closeable {

    /** Prefix for this service's scratch directory, created by {@link OtelCollectorServer}. */
    static final String SCRATCH_PREFIX = "otel-logs-arrow-";

    private static final Logger log = LoggerFactory.getLogger(OtelLogService.class);

    private final OtelCollectorMetrics metrics;
    private final OtelServiceBase base;
    private final boolean attributesStringCopy;
    private final AttributeTypeRegistry attributeTypes;

    public OtelLogService(Path scratchDir, IngestionHandler handler, IngestionConfig ingestionConfig,
                          ScheduledExecutorService flushScheduler, OtelCollectorMetrics metrics) {
        this(scratchDir, handler, ingestionConfig, flushScheduler, metrics, true);
    }

    /** @param attributesStringCopy see {@code attributes.string_copy} in reference.conf */
    public OtelLogService(Path scratchDir, IngestionHandler handler, IngestionConfig ingestionConfig,
                          ScheduledExecutorService flushScheduler, OtelCollectorMetrics metrics,
                          boolean attributesStringCopy) {
        this(scratchDir, handler, ingestionConfig, flushScheduler, metrics, attributesStringCopy, null);
    }

    /** @param attributeTypes records the attribute name/type mappings seen; null records none */
    public OtelLogService(Path scratchDir, IngestionHandler handler, IngestionConfig ingestionConfig,
                          ScheduledExecutorService flushScheduler, OtelCollectorMetrics metrics,
                          boolean attributesStringCopy, AttributeTypeRegistry attributeTypes) {
        this.metrics = metrics;
        this.attributesStringCopy = attributesStringCopy;
        this.attributeTypes = attributeTypes;
        this.base = new OtelServiceBase(scratchDir, handler, ingestionConfig, flushScheduler, metrics);
    }

    @Override
    public void export(ExportLogsServiceRequest request,
                       StreamObserver<ExportLogsServiceResponse> responseObserver) {
        ParquetIngestionQueue queue = base.resolveQueue(responseObserver);
        if (queue == null) return;

        List<LogEntry> entries = new ArrayList<>();
        for (ResourceLogs resourceLogs : request.getResourceLogsList()) {
            var resource = resourceLogs.hasResource() ? resourceLogs.getResource() : null;
            for (ScopeLogs scopeLogs : resourceLogs.getScopeLogsList()) {
                var scope = scopeLogs.hasScope() ? scopeLogs.getScope() : null;
                for (var record : scopeLogs.getLogRecordsList()) {
                    entries.add(new LogEntry(record, resource, scope));
                }
            }
        }
        int recordCount = entries.size();
        String queueId = JwtServerInterceptor.QUEUE_CONTEXT_KEY.get();
        log.debug("Received {} log records → queue '{}'", recordCount, queueId);
        var sample = metrics.startSample();

        try {
            String typesTable = base.attributeTypesTable(queueId);
            AttributeTypeScan scan = AttributeTypeRegistry.scanFor(attributeTypes, queueId, typesTable);
            Path arrowFile = base.writeArrowFile(queueId, entries, OtelLogSchema.SCHEMA,
                    (batch, root) -> LogRecordBatchWriter.write(batch, root, attributesStringCopy, scan));
            // The mappings this batch is the first to carry ride its registration transaction, so
            // they are settled by its own outcome rather than written on a timer.
            // Null rather than an empty claim when the queue records nothing: a claim is a thing
            // to be settled exactly once, and there is nothing to settle here.
            AttributeTypeRegistry.Claim claimed = scan == null ? null
                    : attributeTypes.claim(queueId, Signal.LOGS, scan);
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
                    .whenComplete(OtelServiceBase.batchCompleteHandler(arrowFile, recordCount, queueId,
                            sample, metrics,
                            responseObserver, ExportLogsServiceResponse.getDefaultInstance()));
        } catch (IOException e) {
            metrics.recordError(queueId, sample);
            log.error("Failed to write Arrow file for {} log records", recordCount, e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void close() {
        base.close();
    }
}
