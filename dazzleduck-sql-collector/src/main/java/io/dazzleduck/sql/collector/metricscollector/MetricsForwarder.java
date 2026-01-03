package io.dazzleduck.sql.collector.metricscollector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Forwards collected metrics to a remote server in Apache Arrow IPC format.
 * Includes retry logic with exponential backoff.
 */
public class MetricsForwarder {

    private static final Logger log = LoggerFactory.getLogger(MetricsForwarder.class);

    private final CollectorProperties properties;
    private final HttpClient httpClient;
    private final Schema arrowSchema;

    // Simple counters for monitoring
    private final AtomicLong metricsSentCount = new AtomicLong(0);
    private final AtomicLong metricsDroppedCount = new AtomicLong(0);
    private final AtomicLong sendSuccessCount = new AtomicLong(0);
    private final AtomicLong sendFailureCount = new AtomicLong(0);

    public MetricsForwarder(CollectorProperties properties) {
        this.properties = properties;

        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(properties.getConnectionTimeoutMs()))
            .build();

        // Arrow schema for collected metrics
        this.arrowSchema = new Schema(List.of(
            new Field("timestamp", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
            new Field("type", FieldType.notNullable(new ArrowType.Utf8()), null),
            new Field("source_url", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("collector_id", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("collector_name", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("collector_host", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("labels", FieldType.notNullable(new ArrowType.Map(false)), List.of(
                new Field("entries", FieldType.notNullable(new ArrowType.Struct()), List.of(
                    new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                    new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)
                ))
            )),
            new Field("value", FieldType.nullable(new ArrowType.FloatingPoint(
                org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null)
        ));
    }

    /**
     * Send collected metrics to the remote server as Arrow bytes.
     * Returns true if successful, false otherwise.
     */
    public boolean sendMetrics(List<CollectedMetric> metrics) {
        if (metrics.isEmpty()) {
            return true;
        }

        byte[] arrowBytes = convertToArrowBytes(metrics);
        if (arrowBytes == null || arrowBytes.length == 0) {
            log.error("Failed to serialize metrics to Arrow format");
            metricsDroppedCount.addAndGet(metrics.size());
            return false;
        }

        String url = properties.getServerUrl() + "?path=" + properties.getPath();

        int retries = 0;
        int maxRetries = properties.getMaxRetries();
        int retryDelay = properties.getRetryDelayMs();

        while (retries <= maxRetries) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/vnd.apache.arrow.stream")
                    .timeout(Duration.ofMillis(properties.getReadTimeoutMs()))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(arrowBytes))
                    .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    metricsSentCount.addAndGet(metrics.size());
                    sendSuccessCount.incrementAndGet();
                    log.debug("Successfully forwarded {} metrics ({} bytes)",
                        metrics.size(), arrowBytes.length);
                    return true;
                } else {
                    log.warn("Forwarding failed with status {}: {}",
                        response.statusCode(), response.body());
                }

            } catch (IOException e) {
                log.warn("Forwarding IO error (attempt {}): {}", retries + 1, e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Forwarding interrupted");
                return false;
            }

            retries++;
            if (retries <= maxRetries) {
                try {
                    Thread.sleep(retryDelay * (long) Math.pow(2, retries - 1));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }

        sendFailureCount.incrementAndGet();
        log.error("Failed to forward {} metrics after {} retries", metrics.size(), maxRetries);
        return false;
    }

    /**
     * Convert collected metrics to Arrow IPC format.
     */
    private byte[] convertToArrowBytes(List<CollectedMetric> metrics) {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            BigIntVector timestampVector = (BigIntVector) root.getVector("timestamp");
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            VarCharVector typeVector = (VarCharVector) root.getVector("type");
            VarCharVector sourceUrlVector = (VarCharVector) root.getVector("source_url");
            VarCharVector collectorIdVector = (VarCharVector) root.getVector("collector_id");
            VarCharVector collectorNameVector = (VarCharVector) root.getVector("collector_name");
            VarCharVector collectorHostVector = (VarCharVector) root.getVector("collector_host");
            MapVector labelsVector = (MapVector) root.getVector("labels");
            Float8Vector valueVector = (Float8Vector) root.getVector("value");

            root.allocateNew();
            UnionMapWriter labelsWriter = labelsVector.getWriter();

            for (int i = 0; i < metrics.size(); i++) {
                CollectedMetric metric = metrics.get(i);

                timestampVector.setSafe(i, metric.timestamp().toEpochMilli());
                setSafeString(nameVector, i, metric.name());
                setSafeString(typeVector, i, metric.type());
                setSafeString(sourceUrlVector, i, metric.sourceUrl());
                setSafeString(collectorIdVector, i, metric.collectorId());
                setSafeString(collectorNameVector, i, metric.collectorName());
                setSafeString(collectorHostVector, i, metric.collectorHost());

                // Write labels map
                labelsWriter.setPosition(i);
                labelsWriter.startMap();
                for (var entry : metric.labels().entrySet()) {
                    labelsWriter.startEntry();
                    labelsWriter.key().varChar().writeVarChar(entry.getKey());
                    if (entry.getValue() != null) {
                        labelsWriter.value().varChar().writeVarChar(entry.getValue());
                    }
                    labelsWriter.endEntry();
                }
                labelsWriter.endMap();

                double value = metric.value();
                if (Double.isNaN(value) || Double.isInfinite(value)) {
                    valueVector.setNull(i);
                } else {
                    valueVector.setSafe(i, value);
                }
            }

            root.setRowCount(metrics.size());

            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            return out.toByteArray();

        } catch (Exception e) {
            log.error("Failed to convert metrics to Arrow format: {}", e.getMessage());
            return null;
        }
    }

    private void setSafeString(VarCharVector vector, int index, String value) {
        if (value != null) {
            vector.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
        } else {
            vector.setNull(index);
        }
    }

    // Getters for monitoring
    public long getMetricsSentCount() {
        return metricsSentCount.get();
    }

    public long getMetricsDroppedCount() {
        return metricsDroppedCount.get();
    }

    public long getSendSuccessCount() {
        return sendSuccessCount.get();
    }

    public long getSendFailureCount() {
        return sendFailureCount.get();
    }
}
