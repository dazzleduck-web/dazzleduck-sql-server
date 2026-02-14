package io.dazzleduck.sql.scrapper;

import io.dazzleduck.sql.client.HttpArrowProducer;
import io.dazzleduck.sql.common.types.JavaRow;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Forwards collected metrics to a remote server using HttpArrowProducer from dazzleduck-sql-client.
 * Metrics are sent as Apache Arrow streams with automatic batching and retry logic.
 */
public class MetricsForwarder {

    private static final Logger log = LoggerFactory.getLogger(MetricsForwarder.class);

    private final CollectorProperties properties;
    private final HttpArrowProducer producer;
    private final Schema arrowSchema;

    // Column indices in the schema
    private static final int COL_TIMESTAMP = 0;
    private static final int COL_NAME = 1;
    private static final int COL_TYPE = 2;
    private static final int COL_SOURCE_URL = 3;
    private static final int COL_COLLECTOR_ID = 4;
    private static final int COL_COLLECTOR_NAME = 5;
    private static final int COL_COLLECTOR_HOST = 6;
    private static final int COL_LABELS = 7;
    private static final int COL_VALUE = 8;

    // Simple counters for monitoring
    private final AtomicLong metricsSentCount = new AtomicLong(0);
    private final AtomicLong metricsDroppedCount = new AtomicLong(0);

    public MetricsForwarder(CollectorProperties properties) {
        this.properties = properties;

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

        // Create HttpArrowProducer with configuration from properties
        this.producer = new HttpArrowProducer(
            arrowSchema,
            properties.getBaseUrl(),
            properties.getUsername(),
            properties.getPassword(),
            properties.getPath(),
            Duration.ofMillis(properties.getReadTimeoutMs()),
            properties.getMinBatchSize(),
            properties.getMaxBatchSize(),
            Duration.ofMillis(properties.getFlushIntervalMs()),
            properties.getMaxRetries(),
            properties.getRetryDelayMs(),
            List.of(),  // transformations
            List.of(),  // partitionBy
            properties.getMaxInMemorySize(),
            properties.getMaxOnDiskSize()
        );

        log.info("MetricsForwarder initialized with HttpArrowProducer: serverUrl={}, path={}",
            properties.getServerUrl(), properties.getPath());
    }

    /**
     * Send collected metrics to the remote server.
     * Returns true if metrics were queued successfully.
     */
    public boolean sendMetrics(List<CollectedMetric> metrics) {
        if (metrics.isEmpty()) {
            return true;
        }

        try {
            for (CollectedMetric metric : metrics) {
                JavaRow row = toJavaRow(metric);
                producer.addRow(row);
            }
            metricsSentCount.addAndGet(metrics.size());
            log.debug("Queued {} metrics for sending", metrics.size());
            return true;
        } catch (Exception e) {
            log.error("Failed to queue metrics: {}", e.getMessage());
            metricsDroppedCount.addAndGet(metrics.size());
            return false;
        }
    }

    /**
     * Convert CollectedMetric to JavaRow for Arrow serialization.
     */
    private JavaRow toJavaRow(CollectedMetric metric) {
        Object[] values = new Object[9];
        values[COL_TIMESTAMP] = metric.timestamp().toEpochMilli();
        values[COL_NAME] = metric.name();
        values[COL_TYPE] = metric.type();
        values[COL_SOURCE_URL] = metric.sourceUrl();
        values[COL_COLLECTOR_ID] = metric.collectorId();
        values[COL_COLLECTOR_NAME] = metric.collectorName();
        values[COL_COLLECTOR_HOST] = metric.collectorHost();
        values[COL_LABELS] = convertLabels(metric.labels());

        double value = metric.value();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            values[COL_VALUE] = null;
        } else {
            values[COL_VALUE] = value;
        }

        return new JavaRow(values);
    }

    /**
     * Convert labels map to format expected by Arrow Map type.
     */
    private Map<String, String> convertLabels(Map<String, String> labels) {
        return labels;
    }

    /**
     * Close the forwarder and flush any pending metrics.
     */
    public void close() {
        try {
            producer.close();
            log.info("MetricsForwarder closed");
        } catch (Exception e) {
            log.error("Error closing MetricsForwarder: {}", e.getMessage());
        }
    }

    // Getters for monitoring
    public long getMetricsSentCount() {
        return metricsSentCount.get();
    }

    public long getMetricsDroppedCount() {
        return metricsDroppedCount.get();
    }

    /**
     * Get the Arrow schema used for metrics.
     */
    public Schema getArrowSchema() {
        return arrowSchema;
    }
}
