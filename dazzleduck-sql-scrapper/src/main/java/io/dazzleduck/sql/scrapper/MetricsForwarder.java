package io.dazzleduck.sql.scrapper;

import io.dazzleduck.sql.client.HttpArrowProducer;
import io.dazzleduck.sql.common.types.JavaRow;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
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

    // Column indices — must match ArrowMetricSchema from dazzleduck-sql-micrometer
    private static final int COL_TIMESTAMP = 0;
    private static final int COL_NAME = 1;
    private static final int COL_TYPE = 2;
    private static final int COL_TAGS = 3;
    private static final int COL_VALUE = 4;
    private static final int COL_MIN = 5;
    private static final int COL_MAX = 6;
    private static final int COL_MEAN = 7;

    // Simple counters for monitoring
    private final AtomicLong metricsSentCount = new AtomicLong(0);
    private final AtomicLong metricsDroppedCount = new AtomicLong(0);

    public MetricsForwarder(CollectorProperties properties) {
        this.properties = properties;

        // Arrow schema — matches ArrowMetricSchema from dazzleduck-sql-micrometer exactly
        this.arrowSchema = new Schema(List.of(
            new Field("timestamp", FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
            new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
            new Field("type", FieldType.notNullable(new ArrowType.Utf8()), null),
            new Field("tags", FieldType.notNullable(new ArrowType.Map(false)), List.of(
                new Field("entries", FieldType.notNullable(new ArrowType.Struct()), List.of(
                    new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                    new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)
                ))
            )),
            new Field("value", fp(), null),
            new Field("min", fp(), null),
            new Field("max", fp(), null),
            new Field("mean", fp(), null)
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
            properties.getProject(),  // transformations
            properties.getPartition(),  // partitionBy
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
        Object[] values = new Object[8];
        values[COL_TIMESTAMP] = metric.timestamp().toEpochMilli();
        values[COL_NAME] = metric.name();
        values[COL_TYPE] = metric.type();
        values[COL_TAGS] = metric.tags();

        double value = metric.value();
        values[COL_VALUE] = (Double.isNaN(value) || Double.isInfinite(value)) ? null : value;

        values[COL_MIN] = metric.min();
        values[COL_MAX] = metric.max();
        values[COL_MEAN] = metric.mean();

        return new JavaRow(values);
    }

    private static FieldType fp() {
        return FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
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
