package io.dazzleduck.sql.scrapper;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MetricsForwarder - queuing metrics to HttpArrowProducer.
 *
 * Note: HttpArrowProducer handles actual HTTP sending asynchronously with authentication,
 * so these tests focus on the queuing behavior and schema validation.
 */
class MetricsForwarderTest {

    private CollectorProperties properties;

    @BeforeEach
    void setUp() {
        properties = new CollectorProperties();
        properties.setServerUrl("http://localhost:8081");
        properties.setPath("test_metrics");
        properties.setUsername("test");
        properties.setPassword("test");
        properties.setConnectionTimeoutMs(100);
        properties.setReadTimeoutMs(100);
        properties.setMaxRetries(0);
        properties.setRetryDelayMs(10);
        properties.setFlushIntervalMs(100);
        properties.setMinBatchSize(1024);
        properties.setMaxBatchSize(16 * 1024 * 1024);
        properties.setMaxInMemorySize(10 * 1024 * 1024);
        properties.setMaxOnDiskSize(1024 * 1024 * 1024);
    }

    @Test
    @DisplayName("Should return true for empty metrics list")
    void sendMetrics_EmptyList() {
        MetricsForwarder forwarder = new MetricsForwarder(properties);
        try {
            boolean result = forwarder.sendMetrics(List.of());
            assertTrue(result);
            assertEquals(0, forwarder.getMetricsSentCount());
        } finally {
            forwarder.close();
        }
    }

    @Test
    @DisplayName("Should queue metrics successfully")
    void sendMetrics_QueueSuccess() {
        MetricsForwarder forwarder = new MetricsForwarder(properties);
        try {
            List<CollectedMetric> metrics = List.of(createMetric("test_metric", 100.0));

            boolean result = forwarder.sendMetrics(metrics);

            assertTrue(result, "Should return true when metrics are queued");
            assertEquals(1, forwarder.getMetricsSentCount());
        } finally {
            forwarder.close();
        }
    }

    @Test
    @DisplayName("Should queue multiple metrics")
    void sendMetrics_MultipleMetrics() {
        MetricsForwarder forwarder = new MetricsForwarder(properties);
        try {
            List<CollectedMetric> metrics = List.of(
                createMetric("metric_1", 1.0),
                createMetric("metric_2", 2.0),
                createMetric("metric_3", 3.0)
            );

            boolean result = forwarder.sendMetrics(metrics);

            assertTrue(result);
            assertEquals(3, forwarder.getMetricsSentCount());
        } finally {
            forwarder.close();
        }
    }

    @Test
    @DisplayName("Should track cumulative sent count")
    void sendMetrics_CumulativeCount() {
        MetricsForwarder forwarder = new MetricsForwarder(properties);
        try {
            assertEquals(0, forwarder.getMetricsSentCount());

            forwarder.sendMetrics(List.of(createMetric("m1", 1.0)));
            assertEquals(1, forwarder.getMetricsSentCount());

            forwarder.sendMetrics(List.of(createMetric("m2", 2.0), createMetric("m3", 3.0)));
            assertEquals(3, forwarder.getMetricsSentCount());
        } finally {
            forwarder.close();
        }
    }

    @Test
    @DisplayName("Should have correct Arrow schema")
    void getArrowSchema_HasCorrectFields() {
        MetricsForwarder forwarder = new MetricsForwarder(properties);
        try {
            Schema schema = forwarder.getArrowSchema();

            assertNotNull(schema);
            assertEquals(9, schema.getFields().size());

            assertEquals("timestamp", schema.getFields().get(0).getName());
            assertEquals("name", schema.getFields().get(1).getName());
            assertEquals("type", schema.getFields().get(2).getName());
            assertEquals("source_url", schema.getFields().get(3).getName());
            assertEquals("collector_id", schema.getFields().get(4).getName());
            assertEquals("collector_name", schema.getFields().get(5).getName());
            assertEquals("collector_host", schema.getFields().get(6).getName());
            assertEquals("labels", schema.getFields().get(7).getName());
            assertEquals("value", schema.getFields().get(8).getName());
        } finally {
            forwarder.close();
        }
    }

    @Test
    @DisplayName("Should handle metrics with special double values")
    void sendMetrics_SpecialValues() {
        MetricsForwarder forwarder = new MetricsForwarder(properties);
        try {
            List<CollectedMetric> metrics = List.of(
                createMetric("nan_metric", Double.NaN),
                createMetric("inf_metric", Double.POSITIVE_INFINITY),
                createMetric("neg_inf_metric", Double.NEGATIVE_INFINITY)
            );

            boolean result = forwarder.sendMetrics(metrics);

            assertTrue(result, "Should handle NaN and Infinity values");
            assertEquals(3, forwarder.getMetricsSentCount());
        } finally {
            forwarder.close();
        }
    }

    @Test
    @DisplayName("Should handle metrics with labels")
    void sendMetrics_WithLabels() {
        MetricsForwarder forwarder = new MetricsForwarder(properties);
        try {
            CollectedMetric metric = new CollectedMetric(
                "labeled_metric",
                "counter",
                "http://app:8080/metrics",
                "collector-1",
                "my-collector",
                "host1",
                Map.of("env", "prod", "region", "us-east", "service", "api"),
                999.0
            );

            boolean result = forwarder.sendMetrics(List.of(metric));

            assertTrue(result);
            assertEquals(1, forwarder.getMetricsSentCount());
        } finally {
            forwarder.close();
        }
    }

    private CollectedMetric createMetric(String name, double value) {
        return new CollectedMetric(
            name,
            "gauge",
            "http://localhost:8080/metrics",
            "test-collector",
            "Test Collector",
            "localhost",
            Map.of("env", "test"),
            value
        );
    }
}
