package io.dazzleduck.sql.collector.metricscollector;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MetricsForwarder - Arrow serialization and HTTP forwarding.
 */
class MetricsForwarderTest {

    private MockWebServer server;
    private CollectorProperties properties;
    private MetricsForwarder forwarder;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();

        properties = new CollectorProperties();
        properties.setServerUrl(server.url("/ingest").toString());
        properties.setPath("scraped_metrics");
        properties.setConnectionTimeoutMs(5000);
        properties.setReadTimeoutMs(5000);
        properties.setMaxRetries(1);
        properties.setRetryDelayMs(100);

        forwarder = new MetricsForwarder(properties);
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    @DisplayName("Should forward metrics successfully")
    void sendMetrics_Success() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));

        List<CollectedMetric> metrics = List.of(createMetric("test_metric", 100.0));

        boolean result = forwarder.sendMetrics(metrics);

        assertTrue(result);
        assertEquals(1, server.getRequestCount());

        RecordedRequest request = server.takeRequest();
        assertEquals("POST", request.getMethod());
        assertEquals("application/vnd.apache.arrow.stream", request.getHeader("Content-Type"));
        assertTrue(request.getPath().contains("path=scraped_metrics"));
    }

    @Test
    @DisplayName("Should return true for empty metrics list")
    void sendMetrics_EmptyList() {
        boolean result = forwarder.sendMetrics(List.of());

        assertTrue(result);
        assertEquals(0, server.getRequestCount());
    }

    @Test
    @DisplayName("Should retry on failure")
    void sendMetrics_Retry() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("Error"));
        server.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));

        List<CollectedMetric> metrics = List.of(createMetric("test_metric", 100.0));

        boolean result = forwarder.sendMetrics(metrics);

        assertTrue(result);
        assertEquals(2, server.getRequestCount());
    }

    @Test
    @DisplayName("Should fail after max retries")
    void sendMetrics_FailAfterRetries() {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("Error"));
        server.enqueue(new MockResponse().setResponseCode(500).setBody("Error"));
        server.enqueue(new MockResponse().setResponseCode(500).setBody("Error"));

        List<CollectedMetric> metrics = List.of(createMetric("test_metric", 100.0));

        boolean result = forwarder.sendMetrics(metrics);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should send valid Arrow format")
    void sendMetrics_ValidArrowFormat() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));

        List<CollectedMetric> metrics = List.of(
            new CollectedMetric(
                "test_counter",
                "counter",
                "http://app:8080/metrics",
                "collector-1",
                "my-collector",
                "host1",
                Map.of("env", "prod", "region", "us-east"),
                999.0
            )
        );

        boolean result = forwarder.sendMetrics(metrics);

        assertTrue(result);

        RecordedRequest request = server.takeRequest();
        byte[] arrowBytes = request.getBody().readByteArray();

        try (BufferAllocator allocator = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(
                 Channels.newChannel(new ByteArrayInputStream(arrowBytes)), allocator)) {

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertTrue(reader.loadNextBatch());
            assertEquals(1, root.getRowCount());

            // Verify fields exist
            assertNotNull(root.getVector("name"));
            assertNotNull(root.getVector("type"));
            assertNotNull(root.getVector("source_url"));
            assertNotNull(root.getVector("collector_id"));
            assertNotNull(root.getVector("labels"));
            assertNotNull(root.getVector("value"));

            // Verify data
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            assertEquals("test_counter", new String(nameVector.get(0)));

            Float8Vector valueVector = (Float8Vector) root.getVector("value");
            assertEquals(999.0, valueVector.get(0), 0.001);
        }
    }

    @Test
    @DisplayName("Should handle multiple metrics in batch")
    void sendMetrics_MultipleBatch() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));

        List<CollectedMetric> metrics = List.of(
            createMetric("metric_1", 1.0),
            createMetric("metric_2", 2.0),
            createMetric("metric_3", 3.0)
        );

        boolean result = forwarder.sendMetrics(metrics);

        assertTrue(result);

        RecordedRequest request = server.takeRequest();
        byte[] arrowBytes = request.getBody().readByteArray();

        try (BufferAllocator allocator = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(
                 Channels.newChannel(new ByteArrayInputStream(arrowBytes)), allocator)) {

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertTrue(reader.loadNextBatch());
            assertEquals(3, root.getRowCount());
        }
    }

    @Test
    @DisplayName("Should track sent count")
    void sendMetrics_TracksCount() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));

        List<CollectedMetric> metrics = List.of(
            createMetric("metric_1", 1.0),
            createMetric("metric_2", 2.0)
        );

        assertEquals(0, forwarder.getMetricsSentCount());

        forwarder.sendMetrics(metrics);

        assertEquals(2, forwarder.getMetricsSentCount());
        assertEquals(1, forwarder.getSendSuccessCount());
    }

    @Test
    @DisplayName("Should track failure count")
    void sendMetrics_TracksFailures() {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("Error"));
        server.enqueue(new MockResponse().setResponseCode(500).setBody("Error"));

        List<CollectedMetric> metrics = List.of(createMetric("metric_1", 1.0));

        forwarder.sendMetrics(metrics);

        assertEquals(1, forwarder.getSendFailureCount());
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
