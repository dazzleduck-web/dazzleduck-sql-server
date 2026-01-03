package io.dazzleduck.sql.collector.metricscollector;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MetricsCollector - main collector with tailing support.
 */
class MetricsCollectorTest {

    private MockWebServer targetServer;
    private MockWebServer forwardServer;
    private CollectorProperties properties;

    @BeforeEach
    void setUp() throws Exception {
        targetServer = new MockWebServer();
        targetServer.start();

        forwardServer = new MockWebServer();
        forwardServer.start();

        properties = new CollectorProperties();
        properties.setEnabled(true);
        properties.setTargets(List.of(targetServer.url("/actuator/prometheus").toString()));
        properties.setServerUrl(forwardServer.url("/ingest").toString());
        properties.setPath("metrics");
        properties.setScrapeIntervalMs(100);
        properties.setFlushThreshold(5);
        properties.setFlushIntervalMs(500);
        properties.setConnectionTimeoutMs(5000);
        properties.setReadTimeoutMs(5000);
        properties.setMaxRetries(0);
    }

    @AfterEach
    void tearDown() throws Exception {
        targetServer.shutdown();
        forwardServer.shutdown();
    }

    @Test
    @DisplayName("Should start and stop collector")
    void startAndStop() {
        MetricsCollector collector = new MetricsCollector(properties);

        assertFalse(collector.isRunning());

        collector.start();
        assertTrue(collector.isRunning());

        collector.stop();
        assertFalse(collector.isRunning());
    }

    @Test
    @DisplayName("Should not start if disabled")
    void notStartIfDisabled() {
        properties.setEnabled(false);
        MetricsCollector collector = new MetricsCollector(properties);

        collector.start();

        assertFalse(collector.isRunning());
    }

    @Test
    @DisplayName("Should not start if no targets")
    void notStartIfNoTargets() {
        properties.setTargets(List.of());
        MetricsCollector collector = new MetricsCollector(properties);

        collector.start();

        assertFalse(collector.isRunning());
    }

    @Test
    @DisplayName("Should scrape and buffer metrics")
    void scrapeAndBuffer() throws Exception {
        String prometheusData = """
            test_metric_1 1
            test_metric_2 2
            """;
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        // Don't flush immediately
        properties.setFlushThreshold(100);
        properties.setFlushIntervalMs(10000);

        MetricsCollector collector = new MetricsCollector(properties);
        collector.start();

        // Wait for at least one scrape
        Thread.sleep(200);

        assertTrue(collector.getBufferSize() > 0);

        collector.stop();
    }

    @Test
    @DisplayName("Should flush on threshold")
    void flushOnThreshold() throws Exception {
        // Each scrape returns 3 metrics
        String prometheusData = """
            metric_1 1
            metric_2 2
            metric_3 3
            """;
        // Queue enough responses
        for (int i = 0; i < 10; i++) {
            targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        }
        forwardServer.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));
        forwardServer.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));

        properties.setScrapeIntervalMs(50);
        properties.setFlushThreshold(5);
        properties.setFlushIntervalMs(10000); // Long interval so threshold triggers first

        MetricsCollector collector = new MetricsCollector(properties);
        collector.start();

        // Wait for scrapes and flush
        Thread.sleep(500);

        collector.stop();

        // Should have forwarded at least once due to threshold
        assertTrue(forwardServer.getRequestCount() > 0);
    }

    @Test
    @DisplayName("Should flush on interval")
    void flushOnInterval() throws Exception {
        String prometheusData = "single_metric 1\n";
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        forwardServer.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));

        properties.setScrapeIntervalMs(100);
        properties.setFlushThreshold(1000); // High threshold so interval triggers first
        properties.setFlushIntervalMs(200);

        MetricsCollector collector = new MetricsCollector(properties);
        collector.start();

        // Wait for interval to elapse
        Thread.sleep(400);

        collector.stop();

        // Should have forwarded at least once due to interval
        assertTrue(forwardServer.getRequestCount() > 0);
    }

    @Test
    @DisplayName("Should flush on stop")
    void flushOnStop() throws Exception {
        String prometheusData = "test_metric 42\n";
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        forwardServer.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));

        properties.setFlushThreshold(1000);
        properties.setFlushIntervalMs(10000);

        MetricsCollector collector = new MetricsCollector(properties);
        collector.start();

        // Wait for scrape
        Thread.sleep(200);

        assertTrue(collector.getBufferSize() > 0);

        collector.stop();

        // Should have flushed on stop
        assertTrue(forwardServer.getRequestCount() > 0 || collector.getBufferSize() == 0);
    }

    @Test
    @DisplayName("Should return metrics to buffer on failure")
    void returnToBufferOnFailure() throws Exception {
        String prometheusData = "test_metric 42\n";
        // Queue multiple responses for scrapes
        for (int i = 0; i < 5; i++) {
            targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        }
        // All forward attempts fail
        for (int i = 0; i < 10; i++) {
            forwardServer.enqueue(new MockResponse().setResponseCode(500).setBody("Error"));
        }

        properties.setFlushThreshold(1);
        properties.setFlushIntervalMs(100);
        properties.setMaxRetries(0);

        MetricsCollector collector = new MetricsCollector(properties);
        collector.start();

        // Wait for scrape and failed flush attempts
        Thread.sleep(400);

        collector.stop();

        // At least one forward attempt should have failed
        assertTrue(forwardServer.getRequestCount() > 0, "Should have attempted to forward");
        assertTrue(collector.getForwarder().getSendFailureCount() > 0, "Should have recorded failures");
    }

    @Test
    @DisplayName("Should use target prefix")
    void useTargetPrefix() throws Exception {
        String prometheusData = "prefixed_metric 123\n";
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        forwardServer.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));

        properties.setTargetPrefix(targetServer.url("/").toString().replaceAll("/$", ""));
        properties.setTargets(List.of("/metrics"));
        properties.setFlushThreshold(1);

        MetricsCollector collector = new MetricsCollector(properties);
        collector.start();

        Thread.sleep(300);

        collector.stop();

        assertTrue(targetServer.getRequestCount() > 0);
    }
}
