package io.dazzleduck.sql.collector.metricscollector;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MetricsScraper - Prometheus metrics scraping and parsing.
 */
class MetricsScraperTest {

    private MockWebServer targetServer;
    private CollectorProperties properties;
    private MetricsBuffer buffer;
    private MetricsScraper scraper;

    @BeforeEach
    void setUp() throws Exception {
        targetServer = new MockWebServer();
        targetServer.start();

        properties = new CollectorProperties();
        properties.setEnabled(true);
        properties.setTargets(List.of(targetServer.url("/actuator/prometheus").toString()));
        properties.setCollectorId("test-collector");
        properties.setCollectorName("Test Collector");
        properties.setConnectionTimeoutMs(5000);
        properties.setReadTimeoutMs(5000);

        buffer = new MetricsBuffer(properties.getMaxBufferSize());
        scraper = new MetricsScraper(properties, buffer);
    }

    @AfterEach
    void tearDown() throws Exception {
        targetServer.shutdown();
    }

    @Test
    @DisplayName("Should scrape metrics from target endpoint")
    void scrapeTarget_Success() throws Exception {
        String prometheusData = """
            # HELP http_requests_total Total HTTP requests
            # TYPE http_requests_total counter
            http_requests_total{method="GET",status="200"} 1234
            http_requests_total{method="POST",status="201"} 567
            # HELP process_cpu_usage CPU usage
            # TYPE process_cpu_usage gauge
            process_cpu_usage 0.25
            """;
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        List<CollectedMetric> metrics = scraper.scrapeTarget(targetServer.url("/actuator/prometheus").toString());

        assertEquals(3, metrics.size());

        CollectedMetric getRequest = metrics.stream()
            .filter(m -> m.name().equals("http_requests_total") && "GET".equals(m.labels().get("method")))
            .findFirst()
            .orElseThrow();
        assertEquals("counter", getRequest.type());
        assertEquals(1234.0, getRequest.value());
        assertEquals("200", getRequest.labels().get("status"));

        CollectedMetric cpuUsage = metrics.stream()
            .filter(m -> m.name().equals("process_cpu_usage"))
            .findFirst()
            .orElseThrow();
        assertEquals("gauge", cpuUsage.type());
        assertEquals(0.25, cpuUsage.value());
    }

    @Test
    @DisplayName("Should parse labels correctly")
    void scrapeTarget_ParsesLabels() throws Exception {
        String prometheusData = """
            # TYPE jvm_memory_used gauge
            jvm_memory_used{area="heap",id="G1 Eden Space"} 12345678
            jvm_memory_used{area="nonheap",id="Metaspace"} 87654321
            """;
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        List<CollectedMetric> metrics = scraper.scrapeTarget(targetServer.url("/actuator/prometheus").toString());

        assertEquals(2, metrics.size());

        CollectedMetric heapMetric = metrics.stream()
            .filter(m -> "heap".equals(m.labels().get("area")))
            .findFirst()
            .orElseThrow();
        assertEquals("jvm_memory_used", heapMetric.name());
        assertEquals("G1 Eden Space", heapMetric.labels().get("id"));
        assertEquals(12345678.0, heapMetric.value());
    }

    @Test
    @DisplayName("Should handle metrics without labels")
    void scrapeTarget_NoLabels() throws Exception {
        String prometheusData = """
            # TYPE simple_metric gauge
            simple_metric 42.5
            """;
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        List<CollectedMetric> metrics = scraper.scrapeTarget(targetServer.url("/actuator/prometheus").toString());

        assertEquals(1, metrics.size());
        assertEquals("simple_metric", metrics.get(0).name());
        assertEquals(42.5, metrics.get(0).value());
        assertTrue(metrics.get(0).labels().isEmpty());
    }

    @Test
    @DisplayName("Should handle special values (NaN, Inf)")
    void scrapeTarget_SpecialValues() throws Exception {
        String prometheusData = """
            # TYPE special_values gauge
            special_nan NaN
            special_pos_inf +Inf
            special_neg_inf -Inf
            """;
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        List<CollectedMetric> metrics = scraper.scrapeTarget(targetServer.url("/actuator/prometheus").toString());

        assertEquals(3, metrics.size());

        assertTrue(Double.isNaN(metrics.stream()
            .filter(m -> m.name().equals("special_nan"))
            .findFirst().orElseThrow().value()));

        assertEquals(Double.POSITIVE_INFINITY, metrics.stream()
            .filter(m -> m.name().equals("special_pos_inf"))
            .findFirst().orElseThrow().value());

        assertEquals(Double.NEGATIVE_INFINITY, metrics.stream()
            .filter(m -> m.name().equals("special_neg_inf"))
            .findFirst().orElseThrow().value());
    }

    @Test
    @DisplayName("Should handle scientific notation")
    void scrapeTarget_ScientificNotation() throws Exception {
        String prometheusData = """
            # TYPE scientific gauge
            scientific_small 1.5e-10
            scientific_large 2.5E+8
            """;
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        List<CollectedMetric> metrics = scraper.scrapeTarget(targetServer.url("/actuator/prometheus").toString());

        assertEquals(2, metrics.size());
        assertEquals(1.5e-10, metrics.stream()
            .filter(m -> m.name().equals("scientific_small"))
            .findFirst().orElseThrow().value(), 1e-15);
        assertEquals(2.5e+8, metrics.stream()
            .filter(m -> m.name().equals("scientific_large"))
            .findFirst().orElseThrow().value(), 1);
    }

    @Test
    @DisplayName("Should default to gauge type when TYPE not specified")
    void scrapeTarget_DefaultsToGauge() throws Exception {
        String prometheusData = """
            untyped_metric{label="value"} 123
            """;
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        List<CollectedMetric> metrics = scraper.scrapeTarget(targetServer.url("/actuator/prometheus").toString());

        assertEquals(1, metrics.size());
        assertEquals("gauge", metrics.get(0).type());
    }

    @Test
    @DisplayName("Should throw exception on HTTP error")
    void scrapeTarget_HttpError() {
        targetServer.enqueue(new MockResponse().setResponseCode(500).setBody("Internal Server Error"));

        assertThrows(IOException.class, () ->
            scraper.scrapeTarget(targetServer.url("/actuator/prometheus").toString())
        );
    }

    @Test
    @DisplayName("Should scrape all targets and add to buffer")
    void scrapeAllTargets_AddsToBuffer() throws Exception {
        String prometheusData = """
            # TYPE test_metric counter
            test_metric 100
            """;
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        int count = scraper.scrapeAllTargets();

        assertEquals(1, count);
        assertEquals(1, buffer.getSize());
    }

    @Test
    @DisplayName("Should handle failed targets gracefully")
    void scrapeAllTargets_HandlesErrors() {
        targetServer.enqueue(new MockResponse().setResponseCode(500).setBody("Error"));

        int count = scraper.scrapeAllTargets();

        assertEquals(0, count);
        assertTrue(buffer.isEmpty());
    }

    @Test
    @DisplayName("Should scrape from multiple targets")
    void scrapeAllTargets_MultipleTargets() throws Exception {
        MockWebServer target2 = new MockWebServer();
        target2.start();

        properties.setTargets(List.of(
            targetServer.url("/metrics").toString(),
            target2.url("/metrics").toString()
        ));
        buffer = new MetricsBuffer(properties.getMaxBufferSize());
        scraper = new MetricsScraper(properties, buffer);

        targetServer.enqueue(new MockResponse().setBody("metric_a 1\n").setResponseCode(200));
        target2.enqueue(new MockResponse().setBody("metric_b 2\n").setResponseCode(200));

        int count = scraper.scrapeAllTargets();

        assertEquals(2, count);
        assertEquals(2, buffer.getSize());

        target2.shutdown();
    }

    @Test
    @DisplayName("Should set collector metadata on metrics")
    void scrapeTarget_SetsMetadata() throws Exception {
        String prometheusData = "test_metric 42\n";
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        List<CollectedMetric> metrics = scraper.scrapeTarget(targetServer.url("/actuator/prometheus").toString());

        assertEquals(1, metrics.size());
        CollectedMetric metric = metrics.get(0);
        assertEquals("test-collector", metric.collectorId());
        assertEquals("Test Collector", metric.collectorName());
        assertNotNull(metric.sourceUrl());
        assertNotNull(metric.timestamp());
    }

    @Test
    @DisplayName("Should handle histogram metrics")
    void scrapeTarget_Histogram() throws Exception {
        String prometheusData = """
            # TYPE http_request_duration_seconds histogram
            http_request_duration_seconds_bucket{le="0.1"} 24054
            http_request_duration_seconds_bucket{le="0.5"} 33444
            http_request_duration_seconds_bucket{le="1"} 100392
            http_request_duration_seconds_bucket{le="+Inf"} 144320
            http_request_duration_seconds_sum 53423
            http_request_duration_seconds_count 144320
            """;
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        List<CollectedMetric> metrics = scraper.scrapeTarget(targetServer.url("/actuator/prometheus").toString());

        assertEquals(6, metrics.size());
        assertEquals(6, metrics.stream().filter(m -> "histogram".equals(m.type())).count());
    }

    @Test
    @DisplayName("Should use target prefix for relative URLs")
    void scrapeAllTargets_UsesPrefix() throws Exception {
        properties.setTargetPrefix(targetServer.url("/").toString().replaceAll("/$", ""));
        properties.setTargets(List.of("/metrics"));
        buffer = new MetricsBuffer(properties.getMaxBufferSize());
        scraper = new MetricsScraper(properties, buffer);

        targetServer.enqueue(new MockResponse().setBody("prefixed_metric 99\n").setResponseCode(200));

        int count = scraper.scrapeAllTargets();

        assertEquals(1, count);
        assertEquals(1, buffer.getSize());
    }
}
