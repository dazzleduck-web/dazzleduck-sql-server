package io.dazzleduck.sql.collector.metricscollector;

import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MetricsBuffer - thread-safe buffer operations.
 */
class MetricsBufferTest {

    private MetricsBuffer buffer;

    @BeforeEach
    void setUp() {
        buffer = new MetricsBuffer(100);
    }

    @Test
    @DisplayName("Should add and drain metrics")
    void addAndDrain() {
        CollectedMetric metric = createMetric("test_metric", 42.0);

        buffer.add(metric);

        assertEquals(1, buffer.getSize());
        assertFalse(buffer.isEmpty());

        List<CollectedMetric> drained = buffer.drain();

        assertEquals(1, drained.size());
        assertEquals("test_metric", drained.get(0).name());
        assertEquals(0, buffer.getSize());
        assertTrue(buffer.isEmpty());
    }

    @Test
    @DisplayName("Should add all metrics from list")
    void addAll() {
        List<CollectedMetric> metrics = List.of(
            createMetric("metric_1", 1.0),
            createMetric("metric_2", 2.0),
            createMetric("metric_3", 3.0)
        );

        buffer.addAll(metrics);

        assertEquals(3, buffer.getSize());
    }

    @Test
    @DisplayName("Should drop oldest metrics when buffer is full")
    void dropsOldestWhenFull() {
        buffer = new MetricsBuffer(3);

        buffer.add(createMetric("metric_1", 1.0));
        buffer.add(createMetric("metric_2", 2.0));
        buffer.add(createMetric("metric_3", 3.0));
        buffer.add(createMetric("metric_4", 4.0));

        assertEquals(3, buffer.getSize());

        List<CollectedMetric> drained = buffer.drain();
        assertEquals(3, drained.size());

        // First metric should have been dropped
        assertFalse(drained.stream().anyMatch(m -> m.name().equals("metric_1")));
        assertTrue(drained.stream().anyMatch(m -> m.name().equals("metric_2")));
        assertTrue(drained.stream().anyMatch(m -> m.name().equals("metric_3")));
        assertTrue(drained.stream().anyMatch(m -> m.name().equals("metric_4")));
    }

    @Test
    @DisplayName("Should return metrics for retry")
    void returnForRetry() {
        buffer.add(createMetric("original", 1.0));

        List<CollectedMetric> drained = buffer.drain();
        assertEquals(1, drained.size());
        assertTrue(buffer.isEmpty());

        buffer.returnForRetry(drained);

        assertEquals(1, buffer.getSize());
        List<CollectedMetric> drainedAgain = buffer.drain();
        assertEquals("original", drainedAgain.get(0).name());
    }

    @Test
    @DisplayName("Should clear buffer")
    void clear() {
        buffer.add(createMetric("metric_1", 1.0));
        buffer.add(createMetric("metric_2", 2.0));

        assertEquals(2, buffer.getSize());

        buffer.clear();

        assertEquals(0, buffer.getSize());
        assertTrue(buffer.isEmpty());
    }

    @Test
    @DisplayName("Should drain empty buffer")
    void drainEmpty() {
        List<CollectedMetric> drained = buffer.drain();

        assertTrue(drained.isEmpty());
    }

    @Test
    @DisplayName("Should handle concurrent access")
    void concurrentAccess() throws InterruptedException {
        buffer = new MetricsBuffer(10000);

        Thread producer1 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                buffer.add(createMetric("producer1_" + i, i));
            }
        });

        Thread producer2 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                buffer.add(createMetric("producer2_" + i, i));
            }
        });

        producer1.start();
        producer2.start();
        producer1.join();
        producer2.join();

        assertEquals(2000, buffer.getSize());
    }

    private CollectedMetric createMetric(String name, double value) {
        return new CollectedMetric(
            name,
            "gauge",
            "http://localhost:8080/metrics",
            "test-collector",
            "Test Collector",
            "localhost",
            Map.of(),
            value
        );
    }
}
