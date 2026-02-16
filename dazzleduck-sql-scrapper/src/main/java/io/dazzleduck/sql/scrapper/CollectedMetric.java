package io.dazzleduck.sql.scrapper;

import java.time.Instant;
import java.util.Map;

/**
 * Represents a single collected metric entry from a Prometheus endpoint.
 */
public record CollectedMetric(
    Instant timestamp,
    String name,
    String type,
    String sourceUrl,
    String collectorId,
    String collectorName,
    String collectorHost,
    Map<String, String> labels,
    double value
) {
    /**
     * Create a metric with current timestamp.
     */
    public CollectedMetric(
            String name,
            String type,
            String sourceUrl,
            String collectorId,
            String collectorName,
            String collectorHost,
            Map<String, String> labels,
            double value) {
        this(Instant.now(), name, type, sourceUrl,
             collectorId, collectorName, collectorHost, labels, value);
    }
}
