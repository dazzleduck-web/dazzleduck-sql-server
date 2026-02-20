package io.dazzleduck.sql.scrapper;

import java.time.Instant;
import java.util.Map;

/**
 * Represents a single collected metric entry from a Prometheus endpoint.
 * Schema matches ArrowMetricSchema from dazzleduck-sql-micrometer.
 */
public record CollectedMetric(
    Instant timestamp,
    String name,
    String type,
    Map<String, String> tags,
    double value,
    double min,
    double max,
    double mean
) {
    /**
     * Create a metric with current timestamp and zero min/max/mean.
     * Used for Prometheus counter/gauge metrics which do not expose aggregates.
     */
    public CollectedMetric(
            String name,
            String type,
            Map<String, String> tags,
            double value) {
        this(Instant.now(), name, type, tags, value, 0.0, 0.0, 0.0);
    }
}
