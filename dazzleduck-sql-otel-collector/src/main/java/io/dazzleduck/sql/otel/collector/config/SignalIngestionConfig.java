package io.dazzleduck.sql.otel.collector.config;

import java.util.List;

/**
 * Per-signal ingestion configuration (logs, traces, or metrics).
 *
 * <p>The {@code transformation} is expressed as a comma-separated list of SQL expressions
 * that are appended as {@code SELECT *, <transformation> FROM __this} before the COPY command.
 * Example: {@code "CAST(timestamp AS DATE) AS date, severity_text AS level"}
 *
 * <p>Transformations are refreshed every 2 minutes by the {@link io.dazzleduck.sql.otel.collector.SignalWriter}
 * scheduler via {@link io.dazzleduck.sql.commons.ingestion.IngestionHandler#getTransformation}.
 */
public record SignalIngestionConfig(
        String outputPath,
        List<String> partitionBy,
        String transformation,
        long minBucketSizeBytes,
        long maxDelayMs
) {}
