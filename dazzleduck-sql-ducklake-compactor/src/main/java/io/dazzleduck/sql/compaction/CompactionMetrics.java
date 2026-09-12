package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigConstants;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.instrumentation.micrometer.v1_5.OpenTelemetryMeterRegistry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;

/**
 * Chooses where this service's meters go, and owns the export pipeline when they leave the process.
 *
 * <p>{@link CompactionState} instruments everything with Micrometer, so the meters reach
 * OpenTelemetry through the micrometer-1.5 bridge instead of being rewritten against the OTel
 * metrics API.
 *
 * @param sdk the OpenTelemetry SDK backing {@code registry}, or null when export is disabled
 */
record CompactionMetrics(MeterRegistry registry, OpenTelemetrySdk sdk) implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionMetrics.class);

    private static final String BEARER_PREFIX = "Bearer ";

    /**
     * Falls back to logging meters when export is disabled, so a compactor running without a
     * collector still prints what it would have sent rather than discarding every meter.
     */
    static CompactionMetrics create(Config config) {
        if (!config.getBoolean(ConfigConstants.ENABLED_KEY)) {
            logger.info("OTLP metric export disabled — falling back to the logging registry");
            return new CompactionMetrics(new LoggingMeterRegistry(), null);
        }

        String endpoint = config.getString("endpoint");
        String serviceName = config.getString("service_name");
        Duration interval = config.getDuration("export_interval");

        OtlpGrpcMetricExporterBuilder exporter = OtlpGrpcMetricExporter.builder()
                .setEndpoint(endpoint)
                .setTimeout(config.getDuration("request_timeout"));

        String token = config.hasPath("token") ? config.getString("token").trim() : "";
        if (token.isEmpty()) {
            // Loud on startup rather than a per-export gRPC error nobody reads.
            logger.warn("No OTLP token configured — the collector rejects exports that carry no "
                    + "signed token with an x-dd-ingestion-queue claim");
        } else {
            // The header value is sent verbatim, so a raw token would be rejected on every export.
            exporter.addHeader("Authorization",
                    token.startsWith(BEARER_PREFIX) ? token : BEARER_PREFIX + token);
        }

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .setResource(Resource.getDefault().toBuilder()
                        .put("service.name", serviceName)
                        .build())
                .registerMetricReader(PeriodicMetricReader.builder(exporter.build())
                        .setInterval(interval)
                        .build())
                .build();

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();

        logger.info("Exporting metrics to {} every {}s as service '{}'",
                endpoint, interval.toSeconds(), serviceName);
        return new CompactionMetrics(OpenTelemetryMeterRegistry.create(sdk), sdk);
    }

    @Override
    public void close() {
        if (sdk != null) {
            sdk.close(); // flushes whatever the last interval buffered
        }
    }
}
