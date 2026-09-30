package io.dazzleduck.sql.compaction;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.filter.ThresholdFilter;
import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigConstants;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
import io.opentelemetry.instrumentation.micrometer.v1_5.OpenTelemetryMeterRegistry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * Chooses where this service's meters and log lines go, and owns the export pipeline when they
 * leave the process.
 *
 * <p>{@link CompactionState} instruments everything with Micrometer, so the meters reach
 * OpenTelemetry through the micrometer-1.5 bridge instead of being rewritten against the OTel
 * metrics API. Log lines keep going through SLF4J/Logback; when log export is on, an
 * {@link OpenTelemetryAppender} is attached to the root logger next to the console appender.
 *
 * <p>Each signal needs its own token: the collector routes every export by the token's
 * {@code x-dd-ingestion-queue} claim regardless of signal, and log and metric queues have different
 * schemas.
 *
 * @param sdk the OpenTelemetry SDK behind {@code registry} and {@code appender}, or null when
 *            neither signal is exported
 * @param appender the appender attached to the root logger, or null when log export is disabled
 */
record CompactionTelemetry(MeterRegistry registry, OpenTelemetrySdk sdk, OpenTelemetryAppender appender)
        implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionTelemetry.class);

    private static final String BEARER_PREFIX = "Bearer ";

    private static final Set<Level> LEVELS = Set.of(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR);

    /**
     * Falls back to logging meters when metric export is disabled, so a compactor running without
     * a collector still prints what it would have sent rather than discarding every meter. Logs
     * simply stay on the console when their export is disabled.
     */
    static CompactionTelemetry create(Config metrics, Config logs) {
        return create(metrics, logs, CompactionTelemetry::otlpLogExporter);
    }

    /** {@code logExporterFactory} is the only network-touching piece of the log path; tests swap it. */
    static CompactionTelemetry create(Config metrics, Config logs,
                                      Function<Config, LogRecordExporter> logExporterFactory) {
        // Done here rather than as HOCON substitutions in the file, so --conf overrides of the
        // metrics values flow through (the file is resolved before those are merged).
        logs = logs.withFallback(metrics.withOnlyPath("endpoint"))
                .withFallback(metrics.withOnlyPath("request_timeout"));
        boolean metricsOn = metrics.getBoolean(ConfigConstants.ENABLED_KEY);
        boolean logsOn = logs.getBoolean(ConfigConstants.ENABLED_KEY);
        if (!metricsOn) {
            logger.info("OTLP metric export disabled — falling back to the logging registry");
        }
        if (!logsOn) {
            logger.info("OTLP log export disabled — logs stay on the console");
        }
        if (!metricsOn && !logsOn) {
            return new CompactionTelemetry(new LoggingMeterRegistry(), null, null);
        }

        // Validate everything that can refuse to start before building or attaching anything.
        Level level = logsOn ? parseLevel(logs.getString("level")) : null;
        String metricToken = metricsOn ? bearerToken(metrics, "metrics", "DD_METRICS_OTLP_TOKEN") : null;
        String logToken = logsOn ? bearerToken(logs, "logs", "DD_LOGS_OTLP_TOKEN") : null;
        if (metricToken != null && metricToken.equals(logToken)) {
            // The collector routes by the token's queue claim, so one token for both would write log
            // rows into the metrics queue (or the reverse). Always a misconfiguration.
            throw new IllegalStateException("logs.token is the same as metrics.token: log and metric"
                    + " queues have different schemas, so logs need their own token whose"
                    + " x-dd-ingestion-queue claim names a log queue (DD_LOGS_OTLP_TOKEN)");
        }
        // Every exported record passes through the redactor: DuckDB errors can echo the startup
        // script's credentials (connection-string passwords, CREATE SECRET values).
        LogRecordExporter logExporter = logsOn
                ? new RedactingLogRecordExporter(logExporterFactory.apply(logs))
                : null;

        String serviceName = metrics.getString("service_name");
        Resource resource = Resource.getDefault().toBuilder()
                .put("service.name", serviceName)
                .build();

        OpenTelemetrySdkBuilder sdkBuilder = OpenTelemetrySdk.builder();
        if (metricsOn) {
            sdkBuilder.setMeterProvider(meterProvider(metrics, metricToken, resource));
        }
        if (logsOn) {
            sdkBuilder.setLoggerProvider(SdkLoggerProvider.builder()
                    .setResource(resource)
                    .addLogRecordProcessor(BatchLogRecordProcessor.builder(logExporter)
                            .setExporterTimeout(logs.getDuration("request_timeout"))
                            .build())
                    .build());
        }
        OpenTelemetrySdk sdk = sdkBuilder.build();

        MeterRegistry registry = metricsOn ? OpenTelemetryMeterRegistry.create(sdk) : new LoggingMeterRegistry();
        OpenTelemetryAppender appender = logsOn ? attachAppender(sdk, level) : null;

        if (metricsOn) {
            logger.info("Exporting metrics to {} every {}s as service '{}'",
                    metrics.getString("endpoint"), metrics.getDuration("export_interval").toSeconds(), serviceName);
        }
        if (logsOn) {
            logger.info("Exporting logs at {} and above to {} as service '{}'",
                    level, logs.getString("endpoint"), serviceName);
        }
        return new CompactionTelemetry(registry, sdk, appender);
    }

    private static SdkMeterProvider meterProvider(Config metrics, String token, Resource resource) {
        OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder()
                .setEndpoint(metrics.getString("endpoint"))
                .setTimeout(metrics.getDuration("request_timeout"))
                .addHeader("Authorization", token)
                .build();
        return SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(PeriodicMetricReader.builder(exporter)
                        .setInterval(metrics.getDuration("export_interval"))
                        .build())
                .build();
    }

    private static LogRecordExporter otlpLogExporter(Config logs) {
        return OtlpGrpcLogRecordExporter.builder()
                .setEndpoint(logs.getString("endpoint"))
                .setTimeout(logs.getDuration("request_timeout"))
                .addHeader("Authorization", bearerToken(logs, "logs", "DD_LOGS_OTLP_TOKEN"))
                .build();
    }

    private static OpenTelemetryAppender attachAppender(OpenTelemetrySdk sdk, Level level) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        ThresholdFilter threshold = new ThresholdFilter();
        threshold.setLevel(level.toString());
        threshold.start();

        OpenTelemetryAppender appender = new OpenTelemetryAppender();
        appender.setName("otlp");
        appender.setContext(context);
        appender.addFilter(threshold);
        appender.setCaptureExperimentalAttributes(true); // thread.name tells concurrent tier threads apart
        appender.setOpenTelemetry(sdk); // before start(): nothing is ever buffered for replay
        appender.start();
        ch.qos.logback.classic.Logger root = context.getLogger(Logger.ROOT_LOGGER_NAME);
        if (level.toInt() < root.getEffectiveLevel().toInt()) {
            logger.warn("logs.level={} is below the root logger's {}: lines under {} are never produced,"
                    + " so none are exported", level, root.getEffectiveLevel(), root.getEffectiveLevel());
        }
        root.addAppender(appender);
        return appender;
    }

    /**
     * Fail fast: an enabled exporter with no token has every RPC rejected by the collector
     * (INVALID_ARGUMENT — it requires a signed token with an x-dd-ingestion-queue claim), so the
     * signal would silently never land. Refuse to start rather than export into a void.
     */
    private static String bearerToken(Config config, String signal, String envVar) {
        String token = config.hasPath("token") ? config.getString("token").trim() : "";
        if (token.isEmpty()) {
            throw new IllegalStateException(capitalize(signal) + " export is enabled but no OTLP token is"
                    + " configured — set " + signal + ".token (" + envVar + ") to a signed token carrying an"
                    + " x-dd-ingestion-queue claim naming the " + signal + " queue, or set "
                    + signal + ".enabled=false");
        }
        // The header value is sent verbatim, so a raw token would be rejected on every export.
        return token.startsWith(BEARER_PREFIX) ? token : BEARER_PREFIX + token;
    }

    private static String capitalize(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static Level parseLevel(String name) {
        // Level.toLevel also accepts OFF and ALL; OFF would be an enabled export that ships nothing.
        Level level = Level.toLevel(name.trim().toUpperCase(Locale.ROOT), null);
        if (level == null || !LEVELS.contains(level)) {
            throw new IllegalArgumentException("logs.level must be one of TRACE, DEBUG, INFO, WARN, ERROR; got '"
                    + name + "'");
        }
        return level;
    }

    @Override
    public void close() {
        registry.close(); // stops the registry's own publishing/step thread (logging or OTel bridge)
        if (appender != null) {
            // Detach before stop, so no line lands on a stopped appender and Logback stays quiet.
            ((LoggerContext) LoggerFactory.getILoggerFactory())
                    .getLogger(Logger.ROOT_LOGGER_NAME).detachAppender(appender);
            appender.stop();
        }
        if (sdk != null) {
            sdk.close(); // flushes the last metric interval and the batched log records
        }
    }
}
