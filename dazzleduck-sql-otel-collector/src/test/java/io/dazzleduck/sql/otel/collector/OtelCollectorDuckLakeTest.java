package io.dazzleduck.sql.otel.collector;

import com.google.protobuf.ByteString;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionHandler;
import io.dazzleduck.sql.commons.ingestion.QueueIdToTableMapping;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.jsonwebtoken.Jwts;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import io.opentelemetry.proto.metrics.v1.*;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end integration tests for the otel-collector → DuckLake pipeline.
 * Sends OTLP data over gRPC and verifies records are registered in a DuckLake catalog.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OtelCollectorDuckLakeTest {

    static final String SECRET_KEY_BASE64 =
            "VGhpcyBpcyBhIDY0IGJpdCBsb25nIGtleSB3aGljaCBzaG91bGQgYmUgY2hhbmdlZCBpbiBwcm9kdWN0aW9uLiBTbyBjaGFuZ2UgbWUgYW5kIG1ha2Ugc3VyZSBpdHMgMTI4IGJpdCBsb25nIG9yIG1vcmU";
    static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    static final String CATALOG = "otel_ducklake_test";

    OtelCollectorServer otelServer;
    ManagedChannel channel;
    LogsServiceGrpc.LogsServiceBlockingStub logsStub;
    TraceServiceGrpc.TraceServiceBlockingStub tracesStub;
    MetricsServiceGrpc.MetricsServiceBlockingStub metricsStub;

    @BeforeAll
    void setup() throws Exception {
        Path tempDir = Files.createTempDirectory("otel-ducklake-test");
        Path dataDir = tempDir.resolve("data");
        Files.createDirectories(dataDir);

        Path logsDir   = tempDir.resolve("logs");
        Path tracesDir = tempDir.resolve("traces");
        Path metricsDir = tempDir.resolve("metrics");

        // Set up DuckLake catalog with narrow tables matching the columns we want to query.
        // ignore_extra_columns => true means the wider Parquet schema written by the collector
        // is fine — DuckLake only picks up the columns declared in the table.
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "LOAD arrow",
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), CATALOG, dataDir),
                    "CREATE TABLE %s.main.logs    (severity_number INT,  severity_text VARCHAR, body VARCHAR)".formatted(CATALOG),
                    "CREATE TABLE %s.main.spans   (name VARCHAR, kind VARCHAR, duration_ms BIGINT)".formatted(CATALOG),
                    "CREATE TABLE %s.main.metrics (name VARCHAR, metric_type VARCHAR, value_double DOUBLE)".formatted(CATALOG)
            });
        }

        // Each factory maps the last path segment of the output dir to a DuckLake table.
        // SignalWriter sets queueName = outputPath, so DuckLakeIngestionTaskFactory resolves
        // the table via suffix (last segment of the path).
        var logFactory = new DuckLakeIngestionHandler(Map.of(
                "logs",    new QueueIdToTableMapping("logs",    CATALOG, "main", "logs",    Map.of(), null)));
        var traceFactory = new DuckLakeIngestionHandler(Map.of(
                "traces",  new QueueIdToTableMapping("traces",  CATALOG, "main", "spans",   Map.of(), null)));
        var metricFactory = new DuckLakeIngestionHandler(Map.of(
                "metrics", new QueueIdToTableMapping("metrics", CATALOG, "main", "metrics", Map.of(), null)));

        CollectorProperties props = new CollectorProperties();
        props.setGrpcPort(freePort());
        props.setLogIngestionConfig(new io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig(
                logsDir.toString(), java.util.List.of(), null, 1L, 60_000L));
        props.setTraceIngestionConfig(new io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig(
                tracesDir.toString(), java.util.List.of(), null, 1L, 60_000L));
        props.setMetricIngestionConfig(new io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig(
                metricsDir.toString(), java.util.List.of(), null, 1L, 60_000L));
        props.setAuthentication("jwt");
        props.setSecretKey(SECRET_KEY_BASE64);
        props.setStartupScript("LOAD arrow;");
        props.setLogIngestionTaskFactory(logFactory);
        props.setTraceIngestionTaskFactory(traceFactory);
        props.setMetricIngestionTaskFactory(metricFactory);

        otelServer = new OtelCollectorServer(props);
        otelServer.start();

        channel = ManagedChannelBuilder.forAddress("localhost", props.getGrpcPort())
                .usePlaintext().build();
        var interceptor = MetadataUtils.newAttachHeadersInterceptor(bearerMeta());
        logsStub    = LogsServiceGrpc.newBlockingStub(channel).withInterceptors(interceptor);
        tracesStub  = TraceServiceGrpc.newBlockingStub(channel).withInterceptors(interceptor);
        metricsStub = MetricsServiceGrpc.newBlockingStub(channel).withInterceptors(interceptor);
    }

    @AfterAll
    void cleanup() throws Exception {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (otelServer != null) otelServer.close();
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    // -----------------------------------------------------------------------
    // Logs
    // -----------------------------------------------------------------------

    @Test
    void logRecord_registeredInDuckLakeLogsTable() throws Exception {
        logsStub.export(ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder()
                                .addLogRecords(LogRecord.newBuilder()
                                        .setSeverityNumber(SeverityNumber.SEVERITY_NUMBER_INFO)
                                        .setSeverityText("INFO")
                                        .setBody(io.opentelemetry.proto.common.v1.AnyValue.newBuilder()
                                                .setStringValue("hello ducklake").build())
                                        .build())
                                .build())
                        .build())
                .build());

        // severity_number=9 for INFO in OTLP
        TestUtils.isEqual(
                "SELECT 9 AS severity_number, 'INFO' AS severity_text, 'hello ducklake' AS body",
                "SELECT severity_number, severity_text, body FROM %s.main.logs".formatted(CATALOG));
    }

    // -----------------------------------------------------------------------
    // Traces
    // -----------------------------------------------------------------------

    @Test
    void span_registeredInDuckLakeSpansTable() throws Exception {
        byte[] traceId = new byte[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
        byte[] spanId  = new byte[]{0,1,2,3,4,5,6,7};

        tracesStub.export(ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder()
                                .addSpans(Span.newBuilder()
                                        .setTraceId(ByteString.copyFrom(traceId))
                                        .setSpanId(ByteString.copyFrom(spanId))
                                        .setName("checkout")
                                        .setKind(Span.SpanKind.SPAN_KIND_SERVER)
                                        .setStartTimeUnixNano(1_000_000_000_000L)
                                        .setEndTimeUnixNano(1_002_000_000_000L) // 2000 ms
                                        .build())
                                .build())
                        .build())
                .build());

        TestUtils.isEqual(
                "SELECT 'checkout' AS name, 'SERVER' AS kind, 2000 AS duration_ms",
                "SELECT name, kind, duration_ms FROM %s.main.spans".formatted(CATALOG));
    }

    // -----------------------------------------------------------------------
    // Metrics
    // -----------------------------------------------------------------------

    @Test
    void gaugeMetric_registeredInDuckLakeMetricsTable() throws Exception {
        metricsStub.export(ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder()
                        .addScopeMetrics(ScopeMetrics.newBuilder()
                                .addMetrics(Metric.newBuilder()
                                        .setName("jvm.memory.used")
                                        .setUnit("By")
                                        .setGauge(Gauge.newBuilder()
                                                .addDataPoints(NumberDataPoint.newBuilder()
                                                        .setAsDouble(512.0)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());

        TestUtils.isEqual(
                "SELECT 'jvm.memory.used' AS name, 'GAUGE' AS metric_type, 512.0 AS value_double",
                "SELECT name, metric_type, value_double FROM %s.main.metrics".formatted(CATALOG));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static int freePort() throws IOException {
        try (var s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    private static Metadata bearerMeta() {
        SecretKey key = Validator.fromBase64String(SECRET_KEY_BASE64);
        Calendar exp = Calendar.getInstance();
        exp.add(Calendar.HOUR, 1);
        String token = Jwts.builder().subject("admin").expiration(exp.getTime()).signWith(key).compact();
        var meta = new Metadata();
        meta.put(AUTHORIZATION_KEY, "Bearer " + token);
        return meta;
    }
}
