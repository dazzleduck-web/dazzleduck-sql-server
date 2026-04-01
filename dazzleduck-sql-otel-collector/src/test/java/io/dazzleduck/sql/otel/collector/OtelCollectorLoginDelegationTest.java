package io.dazzleduck.sql.otel.collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import io.jsonwebtoken.Jwts;
import com.google.protobuf.ByteString;
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
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for OtelCollectorServer JWT authentication,
 * covering both login delegation (login_url configured) and local auth.
 */
public class OtelCollectorLoginDelegationTest {

    static final String SECRET_KEY_BASE64 =
            "VGhpcyBpcyBhIDY0IGJpdCBsb25nIGtleSB3aGljaCBzaG91bGQgYmUgY2hhbmdlZCBpbiBwcm9kdWN0aW9uLiBTbyBjaGFuZ2UgbWUgYW5kIG1ha2Ugc3VyZSBpdHMgMTI4IGJpdCBsb25nIG9yIG1vcmU";
    static final String VALID_USER = "admin";
    static final String VALID_PASS = "admin";
    static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    static final ObjectMapper MAPPER = new ObjectMapper();

    static int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    static Metadata basicAuthMetadata(String username, String password) {
        String encoded = Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        var meta = new Metadata();
        meta.put(AUTHORIZATION_KEY, "Basic " + encoded);
        return meta;
    }

    static ExportLogsServiceRequest sampleRequest() {
        return ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder()
                                .addLogRecords(LogRecord.newBuilder().build())
                                .build())
                        .build())
                .build();
    }

    static String generateValidToken() {
        SecretKey key = Validator.fromBase64String(SECRET_KEY_BASE64);
        Calendar exp = Calendar.getInstance();
        exp.add(Calendar.HOUR, 1);
        return Jwts.builder()
                .subject(VALID_USER)
                .expiration(exp.getTime())
                .signWith(key)
                .compact();
    }

    // -------------------------------------------------------------------------
    // Delegation tests: login_url points to stub HTTP login service
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithLoginDelegation {

        HttpServer stubLoginServer;
        OtelCollectorServer otelServer;
        ManagedChannel channel;
        LogsServiceGrpc.LogsServiceBlockingStub stub;

        @BeforeAll
        void setup() throws Exception {
            int stubPort = findFreePort();
            stubLoginServer = startStubLoginServer(stubPort);

            int otelPort = findFreePort();
            var outputPath = Files.createTempDirectory("otel-test-delegation").resolve("output");

            CollectorProperties props = new CollectorProperties();
            props.setGrpcPort(otelPort);
            props.setLogIngestionConfig(new io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig(
                    outputPath.toString(), java.util.List.of(), null, 1_048_576L, 5000L));
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);
            props.setLoginUrl("http://localhost:" + stubPort + "/v1/login");
            props.setJwtExpiration(Duration.ofHours(1));

            otelServer = new OtelCollectorServer(props);
            otelServer.start();

            channel = ManagedChannelBuilder.forAddress("localhost", otelPort)
                    .usePlaintext()
                    .build();
            stub = LogsServiceGrpc.newBlockingStub(channel);
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (otelServer != null) otelServer.close();
            if (stubLoginServer != null) stubLoginServer.stop(0);
        }

        @Test
        void validCredentials_delegated_succeeds() {
            var s = stub.withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(basicAuthMetadata(VALID_USER, VALID_PASS)));
            assertDoesNotThrow(() -> s.export(sampleRequest()));
        }

        @Test
        void invalidCredentials_delegated_rejected() {
            var s = stub.withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(basicAuthMetadata(VALID_USER, "wrongpassword")));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleRequest()));
            assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        }

        @Test
        void noAuthHeader_rejected() {
            var ex = assertThrows(StatusRuntimeException.class, () -> stub.export(sampleRequest()));
            assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        }

        @Test
        void validBearerToken_accepted() {
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer " + generateValidToken());
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
            assertDoesNotThrow(() -> s.export(sampleRequest()));
        }

        @Test
        void invalidBearerToken_rejected() {
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer not.a.valid.token");
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleRequest()));
            assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        }

        /**
         * Starts a minimal HTTP server that validates credentials and returns a signed JWT,
         * or 401 on invalid credentials. Uses the same secret key as the otel-collector.
         */
        private HttpServer startStubLoginServer(int port) throws IOException {
            SecretKey key = Validator.fromBase64String(SECRET_KEY_BASE64);
            var server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/v1/login", exchange -> {
                if (!"POST".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    exchange.close();
                    return;
                }
                try {
                    var node = MAPPER.readTree(exchange.getRequestBody().readAllBytes());
                    String username = node.get("username").asText();
                    String password = node.get("password").asText();

                    if (VALID_USER.equals(username) && VALID_PASS.equals(password)) {
                        Calendar exp = Calendar.getInstance();
                        exp.add(Calendar.HOUR, 1);
                        String token = Jwts.builder()
                                .subject(username)
                                .expiration(exp.getTime())
                                .signWith(key)
                                .compact();
                        byte[] resp = MAPPER.writeValueAsBytes(
                                Map.of("accessToken", token, "tokenType", "Bearer"));
                        exchange.getResponseHeaders().add("Content-Type", "application/json");
                        exchange.sendResponseHeaders(200, resp.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(resp);
                        }
                    } else {
                        exchange.sendResponseHeaders(401, -1);
                    }
                } catch (Exception e) {
                    exchange.sendResponseHeaders(500, -1);
                } finally {
                    exchange.close();
                }
            });
            server.start();
            return server;
        }
    }

    // -------------------------------------------------------------------------
    // Local auth tests: no login_url, users validated from config
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithLocalAuth {

        OtelCollectorServer otelServer;
        ManagedChannel channel;
        LogsServiceGrpc.LogsServiceBlockingStub stub;

        @BeforeAll
        void setup() throws Exception {
            int otelPort = findFreePort();
            var outputPath = Files.createTempDirectory("otel-test-local").resolve("output");

            CollectorProperties props = new CollectorProperties();
            props.setGrpcPort(otelPort);
            props.setLogIngestionConfig(new io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig(
                    outputPath.toString(), java.util.List.of(), null, 1_048_576L, 5000L));
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);
            props.setUsers(Map.of(VALID_USER, VALID_PASS));
            props.setJwtExpiration(Duration.ofHours(1));
            // no loginUrl → local credential validation

            otelServer = new OtelCollectorServer(props);
            otelServer.start();

            channel = ManagedChannelBuilder.forAddress("localhost", otelPort)
                    .usePlaintext()
                    .build();
            stub = LogsServiceGrpc.newBlockingStub(channel);
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (otelServer != null) otelServer.close();
        }

        @Test
        void validCredentials_local_succeeds() {
            var s = stub.withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(basicAuthMetadata(VALID_USER, VALID_PASS)));
            assertDoesNotThrow(() -> s.export(sampleRequest()));
        }

        @Test
        void invalidCredentials_local_rejected() {
            var s = stub.withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(basicAuthMetadata(VALID_USER, "wrongpassword")));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleRequest()));
            assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        }

        @Test
        void noAuthHeader_local_rejected() {
            var ex = assertThrows(StatusRuntimeException.class, () -> stub.export(sampleRequest()));
            assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        }

        @Test
        void validBearerToken_local_accepted() {
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer " + generateValidToken());
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
            assertDoesNotThrow(() -> s.export(sampleRequest()));
        }
    }

    // -------------------------------------------------------------------------
    // Transformation tests: derived columns appear in written Parquet
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithTransformations {

        OtelCollectorServer otelServer;
        ManagedChannel channel;
        LogsServiceGrpc.LogsServiceBlockingStub stub;
        Path outputPath;

        @BeforeAll
        void setup() throws Exception {
            int otelPort = findFreePort();
            Path tempDir = Files.createTempDirectory("otel-test-transform");
            // outputPath is a file prefix inside tempDir, not the dir itself
            outputPath = tempDir.resolve("output");

            CollectorProperties props = new CollectorProperties();
            props.setGrpcPort(otelPort);
            props.setLogIngestionConfig(new io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig(
                    outputPath.toString(), java.util.List.of(), "severity_number * 2 as doubled_severity", 1L, 60_000L));
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);

            otelServer = new OtelCollectorServer(props);
            otelServer.start();

            channel = ManagedChannelBuilder.forAddress("localhost", otelPort)
                    .usePlaintext()
                    .build();
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer " + generateValidToken());
            stub = LogsServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (otelServer != null) otelServer.close();
        }

        /**
         * Sends a log record and verifies the derived column and all original columns
         * are present in the written Parquet file.
         * The response is only sent after the Parquet write completes, so the file
         * is guaranteed to exist by the time stub.export() returns.
         */
        @Test
        void derivedAndOriginalColumns_presentInParquet() throws Exception {
            stub.export(ExportLogsServiceRequest.newBuilder()
                    .addResourceLogs(ResourceLogs.newBuilder()
                            .addScopeLogs(ScopeLogs.newBuilder()
                                    .addLogRecords(LogRecord.newBuilder()
                                            .setSeverityNumber(SeverityNumber.SEVERITY_NUMBER_INFO)
                                            .setSeverityText("INFO")
                                            .build())
                                    .build())
                            .build())
                    .build());

            // severity_number=9 (OTLP INFO), doubled_severity = 9 * 2 = 18
            TestUtils.isEqual(
                    "SELECT 9 as severity_number, 'INFO' as severity_text, 18 as doubled_severity",
                    "SELECT severity_number, severity_text, doubled_severity FROM read_parquet('" + outputPath + "')"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Trace ingestion tests: spans land in Parquet with correct schema
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithTraces {

        OtelCollectorServer otelServer;
        ManagedChannel channel;
        TraceServiceGrpc.TraceServiceBlockingStub stub;
        Path tracesOutputPath;

        @BeforeAll
        void setup() throws Exception {
            int otelPort = findFreePort();
            Path tempDir = Files.createTempDirectory("otel-test-traces");
            tracesOutputPath = tempDir.resolve("traces");

            CollectorProperties props = new CollectorProperties();
            props.setGrpcPort(otelPort);
            props.setTraceIngestionConfig(new io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig(
                    tracesOutputPath.toString(), java.util.List.of(), null, 1L, 60_000L));
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);

            otelServer = new OtelCollectorServer(props);
            otelServer.start();

            channel = ManagedChannelBuilder.forAddress("localhost", otelPort)
                    .usePlaintext()
                    .build();
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer " + generateValidToken());
            stub = TraceServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (otelServer != null) otelServer.close();
        }

        @Test
        void spanFields_presentInParquet() throws Exception {
            byte[] traceIdBytes = new byte[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
            byte[] spanIdBytes  = new byte[]{0,1,2,3,4,5,6,7};
            long startNanos = 1_000_000_000_000L; // 1_000_000 ms
            long endNanos   = 1_001_000_000_000L; // 1_001_000 ms → duration=1000ms

            stub.export(ExportTraceServiceRequest.newBuilder()
                    .addResourceSpans(ResourceSpans.newBuilder()
                            .addScopeSpans(ScopeSpans.newBuilder()
                                    .addSpans(Span.newBuilder()
                                            .setTraceId(ByteString.copyFrom(traceIdBytes))
                                            .setSpanId(ByteString.copyFrom(spanIdBytes))
                                            .setName("test-span")
                                            .setKind(Span.SpanKind.SPAN_KIND_SERVER)
                                            .setStartTimeUnixNano(startNanos)
                                            .setEndTimeUnixNano(endNanos)
                                            .setStatus(io.opentelemetry.proto.trace.v1.Status.newBuilder()
                                                    .setCode(io.opentelemetry.proto.trace.v1.Status.StatusCode.STATUS_CODE_OK)
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build());

            TestUtils.isEqual(
                    "SELECT 'test-span' as name, 'SERVER' as kind, 1000 as duration_ms, 'OK' as status_code",
                    "SELECT name, kind, duration_ms, status_code FROM read_parquet('" + tracesOutputPath + "')"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Metrics ingestion tests: data points land in Parquet with correct schema
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithMetrics {

        OtelCollectorServer otelServer;
        ManagedChannel channel;
        MetricsServiceGrpc.MetricsServiceBlockingStub stub;
        Path metricsOutputPath;

        @BeforeAll
        void setup() throws Exception {
            int otelPort = findFreePort();
            Path tempDir = Files.createTempDirectory("otel-test-metrics");
            metricsOutputPath = tempDir.resolve("metrics");

            CollectorProperties props = new CollectorProperties();
            props.setGrpcPort(otelPort);
            props.setMetricIngestionConfig(new io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig(
                    metricsOutputPath.toString(), java.util.List.of(), null, 1L, 60_000L));
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);

            otelServer = new OtelCollectorServer(props);
            otelServer.start();

            channel = ManagedChannelBuilder.forAddress("localhost", otelPort)
                    .usePlaintext()
                    .build();
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer " + generateValidToken());
            stub = MetricsServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (otelServer != null) otelServer.close();
        }

        @Test
        void gaugeMetric_presentInParquet() throws Exception {
            stub.export(ExportMetricsServiceRequest.newBuilder()
                    .addResourceMetrics(ResourceMetrics.newBuilder()
                            .addScopeMetrics(ScopeMetrics.newBuilder()
                                    .addMetrics(Metric.newBuilder()
                                            .setName("cpu.usage")
                                            .setUnit("1")
                                            .setGauge(Gauge.newBuilder()
                                                    .addDataPoints(NumberDataPoint.newBuilder()
                                                            .setAsDouble(0.42)
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build());

            TestUtils.isEqual(
                    "SELECT 'cpu.usage' as name, 'GAUGE' as metric_type, 0.42 as value_double",
                    "SELECT name, metric_type, value_double FROM read_parquet('" + metricsOutputPath + "')"
            );
        }
    }
}
