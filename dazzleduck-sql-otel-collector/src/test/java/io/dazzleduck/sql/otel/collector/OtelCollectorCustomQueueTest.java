package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.*;
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
import java.net.ServerSocket;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for per-request queue routing via the {@value Headers#CLAIM_INGESTION_QUEUE}
 * JWT claim and for {@code jwt_token.verify_signature = false} behaviour.
 */
public class OtelCollectorCustomQueueTest {

    static final String SECRET_KEY_BASE64 =
            "VGhpcyBpcyBhIDY0IGJpdCBsb25nIGtleSB3aGljaCBzaG91bGQgYmUgY2hhbmdlZCBpbiBwcm9kdWN0aW9uLiBTbyBjaGFuZ2UgbWUgYW5kIG1ha2Ugc3VyZSBpdHMgMTI4IGJpdCBsb25nIG9yIG1vcmU";
    static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    @BeforeAll
    static void loadExtensions() throws Exception {
        io.dazzleduck.sql.commons.ConnectionPool.executeBatch(new String[]{
                "INSTALL arrow FROM community", "LOAD arrow"
        });
    }

    static int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    static ExportLogsServiceRequest sampleLogRequest() {
        return ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder()
                                .addLogRecords(LogRecord.newBuilder().build())
                                .build())
                        .build())
                .build();
    }

    static ExportTraceServiceRequest sampleTraceRequest() {
        return ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder()
                                .addSpans(Span.newBuilder().build())
                                .build())
                        .build())
                .build();
    }

    static ExportMetricsServiceRequest sampleMetricRequest() {
        return ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder()
                        .addScopeMetrics(ScopeMetrics.newBuilder()
                                .addMetrics(Metric.newBuilder()
                                        .setName("test.metric")
                                        .setGauge(Gauge.newBuilder()
                                                .addDataPoints(NumberDataPoint.newBuilder()
                                                        .setAsDouble(1.0).build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    /** Generates a signed JWT with no queue claim. */
    static String tokenWithoutQueueClaim() {
        SecretKey key = Validator.fromBase64String(SECRET_KEY_BASE64);
        Calendar exp = Calendar.getInstance();
        exp.add(Calendar.HOUR, 1);
        return Jwts.builder().subject("admin").expiration(exp.getTime()).signWith(key).compact();
    }

    /** Generates a signed JWT embedding the given queue ID as a claim. */
    static String tokenWithQueueClaim(String queueId) {
        return tokenWithQueueClaim(queueId, java.util.Map.of());
    }

    /** Generates a signed JWT with the queue claim plus the given extra claims. */
    static String tokenWithQueueClaim(String queueId, java.util.Map<String, String> extraClaims) {
        SecretKey key = Validator.fromBase64String(SECRET_KEY_BASE64);
        Calendar exp = Calendar.getInstance();
        exp.add(Calendar.HOUR, 1);
        var builder = Jwts.builder()
                .subject("admin")
                .claim(Headers.CLAIM_INGESTION_QUEUE, queueId)
                .expiration(exp.getTime())
                .signWith(key);
        extraClaims.forEach(builder::claim);
        return builder.compact();
    }

    static Metadata bearerMetadata(String token) {
        var meta = new Metadata();
        meta.put(AUTHORIZATION_KEY, "Bearer " + token);
        return meta;
    }

    static io.dazzleduck.sql.commons.ingestion.IngestionHandler noopHandler(String outputPath, String... knownQueues) {
        return noopHandler(id -> outputPath, false, knownQueues);
    }

    static io.dazzleduck.sql.commons.ingestion.IngestionHandler noopHandler(
            java.util.function.Function<String, String> pathFor, boolean extractClaims, String... knownQueues) {
        java.util.Set<String> known = new java.util.LinkedHashSet<>(java.util.List.of(knownQueues));
        return new io.dazzleduck.sql.commons.ingestion.IngestionHandler() {
            @Override public io.dazzleduck.sql.commons.ingestion.PostIngestionTask
            createPostIngestionTask(io.dazzleduck.sql.commons.ingestion.IngestionResult r) {
                return io.dazzleduck.sql.commons.ingestion.PostIngestionTask.NOOP;
            }
            @Override public java.util.Set<String> getKnownQueues() { return known; }
            // Mirror real handlers: a target path exists only for a known queue (null ⇒ unknown).
            @Override public String getTargetPath(String id) { return known.contains(id) ? pathFor.apply(id) : null; }
            @Override public String[] getPartitionBy(String id) { return new String[0]; }
            @Override public boolean extractClaims(String id) { return extractClaims; }
        };
    }

    static io.dazzleduck.sql.commons.ingestion.IngestionConfig smallBucketConfig() {
        return new io.dazzleduck.sql.commons.ingestion.IngestionConfig(
                1L,
                io.dazzleduck.sql.commons.ingestion.IngestionConfig.DEFAULT_MAX_BUCKET_SIZE,
                io.dazzleduck.sql.commons.ingestion.IngestionConfig.DEFAULT_MAX_BATCHES,
                io.dazzleduck.sql.commons.ingestion.IngestionConfig.DEFAULT_MAX_PENDING_WRITE,
                Duration.ofMillis(60_000),
                io.dazzleduck.sql.commons.ingestion.IngestionConfig.DEFAULT_CONFIG_REFRESH);
    }

    // -------------------------------------------------------------------------
    // Custom queue routing via JWT claim
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithCustomQueue {

        OtelCollectorServer server;
        ManagedChannel channel;
        LogsServiceGrpc.LogsServiceBlockingStub stub;

        @BeforeAll
        void setup() throws Exception {
            int port = findFreePort();
            var outputPath = Files.createTempDirectory("otel-custom-queue").resolve("output");
            Files.createDirectories(outputPath); // operator provisions the output dir

            CollectorProperties props = new CollectorProperties();
            props.setShutdownGracePeriod(Duration.ZERO); // no LB-drain wait in tests
            props.setGrpcPort(port);
            // Writers map contains "app-logs" plus traces/metrics defaults
            props.setIngestionHandler(noopHandler(outputPath.toString(), "app-logs", "traces", "metrics"));
            props.setIngestionConfig(smallBucketConfig());
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);

            server = new OtelCollectorServer(props);
            server.start();

            channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
            stub = LogsServiceGrpc.newBlockingStub(channel);
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) { channel.shutdown(); channel.awaitTermination(5, TimeUnit.SECONDS); }
            if (server != null) server.close();
        }

        @Test
        void queueClaim_matchingWriter_succeeds() {
            // JWT carries x-dd-ingestion-queue=app-logs, which is in the writers map
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim("app-logs"))));
            assertDoesNotThrow(() -> s.export(sampleLogRequest()));
        }

        @Test
        void queueClaim_unknownQueue_returnsInvalidArgument() {
            // JWT carries a queue name not in the writers map
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim("does-not-exist"))));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleLogRequest()));
            assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
        }

        @Test
        void noQueueClaim_returnsInvalidArgument() {
            // No claim — claim is always required, there is no fallback
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithoutQueueClaim())));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleLogRequest()));
            assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
        }
    }

    // -------------------------------------------------------------------------
    // Default queue fallback when "logs" is present
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithDefaultQueues {

        OtelCollectorServer server;
        ManagedChannel channel;
        LogsServiceGrpc.LogsServiceBlockingStub stub;

        @BeforeAll
        void setup() throws Exception {
            int port = findFreePort();
            var outputPath = Files.createTempDirectory("otel-default-queue").resolve("output");
            Files.createDirectories(outputPath); // operator provisions the output dir

            CollectorProperties props = new CollectorProperties();
            props.setShutdownGracePeriod(Duration.ZERO); // no LB-drain wait in tests
            props.setGrpcPort(port);
            // Default queues — "logs" present, so fallback works
            props.setIngestionHandler(noopHandler(outputPath.toString(), "logs", "traces", "metrics"));
            props.setIngestionConfig(smallBucketConfig());
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);

            server = new OtelCollectorServer(props);
            server.start();

            channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
            stub = LogsServiceGrpc.newBlockingStub(channel);
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) { channel.shutdown(); channel.awaitTermination(5, TimeUnit.SECONDS); }
            if (server != null) server.close();
        }

        @Test
        void noQueueClaim_returnsInvalidArgument() {
            // No fallback — claim is always required
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithoutQueueClaim())));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleLogRequest()));
            assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
        }

        @Test
        void queueClaim_explicit_succeeds() {
            // Explicit "logs" claim routes to the "logs" writer
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim("logs"))));
            assertDoesNotThrow(() -> s.export(sampleLogRequest()));
        }

        @Test
        void queueClaim_unknownQueue_returnsInvalidArgument() {
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim("unknown-queue"))));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleLogRequest()));
            assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
        }
    }

    // -------------------------------------------------------------------------
    // Unknown queue — verified for all three signal types
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class UnknownQueueAllSignals {

        OtelCollectorServer server;
        ManagedChannel channel;
        LogsServiceGrpc.LogsServiceBlockingStub logsStub;
        TraceServiceGrpc.TraceServiceBlockingStub traceStub;
        MetricsServiceGrpc.MetricsServiceBlockingStub metricsStub;

        @BeforeAll
        void setup() throws Exception {
            int port = findFreePort();
            var outputPath = Files.createTempDirectory("otel-unknown-queue").resolve("output");
            Files.createDirectories(outputPath); // operator provisions the output dir

            CollectorProperties props = new CollectorProperties();
            props.setShutdownGracePeriod(Duration.ZERO); // no LB-drain wait in tests
            props.setGrpcPort(port);
            props.setIngestionHandler(noopHandler(outputPath.toString(), "logs", "traces", "metrics"));
            props.setIngestionConfig(smallBucketConfig());
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);

            server = new OtelCollectorServer(props);
            server.start();

            channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
            logsStub    = LogsServiceGrpc.newBlockingStub(channel);
            traceStub   = TraceServiceGrpc.newBlockingStub(channel);
            metricsStub = MetricsServiceGrpc.newBlockingStub(channel);
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) { channel.shutdown(); channel.awaitTermination(5, TimeUnit.SECONDS); }
            if (server != null) server.close();
        }

        @Test
        void logsService_unknownQueue_returnsInvalidArgument() {
            var s = logsStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim("no-such-queue"))));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleLogRequest()));
            assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
            assertTrue(ex.getStatus().getDescription().contains("no-such-queue"));
        }

        @Test
        void traceService_unknownQueue_returnsInvalidArgument() {
            var s = traceStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim("no-such-queue"))));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleTraceRequest()));
            assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
            assertTrue(ex.getStatus().getDescription().contains("no-such-queue"));
        }

        @Test
        void metricsService_unknownQueue_returnsInvalidArgument() {
            var s = metricsStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim("no-such-queue"))));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleMetricRequest()));
            assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
            assertTrue(ex.getStatus().getDescription().contains("no-such-queue"));
        }

        @Test
        void logsService_missingClaim_returnsInvalidArgument() {
            var s = logsStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithoutQueueClaim())));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleLogRequest()));
            assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
        }

        @Test
        void traceService_missingClaim_returnsInvalidArgument() {
            var s = traceStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithoutQueueClaim())));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleTraceRequest()));
            assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
        }

        @Test
        void metricsService_missingClaim_returnsInvalidArgument() {
            var s = metricsStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithoutQueueClaim())));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleMetricRequest()));
            assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
        }
    }

    // -------------------------------------------------------------------------
    // verify_signature = false — regression test for the parseSignedClaims bug
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithVerifySignatureDisabled {

        OtelCollectorServer server;
        ManagedChannel channel;
        LogsServiceGrpc.LogsServiceBlockingStub stub;

        @BeforeAll
        void setup() throws Exception {
            int port = findFreePort();
            var outputPath = Files.createTempDirectory("otel-no-verify").resolve("output");
            Files.createDirectories(outputPath); // operator provisions the output dir

            CollectorProperties props = new CollectorProperties();
            props.setShutdownGracePeriod(Duration.ZERO); // no LB-drain wait in tests
            props.setGrpcPort(port);
            props.setIngestionHandler(noopHandler(outputPath.toString(), "logs", "traces", "metrics"));
            props.setIngestionConfig(smallBucketConfig());
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);
            props.setVerifySignature(false); // disable signature verification

            server = new OtelCollectorServer(props);
            server.start();

            channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
            stub = LogsServiceGrpc.newBlockingStub(channel);
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) { channel.shutdown(); channel.awaitTermination(5, TimeUnit.SECONDS); }
            if (server != null) server.close();
        }

        @Test
        void signedToken_accepted_withSignatureVerificationDisabled() {
            // Regression test: before the fix, parseSignedClaims on an unsecured() parser
            // threw UnsupportedJwtException for every signed token, returning UNAUTHENTICATED.
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim("logs"))));
            assertDoesNotThrow(() -> s.export(sampleLogRequest()),
                    "Signed JWT must be accepted when verify_signature=false");
        }

        @Test
        void signedTokenWithQueueClaim_routesCorrectly_withSignatureVerificationDisabled() {
            // Claims are still extracted from the token even without signature verification
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim("logs"))));
            assertDoesNotThrow(() -> s.export(sampleLogRequest()));
        }

        @Test
        void malformedToken_rejectedEvenWithVerificationDisabled() {
            // A string that is not a valid JWT at all must still be rejected
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer not.a.valid.jwt");
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleLogRequest()));
            assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        }
    }
}
