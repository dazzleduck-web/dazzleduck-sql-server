package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.MetadataUtils;
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
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.SECRET_KEY_BASE64;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.bearerMetadata;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.findFreePort;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.noopHandler;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.smallBucketConfig;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.tokenWithQueueClaim;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Every timestamp column is written as a UTC-adjusted timestamp, so DuckDB reads it as {@code TIMESTAMP WITH TIME
 * ZONE} holding the exact instant the OTLP record carried (to the millisecond), whatever the reader's time zone.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OtelCollectorTimestampTest {

    /** 2023-11-14 22:13:20.123456789 UTC; written at millisecond precision. */
    static final long NANOS = 1_700_000_000_123_456_789L;
    static final long MILLIS = 1_700_000_000_123L;
    static final String UTC_TEXT = "2023-11-14 22:13:20.123";

    String zoneBefore;
    Path outputRoot;
    OtelCollectorServer server;
    ManagedChannel channel;

    @BeforeAll
    void setup() throws Exception {
        // Read first, so cleanup() can restore it whatever fails later in setup().
        zoneBefore = ConnectionPool.collectFirst("SELECT current_setting('TimeZone')", String.class);
        ConnectionPool.executeBatch(new String[]{"INSTALL arrow FROM community", "LOAD arrow"});
        // A non-UTC instance zone that start() must replace.
        ConnectionPool.executeOnSingleton("SET GLOBAL TimeZone = 'America/Los_Angeles'");
        outputRoot = Files.createTempDirectory("otel-timestamp-test");
        for (String q : new String[]{"logs", "traces", "metrics"}) {
            Files.createDirectories(outputRoot.resolve(q));
        }
        var props = new CollectorProperties();
        props.setShutdownGracePeriod(Duration.ZERO);
        props.setGrpcPort(findFreePort());
        props.setIngestionHandler(noopHandler(id -> outputRoot.resolve(id).toString(), false, "logs", "traces", "metrics"));
        props.setIngestionConfig(smallBucketConfig());
        props.setAuthentication("jwt");
        props.setSecretKey(SECRET_KEY_BASE64);
        props.setTimeZone("UTC");
        server = new OtelCollectorServer(props);
        server.start();
        channel = ManagedChannelBuilder.forAddress("localhost", props.getGrpcPort()).usePlaintext().build();

        LogsServiceGrpc.newBlockingStub(channel).withInterceptors(auth("logs")).export(
                ExportLogsServiceRequest.newBuilder().addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder().addLogRecords(LogRecord.newBuilder()
                                .setTimeUnixNano(NANOS).setObservedTimeUnixNano(NANOS)))).build());
        TraceServiceGrpc.newBlockingStub(channel).withInterceptors(auth("traces")).export(
                ExportTraceServiceRequest.newBuilder().addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder().addSpans(Span.newBuilder().setName("op")
                                .setStartTimeUnixNano(NANOS).setEndTimeUnixNano(NANOS)))).build());
        MetricsServiceGrpc.newBlockingStub(channel).withInterceptors(auth("metrics")).export(
                ExportMetricsServiceRequest.newBuilder().addResourceMetrics(ResourceMetrics.newBuilder()
                        .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(Metric.newBuilder().setName("m")
                                .setGauge(Gauge.newBuilder().addDataPoints(NumberDataPoint.newBuilder()
                                        .setStartTimeUnixNano(NANOS).setTimeUnixNano(NANOS).setAsInt(1)))))).build());
    }

    @AfterAll
    void cleanup() throws Exception {
        try {
            if (channel != null) { channel.shutdown(); channel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS); }
        } finally {
            try {
                if (server != null) server.close();
            } finally {
                // Null only if setup() failed before reading it; then the zone was never changed.
                if (zoneBefore != null) {
                    ConnectionPool.executeOnSingleton("SET GLOBAL TimeZone = '%s'".formatted(zoneBefore));
                }
            }
        }
    }

    private static io.grpc.ClientInterceptor auth(String queue) {
        return MetadataUtils.newAttachHeadersInterceptor(bearerMetadata(tokenWithQueueClaim(queue)));
    }

    private String from(String queue) {
        return " FROM read_parquet('%s/%s/*.parquet')".formatted(outputRoot.toString().replace('\\', '/'), queue);
    }

    /** Type, exact epoch millis, and the UTC rendering: all three are independent of the session's time zone. */
    private void assertInstant(String queue, String column) throws Exception {
        String sql = ("SELECT concat_ws('|', typeof(\"%1$s\"), epoch_ms(\"%1$s\"), strftime(timezone('UTC', \"%1$s\"),"
                + " '%%Y-%%m-%%d %%H:%%M:%%S.%%g'))").formatted(column) + from(queue);
        assertEquals("TIMESTAMP WITH TIME ZONE|" + MILLIS + "|" + UTC_TEXT,
                ConnectionPool.collectFirst(sql, String.class), queue + "." + column);
    }

    @Test
    void logTimestampsAreInstants() throws Exception {
        assertInstant("logs", "timestamp");
        assertInstant("logs", "observed_timestamp");
    }

    @Test
    void spanTimesAreInstants() throws Exception {
        assertInstant("traces", "start_time_ms");
        assertInstant("traces", "end_time_ms");
    }

    @Test
    void metricTimesAreInstants() throws Exception {
        assertInstant("metrics", "start_time_ms");
        assertInstant("metrics", "time_ms");
    }

    /** The Arrow schemas declare the zone; a zone-less timestamp would read back as a plain TIMESTAMP. */
    @Test
    void everyTimestampFieldIsUtc() {
        for (var entry : Map.of("logs", OtelLogSchema.SCHEMA, "traces", OtelTraceSchema.SCHEMA,
                "metrics", OtelMetricSchema.SCHEMA).entrySet()) {
            List<Field> timestamps = timestampFields(entry.getValue());
            assertEquals(2, timestamps.size(), entry.getKey());
            for (Field f : timestamps) {
                ArrowType.Timestamp t = (ArrowType.Timestamp) f.getType();
                assertEquals(TimeUnit.MILLISECOND, t.getUnit(), entry.getKey() + "." + f.getName());
                assertEquals("UTC", t.getTimezone(), entry.getKey() + "." + f.getName());
            }
        }
    }

    private static List<Field> timestampFields(Schema schema) {
        return schema.getFields().stream().filter(f -> f.getType() instanceof ArrowType.Timestamp).toList();
    }

    /**
     * With {@code time_zone = "UTC"}, starting the server sets the instance to UTC on every pooled connection, not
     * only the singleton, so {@code CAST(timestamp AS DATE)} and similar transformations give the UTC date
     * regardless of the host's zone.
     */
    @Test
    void configuredTimeZoneReachesEveryConnection() throws Exception {
        assertEquals("UTC", ConnectionPool.collectFirst("SELECT current_setting('TimeZone')", String.class));
        assertEquals("2023-11-14", ConnectionPool.collectFirst(
                "SELECT CAST(make_timestamptz(" + MILLIS * 1000 + "::BIGINT) AS DATE)::VARCHAR", String.class));
    }

    /** By default the collector leaves DuckDB's zone alone; only a configured time_zone changes it. */
    @Test
    void defaultLeavesTimeZoneUnchanged() throws Exception {
        assertEquals(null, new CollectorProperties().getTimeZone());
        assertEquals("", new io.dazzleduck.sql.otel.collector.config.CollectorConfig().getTimeZone());
        // Start from a non-UTC zone, so applying UTC by mistake would show.
        ConnectionPool.executeOnSingleton("SET GLOBAL TimeZone = 'America/Los_Angeles'");
        try {
            OtelCollectorServer.applyTimeZone(null);
            OtelCollectorServer.applyTimeZone("");
            assertEquals("America/Los_Angeles",
                    ConnectionPool.collectFirst("SELECT current_setting('TimeZone')", String.class));
        } finally {
            // Back to what setup() configured, which configuredTimeZoneReachesEveryConnection asserts.
            ConnectionPool.executeOnSingleton("SET GLOBAL TimeZone = 'UTC'");
        }
    }
}
