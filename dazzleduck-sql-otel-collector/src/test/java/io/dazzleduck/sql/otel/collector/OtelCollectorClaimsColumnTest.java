package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.SECRET_KEY_BASE64;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.bearerMetadata;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.findFreePort;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.noopHandler;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.sampleLogRequest;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.sampleMetricRequest;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.sampleTraceRequest;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.smallBucketConfig;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.tokenWithQueueClaim;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * End-to-end tests for the per-queue {@code extract_claims} claims column: with the flag on,
 * ingested rows carry a {@code claims} map built from the caller's JWT (registered claims
 * excluded); with the flag off, the written schema is unchanged.
 */
public class OtelCollectorClaimsColumnTest {

    private static final String ORG_ID = "acme";

    @BeforeAll
    static void loadExtensions() throws Exception {
        ConnectionPool.executeBatch(new String[]{"INSTALL arrow FROM community", "LOAD arrow"});
    }

    static long count(String sql) throws Exception {
        Long result = ConnectionPool.collectFirst(sql, Long.class);
        assertNotNull(result);
        return result;
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    abstract static class ServerFixture {

        Path outputRoot;
        OtelCollectorServer server;
        ManagedChannel channel;

        abstract IngestionHandler handler();

        @BeforeAll
        void setup() throws Exception {
            outputRoot = Files.createTempDirectory("otel-claims-test");
            for (String q : new String[]{"logs", "traces", "metrics"}) {
                Files.createDirectories(outputRoot.resolve(q));
            }
            var props = new CollectorProperties();
            props.setShutdownGracePeriod(Duration.ZERO);
            props.setGrpcPort(findFreePort());
            props.setIngestionHandler(handler());
            props.setIngestionConfig(smallBucketConfig());
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);

            server = new OtelCollectorServer(props);
            server.start();
            channel = ManagedChannelBuilder.forAddress("localhost", props.getGrpcPort()).usePlaintext().build();
        }

        @AfterAll
        void cleanup() throws Exception {
            try {
                if (channel != null) { channel.shutdown(); channel.awaitTermination(5, TimeUnit.SECONDS); }
            } finally {
                if (server != null) server.close();
            }
        }

        String parquetGlob(String queue) {
            return "read_parquet('%s/%s/*.parquet')".formatted(outputRoot.toString().replace('\\', '/'), queue);
        }
    }

    @Nested
    class WithExtractClaims extends ServerFixture {

        @Override IngestionHandler handler() {
            return noopHandler(id -> outputRoot.resolve(id).toString(), true, "logs", "traces", "metrics");
        }

        @Test
        void logs_rowsCarryFilteredClaims() throws Exception {
            var stub = LogsServiceGrpc.newBlockingStub(channel).withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(bearerMetadata(tokenWithQueueClaim("logs", Map.of("org_id", ORG_ID)))));
            stub.export(sampleLogRequest());

            assertEquals(1, count("SELECT COUNT(*) FROM " + parquetGlob("logs")
                    + " WHERE claims['org_id'] = '" + ORG_ID + "'"
                    + " AND claims['sub'] = 'admin'"
                    + " AND claims['x-dd-ingestion-queue'] = 'logs'"));
            assertEquals(0, count("SELECT COUNT(*) FROM " + parquetGlob("logs")
                    + " WHERE list_contains(map_keys(claims), 'exp')"));
        }

        @Test
        void traces_haveClaimsColumn() throws Exception {
            var stub = TraceServiceGrpc.newBlockingStub(channel).withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(bearerMetadata(tokenWithQueueClaim("traces", Map.of("org_id", ORG_ID)))));
            stub.export(sampleTraceRequest());
            assertOrgClaimStamped("traces");
        }

        @Test
        void metrics_haveClaimsColumn() throws Exception {
            var stub = MetricsServiceGrpc.newBlockingStub(channel).withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(bearerMetadata(tokenWithQueueClaim("metrics", Map.of("org_id", ORG_ID)))));
            stub.export(sampleMetricRequest());
            assertOrgClaimStamped("metrics");
        }

        private void assertOrgClaimStamped(String queue) throws Exception {
            assertEquals(1, count("SELECT COUNT(*) FROM " + parquetGlob(queue)
                    + " WHERE claims['org_id'] = '" + ORG_ID + "'"));
        }
    }

    @Nested
    class WithoutExtractClaims extends ServerFixture {

        @Override IngestionHandler handler() {
            return noopHandler(outputRoot.resolve("logs").toString(), "logs");
        }

        @Test
        void logs_writtenWithoutClaimsColumn() throws Exception {
            var stub = LogsServiceGrpc.newBlockingStub(channel).withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(bearerMetadata(tokenWithQueueClaim("logs", Map.of("org_id", ORG_ID)))));
            stub.export(sampleLogRequest());

            assertEquals(0, count("SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM " + parquetGlob("logs")
                    + ") WHERE column_name = 'claims'"));
            assertEquals(1, count("SELECT COUNT(*) FROM " + parquetGlob("logs")));
        }
    }
}
