package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.SECRET_KEY_BASE64;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.bearerMetadata;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.findFreePort;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.noopHandler;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.sampleLogRequest;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.smallBucketConfig;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.tokenWithQueueClaim;
import static io.dazzleduck.sql.otel.collector.OtelCollectorLoginDelegationTest.basicAuthMetadata;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for cluster scoping via the {@value Headers#CLAIM_CLUSTER} JWT claim.
 * <p>
 * When {@code otel_collector.cluster} is configured, every call must carry a matching
 * claim (audience-style check); a missing or different claim is rejected with
 * {@code PERMISSION_DENIED}. When the config is unset the claim is ignored entirely,
 * so existing deployments and gradual rollouts are unaffected.
 */
public class OtelCollectorClusterClaimTest {

    private static final String CLUSTER = "prod-east";
    private static final String QUEUE = "logs";

    @BeforeAll
    static void loadExtensions() throws Exception {
        io.dazzleduck.sql.commons.ConnectionPool.executeBatch(new String[]{
                "INSTALL arrow FROM community", "LOAD arrow"
        });
    }

    /** Shared server/channel/stub lifecycle; subclasses choose the cluster config. */
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    abstract static class ServerFixture {

        OtelCollectorServer server;
        ManagedChannel channel;
        LogsServiceGrpc.LogsServiceBlockingStub stub;

        /** Value for {@code otel_collector.cluster}; null = enforcement off. */
        abstract String cluster();

        abstract String tempDirPrefix();

        @BeforeAll
        void setup() throws Exception {
            var outputPath = Files.createTempDirectory(tempDirPrefix()).resolve("output");
            Files.createDirectories(outputPath); // operator provisions the output dir

            CollectorProperties props = new CollectorProperties();
            props.setShutdownGracePeriod(Duration.ZERO); // no LB-drain wait in tests
            props.setGrpcPort(findFreePort());
            props.setIngestionHandler(noopHandler(outputPath.toString(), QUEUE));
            props.setIngestionConfig(smallBucketConfig());
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);
            props.setUsers(Map.of("admin", "admin")); // for the Basic-auth path
            props.setCluster(cluster());

            server = new OtelCollectorServer(props);
            server.start();

            channel = ManagedChannelBuilder.forAddress("localhost", props.getGrpcPort()).usePlaintext().build();
            stub = LogsServiceGrpc.newBlockingStub(channel);
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) { channel.shutdown(); channel.awaitTermination(5, TimeUnit.SECONDS); }
            if (server != null) server.close();
        }
    }

    @Nested
    class WithClusterConfigured extends ServerFixture {

        @Override String cluster() { return CLUSTER; }
        @Override String tempDirPrefix() { return "otel-cluster-enforced"; }

        @Test
        void matchingClusterClaim_succeeds() {
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim(QUEUE, CLUSTER))));
            assertDoesNotThrow(() -> s.export(sampleLogRequest()));
        }

        @Test
        void wrongClusterClaim_returnsPermissionDenied() {
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim(QUEUE, "prod-west"))));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleLogRequest()));
            assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
        }

        @Test
        void missingClusterClaim_returnsPermissionDenied() {
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim(QUEUE))));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleLogRequest()));
            assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
            assertTrue(ex.getStatus().getDescription().contains(Headers.CLAIM_CLUSTER),
                    "Rejection must name the claim. Actual: " + ex.getStatus().getDescription());
        }

        @Test
        void clusterCheckRunsBeforeQueueCheck() {
            // Wrong cluster AND unknown queue: the cluster rejection (PERMISSION_DENIED,
            // naming the claim) must win over the queue check (INVALID_ARGUMENT).
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim("no-such-queue", "prod-west"))));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleLogRequest()));
            assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
            assertTrue(ex.getStatus().getDescription().contains(Headers.CLAIM_CLUSTER),
                    "Cluster check must run before queue validation. Actual: " + ex.getStatus().getDescription());
        }

        @Test
        void basicAuth_localTokenHasNoClusterClaim_returnsPermissionDenied() {
            // Locally minted tokens carry no claims at all, so under enforcement a
            // Basic-authenticated call is rejected by the cluster check like any other
            // claim-less token.
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    basicAuthMetadata("admin", "admin")));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleLogRequest()));
            assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
        }
    }

    @Nested
    class WithoutClusterConfigured extends ServerFixture {

        @Override String cluster() { return null; }
        @Override String tempDirPrefix() { return "otel-cluster-off"; }

        @Test
        void tokenWithClusterClaim_isIgnoredAndSucceeds() {
            // Gradual rollout: tokens may carry the claim before any collector enforces it.
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim(QUEUE, "some-cluster"))));
            assertDoesNotThrow(() -> s.export(sampleLogRequest()));
        }

        @Test
        void tokenWithoutClusterClaim_succeeds() {
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearerMetadata(tokenWithQueueClaim(QUEUE))));
            assertDoesNotThrow(() -> s.export(sampleLogRequest()));
        }
    }
}
