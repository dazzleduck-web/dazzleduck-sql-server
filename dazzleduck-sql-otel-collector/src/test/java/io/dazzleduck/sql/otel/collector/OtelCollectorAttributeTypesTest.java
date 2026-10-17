package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionHandler;
import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.QueueIdToTableMapping;
import io.dazzleduck.sql.otel.collector.config.AttributeTypeSettings;
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
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.SECRET_KEY_BASE64;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.bearerMetadata;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.findFreePort;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.tokenWithQueueClaim;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end against a real DuckLake catalog: a stream records its attribute name/type mappings
 * only when its queue mapping names an {@code attribute_types_table}, into that table in the
 * stream's OWN schema, written by the transaction that registers the batch's files.
 *
 * <p>The four queues cover what the configuration is for: {@code logs} and {@code traces} both
 * name {@code otel_attribute_types} in schema {@code main} and so share one table; {@code metrics}
 * names the same table name but writes to schema {@code other}, so it is a different table; and
 * {@code untracked} names none and records nothing.
 *
 * <p>No polling anywhere: the export RPC completes only once its batch has been registered, and
 * the mappings are in that same transaction, so they are committed by the time the call returns.
 * That is the property this design exists for.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OtelCollectorAttributeTypesTest {

    private static final String CATALOG = "otel_attr_types_test";
    private static final String REGISTRY = "otel_attribute_types";

    private OtelCollectorServer server;
    private ManagedChannel channel;
    private DuckLakeIngestionHandler handler;

    @BeforeAll
    void setup() throws Exception {
        ConnectionPool.executeBatch(new String[]{"INSTALL arrow FROM community", "LOAD arrow"});
        Path tempDir = Files.createTempDirectory("otel-attr-types-test");
        Path dataDir = tempDir.resolve("data");
        Files.createDirectories(dataDir);

        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), CATALOG, dataDir),
                    "CREATE SCHEMA %s.other".formatted(CATALOG),
                    "CREATE TABLE %s.main.logs      (body VARCHAR)".formatted(CATALOG),
                    "CREATE TABLE %s.main.spans     (name VARCHAR)".formatted(CATALOG),
                    "CREATE TABLE %s.main.untracked (body VARCHAR)".formatted(CATALOG),
                    "CREATE TABLE %s.other.metrics  (name VARCHAR)".formatted(CATALOG)});
        }
        // The operator's responsibility, as elsewhere: each table's data directory.
        for (String path : new String[]{"main/logs", "main/spans", "main/untracked", "other/metrics"}) {
            Files.createDirectories(dataDir.resolve(path));
        }

        Map<String, String> tracked = Map.of(QueueIdToTableMapping.ATTRIBUTE_TYPES_TABLE_KEY, REGISTRY);
        handler = new DuckLakeIngestionHandler(Map.of(
                "logs",      new QueueIdToTableMapping("logs",      CATALOG, "main",  "logs",      tracked, null),
                "traces",    new QueueIdToTableMapping("traces",    CATALOG, "main",  "spans",     tracked, null),
                "metrics",   new QueueIdToTableMapping("metrics",   CATALOG, "other", "metrics",   tracked, null),
                "untracked", new QueueIdToTableMapping("untracked", CATALOG, "main",  "untracked", Map.of(), null)));

        var props = new CollectorProperties();
        props.setShutdownGracePeriod(Duration.ZERO);
        props.setGrpcPort(findFreePort());
        props.setHealthPort(findFreePort());
        props.setAuthentication("jwt");
        props.setSecretKey(SECRET_KEY_BASE64);
        props.setStartupScript("LOAD arrow;");
        props.setIngestionHandler(handler);
        // minBucketSize 1: every export is written and registered on its own, so the RPC's
        // completion is that batch's commit.
        props.setIngestionConfig(new IngestionConfig(1L, IngestionConfig.DEFAULT_MAX_BUCKET_SIZE,
                IngestionConfig.DEFAULT_MAX_BATCHES, IngestionConfig.DEFAULT_MAX_PENDING_WRITE,
                Duration.ofSeconds(60)));
        props.setAttributeTypeSettings(new AttributeTypeSettings(true, 100_000, 100_000));

        server = new OtelCollectorServer(props);
        server.start();
        channel = ManagedChannelBuilder.forAddress("localhost", props.getGrpcPort()).usePlaintext().build();
    }

    @AfterAll
    void cleanup() throws Exception {
        try {
            if (channel != null) {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
        } finally {
            if (server != null) {
                server.close();
            }
            try (Connection conn = ConnectionPool.getConnection()) {
                ConnectionPool.execute(conn, "DETACH " + CATALOG);
            }
        }
    }

    private static KeyValue kv(String key, AnyValue value) {
        return KeyValue.newBuilder().setKey(key).setValue(value).build();
    }

    private static AnyValue str(String v) {
        return AnyValue.newBuilder().setStringValue(v).build();
    }

    private static AnyValue num(long v) {
        return AnyValue.newBuilder().setIntValue(v).build();
    }

    private io.grpc.ClientInterceptor auth(String queue) {
        return MetadataUtils.newAttachHeadersInterceptor(bearerMetadata(tokenWithQueueClaim(queue)));
    }

    private void exportLog(String queue, KeyValue... attributes) {
        LogsServiceGrpc.newBlockingStub(channel).withInterceptors(auth(queue)).export(
                ExportLogsServiceRequest.newBuilder().addResourceLogs(ResourceLogs.newBuilder()
                        .setResource(Resource.newBuilder().addAttributes(kv("service.name", str("api"))))
                        .addScopeLogs(ScopeLogs.newBuilder().addLogRecords(LogRecord.newBuilder()
                                .setTimeUnixNano(1_000_000_000L)
                                .setBody(str("hello"))
                                .addAllAttributes(List.of(attributes))))).build());
    }

    /** One schema's registry rows, as sorted {@code queue/signal/scope/name/type}. */
    private static String recorded(String schema) throws Exception {
        String text = ConnectionPool.collectFirst(
                ("SELECT coalesce(string_agg(r, ' ' ORDER BY r), '') FROM (SELECT concat_ws('/',"
                        + " ingestion_queue, signal, attribute_scope, attribute_name, value_type) AS r"
                        + " FROM %s.%s.%s)").formatted(CATALOG, schema, REGISTRY), String.class);
        return text == null ? "" : text;
    }

    private static long rowCount(String schema) throws Exception {
        return ConnectionPool.collectFirst(
                "SELECT count(*) FROM %s.%s.%s".formatted(CATALOG, schema, REGISTRY), Long.class);
    }

    /**
     * The mappings are committed by the time the RPC returns — nothing is polled or slept on. This
     * is also the test that the table is created on first use, inside that transaction.
     */
    @Test
    void mappingsAreCommittedWithTheBatchThatCarriedThem() throws Exception {
        exportLog("logs", kv("committed.level", str("info")), kv("committed.retries", num(2)));
        String rows = recorded("main");
        assertEquals("logs/logs/attribute/committed.level/string logs/logs/attribute/committed.retries/int",
                matching(rows, "logs/logs/attribute/committed."));
        assertTrue(rows.contains("logs/logs/resource/service.name/string"),
                "the resource attributes are recorded in their own scope: " + rows);
    }

    /** Two streams naming one table share it, told apart by queue and signal. */
    @Test
    void twoStreamsSharingATableBothRecordIntoIt() throws Exception {
        exportLog("logs", kv("shared", str("v")));
        TraceServiceGrpc.newBlockingStub(channel).withInterceptors(auth("traces")).export(
                ExportTraceServiceRequest.newBuilder().addResourceSpans(ResourceSpans.newBuilder()
                        .setResource(Resource.newBuilder().addAttributes(kv("service.name", str("api"))))
                        .addScopeSpans(ScopeSpans.newBuilder().addSpans(Span.newBuilder()
                                .setName("op")
                                .addAttributes(kv("shared", num(1)))
                                .addEvents(Span.Event.newBuilder().setName("ev")
                                        .addAttributes(kv("ev.attr", str("x"))))
                                .addLinks(Span.Link.newBuilder()
                                        .addAttributes(kv("link.attr", str("y"))))))).build());

        String rows = recorded("main");
        assertEquals("logs/logs/attribute/shared/string", matching(rows, "logs/logs/attribute/shared"));
        // The span's own attribute of the same name, with another type, is a separate row — and its
        // event and link attributes get their own scopes.
        assertEquals("traces/traces/attribute/shared/int traces/traces/event/ev.attr/string"
                        + " traces/traces/link/link.attr/string traces/traces/resource/service.name/string",
                matching(rows, "traces/"));
    }

    /**
     * The same table NAME in another schema is another table: {@code metrics} writes to
     * {@code other}, so its mappings are not in {@code main}'s registry at all.
     */
    @Test
    void aStreamInAnotherSchemaRecordsIntoThatSchemasTable() throws Exception {
        MetricsServiceGrpc.newBlockingStub(channel).withInterceptors(auth("metrics")).export(
                ExportMetricsServiceRequest.newBuilder().addResourceMetrics(ResourceMetrics.newBuilder()
                        .setResource(Resource.newBuilder().addAttributes(kv("region", str("us"))))
                        .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(Metric.newBuilder()
                                .setName("m").setGauge(Gauge.newBuilder().addDataPoints(
                                        NumberDataPoint.newBuilder().setTimeUnixNano(1_000_000_000L).setAsInt(1)
                                                .addAttributes(kv("endpoint", str("/v1")))))))).build());
        assertEquals("metrics/metrics/attribute/endpoint/string metrics/metrics/resource/region/string",
                recorded("other"));
        assertEquals("", matching(recorded("main"), "metrics/"),
                "main's registry knows nothing about a stream writing to another schema");
    }

    /** A stream whose mapping names no table records nothing — the parameter is what opts it in. */
    @Test
    void aStreamThatNamesNoTableRecordsNothing() throws Exception {
        long before = rowCount("main");
        exportLog("untracked", kv("ignored", str("v")));
        assertEquals(before, rowCount("main"));
        assertEquals("", matching(recorded("main"), "untracked/"));
    }

    /** A type change appends a second row and leaves the first alone. */
    @Test
    void aTypeChangeBetweenExportsBecomesASecondRow() throws Exception {
        exportLog("logs", kv("build", str("2026.1")));
        exportLog("logs", kv("build", num(20261)));
        assertEquals("logs/logs/attribute/build/int logs/logs/attribute/build/string",
                matching(recorded("main"), "logs/logs/attribute/build"));
    }

    /** Repeated identical exports add nothing: in-memory first, and the keyed insert behind it. */
    @Test
    void repeatedExportsOfTheSameAttributesAddNoRows() throws Exception {
        exportLog("logs", kv("steady", str("v")));
        long after = rowCount("main");
        for (int i = 0; i < 5; i++) {
            exportLog("logs", kv("steady", str("v")));
        }
        assertEquals(after, rowCount("main"));
    }


    /**
     * The table is append-only and tolerates duplicates, so what keeps a restart from re-appending
     * everything is the cache each process builds at startup. A second collector over the same
     * catalog, exporting attributes the first one already recorded, adds no rows.
     */
    @Test
    void aRestartedCollectorReadsWhatIsThereAndAppendsNothingAgain() throws Exception {
        exportLog("logs", kv("restart.attr", str("v")));
        long before = rowCount("main");

        var props = new CollectorProperties();
        props.setShutdownGracePeriod(Duration.ZERO);
        props.setGrpcPort(findFreePort());
        // Its own health port too: the fixture's collector already holds the default one.
        props.setHealthPort(findFreePort());
        props.setAuthentication("jwt");
        props.setSecretKey(SECRET_KEY_BASE64);
        props.setIngestionHandler(handler);
        props.setIngestionConfig(new IngestionConfig(1L, IngestionConfig.DEFAULT_MAX_BUCKET_SIZE,
                IngestionConfig.DEFAULT_MAX_BATCHES, IngestionConfig.DEFAULT_MAX_PENDING_WRITE,
                Duration.ofSeconds(60)));
        props.setAttributeTypeSettings(new AttributeTypeSettings(true, 100_000, 100_000));
        var restarted = new OtelCollectorServer(props);
        restarted.start();
        ManagedChannel other = ManagedChannelBuilder.forAddress("localhost", props.getGrpcPort())
                .usePlaintext().build();
        try {
            LogsServiceGrpc.newBlockingStub(other).withInterceptors(auth("logs")).export(
                    ExportLogsServiceRequest.newBuilder().addResourceLogs(ResourceLogs.newBuilder()
                            .setResource(Resource.newBuilder().addAttributes(kv("service.name", str("api"))))
                            .addScopeLogs(ScopeLogs.newBuilder().addLogRecords(LogRecord.newBuilder()
                                    .setTimeUnixNano(1_000_000_000L).setBody(str("hello"))
                                    .addAttributes(kv("restart.attr", str("v")))))).build());
            assertEquals(before, rowCount("main"),
                    "the restarted collector loaded the mappings and re-appended none");
        } finally {
            other.shutdown();
            other.awaitTermination(5, TimeUnit.SECONDS);
            restarted.close();
        }
    }

    /**
     * The rows reach the table the way data does — as registered Parquet files, not an INSERT. So
     * the catalog lists files for the registry table, and they sit under its own data directory.
     */
    @Test
    void theRowsArriveAsRegisteredParquetFiles() throws Exception {
        exportLog("logs", kv("as.a.file", str("v")));
        long files = ConnectionPool.collectFirst(
                "SELECT count(*) FROM ducklake_list_files('%s', '%s', schema => 'main')"
                        .formatted(CATALOG, REGISTRY), Long.class);
        assertTrue(files > 0, "the registry table has data files, so nothing was inserted row-wise");
        String path = ConnectionPool.collectFirst(
                "SELECT data_file FROM ducklake_list_files('%s', '%s', schema => 'main') LIMIT 1"
                        .formatted(CATALOG, REGISTRY), String.class);
        assertTrue(path.endsWith(".parquet"), path);
        assertTrue(path.contains("/main/" + REGISTRY + "/"),
                "written under the registry table's own data directory, not the signal table's: " + path);
    }

    /** The rows of {@code all} whose rendered text starts with {@code prefix}, space separated. */
    private static String matching(String all, String prefix) {
        return String.join(" ", java.util.Arrays.stream(all.split(" "))
                .filter(row -> row.startsWith(prefix)).sorted().toList());
    }
}