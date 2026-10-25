package io.dazzleduck.sql.otel.collector;

import com.google.protobuf.ByteString;
import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.jsonwebtoken.Jwts;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Throughput, latency, and storage benchmark for the OtelCollector ingestion pipeline.
 *
 * Run with: ./mvnw test -pl dazzleduck-sql-otel-collector -Dtest=OtelCollectorBenchmark
 *
 * <p>The default sweep is bounded by the 1 s flush ({@code maxDelayMs}): every request waits for
 * its batch to be persisted, so records/s is roughly clients x batch / 1 s whatever the CPU cost.
 * To measure CPU, use the steady mode, which offers a FIXED rate so two builds can be compared by
 * the CPU they spend on the same work (record a JFR alongside, e.g. via JAVA_TOOL_OPTIONS):
 * <pre>
 *   -Dbenchmark.rate=15500        records/s to offer; enables steady mode
 *   -Dbenchmark.duration=120      seconds (after a 20 s warmup at the same rate)
 *   -Dbenchmark.batch=500         records per export request
 *   -Dbenchmark.clients=32        concurrent requests; must cover rate x request latency
 *   -Dbenchmark.attributes=realistic
 *   -Dbenchmark.string_copy=false  otel_collector.attributes.string_copy (default true)
 *   -Dbenchmark.attribute_types=true  record attribute name/type mappings (default off), to measure
 *                                  what AttributeTypeScan's per-row layout check costs
 *   -Dbenchmark.signal=traces      steady mode only: send spans instead of log records
 * </pre>
 * {@code benchmark.attributes=realistic} gives each record 40 attributes of every OTLP value type
 * (25 string, 6 int, 4 double, 3 bool, 1 bytes, 1 array) and its resource 17, in the 30-60 range
 * the collector was profiled at; the default {@code minimal} keeps the original two strings.
 */
public class OtelCollectorBenchmark {

    private static final String SECRET_KEY_BASE64 =
            "VGhpcyBpcyBhIDY0IGJpdCBsb25nIGtleSB3aGljaCBzaG91bGQgYmUgY2hhbmdlZCBpbiBwcm9kdWN0aW9uLiBTbyBjaGFuZ2UgbWUgYW5kIG1ha2Ugc3VyZSBpdHMgMTI4IGJpdCBsb25nIG9yIG1vcmU";

    private static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private static final int   PARALLEL_CLIENTS = Integer.getInteger("benchmark.clients", 2);

    private static final int[] BATCH_SIZES   = parseIntArray("benchmark.batches",   "100,1000,10000");
    private static final int[] DURATIONS_SEC = parseIntArray("benchmark.durations", "2,5");
    private static final int   STEADY_RATE   = Integer.getInteger("benchmark.rate", 0);
    private static final int   STEADY_SEC    = Integer.getInteger("benchmark.duration", 120);
    private static final int   STEADY_BATCH  = Integer.getInteger("benchmark.batch", 500);
    private static final int   STEADY_WARMUP_SEC = 20;
    /** Steady mode only: "logs" (default) or "traces" — spans with 3 events and 1 link each. */
    private static final boolean STEADY_TRACES = "traces".equals(System.getProperty("benchmark.signal", "logs"));
    private static final boolean REALISTIC_ATTRIBUTES =
            "realistic".equals(System.getProperty("benchmark.attributes", "minimal"));
    private static final int   WARMUP_SEC    = 3;
    private static final int   MAX_SAMPLES   = 100_000;
    private static final int   MSG_BODY_LEN  = 200;
    private static final long  MIN_BUCKET_BYTES = 8L * 1024 * 1024; // 8 MB

    private static final String[] SERVICES   = {"payment-service", "auth-service", "order-service", "inventory-service", "notification-service"};
    private static final String[] ENVS        = {"prod", "staging", "dev"};
    private static final String[] LOG_LEVELS  = {"INFO", "WARN", "ERROR", "DEBUG"};
    private static final SeverityNumber[] SEVERITIES = {
        SeverityNumber.SEVERITY_NUMBER_INFO, SeverityNumber.SEVERITY_NUMBER_WARN,
        SeverityNumber.SEVERITY_NUMBER_ERROR, SeverityNumber.SEVERITY_NUMBER_DEBUG
    };
    private static final String BODY_CHARS =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .:/-_[]()";
    private static final Random RNG = new Random(42);

    @org.junit.jupiter.api.BeforeAll
    static void loadExtensions() throws Exception {
        // OtelCollectorServer does not run the startup script (Main does); load read_arrow here.
        io.dazzleduck.sql.commons.ConnectionPool.executeBatch(new String[]{
                "INSTALL arrow FROM community", "LOAD arrow"
        });
    }

    @Test
    void benchmark() throws Exception {
        int port = findFreePort();
        Path outputDir = Files.createTempDirectory("otel-benchmark");
        Path logsDir   = outputDir.resolve("logs");

        CollectorProperties props = new CollectorProperties();
        props.setGrpcPort(port);
        Map<String, String> signalPaths = Map.of(
                "logs",    logsDir.toString(),
                "traces",  outputDir.resolve("traces").toString(),
                "metrics", outputDir.resolve("metrics").toString());
        for (String path : signalPaths.values()) {
            Files.createDirectories(Path.of(path));
        }
        props.setIngestionHandler(new io.dazzleduck.sql.commons.ingestion.IngestionHandler() {
            @Override public io.dazzleduck.sql.commons.ingestion.PostIngestionTask
            createPostIngestionTask(io.dazzleduck.sql.commons.ingestion.IngestionResult r) {
                return io.dazzleduck.sql.commons.ingestion.PostIngestionTask.NOOP;
            }
            @Override public String getTargetPath(String id) { return signalPaths.getOrDefault(id, "./" + id); }
            @Override public String[] getPartitionBy(String id) { return new String[0]; }
            // -Dbenchmark.attribute_types=true makes every queue name a registry table, which is
            // what turns detection on; the rows themselves go nowhere (see below).
            @Override public String getAttributeTypesTable(String id) {
                return Boolean.getBoolean("benchmark.attribute_types") ? "benchmark_attribute_types" : null;
            }

            // Cache one queue per id, as the DuckLake handlers do. The interface default builds a
            // new queue (and writer thread) on EVERY export and never closes it, so requests never
            // share a batch and the thread count grows without bound under load.
            private final Map<String, io.dazzleduck.sql.commons.ingestion.ParquetIngestionQueue> queues =
                    new java.util.concurrent.ConcurrentHashMap<>();
            @Override public io.dazzleduck.sql.commons.ingestion.ParquetIngestionQueue getOrCreateQueue(
                    String id, QueueCreator creator, QueueEventListener listener) {
                return queues.computeIfAbsent(id, k -> {
                    var queue = creator.create(k, getTargetPath(k));
                    listener.onCreated(k);
                    return queue;
                });
            }
        });
        props.setIngestionConfig(new io.dazzleduck.sql.commons.ingestion.IngestionConfig(
                MIN_BUCKET_BYTES,
                io.dazzleduck.sql.commons.ingestion.IngestionConfig.DEFAULT_MAX_BUCKET_SIZE,
                io.dazzleduck.sql.commons.ingestion.IngestionConfig.DEFAULT_MAX_BATCHES,
                io.dazzleduck.sql.commons.ingestion.IngestionConfig.DEFAULT_MAX_PENDING_WRITE,
                java.time.Duration.ofMillis(1000L)));
        props.setSecretKey(SECRET_KEY_BASE64);
        props.setUsers(Map.of("admin", "admin"));
        props.setAttributesStringCopy(Boolean.parseBoolean(System.getProperty("benchmark.string_copy", "true")));

        OtelCollectorServer server = new OtelCollectorServer(props);
        server.start();

        Metadata authHeader = new Metadata();
        authHeader.put(AUTHORIZATION_KEY, "Bearer " + generateToken());

        List<ManagedChannel> channels = new ArrayList<>(PARALLEL_CLIENTS);
        List<LogsServiceGrpc.LogsServiceStub> stubs = new ArrayList<>(PARALLEL_CLIENTS);
        for (int c = 0; c < PARALLEL_CLIENTS; c++) {
            ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", port)
                    .usePlaintext().build();
            channels.add(ch);
            stubs.add(LogsServiceGrpc.newStub(ch)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(authHeader)));
        }

        if (STEADY_RATE > 0) {
            runSteady(channels);
        } else {
            runSweep(stubs, logsDir);
        }

        for (ManagedChannel ch : channels) {
            ch.shutdown();
            ch.awaitTermination(5, TimeUnit.SECONDS);
        }
        server.close();
    }

    private void runSweep(List<LogsServiceGrpc.LogsServiceStub> stubs, Path logsDir) throws IOException {
        System.out.println();
        System.out.println("=== OtelCollector Ingestion Benchmark (LOGS) ===");
        System.out.printf("  parallel clients: %d%n", PARALLEL_CLIENTS);
        System.out.printf("%n  [warmup] batch=100, %ds...%n%n", WARMUP_SEC);
        runLoop(stubs, 100, Duration.ofSeconds(WARMUP_SEC), logsDir); // discard

        // Collect all run results
        int combinations = BATCH_SIZES.length * DURATIONS_SEC.length;
        RunResult[] results = new RunResult[combinations];
        int idx = 0;
        for (int batchSize : BATCH_SIZES) {
            // Measure message size once per batch size (representative sample)
            int msgSizeBytes = buildRequest(batchSize).getSerializedSize();
            for (int durationSec : DURATIONS_SEC) {
                results[idx++] = runLoop(stubs, batchSize, Duration.ofSeconds(durationSec), logsDir)
                        .withMeta(batchSize, durationSec, msgSizeBytes);
            }
        }

        printThroughputTable(results);
        printStorageTable(results);
    }

    // -------------------------------------------------------------------------
    // Steady mode — fixed offered rate, for CPU comparisons between builds
    // -------------------------------------------------------------------------

    private void runSteady(List<ManagedChannel> channels) throws InterruptedException {
        // Built once and reused, so the load generator spends no CPU building messages while the
        // collector is being measured; 64 distinct batches keep the content from being uniform.
        SteadySender sender;
        int requestBytes;
        if (STEADY_TRACES) {
            List<TraceServiceGrpc.TraceServiceStub> stubs = new ArrayList<>();
            for (ManagedChannel ch : channels) {
                stubs.add(TraceServiceGrpc.newStub(ch).withInterceptors(
                        MetadataUtils.newAttachHeadersInterceptor(bearer(generateToken("traces")))));
            }
            List<ExportTraceServiceRequest> pool = new ArrayList<>(64);
            for (int i = 0; i < 64; i++) pool.add(buildTraceRequest(STEADY_BATCH));
            requestBytes = pool.get(0).getSerializedSize();
            sender = (client, index, done) -> stubs.get(client).export(pool.get(index % pool.size()), observer(done));
        } else {
            List<LogsServiceGrpc.LogsServiceStub> stubs = new ArrayList<>();
            for (ManagedChannel ch : channels) {
                stubs.add(LogsServiceGrpc.newStub(ch).withInterceptors(
                        MetadataUtils.newAttachHeadersInterceptor(bearer(generateToken("logs")))));
            }
            List<ExportLogsServiceRequest> pool = new ArrayList<>(64);
            for (int i = 0; i < 64; i++) pool.add(buildRequest(STEADY_BATCH));
            requestBytes = pool.get(0).getSerializedSize();
            sender = (client, index, done) -> stubs.get(client).export(pool.get(index % pool.size()), observer(done));
        }

        System.out.println();
        System.out.printf("=== OtelCollector Steady-Rate Benchmark (%s) ===%n", STEADY_TRACES ? "TRACES" : "LOGS");
        System.out.printf("  rate %,d rec/s, batch %d, clients %d, attributes %s, %d B/request%n",
                STEADY_RATE, STEADY_BATCH, channels.size(), REALISTIC_ATTRIBUTES ? "realistic" : "minimal",
                requestBytes);
        System.out.printf("  [warmup] %ds%n", STEADY_WARMUP_SEC);
        steadyLoop(sender, channels.size(), Duration.ofSeconds(STEADY_WARMUP_SEC));
        System.out.printf("  [measure] %ds%n", STEADY_SEC);
        // Epoch-ms markers bound the measured window, so a JFR summary can drop the warmup.
        System.out.println("STEADY-START " + System.currentTimeMillis());
        long t0 = System.nanoTime();
        long cpu0 = processCpuNanos();
        long[] result = steadyLoop(sender, channels.size(), Duration.ofSeconds(STEADY_SEC));
        double seconds = (System.nanoTime() - t0) / 1e9;
        long cpuNanos = processCpuNanos() - cpu0;
        System.out.println("STEADY-END " + System.currentTimeMillis());
        System.out.printf("  sent %,d records in %.1fs = %,.0f rec/s; errors %d; requests behind schedule %d%n",
                result[0], seconds, result[0] / seconds, result[1], result[2]);
        // At a FIXED offered rate the interesting number is not rec/s — both builds deliver the
        // rate until they saturate — but the CPU spent delivering it. This process runs the clients
        // too, so the absolute figure is not the collector's alone; the DIFFERENCE between two runs
        // of the same profile is, since the client work is identical.
        if (cpuNanos > 0) {
            System.out.printf("  process CPU %.1fs over the window = %.0f ns/record (clients included)%n",
                    cpuNanos / 1e9, cpuNanos / (double) Math.max(1, result[0]));
        }
    }

    /** Whole-process CPU time, or -1 where the JDK does not expose it. */
    private static long processCpuNanos() {
        var os = java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        return os instanceof com.sun.management.OperatingSystemMXBean sun ? sun.getProcessCpuTime() : -1L;
    }

    /** Sends pooled request {@code index} on client {@code client}; {@code done} gets success. */
    private interface SteadySender {
        void send(int client, int index, java.util.function.Consumer<Boolean> done);
    }

    private static <T> StreamObserver<T> observer(java.util.function.Consumer<Boolean> done) {
        return new StreamObserver<>() {
            @Override public void onNext(T v) {}
            @Override public void onError(Throwable t) { done.accept(false); }
            @Override public void onCompleted() { done.accept(true); }
        };
    }

    /** Returns {records acknowledged, errors, requests that could not be sent on schedule}. */
    private static long[] steadyLoop(SteadySender sender, int clients, Duration duration) throws InterruptedException {
        long intervalNs = (long) (1e9 * STEADY_BATCH / STEADY_RATE);
        Semaphore inFlight = new Semaphore(clients);
        AtomicInteger acked = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        long late = 0;
        long start = System.nanoTime();
        long end = start + duration.toNanos();
        for (long i = 0; ; i++) {
            long due = start + i * intervalNs;
            if (due >= end) break;
            long wait = due - System.nanoTime();
            if (wait > 0) {
                TimeUnit.NANOSECONDS.sleep(wait);
            }
            if (!inFlight.tryAcquire()) {
                late++;                       // every client busy: the collector is not keeping up
                inFlight.acquireUninterruptibly();
            }
            sender.send((int) (i % clients), (int) (i % 64), ok -> {
                if (ok) acked.addAndGet(STEADY_BATCH); else errors.incrementAndGet();
                inFlight.release();
            });
        }
        inFlight.acquireUninterruptibly(clients);
        inFlight.release(clients);
        return new long[]{acked.get(), errors.get(), late};
    }

    /**
     * Spans with the record attributes of {@link #buildRequest} (realistic or minimal), the same
     * resource, and 3 events (5 attributes each) plus 1 link (3 attributes) per span.
     */
    private ExportTraceServiceRequest buildTraceRequest(int spanCount) {
        ScopeSpans.Builder scopeSpans = ScopeSpans.newBuilder();
        for (int i = 0; i < spanCount; i++) {
            long start = System.nanoTime();
            Span.Builder span = Span.newBuilder()
                    .setTraceId(uuidToBytes16(UUID.randomUUID()))
                    .setSpanId(uuidToBytes8(UUID.randomUUID()))
                    .setName("GET /api/" + SERVICES[RNG.nextInt(SERVICES.length)])
                    .setKind(Span.SpanKind.SPAN_KIND_SERVER)
                    .setStartTimeUnixNano(start)
                    .setEndTimeUnixNano(start + RNG.nextInt(50_000_000))
                    .addAttributes(attr("service.name", AnyValue.newBuilder().setStringValue(SERVICES[RNG.nextInt(SERVICES.length)])))
                    .addAllAttributes(REALISTIC_ATTRIBUTES ? realisticAttributes(i) : List.of());
            for (int e = 0; e < 3; e++) {
                span.addEvents(Span.Event.newBuilder()
                        .setName("event-" + e)
                        .setTimeUnixNano(start + e * 1_000_000L)
                        .addAttributes(attr("event.string", AnyValue.newBuilder().setStringValue("v-" + RNG.nextInt(100))))
                        .addAttributes(attr("event.int", AnyValue.newBuilder().setIntValue(RNG.nextInt(1000))))
                        .addAttributes(attr("event.double", AnyValue.newBuilder().setDoubleValue(RNG.nextDouble())))
                        .addAttributes(attr("event.bool", AnyValue.newBuilder().setBoolValue(RNG.nextBoolean())))
                        .addAttributes(attr("event.tag", AnyValue.newBuilder().setStringValue(ENVS[RNG.nextInt(ENVS.length)]))));
            }
            span.addLinks(Span.Link.newBuilder()
                    .setTraceId(uuidToBytes16(UUID.randomUUID()))
                    .setSpanId(uuidToBytes8(UUID.randomUUID()))
                    .addAttributes(attr("link.kind", AnyValue.newBuilder().setStringValue("follows_from")))
                    .addAttributes(attr("link.index", AnyValue.newBuilder().setIntValue(i)))
                    .addAttributes(attr("link.sampled", AnyValue.newBuilder().setBoolValue(true))));
            scopeSpans.addSpans(span);
        }
        ResourceSpans.Builder resourceSpans = ResourceSpans.newBuilder().addScopeSpans(scopeSpans.build());
        if (REALISTIC_ATTRIBUTES) {
            resourceSpans.setResource(io.opentelemetry.proto.resource.v1.Resource.newBuilder()
                    .addAllAttributes(realisticResourceAttributes()));
        }
        return ExportTraceServiceRequest.newBuilder().addResourceSpans(resourceSpans.build()).build();
    }

    private static Metadata bearer(String token) {
        Metadata metadata = new Metadata();
        metadata.put(AUTHORIZATION_KEY, "Bearer " + token);
        return metadata;
    }

    // -------------------------------------------------------------------------
    // Core loop — returns a RunResult with throughput + storage stats
    // -------------------------------------------------------------------------

    private RunResult runLoop(List<LogsServiceGrpc.LogsServiceStub> stubs,
                              int batchSize,
                              Duration duration,
                              Path logsDir) throws IOException {
        Set<Path> before = listParquet(logsDir);
        long deadline = System.currentTimeMillis() + duration.toMillis();
        int maxInFlight = stubs.size();

        Semaphore inFlight = new Semaphore(maxInFlight);
        long[] latBuf = new long[MAX_SAMPLES];
        AtomicInteger count = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger stubIdx = new AtomicInteger();

        while (System.currentTimeMillis() < deadline && count.get() < MAX_SAMPLES) {
            inFlight.acquireUninterruptibly();
            int idx = stubIdx.getAndIncrement() % stubs.size();
            ExportLogsServiceRequest request = buildRequest(batchSize);
            long t0 = System.nanoTime();
            stubs.get(idx).export(request, new StreamObserver<>() {
                @Override public void onNext(ExportLogsServiceResponse v) {}
                @Override public void onError(Throwable t) {
                    errors.incrementAndGet();
                    inFlight.release();
                }
                @Override public void onCompleted() {
                    int c = count.getAndIncrement();
                    if (c < MAX_SAMPLES) latBuf[c] = System.nanoTime() - t0;
                    inFlight.release();
                }
            });
        }

        // Drain all in-flight requests
        inFlight.acquireUninterruptibly(maxInFlight);

        int totalCount = count.get();
        long[] latenciesNs = Arrays.copyOf(latBuf, Math.min(totalCount, MAX_SAMPLES));

        Set<Path> after = listParquet(logsDir);
        after.removeAll(before);
        long parquetFiles = after.size();
        long totalParquetBytes = after.stream().mapToLong(p -> {
            try { return Files.size(p); } catch (IOException e) { return 0; }
        }).sum();

        return new RunResult(totalCount, errors.get(), latenciesNs, parquetFiles, totalParquetBytes,
                0, 0, 0);  // meta filled in by withMeta()
    }

    // -------------------------------------------------------------------------
    // Request builder
    // -------------------------------------------------------------------------

    private ExportLogsServiceRequest buildRequest(int recordCount) {
        ScopeLogs.Builder scopeLogs = ScopeLogs.newBuilder();
        for (int i = 0; i < recordCount; i++) {
            int sevIdx = RNG.nextInt(SEVERITIES.length);
            scopeLogs.addLogRecords(LogRecord.newBuilder()
                    .setTimeUnixNano(System.nanoTime())
                    .setSeverityNumber(SEVERITIES[sevIdx])
                    .setSeverityText(LOG_LEVELS[sevIdx])
                    .setBody(AnyValue.newBuilder()
                            .setStringValue(randomBody())
                            .build())
                    .addAttributes(KeyValue.newBuilder()
                            .setKey("service.name")
                            .setValue(AnyValue.newBuilder()
                                    .setStringValue(SERVICES[RNG.nextInt(SERVICES.length)])
                                    .build())
                            .build())
                    .addAttributes(KeyValue.newBuilder()
                            .setKey("deployment.environment")
                            .setValue(AnyValue.newBuilder()
                                    .setStringValue(ENVS[RNG.nextInt(ENVS.length)])
                                    .build())
                            .build())
                    .addAllAttributes(REALISTIC_ATTRIBUTES ? realisticAttributes(i) : List.of())
                    .setTraceId(uuidToBytes16(UUID.randomUUID()))
                    .setSpanId(uuidToBytes8(UUID.randomUUID()))
                    .build());
        }
        ResourceLogs.Builder resourceLogs = ResourceLogs.newBuilder().addScopeLogs(scopeLogs.build());
        if (REALISTIC_ATTRIBUTES) {
            resourceLogs.setResource(io.opentelemetry.proto.resource.v1.Resource.newBuilder()
                    .addAllAttributes(realisticResourceAttributes()));
        }
        return ExportLogsServiceRequest.newBuilder().addResourceLogs(resourceLogs.build()).build();
    }

    /** 38 more attributes (40 with the two above), of every OTLP value type. */
    private static List<KeyValue> realisticAttributes(int record) {
        List<KeyValue> attrs = new ArrayList<>(38);
        for (int i = 0; i < 23; i++) {
            attrs.add(attr("app.string." + i, AnyValue.newBuilder().setStringValue("value-" + RNG.nextInt(50) + "-" + i)));
        }
        for (int i = 0; i < 6; i++) {
            attrs.add(attr("app.int." + i, AnyValue.newBuilder().setIntValue(RNG.nextInt(100_000))));
        }
        for (int i = 0; i < 4; i++) {
            attrs.add(attr("app.double." + i, AnyValue.newBuilder().setDoubleValue(RNG.nextDouble() * 1000)));
        }
        for (int i = 0; i < 3; i++) {
            attrs.add(attr("app.bool." + i, AnyValue.newBuilder().setBoolValue(RNG.nextBoolean())));
        }
        attrs.add(attr("app.bytes", AnyValue.newBuilder().setBytesValue(uuidToBytes8(UUID.randomUUID()))));
        attrs.add(attr("app.tags", AnyValue.newBuilder().setArrayValue(
                io.opentelemetry.proto.common.v1.ArrayValue.newBuilder()
                        .addValues(AnyValue.newBuilder().setStringValue(ENVS[record % ENVS.length]))
                        .addValues(AnyValue.newBuilder().setIntValue(record)))));
        return attrs;
    }

    private static List<KeyValue> realisticResourceAttributes() {
        List<KeyValue> attrs = new ArrayList<>(17);
        for (int i = 0; i < 15; i++) {
            attrs.add(attr("resource.string." + i, AnyValue.newBuilder().setStringValue(SERVICES[i % SERVICES.length])));
        }
        attrs.add(attr("process.pid", AnyValue.newBuilder().setIntValue(4242)));
        attrs.add(attr("host.cpu.count", AnyValue.newBuilder().setIntValue(8)));
        return attrs;
    }

    private static KeyValue attr(String key, AnyValue.Builder value) {
        return KeyValue.newBuilder().setKey(key).setValue(value).build();
    }

    private static String randomBody() {
        char[] buf = new char[MSG_BODY_LEN];
        for (int i = 0; i < MSG_BODY_LEN; i++) {
            buf[i] = BODY_CHARS.charAt(RNG.nextInt(BODY_CHARS.length()));
        }
        return new String(buf);
    }

    private static ByteString uuidToBytes16(UUID uuid) {
        byte[] b = new byte[16];
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        for (int i = 7; i >= 0; i--) { b[i]     = (byte)(msb & 0xff); msb >>= 8; }
        for (int i = 15; i >= 8; i--) { b[i]    = (byte)(lsb & 0xff); lsb >>= 8; }
        return ByteString.copyFrom(b);
    }

    private static ByteString uuidToBytes8(UUID uuid) {
        byte[] b = new byte[8];
        long msb = uuid.getMostSignificantBits();
        for (int i = 7; i >= 0; i--) { b[i] = (byte)(msb & 0xff); msb >>= 8; }
        return ByteString.copyFrom(b);
    }

    // -------------------------------------------------------------------------
    // Result record
    // -------------------------------------------------------------------------

    record RunResult(int requests, int errors, long[] latenciesNs,
                     long parquetFiles, long totalParquetBytes,
                     int batchSize, int durationSec, int msgSizeBytes) {

        RunResult withMeta(int batchSize, int durationSec, int msgSizeBytes) {
            return new RunResult(requests, errors, latenciesNs,
                    parquetFiles, totalParquetBytes, batchSize, durationSec, msgSizeBytes);
        }

        long totalRecords()      { return (long) requests * batchSize; }
        double recordsPerSec()   { return (double) totalRecords() / durationSec; }
        long avgParquetBytes()   { return parquetFiles > 0 ? totalParquetBytes / parquetFiles : 0; }
        long totalNetworkBytes() { return (long) requests * msgSizeBytes; }
        double compressionRatio() {
            return totalParquetBytes > 0 ? (double) totalNetworkBytes() / totalParquetBytes : 0;
        }

        long percentileMs(int pct) {
            if (requests == 0) return 0;
            long[] sorted = Arrays.copyOf(latenciesNs, requests);
            Arrays.sort(sorted);
            int i = (int) Math.ceil(pct / 100.0 * requests) - 1;
            return sorted[Math.max(0, i)] / 1_000_000;
        }
    }

    // -------------------------------------------------------------------------
    // Printing
    // -------------------------------------------------------------------------

    private void printThroughputTable(RunResult[] results) {
        System.out.printf("  %-10s  %-4s  %-10s  %-14s  %-13s  %7s  %7s  %7s  %6s%n",
                "Batch", "Dur", "Requests", "Records", "Rec/sec",
                "P50(ms)", "P95(ms)", "P99(ms)", "Errors");
        System.out.println("  " + "-".repeat(92));
        for (RunResult r : results) {
            System.out.printf("  %-10d  %-4s  %-10d  %-14d  %-,13.0f  %7d  %7d  %7d  %6d%n",
                    r.batchSize(), r.durationSec() + "s",
                    r.requests(), r.totalRecords(), r.recordsPerSec(),
                    r.percentileMs(50), r.percentileMs(95), r.percentileMs(99),
                    r.errors());
        }
        System.out.println("  " + "-".repeat(92));
    }

    private void printStorageTable(RunResult[] results) {
        System.out.println();
        System.out.printf("  %-10s  %-4s  %-12s  %-14s  %-14s  %-14s  %-14s  %s%n",
                "Batch", "Dur", "Msg size", "Network in", "Parquet out", "Parquet files", "Avg file", "Ratio");
        System.out.println("  " + "-".repeat(102));
        for (RunResult r : results) {
            System.out.printf("  %-10d  %-4s  %-12s  %-14s  %-14s  %-14d  %-14s  %.2fx%n",
                    r.batchSize(), r.durationSec() + "s",
                    formatBytes(r.msgSizeBytes()),
                    formatBytes(r.totalNetworkBytes()),
                    formatBytes(r.totalParquetBytes()),
                    r.parquetFiles(),
                    formatBytes(r.avgParquetBytes()),
                    r.compressionRatio());
        }
        System.out.println("  " + "-".repeat(102));
        System.out.println();
        System.out.println("  Note: each request blocks until the batch is persisted to Parquet.");
        System.out.println("  minBucketSizeBytes=8MB, maxDelayMs=1000ms.");
        System.out.println();
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024)            return bytes + " B";
        if (bytes < 1024 * 1024)     return String.format("%.1f KB", bytes / 1024.0);
        return String.format("%.1f MB", bytes / (1024.0 * 1024));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static Set<Path> listParquet(Path dir) throws IOException {
        if (!Files.exists(dir)) return java.util.Collections.emptySet();
        try (var stream = Files.walk(dir)) {
            return stream.filter(p -> p.toString().endsWith(".parquet"))
                    .collect(Collectors.toSet());
        }
    }

    private static String generateToken() {
        return generateToken("logs");
    }

    private static String generateToken(String queue) {
        SecretKey key = Validator.fromBase64String(SECRET_KEY_BASE64);
        Calendar exp = Calendar.getInstance();
        exp.add(Calendar.HOUR, 1);
        return Jwts.builder().subject("admin")
                // Queue resolution requires this claim since the services were unified behind
                // OtelServiceBase.resolveQueue; without it every export is INVALID_ARGUMENT.
                .claim(io.dazzleduck.sql.common.Headers.CLAIM_INGESTION_QUEUE, queue)
                .expiration(exp.getTime()).signWith(key).compact();
    }

    private static int[] parseIntArray(String property, String defaults) {
        String val = System.getProperty(property, defaults);
        return Arrays.stream(val.split(",")).map(String::trim).mapToInt(Integer::parseInt).toArray();
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }
}
