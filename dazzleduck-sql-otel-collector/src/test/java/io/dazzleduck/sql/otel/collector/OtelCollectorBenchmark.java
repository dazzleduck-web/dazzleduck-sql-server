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
 */
public class OtelCollectorBenchmark {

    private static final String SECRET_KEY_BASE64 =
            "VGhpcyBpcyBhIDY0IGJpdCBsb25nIGtleSB3aGljaCBzaG91bGQgYmUgY2hhbmdlZCBpbiBwcm9kdWN0aW9uLiBTbyBjaGFuZ2UgbWUgYW5kIG1ha2Ugc3VyZSBpdHMgMTI4IGJpdCBsb25nIG9yIG1vcmU";

    private static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private static final int   PARALLEL_CLIENTS = Integer.getInteger("benchmark.clients", 2);

    private static final int[] BATCH_SIZES   = parseIntArray("benchmark.batches",   "100,1000,10000");
    private static final int[] DURATIONS_SEC = parseIntArray("benchmark.durations", "2,5");
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

    @Test
    void benchmark() throws Exception {
        int port = findFreePort();
        Path outputDir = Files.createTempDirectory("otel-benchmark");
        Path logsDir   = outputDir.resolve("logs");

        CollectorProperties props = new CollectorProperties();
        props.setGrpcPort(port);
        props.setLogIngestionConfig(new io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig(
                logsDir.toString(), java.util.List.of(), null, MIN_BUCKET_BYTES, 1000L));
        props.setTraceIngestionConfig(new io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig(
                outputDir.resolve("traces").toString(), java.util.List.of(), null, MIN_BUCKET_BYTES, 1000L));
        props.setMetricIngestionConfig(new io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig(
                outputDir.resolve("metrics").toString(), java.util.List.of(), null, MIN_BUCKET_BYTES, 1000L));
        props.setSecretKey(SECRET_KEY_BASE64);
        props.setUsers(Map.of("admin", "admin"));

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

        for (ManagedChannel ch : channels) {
            ch.shutdown();
            ch.awaitTermination(5, TimeUnit.SECONDS);
        }
        server.close();
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
                    .setTraceId(uuidToBytes16(UUID.randomUUID()))
                    .setSpanId(uuidToBytes8(UUID.randomUUID()))
                    .build());
        }
        return ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(scopeLogs.build())
                        .build())
                .build();
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
        SecretKey key = Validator.fromBase64String(SECRET_KEY_BASE64);
        Calendar exp = Calendar.getInstance();
        exp.add(Calendar.HOUR, 1);
        return Jwts.builder().subject("admin").expiration(exp.getTime()).signWith(key).compact();
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
