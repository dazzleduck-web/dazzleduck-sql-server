package bench;

import org.duckdb.DuckDBConnection;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Sidecar-derivation benchmark: one ingested batch (local, uncompressed Arrow IPC files, as the
 * server stores them) is written as a base table plus N sidecar tables, each sidecar a SQL view
 * over the base rows. Strategies differ in what the sidecars are derived from and how the files
 * reach S3 (MinIO here): by DuckDB COPYs, by the AWS SDK, or by DuckDB uploading the staged
 * files unchanged with {@code COPY ... (FORMAT blob)}.
 *
 * Usage: SidecarBench <rowsPerBatch> <inputFiles> <measuredRuns> <s3Endpoint>
 */
public class SidecarBench {

    static final String BUCKET = "bench";
    static final String BASE_TRANSFORMATION = "SELECT *, date_trunc('minute', ts) AS minute FROM __this";
    static final LinkedHashMap<String, String> SIDECARS = new LinkedHashMap<>();
    static {
        SIDECARS.put("metrics", "SELECT minute, service, severity, count(*) AS n, avg(duration_ms) AS avg_ms,"
                + " max(duration_ms) AS max_ms FROM __base GROUP BY ALL");
        SIDECARS.put("errors", "SELECT ts, service, trace_id, http_status, body FROM __base WHERE severity = 'ERROR'");
        SIDECARS.put("attr_keys", "SELECT DISTINCT service, unnest(attr_keys) AS attr_key FROM __base");
    }

    static DuckDBConnection db;
    static S3AsyncClient s3;
    static S3TransferManager tm;
    /** The uploader the spec recommends: the synchronous client on url-connection-client, no Netty. */
    static S3Client s3Sync;
    static final int MAX_CONCURRENT_UPLOADS = 16;
    static final long MULTIPART_THRESHOLD = 16L << 20;
    static final long PART_SIZE = 8L << 20;
    static final ExecutorService uploadPool = Executors.newFixedThreadPool(MAX_CONCURRENT_UPLOADS);
    static Path work;
    static List<Path> inputs;
    static final ExecutorService pool = Executors.newCachedThreadPool();

    /** baseMs, sidecarsMs and uploadMs are -1 for strategies that do not separate them. */
    record Timing(long baseMs, long sidecarsMs, long uploadMs, long totalMs) {
        static Timing total(long t0) {
            return new Timing(-1, -1, -1, ms(t0));
        }
    }

    interface Strategy {
        /** Writes base + sidecars for one batch under {@code prefix}; returns rows per output. */
        Timing run(String prefix, Map<String, Long> rowCounts) throws Exception;
    }

    public static void main(String[] args) throws Exception {
        int rows = Integer.parseInt(args[0]);
        int files = Integer.parseInt(args[1]);
        int runs = Integer.parseInt(args[2]);
        String endpoint = args[3];

        work = Files.createTempDirectory("sidecar-bench");
        db = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        exec(db, "INSTALL arrow FROM community; LOAD arrow; INSTALL httpfs; LOAD httpfs;");
        exec(db, ("CREATE SECRET minio (TYPE s3, KEY_ID 'bench', SECRET 'benchbench', ENDPOINT '%s',"
                + " URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')").formatted(endpoint));

        s3 = S3AsyncClient.builder()
                .endpointOverride(URI.create("http://" + endpoint))
                .forcePathStyle(true)
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("bench", "benchbench")))
                .multipartEnabled(true)
                .build();
        tm = S3TransferManager.builder().s3Client(s3).build();
        s3Sync = S3Client.builder()
                .endpointOverride(URI.create("http://" + endpoint))
                .forcePathStyle(true)
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("bench", "benchbench")))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();
        try {
            s3.createBucket(b -> b.bucket(BUCKET)).join();
        } catch (CompletionException ignored) {
            // exists
        }

        inputs = generateInput(rows, files);
        long inputBytes = 0;
        for (Path p : inputs) inputBytes += Files.size(p);
        System.out.printf("input: %,d rows in %d Arrow IPC files, %,.1f MB uncompressed; DuckDB threads=%s%n",
                rows, files, inputBytes / 1e6, scalar(db, "SELECT current_setting('threads')"));

        LinkedHashMap<String, Strategy> strategies = new LinkedHashMap<>();
        strategies.put("A raw -> S3 direct (sequential COPYs)", SidecarBench::rawDirect);
        strategies.put("B raw -> S3 direct (parallel COPYs)", SidecarBench::rawDirectParallel);
        strategies.put("C temp table -> S3 direct", SidecarBench::tempTableDirect);
        strategies.put("D base -> S3, sidecars read back from S3", SidecarBench::s3Readback);
        strategies.put("E raw -> local, then parallel upload", SidecarBench::rawLocalThenUpload);
        strategies.put("F base -> local parquet, sidecars from it, then upload", SidecarBench::localParquetThenUpload);
        strategies.put("G base -> local parquet, upload overlapped with sidecars", SidecarBench::localParquetPipelined);
        strategies.put("H as G, url-connection client on its own executor", SidecarBench::localParquetPipelinedSync);
        strategies.put("I as G, uploaded by DuckDB: COPY read_blob TO s3 (FORMAT blob)", SidecarBench::localParquetPipelinedDuckDb);

        Map<String, List<Timing>> results = new LinkedHashMap<>();
        // Every strategy must write the same rows, with the same column types, to every output. Row counts
        // are checked on every run; the first measured run also reads each output back from S3 and
        // fingerprints its content.
        Map<String, Map<String, String>> fingerprints = new LinkedHashMap<>();
        Map<String, Long> expectedCounts = null;
        for (int iter = -1; iter < runs; iter++) { // iteration -1 is warm-up
            // Rotate the order each iteration so no strategy always runs first or last.
            List<String> names = new ArrayList<>(strategies.keySet());
            Collections.rotate(names, Math.max(iter, 0));
            for (String name : names) {
                String prefix = "r" + System.nanoTime();
                Map<String, Long> rc = new TreeMap<>();
                Timing t = strategies.get(name).run(prefix, rc);
                if (iter >= 0) results.computeIfAbsent(name, k -> new ArrayList<>()).add(t);
                if (expectedCounts == null) expectedCounts = rc;
                if (!rc.equals(expectedCounts)) {
                    throw new IllegalStateException(name + " wrote " + rc + " rows, expected " + expectedCounts);
                }
                if (iter == 0) fingerprints.put(name, fingerprintOnS3(name, prefix, rc));
                cleanup(prefix);
            }
        }

        Map<String, String> reference = fingerprints.values().iterator().next();
        System.out.println("rows per output: " + expectedCounts);
        reference.forEach((out, fp) -> System.out.println("  " + out + ": " + fp));
        boolean same = true;
        for (var e : fingerprints.entrySet()) {
            for (var out : e.getValue().entrySet()) {
                if (!out.getValue().equals(reference.get(out.getKey()))) {
                    same = false;
                    System.out.println("MISMATCH " + e.getKey() + " " + out.getKey() + ": " + out.getValue());
                }
            }
        }
        System.out.println(same ? "content and column types identical across strategies" : "CONTENT MISMATCH");

        System.out.printf("%n%-58s %10s %10s %10s %11s %10s%n", "strategy (median of " + runs + ")", "total ms",
                "min ms", "base ms", "sidecars ms", "upload ms");
        for (var e : results.entrySet()) {
            List<Timing> ts = e.getValue();
            System.out.printf("%-58s %10d %10d %10s %11s %10s%n", e.getKey(),
                    median(ts.stream().mapToLong(Timing::totalMs)),
                    ts.stream().mapToLong(Timing::totalMs).min().orElse(0),
                    medianOrDash(ts, Timing::baseMs), medianOrDash(ts, Timing::sidecarsMs),
                    medianOrDash(ts, Timing::uploadMs));
        }
        tm.close();
        s3.close();
        s3Sync.close();
        pool.shutdownNow();
        uploadPool.shutdownNow();
        db.close();
        if (!same) System.exit(1);
    }

    // ---- strategies ------------------------------------------------------------------------

    /** A: every output is its own COPY over the raw Arrow files, written straight to S3. */
    static Timing rawDirect(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        rc.put("base", copy(db, baseOver(raw()), s3Path(prefix, "base")));
        for (var sc : SIDECARS.entrySet()) {
            rc.put(sc.getKey(), copy(db, sidecarOver(baseOver(raw()), sc.getValue()), s3Path(prefix, sc.getKey())));
        }
        return Timing.total(t0);
    }

    /** B: as A, but the N+1 COPYs run concurrently on duplicated connections. */
    static Timing rawDirectParallel(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        Map<String, Future<Long>> fs = new LinkedHashMap<>();
        fs.put("base", pool.submit(() -> copyOnDuplicate(baseOver(raw()), s3Path(prefix, "base"))));
        for (var sc : SIDECARS.entrySet()) {
            fs.put(sc.getKey(), pool.submit(() ->
                    copyOnDuplicate(sidecarOver(baseOver(raw()), sc.getValue()), s3Path(prefix, sc.getKey()))));
        }
        for (var f : fs.entrySet()) rc.put(f.getKey(), f.getValue().get());
        return Timing.total(t0);
    }

    /** C: the transformed base materialized once in a temp table; every output COPYs from it to S3. */
    static Timing tempTableDirect(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        try (var conn = (DuckDBConnection) db.duplicate()) {
            exec(conn, "CREATE TEMP TABLE __base_t AS " + baseOver(raw()));
            rc.put("base", copy(conn, "SELECT * FROM __base_t", s3Path(prefix, "base")));
            for (var sc : SIDECARS.entrySet()) {
                rc.put(sc.getKey(), copy(conn, sidecarOver("SELECT * FROM __base_t", sc.getValue()),
                        s3Path(prefix, sc.getKey())));
            }
            exec(conn, "DROP TABLE __base_t");
        }
        return Timing.total(t0);
    }

    /** D: base written to S3 first; each sidecar reads that Parquet back from S3. */
    static Timing s3Readback(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        String base = s3Path(prefix, "base");
        rc.put("base", copy(db, baseOver(raw()), base));
        for (var sc : SIDECARS.entrySet()) {
            rc.put(sc.getKey(), copy(db, sidecarOver("SELECT * FROM read_parquet('" + base + "')", sc.getValue()),
                    s3Path(prefix, sc.getKey())));
        }
        return Timing.total(t0);
    }

    /** E: every output derived from the raw Arrow files into local Parquet; then all uploaded in parallel. */
    static Timing rawLocalThenUpload(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        Path dir = Files.createDirectories(work.resolve(prefix));
        Map<String, Path> out = new LinkedHashMap<>();
        out.put("base", dir.resolve("base.parquet"));
        rc.put("base", copy(db, baseOver(raw()), out.get("base").toString()));
        long baseMs = ms(t0);
        long t1 = System.nanoTime();
        for (var sc : SIDECARS.entrySet()) {
            out.put(sc.getKey(), dir.resolve(sc.getKey() + ".parquet"));
            rc.put(sc.getKey(), copy(db, sidecarOver(baseOver(raw()), sc.getValue()), out.get(sc.getKey()).toString()));
        }
        long sidecarsMs = ms(t1);
        long t2 = System.nanoTime();
        uploadAll(prefix, out);
        long upload = ms(t2);
        deleteDir(dir);
        return new Timing(baseMs, sidecarsMs, upload, ms(t0));
    }

    /** F: base to local Parquet; sidecars derived from that local Parquet; then all uploaded in parallel. */
    static Timing localParquetThenUpload(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        Path dir = Files.createDirectories(work.resolve(prefix));
        Map<String, Path> out = new LinkedHashMap<>();
        out.put("base", dir.resolve("base.parquet"));
        rc.put("base", copy(db, baseOver(raw()), out.get("base").toString()));
        long baseMs = ms(t0);
        long t1 = System.nanoTime();
        String fromBase = "SELECT * FROM read_parquet('" + out.get("base") + "')";
        for (var sc : SIDECARS.entrySet()) {
            out.put(sc.getKey(), dir.resolve(sc.getKey() + ".parquet"));
            rc.put(sc.getKey(), copy(db, sidecarOver(fromBase, sc.getValue()), out.get(sc.getKey()).toString()));
        }
        long sidecarsMs = ms(t1);
        long t2 = System.nanoTime();
        uploadAll(prefix, out);
        long upload = ms(t2);
        deleteDir(dir);
        return new Timing(baseMs, sidecarsMs, upload, ms(t0));
    }

    /** G: as F, but the base file starts uploading as soon as it is written, while the sidecars are derived. */
    static Timing localParquetPipelined(String prefix, Map<String, Long> rc) throws Exception {
        return pipelined(prefix, rc, SidecarBench::upload);
    }

    /** H: as G, uploaded with the client the spec recommends: synchronous, on its own executor. */
    static Timing localParquetPipelinedSync(String prefix, Map<String, Long> rc) throws Exception {
        return pipelined(prefix, rc, SidecarBench::uploadSync);
    }

    /**
     * I: as G, but each file is uploaded by DuckDB itself, byte for byte, with its own S3 writer and
     * secret: {@code COPY (SELECT content FROM read_blob(file)) TO 's3://...' (FORMAT blob)}. No AWS
     * SDK and no credentials outside DuckDB.
     */
    static Timing localParquetPipelinedDuckDb(String prefix, Map<String, Long> rc) throws Exception {
        return pipelined(prefix, rc, SidecarBench::uploadViaDuckDb);
    }

    interface Uploader {
        CompletableFuture<?> upload(String prefix, String name, Path file);
    }

    static Timing pipelined(String prefix, Map<String, Long> rc, Uploader uploader) throws Exception {
        long t0 = System.nanoTime();
        Path dir = Files.createDirectories(work.resolve(prefix));
        Path base = dir.resolve("base.parquet");
        rc.put("base", copy(db, baseOver(raw()), base.toString()));
        long baseMs = ms(t0);
        long t1 = System.nanoTime();
        List<CompletableFuture<?>> uploads = new ArrayList<>();
        uploads.add(uploader.upload(prefix, "base", base));
        String fromBase = "SELECT * FROM read_parquet('" + base + "')";
        for (var sc : SIDECARS.entrySet()) {
            Path p = dir.resolve(sc.getKey() + ".parquet");
            rc.put(sc.getKey(), copy(db, sidecarOver(fromBase, sc.getValue()), p.toString()));
            uploads.add(uploader.upload(prefix, sc.getKey(), p));
        }
        long sidecarsMs = ms(t1);
        long t2 = System.nanoTime();
        CompletableFuture.allOf(uploads.toArray(CompletableFuture[]::new)).join();
        long waitedForUpload = ms(t2);
        deleteDir(dir);
        return new Timing(baseMs, sidecarsMs, waitedForUpload, ms(t0));
    }

    // ---- helpers ---------------------------------------------------------------------------

    static String raw() {
        return "SELECT * FROM read_arrow([" + inputs.stream().map(p -> "'" + p + "'").collect(Collectors.joining(",")) + "])";
    }

    static String baseOver(String source) {
        return "WITH __this AS (" + source + ") " + BASE_TRANSFORMATION;
    }

    static String sidecarOver(String baseRows, String sidecar) {
        return "WITH __base AS (" + baseRows + ") " + sidecar;
    }

    static String s3Path(String prefix, String name) {
        return "s3://" + BUCKET + "/" + prefix + "/" + name + ".parquet";
    }

    static long copy(DuckDBConnection conn, String query, String target) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("COPY (" + query + ") TO '" + target + "' (FORMAT parquet)");
            return st.getUpdateCount();
        }
    }

    static long copyOnDuplicate(String query, String target) throws Exception {
        try (var conn = (DuckDBConnection) db.duplicate()) {
            return copy(conn, query, target);
        }
    }

    static CompletableFuture<?> upload(String prefix, String name, Path file) {
        return tm.uploadFile(UploadFileRequest.builder()
                .putObjectRequest(b -> b.bucket(BUCKET).key(prefix + "/" + name + ".parquet"))
                .source(file).build()).completionFuture();
    }

    /** A single PUT below the multipart threshold; above it, the parts are uploaded concurrently. */
    static CompletableFuture<?> uploadSync(String prefix, String name, Path file) {
        String key = prefix + "/" + name + ".parquet";
        long size;
        try {
            size = Files.size(file);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
        if (size < MULTIPART_THRESHOLD) {
            return CompletableFuture.runAsync(
                    () -> s3Sync.putObject(b -> b.bucket(BUCKET).key(key), RequestBody.fromFile(file)), uploadPool);
        }
        return CompletableFuture.supplyAsync(
                        () -> s3Sync.createMultipartUpload(b -> b.bucket(BUCKET).key(key)).uploadId(), uploadPool)
                .thenCompose(uploadId -> {
                    int parts = (int) ((size + PART_SIZE - 1) / PART_SIZE);
                    List<CompletableFuture<CompletedPart>> fs = IntStream.rangeClosed(1, parts)
                            .mapToObj(n -> CompletableFuture.supplyAsync(() -> {
                                long offset = (n - 1) * PART_SIZE;
                                byte[] bytes = readSlice(file, offset, (int) Math.min(PART_SIZE, size - offset));
                                String etag = s3Sync.uploadPart(b -> b.bucket(BUCKET).key(key).uploadId(uploadId)
                                        .partNumber(n), RequestBody.fromBytes(bytes)).eTag();
                                return CompletedPart.builder().partNumber(n).eTag(etag).build();
                            }, uploadPool))
                            .toList();
                    return CompletableFuture.allOf(fs.toArray(CompletableFuture[]::new))
                            .thenAcceptAsync(v -> s3Sync.completeMultipartUpload(b -> b.bucket(BUCKET).key(key)
                                    .uploadId(uploadId)
                                    .multipartUpload(m -> m.parts(fs.stream().map(CompletableFuture::join).toList()))),
                                    uploadPool);
                });
    }

    /** One file, unchanged, through DuckDB's S3 writer, on a duplicated connection of the upload pool. */
    static CompletableFuture<?> uploadViaDuckDb(String prefix, String name, Path file) {
        return CompletableFuture.runAsync(() -> {
            try (var conn = (DuckDBConnection) db.duplicate()) {
                exec(conn, "COPY (SELECT content FROM read_blob('" + file + "')) TO '" + s3Path(prefix, name)
                        + "' (FORMAT blob)");
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, uploadPool);
    }

    static byte[] readSlice(Path file, long offset, int length) {
        try (FileChannel ch = FileChannel.open(file)) {
            ByteBuffer buf = ByteBuffer.allocate(length);
            while (buf.hasRemaining() && ch.read(buf, offset + buf.position()) >= 0) {
                // read until the slice is full
            }
            return buf.array();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void uploadAll(String prefix, Map<String, Path> files) {
        CompletableFuture.allOf(files.entrySet().stream()
                .map(e -> upload(prefix, e.getKey(), e.getValue()))
                .toArray(CompletableFuture[]::new)).join();
    }

    /**
     * Reads each output back from S3, checks its row count, and returns its column types plus a hash of
     * its rows taken as a multiset (independent of row order), so two strategies compare equal only when
     * they wrote the same rows with the same types. Floating-point columns are rounded to 6 decimals
     * first: a parallel avg() sums in thread order, so the same query can differ in the last bits.
     */
    static Map<String, String> fingerprintOnS3(String name, String prefix, Map<String, Long> rc) throws Exception {
        Map<String, String> out = new TreeMap<>();
        for (String o : rc.keySet()) {
            String file = "read_parquet('" + s3Path(prefix, o) + "')";
            long n = Long.parseLong(scalar(db, "SELECT count(*) FROM " + file));
            if (n != rc.get(o)) throw new IllegalStateException(name + ": " + o + " has " + n + " rows on S3, wrote " + rc.get(o));
            String types = scalar(db, "SELECT string_agg(column_name || ' ' || column_type, ', ')"
                    + " FROM (DESCRIBE SELECT * FROM " + file + ")");
            String columns = scalar(db, "SELECT string_agg(CASE WHEN column_type IN ('DOUBLE', 'FLOAT')"
                    + " THEN 'round(\"' || column_name || '\", 6)' ELSE '\"' || column_name || '\"' END, ', ')"
                    + " FROM (DESCRIBE SELECT * FROM " + file + ")");
            String rows = scalar(db, "SELECT sum(hash(t)::HUGEINT) FROM (SELECT " + columns + " FROM " + file + ") t");
            out.put(o, "[" + types + "] rows hash " + rows);
        }
        return out;
    }

    static void cleanup(String prefix) {
        var keys = s3.listObjectsV2(b -> b.bucket(BUCKET).prefix(prefix + "/")).join().contents().stream()
                .map(o -> ObjectIdentifier.builder().key(o.key()).build()).toList();
        if (!keys.isEmpty()) {
            s3.deleteObjects(b -> b.bucket(BUCKET).delete(Delete.builder().objects(keys).build())).join();
        }
    }

    static List<Path> generateInput(int rows, int files) throws Exception {
        List<Path> out = new ArrayList<>();
        int per = rows / files;
        for (int i = 0; i < files; i++) {
            Path p = work.resolve("in_" + i + ".arrow");
            long off = (long) i * per;
            int n = per + (i == files - 1 ? rows % files : 0); // the last file takes the remainder
            String gen = """
                    SELECT TIMESTAMP '2026-10-31 00:00:00' + to_seconds((r %% 86400)::BIGINT) AS ts,
                           'svc-' || (hash(r) %% 50) AS service,
                           CASE WHEN hash(r + 1) %% 20 = 0 THEN 'ERROR' WHEN hash(r + 1) %% 20 < 3 THEN 'WARN'
                                ELSE 'INFO' END AS severity,
                           md5(r::VARCHAR) AS trace_id,
                           'request handled for ' || md5((r + 7)::VARCHAR) || ' ' || md5((r + 11)::VARCHAR) AS body,
                           ['http.method', 'http.route', 'k' || (hash(r + 2) %% 200)] AS attr_keys,
                           [200, 200, 200, 404, 500][1 + (hash(r + 3) %% 5)::INT] AS http_status,
                           (hash(r + 4) %% 100000) / 100.0 AS duration_ms,
                           't' || (hash(r + 5) %% 10) AS tenant
                    FROM (SELECT range + %d AS r FROM range(%d))
                    """.formatted(off, n);
            exec(db, "COPY (" + gen + ") TO '" + p + "' (FORMAT arrow)");
            out.add(p);
        }
        return out;
    }

    static void exec(DuckDBConnection conn, String sql) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    static String scalar(DuckDBConnection conn, String sql) throws Exception {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    static long ms(long t0) {
        return (System.nanoTime() - t0) / 1_000_000;
    }

    static String medianOrDash(List<Timing> ts, java.util.function.ToLongFunction<Timing> f) {
        return f.applyAsLong(ts.get(0)) < 0 ? "-" : String.valueOf(median(ts.stream().mapToLong(f)));
    }

    static long median(java.util.stream.LongStream s) {
        long[] a = s.sorted().toArray();
        return a.length == 0 ? 0 : a[a.length / 2];
    }

    static void deleteDir(Path dir) throws Exception {
        try (var walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
