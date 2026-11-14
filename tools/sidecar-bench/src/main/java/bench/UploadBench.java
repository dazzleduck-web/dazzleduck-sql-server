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
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

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
import java.util.stream.IntStream;

/**
 * Uploads existing local files of fixed sizes to S3 (MinIO) with three uploaders, to compare them
 * apart from any Parquet writing:
 * <ul>
 *   <li>{@code duckdb-blob}: DuckDB's own S3 writer and secret,
 *       {@code COPY (SELECT content FROM read_blob(file)) TO 's3://...' (FORMAT blob)}</li>
 *   <li>{@code sdk-sync}: the AWS SDK's synchronous client on url-connection-client, as strategy H
 *       uses it but called inline: one PUT below 16 MiB, concurrent 8 MiB parts from 16 MiB (so the
 *       16 MB file is a two-part upload)</li>
 *   <li>{@code sdk-tm}: the AWS SDK's async client and transfer manager (strategy G)</li>
 * </ul>
 * For each size: the time of one upload on its own (median, p95, min of {@code runs}), and of a batch
 * of four uploaded at once (a base file and three sidecars). For DuckDB, also the HTTP requests one
 * upload makes. Every upload's object size is checked against its file.
 *
 * Usage: UploadBench endpoint runs [settings [uploaders]]
 *
 * {@code settings}: DuckDB settings applied globally before any upload, as
 * {@code name=value;name=value} (e.g. {@code s3_uploader_max_filesize='800GB'}), or {@code -} for none.
 * {@code uploaders}: a comma-separated subset of the uploaders, default all.
 */
public class UploadBench {

    static final String BUCKET = SidecarBench.BUCKET;
    static final long MULTIPART_THRESHOLD = SidecarBench.MULTIPART_THRESHOLD;
    static final long PART_SIZE = SidecarBench.PART_SIZE;
    static final LinkedHashMap<String, Long> SIZES = new LinkedHashMap<>();
    static {
        SIZES.put("4 KB", 4L << 10);
        SIZES.put("32 KB", 32L << 10);
        SIZES.put("8 MB", 8L << 20);
        SIZES.put("16 MB", 16L << 20);
    }

    static DuckDBConnection db;
    static S3Client sync;
    static S3AsyncClient async;
    static S3TransferManager tm;
    static final ExecutorService pool = Executors.newFixedThreadPool(16);

    interface Uploader {
        void upload(Path file, String key) throws Exception;
    }

    public static void main(String[] args) throws Exception {
        String endpoint = args[0];
        int runs = Integer.parseInt(args[1]);
        Path work = Files.createTempDirectory("upload-bench");
        db = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        exec(db, "INSTALL httpfs; LOAD httpfs;");
        String settings = args.length > 2 && !args[2].equals("-") ? args[2] : "";
        for (String setting : settings.split(";")) {
            if (!setting.isBlank()) exec(db, "SET GLOBAL " + setting.trim());
        }
        exec(db, ("CREATE SECRET minio (TYPE s3, KEY_ID 'bench', SECRET 'benchbench', ENDPOINT '%s',"
                + " URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')").formatted(endpoint));
        var credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("bench", "benchbench"));
        sync = S3Client.builder().endpointOverride(URI.create("http://" + endpoint)).forcePathStyle(true)
                .region(Region.US_EAST_1).credentialsProvider(credentials)
                .httpClient(UrlConnectionHttpClient.builder().build()).build();
        async = S3AsyncClient.builder().endpointOverride(URI.create("http://" + endpoint)).forcePathStyle(true)
                .region(Region.US_EAST_1).credentialsProvider(credentials).multipartEnabled(true).build();
        tm = S3TransferManager.builder().s3Client(async).build();
        try {
            sync.createBucket(b -> b.bucket(BUCKET));
        } catch (software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException
                 | software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException exists) {
            // already there; anything else (a wrong endpoint, bad credentials) fails here, clearly
        }

        Map<String, Path> files = new LinkedHashMap<>();
        Random random = new Random(42);
        for (var size : SIZES.entrySet()) {
            byte[] bytes = new byte[size.getValue().intValue()];
            random.nextBytes(bytes); // incompressible, like Parquet pages
            files.put(size.getKey(), Files.write(work.resolve(size.getKey().replace(' ', '_') + ".bin"), bytes));
        }

        LinkedHashMap<String, Uploader> uploaders = new LinkedHashMap<>();
        uploaders.put("duckdb-blob", UploadBench::duckdb);
        uploaders.put("sdk-sync", UploadBench::sdkSync);
        uploaders.put("sdk-tm", (file, key) -> tm.uploadFile(UploadFileRequest.builder()
                .putObjectRequest(b -> b.bucket(BUCKET).key(key)).source(file).build()).completionFuture().join());

        if (args.length > 3) {
            Set<String> wanted = new HashSet<>(Arrays.asList(args[3].split(",")));
            Set<String> unknown = new TreeSet<>(wanted);
            unknown.removeAll(uploaders.keySet());
            if (!unknown.isEmpty()) {
                throw new IllegalArgumentException("unknown uploader(s) " + unknown + "; expected some of " + uploaders.keySet());
            }
            uploaders.keySet().retainAll(wanted);
        }
        System.out.printf("DuckDB %s; settings: %s; %d runs per cell after a warm-up; times in ms%n",
                scalar(db, "SELECT version()"), settings.isBlank() ? "defaults" : settings, runs);
        System.out.printf("%-7s %-12s %8s %8s %8s %10s %12s%n", "size", "uploader", "median", "p95", "min",
                "MB/s", "4 at once");
        for (var size : SIZES.entrySet()) {
            Path file = files.get(size.getKey());
            for (var u : uploaders.entrySet()) {
                List<Long> single = new ArrayList<>();
                List<Long> batch = new ArrayList<>();
                for (int i = -1; i < runs; i++) { // -1: warm-up
                    String prefix = "up/" + System.nanoTime();
                    long t0 = System.nanoTime();
                    u.getValue().upload(file, prefix + "/one");
                    long one = ms(t0);
                    check(prefix + "/one", size.getValue());

                    List<Future<?>> fs = new ArrayList<>();
                    long t1 = System.nanoTime();
                    for (int k = 0; k < 4; k++) {
                        String key = prefix + "/batch" + k;
                        fs.add(pool.submit(() -> {
                            u.getValue().upload(file, key);
                            return null;
                        }));
                    }
                    for (Future<?> f : fs) f.get();
                    long four = ms(t1);
                    for (int k = 0; k < 4; k++) check(prefix + "/batch" + k, size.getValue());
                    if (i >= 0) {
                        single.add(one);
                        batch.add(four);
                    }
                    deletePrefix(prefix + "/");
                }
                Collections.sort(single);
                Collections.sort(batch);
                long median = single.get(single.size() / 2);
                System.out.printf("%-7s %-12s %8d %8d %8d %10.1f %12d%n", size.getKey(), u.getKey(), median,
                        single.get(Math.min(single.size() - 1, (int) Math.ceil(single.size() * 0.95) - 1)),
                        single.get(0), size.getValue() / 1e6 / Math.max(median, 1) * 1000, batch.get(batch.size() / 2));
            }
        }

        System.out.println();
        for (var size : SIZES.entrySet()) {
            System.out.printf("duckdb-blob HTTP requests for one %s upload: %s%n", size.getKey(),
                    duckdbRequests(files.get(size.getKey())));
        }
        deletePrefix("up/count-");
        tm.close();
        async.close();
        sync.close();
        pool.shutdownNow();
        db.close();
        for (Path file : files.values()) Files.deleteIfExists(file);
        Files.deleteIfExists(work);
    }

    /** Deletes what a run uploaded under {@code prefix}, as SidecarBench does after each run. */
    static void deletePrefix(String prefix) {
        var keys = sync.listObjectsV2(b -> b.bucket(BUCKET).prefix(prefix)).contents().stream()
                .map(o -> software.amazon.awssdk.services.s3.model.ObjectIdentifier.builder().key(o.key()).build()).toList();
        if (!keys.isEmpty()) {
            sync.deleteObjects(b -> b.bucket(BUCKET).delete(software.amazon.awssdk.services.s3.model.Delete.builder().objects(keys).build()));
        }
    }

    static void duckdb(Path file, String key) throws Exception {
        try (var conn = (DuckDBConnection) db.duplicate()) {
            exec(conn, "COPY (SELECT content FROM read_blob('" + file + "')) TO 's3://" + BUCKET + "/" + key
                    + "' (FORMAT blob)");
        }
    }

    /** Strategy H's uploader: a single PUT below the threshold, otherwise concurrent parts. */
    static void sdkSync(Path file, String key) throws Exception {
        long size = Files.size(file);
        if (size < MULTIPART_THRESHOLD) {
            sync.putObject(b -> b.bucket(BUCKET).key(key), RequestBody.fromFile(file));
            return;
        }
        String uploadId = sync.createMultipartUpload(b -> b.bucket(BUCKET).key(key)).uploadId();
        int parts = (int) ((size + PART_SIZE - 1) / PART_SIZE);
        List<CompletableFuture<CompletedPart>> fs = IntStream.rangeClosed(1, parts)
                .mapToObj(n -> CompletableFuture.supplyAsync(() -> {
                    long offset = (n - 1) * PART_SIZE;
                    byte[] bytes = SidecarBench.readSlice(file, offset, (int) Math.min(PART_SIZE, size - offset));
                    String etag = sync.uploadPart(b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(n),
                            RequestBody.fromBytes(bytes)).eTag();
                    return CompletedPart.builder().partNumber(n).eTag(etag).build();
                }, pool))
                .toList();
        List<CompletedPart> done = fs.stream().map(CompletableFuture::join).toList();
        sync.completeMultipartUpload(b -> b.bucket(BUCKET).key(key).uploadId(uploadId)
                .multipartUpload(m -> m.parts(done)));
    }

    static void check(String key, long size) {
        long actual = sync.headObject(b -> b.bucket(BUCKET).key(key)).contentLength();
        if (actual != size) {
            throw new IllegalStateException(key + " is " + actual + " bytes on S3, expected " + size);
        }
    }

    static String duckdbRequests(Path file) throws Exception {
        try (var conn = (DuckDBConnection) db.duplicate()) {
            exec(conn, "CALL enable_logging('HTTP')");
            exec(conn, "COPY (SELECT content FROM read_blob('" + file + "')) TO 's3://" + BUCKET + "/up/count-"
                    + System.nanoTime() + "' (FORMAT blob)");
            exec(conn, "CALL disable_logging()");
            StringBuilder sb = new StringBuilder();
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT request.type, count(*) FROM duckdb_logs_parsed('HTTP')"
                         + " GROUP BY ALL ORDER BY ALL")) {
                while (rs.next()) sb.append(rs.getString(1)).append('=').append(rs.getLong(2)).append(' ');
            }
            exec(conn, "CALL truncate_duckdb_logs()");
            return sb.toString().trim();
        }
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
}
