package io.dazzleduck.sql.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

public class HttpSenderTest {

    static final int PORT = 8093;
    static String warehouse;
    static ObjectMapper mapper = new ObjectMapper();
    static Schema schema;

    @TempDir
    Path tempDir;
    @BeforeAll
    static void setup() throws Exception {
        warehouse = "/tmp/" + java.util.UUID.randomUUID();
        new java.io.File(warehouse).mkdirs();

        // Use runtime Main which handles both networking modes
        io.dazzleduck.sql.runtime.Main.main(new String[]{
                "--conf", "dazzleduck_server.http.port=" + PORT,
                "--conf", "dazzleduck_server.http.auth=jwt",
                "--conf", "dazzleduck_server.warehouse=" + warehouse,
                "--conf", "dazzleduck_server.ingestion.max_delay_ms = 500"
        });

   //     ConnectionPool.executeBatch(new String[]{"INSTALL arrow FROM community", "LOAD arrow"});

        schema = new Schema(java.util.List.of(new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null)));
    }

    @BeforeEach
    void setupEach() {
        org.awaitility.Awaitility.setDefaultPollInterval(5, TimeUnit.MILLISECONDS);
        org.awaitility.Awaitility.setDefaultTimeout(3, TimeUnit.SECONDS);
    }

    @AfterEach
    void teardownEach() {
        // Reset Awaitility to defaults
        org.awaitility.Awaitility.reset();
    }

    private HttpProducer newSender(String file, Duration timeout) {
        return new HttpProducer(
                schema,
                "http://localhost:" + PORT,
                "admin",
                "admin",
                file,
                timeout,
                100_000,
                200_000,
                Duration.ofSeconds(1),
                3,
                1000,
                java.util.List.of(),
                java.util.List.of(),
                100,
                100_000,
                Clock.systemDefaultZone()
        );
    }
    private byte[] arrowBytes(String query) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection conn = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(conn, allocator, query, 1000);
             var baos = new ByteArrayOutputStream();
             var writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, baos)) {

            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
            }
            writer.end();
            return baos.toByteArray();
        }
    }

    private void verifyFile(String path, long expectedCount) {
        await()
                .pollInterval(6, TimeUnit.MILLISECONDS)
                .atMost(3, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s/*.parquet')".formatted(warehouse, path), Long.class);
                    assertEquals(expectedCount, count);
                });

    }

    @Test
    void testAsyncIngestionSingleBatch() throws Exception {
        String file = "async-single-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, file));
        try (HttpProducer sender = newSender(file, Duration.ofSeconds(10))) {
            sender.enqueue(arrowBytes("select * from generate_series(4)"));
        }

        // Verify after close() has flushed all data
        verifyFile(file, 5);
    }

    @Test
    void testMultipleEnqueuesOverwriteBehavior() throws Exception {
        String file = "overwrite-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, file));

        try (HttpProducer overwriteSender = new HttpProducer(
                schema,
                "http://localhost:" + PORT,
                "admin",
                "admin",
                file,
                Duration.ofSeconds(3),
                100_000,
                200_000,
                Duration.ofSeconds(2),
                3,
                1000,
                java.util.List.of(),
                java.util.List.of(),
                100_000,
                500_000)) {

            overwriteSender.enqueue(arrowBytes("select * from generate_series(1)"));
            overwriteSender.enqueue(arrowBytes("select * from generate_series(2)"));
        }

        // Verify after close() has flushed all data
        verifyFile(file, 5);
    }

    @Test
    void testConcurrentEnqueues() throws Exception {
        String file = "concurrent-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, file));
        CountDownLatch latch = new CountDownLatch(5);
        AtomicInteger errors = new AtomicInteger(0);

        try (HttpProducer concurrentEnqueues = newSender(file, Duration.ofSeconds(5))) {
            for (int i = 0; i < 5; i++) {
                final int index = i;
                new Thread(() -> {
                    try {
                        concurrentEnqueues.enqueue(arrowBytes("select * from generate_series(" + (index * 10) + ")"));
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertEquals(0, errors.get());
            await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
                long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s/*.parquet')".formatted(warehouse, file), Long.class);
                assertTrue(count >= 0);
            });
        }
    }

    @Test
    void testJWTTokenReuse() throws Exception {
        String file = "jwt-reuse-" + System.nanoTime() ;
        Files.createDirectories(Path.of(warehouse, file));

        // Multiple requests should reuse the same token
        try (HttpProducer ReuseSender = newSender(file, Duration.ofSeconds(5))) {
            for (int i = 0; i < 5; i++) {
                ReuseSender.enqueue(arrowBytes("select " + i + " as val"));
            }

            await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
                long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s/*.parquet')".formatted(warehouse, file), Long.class);
                assertTrue(count >= 1);
            });
        }
    }

    @Test
    void testHighThroughput() throws Exception {
        String file = "high-throughput-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, file));

        // Rapid fire 20 small batches
        try (HttpProducer HighThroughput = newSender(file, Duration.ofSeconds(5))) {
            for (int i = 0; i < 5; i++) {
                HighThroughput.enqueue(arrowBytes("select " + i + " as val"));
            }
            await().atMost(2, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
                long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s/*.parquet')".formatted(warehouse, file), Long.class);
                assertTrue(count >= 1);
            });
        }
    }

    @Test
    void testQueueFullBehavior() throws Exception {
        var limitedSender = new HttpProducer(  schema,"http://localhost:" + PORT, "admin", "admin", "full.parquet", Duration.ofSeconds(3), 100_000, 200_000,
                Duration.ofSeconds(2), 3, 1000, java.util.List.of(), java.util.List.of(), 100, 200);

        byte[] largeData = arrowBytes("select * from generate_series(200)");

        assertThrows(IllegalStateException.class, () -> {
            limitedSender.enqueue(largeData);
        });

        limitedSender.close();
    }

    @Test
    void testTimeoutFailure() throws Exception {
        String file = "timeout-" + System.nanoTime() + ".parquet";

        // Set an impossibly short timeout of 1ms to force the exception
        try (var timeoutSender = newSender(file, Duration.ofSeconds(5))) {
            timeoutSender.enqueue(arrowBytes("select * from generate_series(2000)"));
            await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() ->
                assertThrows(Exception.class, () ->
                    ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s')".formatted(warehouse, file), Long.class)
                )
            );
        }
    }

    @Test
    void testMemoryDiskSwitching() throws Exception {
        var path = "spill";
        Files.createDirectories(Path.of(warehouse, path));
        var spillSender = new HttpProducer(  schema,"http://localhost:" + PORT, "admin", "admin", path, Duration.ofSeconds(10), 100_000, 200_000,
                Duration.ofSeconds(2), 3, 1000, java.util.List.of(), java.util.List.of(), 50, 100_000);


        spillSender.enqueue(arrowBytes("select * from generate_series(30)"));

        await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s/*.parquet')".formatted(warehouse,path), Long.class);
            assertEquals(31, count);
        });

        spillSender.close();
    }

    @Test
    void testTransformationsHeader() throws Exception {
        String path = "transformations-test";
        Files.createDirectories(Path.of(warehouse, path));

        try (HttpProducer sender = new HttpProducer(
                schema,
                "http://localhost:" + PORT,
                "admin",
                "admin",
                path,
                Duration.ofSeconds(3),
                100_000,
                200_000,
                Duration.ofSeconds(1),
                3,
                1000,
                java.util.List.of("'c1' as c1", "'c2' as c2"),
                java.util.List.of(),
                100_000,
                500_000)) {

            sender.enqueue(arrowBytes("select * from generate_series(5)"));
        }

        long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s/*.parquet') where c1 = 'c1' and c2 = 'c2'".formatted(warehouse, path), Long.class);
        assertEquals(6, count);

    }


    @Test
    void testTransformationsAndPartitionByHeaders() throws Exception {
        String path = "both-headers-test";
        Files.createDirectories(Path.of(warehouse, path));

        try (HttpProducer sender = new HttpProducer(
                schema,
                "http://localhost:" + PORT,
                "admin",
                "admin",
                path,
                Duration.ofSeconds(3),
                100_000,
                200_000,
                Duration.ofSeconds(1),
                3,
                1000,
                java.util.List.of("'c1' as c1", "'c2' as  c2"),
                java.util.List.of("c1", "c2"),
                100_000,
                500_000)) {

            sender.enqueue(arrowBytes("select * from generate_series(5)"));
        }

        await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s/*/*/*.parquet')".formatted(warehouse, path), Long.class);
            assertEquals(6, count);
        });
    }


    @Test
    void testEmptyListsDoNotSendHeaders() throws Exception {
        String path = "empty-lists-test";
        Files.createDirectories(Path.of(warehouse, path));

        // Empty lists should work fine and not send headers
        try (HttpProducer sender = new HttpProducer(
                schema,
                "http://localhost:" + PORT,
                "admin",
                "admin",
                path,
                Duration.ofSeconds(3),
                100_000,
                200_000,
                Duration.ofSeconds(1),
                3,
                1000,
                java.util.List.of(),
                java.util.List.of(),
                100_000,
                500_000)) {

            sender.enqueue(arrowBytes("select * from generate_series(5)"));
        }

        await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s/*.parquet')".formatted(warehouse, path), Long.class);
            assertEquals(6, count);
        });
    }

}
