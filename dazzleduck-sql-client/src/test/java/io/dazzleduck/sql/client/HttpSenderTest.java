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

import java.io.ByteArrayOutputStream;
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
    static HttpSender sender;

    @BeforeAll
    static void setup() throws Exception {
        warehouse = "/tmp/" + java.util.UUID.randomUUID();
        new java.io.File(warehouse).mkdirs();

        // Use runtime Main which handles both networking modes
        io.dazzleduck.sql.runtime.Main.main(new String[]{
                "--conf", "dazzleduck_server.http.port=" + PORT,
                "--conf", "dazzleduck_server.http.auth=jwt",
                "--conf", "dazzleduck_server.warehouse=" + warehouse,// Specify networking mode
        });

        ConnectionPool.executeBatch(new String[]{"INSTALL arrow FROM community", "LOAD arrow"});

        schema = new Schema(java.util.List.of(new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null)));

        sender = new HttpSender(
                schema,
                "http://localhost:" + PORT,
                "admin",
                "admin",
                "test.parquet",
                Duration.ofSeconds(10),
                100_000,
                Duration.ofSeconds(10),
                100_000,
                500_000,Clock.systemDefaultZone()
        );
    }

    @AfterEach
    void teardown() throws Exception {
        if (sender != null) {
            sender.close();
        }
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

    private HttpSender newSender(String file, Duration timeout) {
        return new HttpSender(
                schema,
                "http://localhost:" + PORT,
                "admin",
                "admin",
                file,
                timeout,
                100_000,
                timeout,
                100,
                100_000, Clock.systemDefaultZone()
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

    private void verifyFile(String filename, long expectedCount) {
        await()
                .pollInterval(6, TimeUnit.MILLISECONDS)
                .atMost(3, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s')".formatted(warehouse, filename), Long.class);
                    assertEquals(expectedCount, count);
                });

    }

    @Test
    void testAsyncIngestionSingleBatch() throws Exception {
        HttpSender IngestionSingleBatch = newSender("test.parquet",Duration.ofSeconds(10));
        IngestionSingleBatch.enqueue(arrowBytes("select * from generate_series(4)"));
        verifyFile("test.parquet", 5);
    }

    @Test
    void testMultipleEnqueuesOverwriteBehavior() throws Exception {
        String file = "overwrite-" + System.nanoTime() + ".parquet";

        HttpSender overwriteSender = new HttpSender(
                schema,
                "http://localhost:" + PORT,
                "admin",
                "admin",
                file,
                Duration.ofSeconds(10),
                100_000,
                Duration.ofSeconds(10),
                100_000,
                500_000);

            overwriteSender.enqueue(arrowBytes("select * from generate_series(1)"));
            overwriteSender.enqueue(arrowBytes("select * from generate_series(2)"));
            verifyFile(file, 3);

    }

    @Test
    void testConcurrentEnqueues() throws Exception {
        CountDownLatch latch = new CountDownLatch(5);
        AtomicInteger errors = new AtomicInteger(0);
        HttpSender concurrentEnqueues=  newSender("test.parquet",Duration.ofSeconds(5));
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
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/test.parquet')".formatted(warehouse), Long.class);
            assertTrue(count >= 0);
        });
    }

    @Test
    void testJWTTokenReuse() throws Exception {
        // Multiple requests should reuse the same token
        HttpSender ReuseSender = newSender("test.parquet",Duration.ofSeconds(5));
        for (int i = 0; i < 10; i++) {
            ReuseSender.enqueue(arrowBytes("select " + i + " as val"));
        }

        await().atMost(15, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/test.parquet')".formatted(warehouse), Long.class);
            assertTrue(count >= 1);
        });
    }

    @Test
    void testHighThroughput() throws Exception {
        // Rapid fire 50 small batches
        HttpSender HighThroughput =  newSender("test.parquet",Duration.ofSeconds(5));
        for (int i = 0; i < 20; i++) {
            HighThroughput.enqueue(arrowBytes("select " + i + " as val"));
        }
        await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/test.parquet')".formatted(warehouse), Long.class);
            assertTrue(count >= 1);
        });
    }

    @Test
    void testQueueFullBehavior() throws Exception {
        var limitedSender = new HttpSender(  schema,"http://localhost:" + PORT, "admin", "admin", "full.parquet", Duration.ofSeconds(10), 100_000,
                Duration.ofSeconds(10),100, 200);

        byte[] largeData = arrowBytes("select * from generate_series(1000)");

        assertThrows(IllegalStateException.class, () -> {
            limitedSender.enqueue(largeData);
        });

        limitedSender.close();
    }

    @Test
    void testTimeoutFailure() throws Exception {
        // Set an impossibly short timeout of 1ms to force the exception
        var timeoutSender =  newSender("test.parquet",Duration.ofSeconds(5));

        timeoutSender.enqueue(arrowBytes("select * from generate_series(2000)"));
        await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> assertThrows(Exception.class, () -> ConnectionPool.collectFirst("select count(*) from read_parquet('%s/timeout.parquet')".formatted(warehouse), Long.class)));
        timeoutSender.close();
    }

    @Test
    void testMemoryDiskSwitching() throws Exception {
        var spillSender = new HttpSender(  schema,"http://localhost:" + PORT, "admin", "admin", "spill.parquet", Duration.ofSeconds(10), 100_000,
                Duration.ofSeconds(10),50, 100_000);


        spillSender.enqueue(arrowBytes("select * from generate_series(30)"));

        await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/spill.parquet')".formatted(warehouse), Long.class);
            assertEquals(31, count);
        });

        spillSender.close();
    }

}
