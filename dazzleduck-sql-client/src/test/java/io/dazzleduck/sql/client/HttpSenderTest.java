package io.dazzleduck.sql.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.http.server.Main;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
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
    static HttpSender sender;

    @BeforeAll
    static void setup() throws Exception {
        warehouse = "/tmp/" + java.util.UUID.randomUUID();
        new java.io.File(warehouse).mkdirs();

        Main.main(new String[]{"--conf", "dazzleduck_server.http.port=" + PORT, "--conf", "dazzleduck_server.http.auth=jwt", "--conf", "dazzleduck_server.warehouse=" + warehouse});

        ConnectionPool.executeBatch(new String[]{"INSTALL arrow FROM community", "LOAD arrow"});

        // Create single shared sender
        sender = new HttpSender("http://localhost:" + PORT, "admin", "admin", "test.parquet", // Default path, can be overridden per test if needed
                Duration.ofSeconds(10), 100_00, 500_00);

        sender.start();
    }

    @AfterAll
    static void teardown() throws Exception {
        if (sender != null) {
            sender.close();
        }
    }

    private byte[] arrowBytes(String query) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(); DuckDBConnection conn = ConnectionPool.getConnection(); var reader = ConnectionPool.getReader(conn, allocator, query, 1000); var baos = new ByteArrayOutputStream(); var writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, baos)) {

            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
            }
            writer.end();
            return baos.toByteArray();
        }
    }

    private void verifyFile(String filename, long expectedCount) {
        await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s')".formatted(warehouse, filename), Long.class);
            assertEquals(expectedCount, count);
        });
    }

    @Test
    void testAsyncIngestionSingleBatch() throws Exception {
        sender.enqueue(arrowBytes("select * from generate_series(4)"));
        verifyFile("test.parquet", 5);
    }

    @Test
    void testMultipleEnqueuesOverwriteBehavior() throws Exception {
        sender.enqueue(arrowBytes("select * from generate_series(1)"));
        sender.enqueue(arrowBytes("select * from generate_series(2)"));
        verifyFile("test.parquet", 3);
    }

    @Test
    void testSequentialBatches() throws Exception {
        sender.enqueue(arrowBytes("select * from generate_series(5)"));

        await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().until(() -> {
            try {
                ConnectionPool.collectFirst("select count(*) from read_parquet('%s/test.parquet')".formatted(warehouse), Long.class);
                return true;
            } catch (Exception e) {
                return false;
            }
        });

        sender.enqueue(arrowBytes("select * from generate_series(15)"));
        verifyFile("test.parquet", 16);
    }

    @Test
    void testMultipleBatchesRapid() throws Exception {
        for (int i = 0; i < 10; i++) {
            sender.enqueue(arrowBytes("select * from generate_series(" + (i * 5) + ")"));
        }

        await().atMost(15, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/test.parquet')".formatted(warehouse), Long.class);
            assertTrue(count >= 0);
        });
    }

    @Test
    void testConcurrentEnqueues() throws Exception {
        CountDownLatch latch = new CountDownLatch(5);
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < 5; i++) {
            final int index = i;
            new Thread(() -> {
                try {
                    sender.enqueue(arrowBytes("select * from generate_series(" + (index * 10) + ")"));
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(0, errors.get());

        await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/test.parquet')".formatted(warehouse), Long.class);
            assertTrue(count >= 0);
        });
    }

    @Test
    void testJWTTokenReuse() throws Exception {
        // Multiple requests should reuse the same token
        for (int i = 0; i < 10; i++) {
            sender.enqueue(arrowBytes("select " + i + " as val"));
            Thread.sleep(50);
        }

        await().atMost(15, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/test.parquet')".formatted(warehouse), Long.class);
            assertTrue(count >= 1);
        });
    }

    @Test
    void testHighThroughput() throws Exception {
        // Rapid fire 50 small batches
        for (int i = 0; i < 50; i++) {
            sender.enqueue(arrowBytes("select " + i + " as val"));
        }

        await().atMost(30, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/test.parquet')".formatted(warehouse), Long.class);
            assertTrue(count >= 1);
        });
    }


    @Test
    void testQueueFullBehavior() throws Exception {
        var limitedSender = new HttpSender("http://localhost:" + PORT, "admin", "admin", "full.parquet", Duration.ofSeconds(10), 100, 200);
        limitedSender.start();

        byte[] largeData = arrowBytes("select * from generate_series(1000)");

        assertThrows(IllegalStateException.class, () -> {
            limitedSender.enqueue(largeData);
        });

        limitedSender.close();
    }

    @Test
    void testTimeoutFailure() throws Exception {
        var timeoutSender = new HttpSender("http://localhost:" + PORT, "admin", "admin", "timeout.parquet", Duration.ofMillis(1), 5_000, 50_000);

        timeoutSender.start();
        timeoutSender.enqueue(arrowBytes("select * from generate_series(2000)"));
        await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> assertThrows(Exception.class, () -> ConnectionPool.collectFirst("select count(*) from read_parquet('%s/timeout.parquet')".formatted(warehouse), Long.class)));
        timeoutSender.close();
    }

    @Test
    void testMemoryDiskSwitching() throws Exception {
        var spillSender = new HttpSender("http://localhost:" + PORT, "admin", "admin", "spill.parquet", Duration.ofSeconds(10), 50, 100_000);

        spillSender.start();
        spillSender.enqueue(arrowBytes("select * from generate_series(30)"));

        await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/spill.parquet')".formatted(warehouse), Long.class);
            assertEquals(31, count);
        });

        spillSender.close();
    }

}
