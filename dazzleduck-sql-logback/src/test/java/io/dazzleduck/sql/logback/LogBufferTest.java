package io.dazzleduck.sql.logback;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class LogBufferTest {

    private LogBuffer buffer;

    @BeforeEach
    void setUp() {
        buffer = new LogBuffer(100);
    }

    @Test
    void offer_shouldAddEntry() {
        LogEntry entry = createEntry("test message");

        assertTrue(buffer.offer(entry));
        assertEquals(1, buffer.getSize());
        assertFalse(buffer.isEmpty());
    }

    @Test
    void offer_shouldRejectWhenFull() {
        LogBuffer smallBuffer = new LogBuffer(2);

        assertTrue(smallBuffer.offer(createEntry("msg1")));
        assertTrue(smallBuffer.offer(createEntry("msg2")));
        assertFalse(smallBuffer.offer(createEntry("msg3")));

        assertEquals(2, smallBuffer.getSize());
    }

    @Test
    void drain_shouldReturnAllEntries() {
        buffer.offer(createEntry("msg1"));
        buffer.offer(createEntry("msg2"));
        buffer.offer(createEntry("msg3"));

        List<LogEntry> entries = buffer.drain();

        assertEquals(3, entries.size());
        assertEquals(0, buffer.getSize());
        assertTrue(buffer.isEmpty());
    }

    @Test
    void drain_shouldReturnEmptyListWhenEmpty() {
        List<LogEntry> entries = buffer.drain();

        assertTrue(entries.isEmpty());
        assertTrue(buffer.isEmpty());
    }

    @Test
    void returnForRetry_shouldAddToRetryQueue() {
        LogEntry entry = createEntry("retry msg");
        List<LogEntry> entries = List.of(entry);

        buffer.returnForRetry(entries);

        assertEquals(1, buffer.getSize());
    }

    @Test
    void drain_shouldReturnRetryEntriesFirst() {
        buffer.offer(createEntry("main1"));
        buffer.offer(createEntry("main2"));

        LogEntry retryEntry = createEntry("retry1");
        buffer.returnForRetry(List.of(retryEntry));

        List<LogEntry> entries = buffer.drain();

        assertEquals(3, entries.size());
        assertEquals("retry1", entries.get(0).message());
        assertEquals("main1", entries.get(1).message());
        assertEquals("main2", entries.get(2).message());
    }

    @Test
    void returnForRetry_shouldRespectMaxSize() {
        LogBuffer smallBuffer = new LogBuffer(2);

        smallBuffer.returnForRetry(List.of(
                createEntry("retry1"),
                createEntry("retry2"),
                createEntry("retry3")
        ));

        assertEquals(2, smallBuffer.getSize());
    }

    @Test
    void concurrentOffers_shouldBeThreadSafe() throws InterruptedException {
        LogBuffer largeBuffer = new LogBuffer(10000);
        int numThreads = 10;
        int entriesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < entriesPerThread; i++) {
                        if (largeBuffer.offer(createEntry("thread-" + threadId + "-msg-" + i))) {
                            successCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(numThreads * entriesPerThread, successCount.get());
        assertEquals(numThreads * entriesPerThread, largeBuffer.getSize());
    }

    @Test
    void concurrentDrain_shouldBeThreadSafe() throws InterruptedException {
        LogBuffer largeBuffer = new LogBuffer(1000);

        // Fill buffer
        for (int i = 0; i < 500; i++) {
            largeBuffer.offer(createEntry("msg-" + i));
        }

        int numThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger totalDrained = new AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                try {
                    List<LogEntry> entries = largeBuffer.drain();
                    totalDrained.addAndGet(entries.size());
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(500, totalDrained.get());
        assertTrue(largeBuffer.isEmpty());
    }

    private LogEntry createEntry(String message) {
        return new LogEntry(
                Instant.now(),
                "INFO",
                "TestLogger",
                "main",
                message,
                "app-1",
                "TestApp",
                "localhost"
        );
    }
}
