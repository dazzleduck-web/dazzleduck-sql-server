package io.dazzleduck.sql.logger;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class AsyncArrowFlightSender implements Closeable {

    private static final int DEFAULT_QUEUE_CAPACITY = 10000;
    private static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofSeconds(2);

    private final BlockingQueue<byte[]> queue;
    final ScheduledExecutorService scheduler;
    private final FlightClient client;
    private final RootAllocator allocator = new RootAllocator();
    private final int batchCount;
    private volatile boolean running = true;

    private static final AsyncArrowFlightSender DEFAULT =
            new AsyncArrowFlightSender("localhost", 32010, DEFAULT_QUEUE_CAPACITY, 1, DEFAULT_FLUSH_INTERVAL);

    public static AsyncArrowFlightSender getDefault() { return DEFAULT; }

    // Standard constructor
    public AsyncArrowFlightSender(String host, int port, int queueCapacity, int batchCount, Duration flushInterval) {
        this(FlightClient.builder(new RootAllocator(), Location.forGrpcInsecure(host, port)).build(),
                queueCapacity, batchCount, flushInterval, true);
    }

    public AsyncArrowFlightSender(String host, int port) {
        this(host, port, DEFAULT_QUEUE_CAPACITY, 1, DEFAULT_FLUSH_INTERVAL);
    }

    // Dependency Injection constructor (for testing)
    public AsyncArrowFlightSender(FlightClient client, int queueCapacity, int batchCount, Duration flushInterval, boolean startThreads) {
        this.client = client;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.batchCount = batchCount;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "arrow-flight-sender");
            t.setDaemon(true);
            return t;
        });

        if (startThreads) {
            scheduler.scheduleAtFixedRate(this::flushSafely,
                    flushInterval.toMillis(), flushInterval.toMillis(), TimeUnit.MILLISECONDS);
            scheduler.execute(this::drainLoop);
        }
    }

    public boolean enqueue(byte[] arrowStreamBytes) {
        if (!running) return false;
        return queue.offer(arrowStreamBytes);
    }

    @Override
    public void close() {
        running = false;
        scheduler.shutdownNow();
        try { client.close(); } catch (Exception ignored) {}
        allocator.close();
    }

    private void drainLoop() {
        try {
            while (running) {
                List<byte[]> batch = new ArrayList<>(batchCount);
                byte[] first = queue.poll(1, TimeUnit.SECONDS);
                if (first != null) batch.add(first);
                queue.drainTo(batch, batchCount - 1);
                if (!batch.isEmpty()) sendBatch(batch);
            }
        } catch (InterruptedException ignored) {}
    }

    private void flushSafely() {
        try {
            List<byte[]> drained = new ArrayList<>();
            queue.drainTo(drained);
            if (!drained.isEmpty()) sendBatch(drained);
        } catch (Exception e) {
            System.err.println("[AsyncArrowFlightSender] flush failed: " + e.getMessage());
        }
    }

    private void sendBatch(List<byte[]> batch) {
        if (batch.isEmpty()) return;

        try {
            for (byte[] bytes : batch) {
                try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
                     ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {

                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    FlightDescriptor descriptor = FlightDescriptor.path("logs");

                    AsyncPutListener listener = new AsyncPutListener();
                    FlightClient.ClientStreamListener stream = client.startPut(descriptor, root, listener);

                    if (stream == null) throw new RuntimeException("startPut returned null");

                    while (reader.loadNextBatch()) {
                        stream.putNext();
                    }

                    stream.completed();
                    listener.getResult();
                }
            }
        } catch (Exception e) {
            System.err.println("[AsyncArrowFlightSender] sendBatch failed: " + e.getMessage());
        }
    }
}
