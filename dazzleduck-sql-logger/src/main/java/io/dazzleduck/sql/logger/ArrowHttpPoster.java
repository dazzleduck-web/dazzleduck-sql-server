package io.dazzleduck.sql.logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ArrowHttpPoster implements AutoCloseable {

    private static final int DEFAULT_QUEUE_CAPACITY = 10000;
    private static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofSeconds(2);

    private final BlockingQueue<byte[]> queue;
    private final ScheduledExecutorService scheduler;
    private final HttpClient httpClient;
    private final URI endpoint;

    private final int batchCount;
    private volatile boolean running = true;

    private static String loadHttpPostUrl() {
        Config cfg = ConfigFactory.load();
        return cfg.getString("dazzleduck_logger.http_post_url");
    }

    public ArrowHttpPoster() {
        this(loadHttpPostUrl(), DEFAULT_QUEUE_CAPACITY, 1, DEFAULT_FLUSH_INTERVAL);
    }

    public ArrowHttpPoster(String httpUrl, int queueCapacity, int batchCount, Duration flushInterval) {
        this.endpoint = URI.create(httpUrl);
        this.httpClient = HttpClient.newHttpClient();
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.batchCount = batchCount;

        this.scheduler = createScheduler();


        // Start workers
        scheduler.scheduleAtFixedRate(this::flushSafely,
                flushInterval.toMillis(), flushInterval.toMillis(), TimeUnit.MILLISECONDS);

        scheduler.execute(this::drainLoop);
    }

    protected ScheduledExecutorService createScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "arrow-http-poster");
            t.setDaemon(true);
            return t;
        });
    }
    public boolean enqueue(byte[] bytes) {
        return running && queue.offer(bytes);
    }

    @Override
    public void close() {
        running = false;
        scheduler.shutdownNow();
    }

    private void drainLoop() {
        try {
            while (running) {
                byte[] first = queue.poll(1, TimeUnit.SECONDS);
                if (first == null) continue;

                List<byte[]> batch = new ArrayList<>();
                batch.add(first);
                queue.drainTo(batch);

                sendAll(batch);
            }
        } catch (Exception ignored) {}
    }

    private void flushSafely() {
        try {
            List<byte[]> drained = new ArrayList<>();
            queue.drainTo(drained);

            if (!drained.isEmpty()) {
                sendAll(drained);
            }

        } catch (Exception e) {
            System.err.println("[ArrowHttpPoster] flush failed: " + e.getMessage());
        }
    }

    private void sendAll(List<byte[]> batches) throws Exception {
        for (byte[] payload : batches) {
            sendHttpPost(payload);
        }
    }

    private void sendHttpPost(byte[] data) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(endpoint)
                .header("Content-Type", "application/vnd.apache.arrow.stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(data))
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 300) {
            throw new RuntimeException("HTTP POST failed: " + resp.statusCode() + " " + resp.body());
        }
    }
}
