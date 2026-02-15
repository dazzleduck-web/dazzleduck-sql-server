package io.dazzleduck.sql.logback.demo;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Continuous demo for the Logback LogForwardingAppender.
 *
 * <p>This demo generates sample logs continuously (every 500ms) that are forwarded
 * to a DazzleDuck server via the LogForwardingAppender configured in logback.xml.
 * Press Ctrl+C to stop.
 *
 * <h2>Prerequisites</h2>
 * <ol>
 *   <li>Start a DazzleDuck server on localhost:8081</li>
 *   <li>Configure logback.xml with LogForwardingAppender</li>
 * </ol>
 *
 * <h2>Run</h2>
 * <pre>
 * ./mvnw exec:java -pl dazzleduck-sql-logback \
 *   -Dexec.mainClass="io.dazzleduck.sql.logback.demo.Demo"
 * </pre>
 */
public class Demo {

    private static final Logger log = LoggerFactory.getLogger(Demo.class);
    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final AtomicLong counter = new AtomicLong(0);
    private static final Random random = new Random();
    private static final ScheduledExecutorService queryScheduler = Executors.newSingleThreadScheduledExecutor();

    private static final String[] USERS = {"alice", "bob", "charlie", "diana", "eve"};
    private static final String[] ACTIONS = {"LOGIN", "LOGOUT", "VIEW", "UPDATE", "DELETE"};
    private static final String[] ENDPOINTS = {"/api/users", "/api/orders", "/api/products"};

    public static void main(String[] args) {
        System.out.println("=== Logback LogForwardingAppender Demo ===");
        System.out.println("Generating logs every 500ms. Press Ctrl+C to stop.\n");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            running.set(false);
            queryScheduler.shutdown();
        }));

        log.info("Demo started - logging to DazzleDuck server");

        // Start background query task every 5 seconds
        String baseUrl = System.getenv().getOrDefault("DAZZLEDUCK_BASE_URL", "http://localhost:8081");
        queryScheduler.scheduleAtFixedRate(() -> runCountQuery(baseUrl), 5, 5, TimeUnit.SECONDS);

        while (running.get()) {
            try {
                generateLog();
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        log.info("Demo stopped. Total logs generated: {}", counter.get());
        System.out.println("\n=== Demo complete ===");
        System.out.println("Total logs generated: " + counter.get());
        System.out.println("Query: SELECT * FROM read_parquet('warehouse/log/*.parquet') ORDER BY timestamp DESC;");
    }

    private static void runCountQuery(String baseUrl) {
        try (BufferAllocator allocator = new RootAllocator()) {
            String query = "SELECT count(*) as count FROM ollylake.main.log";
            String urlStr = baseUrl + "/v1/query?q=" + java.net.URLEncoder.encode(query, "UTF-8");
            URL url = new URL(urlStr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            if (conn.getResponseCode() == 200) {
                try (InputStream inputStream = new BufferedInputStream(conn.getInputStream());
                     ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator)) {

                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        if (root.getRowCount() > 0) {
                            org.apache.arrow.vector.BigIntVector countVector = (org.apache.arrow.vector.BigIntVector) root.getVector("count");
                            long count = countVector.getObject(0);
                            System.out.println("[" + java.time.LocalTime.now() + "] Log count: " + count);
                        }
                    }
                }
            } else {
                System.err.println("Query failed: HTTP " + conn.getResponseCode());
            }
            conn.disconnect();
        } catch (Exception e) {
            System.err.println("Error running count query: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void generateLog() {
        long count = counter.incrementAndGet();
        String requestId = "REQ-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String user = USERS[random.nextInt(USERS.length)];

        MDC.put("request_id", requestId);
        MDC.put("user_id", user);
        MDC.put("log_count", String.valueOf(count));

        try {
            int scenario = random.nextInt(100);

            if (scenario < 50) {
                // Normal info log
                String action = ACTIONS[random.nextInt(ACTIONS.length)];
                log.info("User {} performed action: {}", user, action);
            } else if (scenario < 70) {
                // API request log
                String endpoint = ENDPOINTS[random.nextInt(ENDPOINTS.length)];
                int responseTime = random.nextInt(500) + 10;
                log.info("API request to {} completed in {}ms", endpoint, responseTime);
            } else if (scenario < 85) {
                // Warning log
                log.warn("High latency detected for user {}: {}ms", user, random.nextInt(1000) + 500);
            } else if (scenario < 95) {
                // Debug log
                log.debug("Processing batch item {} for user {}", count, user);
            } else {
                // Error log with exception
                try {
                    throw new RuntimeException("Simulated error #" + count);
                } catch (Exception e) {
                    log.error("Error processing request for user {}", user, e);
                }
            }
        } finally {
            MDC.clear();
        }
    }
}
