package io.dazzleduck.sql.micrometer.demo;

import io.dazzleduck.sql.micrometer.MicrometerForwarder;
import io.dazzleduck.sql.micrometer.config.MicrometerForwarderConfig;
import io.micrometer.core.instrument.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.time.Duration;
import java.time.LocalTime;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Continuous demo for the Micrometer metrics forwarder.
 *
 * <p>This demo generates sample metrics continuously (every 500ms) that are forwarded
 * to a DazzleDuck server via the MicrometerForwarder in Arrow format.
 * Press Ctrl+C to stop.
 *
 * <h2>Prerequisites</h2>
 * <ol>
 *   <li>Start a DazzleDuck server on localhost:8081</li>
 * </ol>
 *
 * <h2>Run</h2>
 * <pre>
 * ./mvnw exec:java -pl dazzleduck-sql-micrometer \
 *   -Dexec.mainClass="io.dazzleduck.sql.micrometer.demo.Demo"
 * </pre>
 *
 * <h2>Environment Variables</h2>
 * <ul>
 *   <li>DAZZLEDUCK_BASE_URL - Server URL (default: http://localhost:8081)</li>
 *   <li>DAZZLEDUCK_USERNAME - Username (default: admin)</li>
 *   <li>DAZZLEDUCK_PASSWORD - Password (default: admin)</li>
 *   <li>DAZZLEDUCK_INGESTION_QUEUE - Ingestion queue (default: metric)</li>
 *   <li>DAZZLEDUCK_PROJECT - Projection expressions, comma-separated (default: *, CAST(timestamp AS DATE) AS date)</li>
 *   <li>DAZZLEDUCK_PARTITION_BY - Partition columns, comma-separated (default: date)</li>
 * </ul>
 */
public class Demo {

    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final AtomicLong counter = new AtomicLong(0);
    private static final Random random = new Random();
    private static final ScheduledExecutorService queryScheduler = Executors.newSingleThreadScheduledExecutor();

    // Simulated data
    private static final String[] ENDPOINTS = {"/api/users", "/api/orders", "/api/products", "/api/health"};
    private static final String[] METHODS = {"GET", "POST", "PUT", "DELETE"};
    private static final String[] STATUS_CODES = {"200", "201", "400", "404", "500"};
    private static final String[] REGIONS = {"us-east", "us-west", "eu-west", "ap-south"};

    public static void main(String[] args) {
        System.out.println("=== Micrometer Metrics Forwarder Demo ===");
        System.out.println("Generating metrics every 500ms. Press Ctrl+C to stop.\n");

        // Get configuration from environment
        String baseUrl = System.getenv().getOrDefault("DAZZLEDUCK_BASE_URL", "http://localhost:8081");
        String username = System.getenv().getOrDefault("DAZZLEDUCK_USERNAME", "admin");
        String password = System.getenv().getOrDefault("DAZZLEDUCK_PASSWORD", "admin");
        String ingestionQueue = System.getenv().getOrDefault("DAZZLEDUCK_INGESTION_QUEUE", "metric");
        String projectEnv = System.getenv().getOrDefault("DAZZLEDUCK_PROJECT", "*, CAST(timestamp AS DATE) AS date");
        String partitionByEnv = System.getenv().getOrDefault("DAZZLEDUCK_PARTITION_BY", "date");

        // Parse comma-separated values
        List<String> projections = List.of(projectEnv.split(","));
        List<String> partitionBy = List.of(partitionByEnv.split(","));

        System.out.println("Configuration:");
        System.out.println("  Base URL: " + baseUrl);
        System.out.println("  Username: " + username);
        System.out.println("  Ingestion Queue: " + ingestionQueue);
        System.out.println("  Projections: " + projections);
        System.out.println("  Partition By: " + partitionBy);
        System.out.println();

        // Build configuration
        MicrometerForwarderConfig config = MicrometerForwarderConfig.builder()
                .baseUrl(baseUrl)
                .username(username)
                .password(password)
                .ingestionQueue(ingestionQueue)
                .project(projections)
                .partitionBy(partitionBy)
                .stepInterval(Duration.ofSeconds(5))  // Publish metrics every 5 seconds
                .maxSendInterval(Duration.ofSeconds(2))
                .minBatchSize(1024)  // 1 KB for demo
                .maxBatchSize(10 * 1024 * 1024)  // 10 MB
                .build();

        // Create and start forwarder
        MicrometerForwarder forwarder = MicrometerForwarder.createAndStart(config);
        MeterRegistry registry = forwarder.getRegistry();

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            running.set(false);
            queryScheduler.shutdown();
        }));

        // Start background query task every 5 seconds
        queryScheduler.scheduleAtFixedRate(() -> runCountQuery(baseUrl), 5, 5, TimeUnit.SECONDS);

        // Create metrics
        Counter requestCounter = Counter.builder("http.requests.total")
                .description("Total HTTP requests")
                .tag("application", "demo")
                .register(registry);

        Counter errorCounter = Counter.builder("http.errors.total")
                .description("Total HTTP errors")
                .tag("application", "demo")
                .register(registry);

        Timer requestTimer = Timer.builder("http.request.duration")
                .description("HTTP request duration")
                .tag("application", "demo")
                .register(registry);

        Gauge.builder("system.cpu.usage", () -> random.nextDouble() * 100)
                .description("CPU usage percentage")
                .tag("application", "demo")
                .register(registry);

        Gauge.builder("system.memory.usage", () -> random.nextDouble() * 100)
                .description("Memory usage percentage")
                .tag("application", "demo")
                .register(registry);

        Gauge.builder("system.active.connections", () -> (double) (random.nextInt(500) + 50))
                .description("Active connections")
                .tag("application", "demo")
                .register(registry);

        DistributionSummary responseSizes = DistributionSummary.builder("http.response.size")
                .description("HTTP response size in bytes")
                .tag("application", "demo")
                .register(registry);

        System.out.println("Demo started - generating metrics...\n");

        // Main loop
        while (running.get()) {
            try {
                generateMetrics(registry, requestCounter, errorCounter, requestTimer, responseSizes);
                counter.incrementAndGet();
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Cleanup
        System.out.println("\nFlushing remaining metrics...");
        forwarder.close();

        System.out.println("\n=== Demo complete ===");
        System.out.println("Total metric batches generated: " + counter.get());
        System.out.println("Query: SELECT * FROM read_parquet('warehouse/ingestion/metric/**/*.parquet') ORDER BY timestamp DESC;");
    }

    private static void runCountQuery(String baseUrl) {
        try (BufferAllocator allocator = new RootAllocator()) {
            String query = "SELECT count(*) as count FROM ollylake.main.metric";
            String urlStr = baseUrl + "/v1/query?q=" + URLEncoder.encode(query, "UTF-8");
            URL url = new URL(urlStr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            if (conn.getResponseCode() == 200) {
                try (InputStream inputStream = new BufferedInputStream(conn.getInputStream());
                     ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator)) {

                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        if (root.getRowCount() > 0) {
                            BigIntVector countVector = (BigIntVector) root.getVector("count");
                            long count = countVector.getObject(0);
                            System.out.println("[" + LocalTime.now() + "] Metric count: " + count);
                        }
                    }
                }
            } else {
                System.err.println("Query failed: HTTP " + conn.getResponseCode());
            }
            conn.disconnect();
        } catch (Exception e) {
            System.err.println("Error running count query: " + e.getMessage());
        }
    }

    private static void generateMetrics(MeterRegistry registry, Counter requestCounter,
                                        Counter errorCounter, Timer requestTimer,
                                        DistributionSummary responseSizes) {
        // Simulate HTTP requests
        String endpoint = ENDPOINTS[random.nextInt(ENDPOINTS.length)];
        String method = METHODS[random.nextInt(METHODS.length)];
        String statusCode = STATUS_CODES[random.nextInt(STATUS_CODES.length)];
        String region = REGIONS[random.nextInt(REGIONS.length)];

        // Increment request counter
        requestCounter.increment();

        // Track request with endpoint-specific counter
        Counter.builder("http.requests")
                .tag("endpoint", endpoint)
                .tag("method", method)
                .tag("status", statusCode)
                .tag("region", region)
                .register(registry)
                .increment();

        // Simulate response time (10-500ms)
        long responseTimeMs = random.nextInt(490) + 10;
        requestTimer.record(Duration.ofMillis(responseTimeMs));

        // Endpoint-specific timer
        Timer.builder("http.endpoint.duration")
                .tag("endpoint", endpoint)
                .tag("method", method)
                .register(registry)
                .record(Duration.ofMillis(responseTimeMs));

        // Track response size
        int responseSize = random.nextInt(10000) + 100;
        responseSizes.record(responseSize);

        // Simulate errors (10% chance)
        if (statusCode.startsWith("4") || statusCode.startsWith("5")) {
            errorCounter.increment();
            Counter.builder("http.errors")
                    .tag("endpoint", endpoint)
                    .tag("status", statusCode)
                    .register(registry)
                    .increment();
        }

        // Simulate database metrics
        Timer.builder("db.query.duration")
                .tag("operation", random.nextBoolean() ? "SELECT" : "INSERT")
                .tag("table", random.nextBoolean() ? "users" : "orders")
                .register(registry)
                .record(Duration.ofMillis(random.nextInt(100) + 5));

        // Simulate queue metrics
        Gauge.builder("queue.size", () -> (double) random.nextInt(1000))
                .tag("queue", "events")
                .register(registry);

        // Print progress every 10 iterations
        if (counter.get() % 10 == 0) {
            System.out.printf("Generated %d metric batches | Requests: %.0f | Errors: %.0f%n",
                    counter.get(), requestCounter.count(), errorCounter.count());
        }
    }
}
