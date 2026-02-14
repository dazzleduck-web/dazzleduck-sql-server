package io.dazzleduck.sql.scrapper;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.scrapper.config.CollectorConfig;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;

import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MetricsCollectorServer - standalone server with HOCON config.
 *
 * Note: HttpArrowProducer handles actual HTTP sending asynchronously with authentication,
 * so these tests focus on server lifecycle and scraping behavior.
 */
class MetricsCollectorServerTest {

    private MockWebServer targetServer;
    private File tempConfigFile;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
        targetServer = new MockWebServer();
        targetServer.start();

        executor = Executors.newSingleThreadExecutor();

        ConfigFactory.invalidateCaches();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        targetServer.shutdown();
        if (tempConfigFile != null && tempConfigFile.exists()) {
            tempConfigFile.delete();
        }
        ConfigFactory.invalidateCaches();
    }

    @Test
    @DisplayName("Should create server with default config")
    void createWithDefaultConfig() {
        MetricsCollectorServer server = new MetricsCollectorServer();
        assertNotNull(server.getConfig());
    }

    @Test
    @DisplayName("Should create server with external config")
    void createWithExternalConfig() throws Exception {
        String hoconContent = String.format("""
            collector {
                enabled = true
                targets = ["%s"]
                server-url = "http://localhost:8081"
                scrape-interval-ms = 100
                tailing {
                    flush-threshold = 1
                    flush-interval-ms = 100
                }
                auth {
                    username = "test"
                    password = "test"
                }
                producer {
                    min-batch-size = 1024
                    max-batch-size = 16777216
                    max-in-memory-size = 10485760
                    max-on-disk-size = 1073741824
                }
            }
            """,
            targetServer.url("/prometheus").toString()
        );

        tempConfigFile = createTempConfigFile(hoconContent);
        MetricsCollectorServer server = new MetricsCollectorServer(tempConfigFile.getAbsolutePath());

        assertNotNull(server.getConfig());
        assertTrue(server.getConfig().isEnabled());
    }

    @Test
    @DisplayName("Should create server with CollectorConfig")
    void createWithCollectorConfig() {
        String hoconString = String.format("""
            collector {
                enabled = true
                targets = ["%s"]
                server-url = "http://localhost:8081"
                auth {
                    username = "test"
                    password = "test"
                }
                producer {
                    min-batch-size = 1024
                    max-batch-size = 16777216
                    max-in-memory-size = 10485760
                    max-on-disk-size = 1073741824
                }
            }
            """,
            targetServer.url("/prometheus").toString()
        );

        Config config = ConfigFactory.parseString(hoconString);
        CollectorConfig collectorConfig = new CollectorConfig(config);
        MetricsCollectorServer server = new MetricsCollectorServer(collectorConfig);

        assertNotNull(server.getConfig());
        assertTrue(server.getConfig().isEnabled());
    }

    @Test
    @DisplayName("Should not start when disabled")
    void shouldNotStartWhenDisabled() throws Exception {
        String hoconContent = """
            collector {
                enabled = false
                auth {
                    username = "test"
                    password = "test"
                }
                producer {
                    min-batch-size = 1024
                    max-batch-size = 16777216
                    max-in-memory-size = 10485760
                    max-on-disk-size = 1073741824
                }
            }
            """;

        tempConfigFile = createTempConfigFile(hoconContent);
        MetricsCollectorServer server = new MetricsCollectorServer(tempConfigFile.getAbsolutePath());

        // Start in background thread
        executor.submit(server::start);

        // Give it a moment
        Thread.sleep(200);

        assertFalse(server.isRunning());
        server.shutdown();
    }

    @Test
    @DisplayName("Should not start with no targets")
    void shouldNotStartWithNoTargets() throws Exception {
        String hoconContent = """
            collector {
                enabled = true
                targets = []
                auth {
                    username = "test"
                    password = "test"
                }
                producer {
                    min-batch-size = 1024
                    max-batch-size = 16777216
                    max-in-memory-size = 10485760
                    max-on-disk-size = 1073741824
                }
            }
            """;

        tempConfigFile = createTempConfigFile(hoconContent);
        MetricsCollectorServer server = new MetricsCollectorServer(tempConfigFile.getAbsolutePath());

        // Start in background thread
        executor.submit(server::start);

        // Give it a moment
        Thread.sleep(200);

        assertFalse(server.isRunning());
        server.shutdown();
    }

    @Test
    @DisplayName("Should start and run with valid config")
    void shouldStartWithValidConfig() throws Exception {
        // Queue responses for scraping
        String prometheusData = "test_metric 123\n";
        for (int i = 0; i < 50; i++) {
            targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        }

        String hoconContent = String.format("""
            collector {
                enabled = true
                targets = ["%s"]
                target-prefix = ""
                server-url = "http://localhost:8081"
                scrape-interval-ms = 100
                tailing {
                    flush-threshold = 1
                    flush-interval-ms = 100
                    max-buffer-size = 10000
                }
                http {
                    connection-timeout-ms = 5000
                    read-timeout-ms = 5000
                    max-retries = 0
                    retry-delay-ms = 100
                }
                auth {
                    username = "test"
                    password = "test"
                }
                producer {
                    min-batch-size = 1024
                    max-batch-size = 16777216
                    max-in-memory-size = 10485760
                    max-on-disk-size = 1073741824
                }
            }
            """,
            targetServer.url("/prometheus").toString()
        );

        tempConfigFile = createTempConfigFile(hoconContent);
        MetricsCollectorServer server = new MetricsCollectorServer(tempConfigFile.getAbsolutePath());

        // Start in background thread
        executor.submit(server::start);

        // Wait for startup using polling (more reliable than fixed sleep)
        // Increased timeout to 10s to account for Arrow memory initialization on first run
        waitUntil(server::isRunning, 10000);

        assertTrue(server.isRunning(), "Server should be running");
        assertNotNull(server.getCollector(), "Collector should not be null");

        // Shutdown
        server.shutdown();

        // Wait for shutdown
        Thread.sleep(500);

        assertFalse(server.isRunning());
    }

    @Test
    @DisplayName("Should scrape metrics from target")
    void shouldScrapeMetrics() throws Exception {
        String prometheusData = """
            test_gauge 1.5
            test_counter 100
            """;

        // Queue enough responses
        for (int i = 0; i < 20; i++) {
            targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        }

        String hoconContent = String.format("""
            collector {
                enabled = true
                targets = ["%s"]
                server-url = "http://localhost:8081"
                scrape-interval-ms = 50
                tailing {
                    flush-threshold = 2
                    flush-interval-ms = 100
                }
                http {
                    max-retries = 0
                }
                auth {
                    username = "test"
                    password = "test"
                }
                producer {
                    min-batch-size = 1024
                    max-batch-size = 16777216
                    max-in-memory-size = 10485760
                    max-on-disk-size = 1073741824
                }
            }
            """,
            targetServer.url("/prometheus").toString()
        );

        tempConfigFile = createTempConfigFile(hoconContent);
        MetricsCollectorServer server = new MetricsCollectorServer(tempConfigFile.getAbsolutePath());

        // Start in background
        executor.submit(server::start);

        // Wait for some scrapes
        Thread.sleep(500);

        assertTrue(server.isRunning());

        // Should have scraped from target
        assertTrue(targetServer.getRequestCount() > 0, "Should have scraped at least once");

        // Should have sent metrics (queued to HttpArrowProducer)
        assertTrue(server.getCollector().getMetricsSentCount() > 0, "Should have sent metrics");

        server.shutdown();
    }

    @Test
    @DisplayName("Should handle shutdown gracefully")
    void shouldHandleShutdownGracefully() throws Exception {
        String prometheusData = "shutdown_metric 42\n";
        for (int i = 0; i < 3; i++) {
            targetServer.enqueue(new MockResponse()
                    .setBody(prometheusData)
                    .setResponseCode(200));
        }

        String hoconContent = String.format("""
        collector {
            enabled = true
            targets = ["%s"]
            server-url = "http://localhost:8081"
            scrape-interval-ms = 10
            tailing {
                flush-threshold = 1
                flush-interval-ms = 50
            }
            http {
                max-retries = 0
            }
            auth {
                username = "test"
                password = "test"
            }
            producer {
                min-batch-size = 1
                max-batch-size = 1024
                max-in-memory-size = 1024
                max-on-disk-size = 1024
            }
        }
        """,
                targetServer.url("/prometheus").toString()
        );

        tempConfigFile = createTempConfigFile(hoconContent);
        MetricsCollectorServer server =
                new MetricsCollectorServer(tempConfigFile.getAbsolutePath());

        executor.submit(server::start);

        // Wait until server is actually running
        waitUntil(server::isRunning, 1000);

        assertTrue(server.isRunning());

        MetricsCollector collector = server.getCollector();
        assertNotNull(collector);

        // Trigger shutdown
        server.shutdown();

        // Wait until fully stopped
        waitUntil(() -> !server.isRunning(), 1000);
        waitUntil(() -> !collector.isRunning(), 1000);

        assertFalse(server.isRunning());
        assertFalse(collector.isRunning());
    }

    static void waitUntil(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Condition not met within timeout");
    }


    @Test
    @DisplayName("Should expose collector for monitoring")
    void shouldExposeCollectorForMonitoring() throws Exception {
        String prometheusData = "monitor_metric 1\n";
        for (int i = 0; i < 20; i++) {
            targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        }

        String hoconContent = String.format("""
            collector {
                enabled = true
                targets = ["%s"]
                server-url = "http://localhost:8081"
                scrape-interval-ms = 50
                tailing {
                    flush-threshold = 1
                    flush-interval-ms = 50
                }
                http {
                    max-retries = 0
                }
                auth {
                    username = "test"
                    password = "test"
                }
                producer {
                    min-batch-size = 1024
                    max-batch-size = 16777216
                    max-in-memory-size = 10485760
                    max-on-disk-size = 1073741824
                }
            }
            """,
            targetServer.url("/prometheus").toString()
        );

        tempConfigFile = createTempConfigFile(hoconContent);
        MetricsCollectorServer server = new MetricsCollectorServer(tempConfigFile.getAbsolutePath());

        executor.submit(server::start);
        Thread.sleep(400);

        MetricsCollector collector = server.getCollector();
        assertNotNull(collector);

        // Should be able to access monitoring metrics
        assertTrue(collector.getMetricsSentCount() >= 0);
        assertTrue(collector.getMetricsDroppedCount() >= 0);
        assertTrue(collector.getBufferSize() >= 0);

        server.shutdown();
    }

    private File createTempConfigFile(String content) throws Exception {
        File file = File.createTempFile("test-server-config-", ".conf");
        file.deleteOnExit();
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
        }
        return file;
    }
}
