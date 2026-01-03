package io.dazzleduck.sql.collector.metricscollector;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.collector.metricscollector.config.CollectorConfig;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MetricsCollectorServer - standalone server with HOCON config.
 */
class MetricsCollectorServerTest {

    private MockWebServer targetServer;
    private MockWebServer forwardServer;
    private File tempConfigFile;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
        targetServer = new MockWebServer();
        targetServer.start();

        forwardServer = new MockWebServer();
        forwardServer.start();

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
        forwardServer.shutdown();
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
                server-url = "%s"
                scrape-interval-ms = 100
                tailing {
                    flush-threshold = 1
                    flush-interval-ms = 100
                }
            }
            """,
            targetServer.url("/prometheus").toString(),
            forwardServer.url("/ingest").toString()
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
                server-url = "%s"
            }
            """,
            targetServer.url("/prometheus").toString(),
            forwardServer.url("/ingest").toString()
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
        // Queue responses for scraping and forwarding
        String prometheusData = "test_metric 123\n";
        for (int i = 0; i < 20; i++) {
            targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        }
        for (int i = 0; i < 20; i++) {
            forwardServer.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));
        }

        String hoconContent = String.format("""
            collector {
                enabled = true
                targets = ["%s"]
                target-prefix = ""
                server-url = "%s"
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
            }
            """,
            targetServer.url("/prometheus").toString(),
            forwardServer.url("/ingest").toString()
        );

        tempConfigFile = createTempConfigFile(hoconContent);
        MetricsCollectorServer server = new MetricsCollectorServer(tempConfigFile.getAbsolutePath());

        // Start in background thread
        executor.submit(server::start);

        // Wait for startup (give it more time)
        Thread.sleep(500);

        assertTrue(server.isRunning(), "Server should be running");
        assertNotNull(server.getCollector(), "Collector should not be null");

        // Shutdown
        server.shutdown();

        // Wait for shutdown
        Thread.sleep(300);

        assertFalse(server.isRunning());
    }

    @Test
    @DisplayName("Should scrape and forward metrics")
    void shouldScrapeAndForwardMetrics() throws Exception {
        String prometheusData = """
            test_gauge 1.5
            test_counter 100
            """;

        // Queue enough responses
        for (int i = 0; i < 20; i++) {
            targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        }
        for (int i = 0; i < 20; i++) {
            forwardServer.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));
        }

        String hoconContent = String.format("""
            collector {
                enabled = true
                targets = ["%s"]
                server-url = "%s"
                scrape-interval-ms = 50
                tailing {
                    flush-threshold = 2
                    flush-interval-ms = 100
                }
                http {
                    max-retries = 0
                }
            }
            """,
            targetServer.url("/prometheus").toString(),
            forwardServer.url("/ingest").toString()
        );

        tempConfigFile = createTempConfigFile(hoconContent);
        MetricsCollectorServer server = new MetricsCollectorServer(tempConfigFile.getAbsolutePath());

        // Start in background
        executor.submit(server::start);

        // Wait for some scrapes and forwards
        Thread.sleep(500);

        assertTrue(server.isRunning());

        // Should have scraped from target
        assertTrue(targetServer.getRequestCount() > 0, "Should have scraped at least once");

        // Should have forwarded to server
        assertTrue(forwardServer.getRequestCount() > 0, "Should have forwarded at least once");

        server.shutdown();
    }

    @Test
    @DisplayName("Should handle shutdown gracefully")
    void shouldHandleShutdownGracefully() throws Exception {
        String prometheusData = "shutdown_metric 42\n";
        for (int i = 0; i < 10; i++) {
            targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        }
        forwardServer.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));
        forwardServer.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));

        String hoconContent = String.format("""
            collector {
                enabled = true
                targets = ["%s"]
                server-url = "%s"
                scrape-interval-ms = 50
                tailing {
                    flush-threshold = 100
                    flush-interval-ms = 10000
                }
                http {
                    max-retries = 0
                }
            }
            """,
            targetServer.url("/prometheus").toString(),
            forwardServer.url("/ingest").toString()
        );

        tempConfigFile = createTempConfigFile(hoconContent);
        MetricsCollectorServer server = new MetricsCollectorServer(tempConfigFile.getAbsolutePath());

        executor.submit(server::start);
        Thread.sleep(200);

        assertTrue(server.isRunning());

        // Get collector before shutdown
        MetricsCollector collector = server.getCollector();
        assertNotNull(collector);

        // Trigger shutdown
        server.shutdown();
        Thread.sleep(100);

        assertFalse(server.isRunning());
        assertFalse(collector.isRunning());
    }

    @Test
    @DisplayName("Should expose collector for monitoring")
    void shouldExposeCollectorForMonitoring() throws Exception {
        String prometheusData = "monitor_metric 1\n";
        for (int i = 0; i < 20; i++) {
            targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        }
        for (int i = 0; i < 20; i++) {
            forwardServer.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));
        }

        String hoconContent = String.format("""
            collector {
                enabled = true
                targets = ["%s"]
                server-url = "%s"
                scrape-interval-ms = 50
                tailing {
                    flush-threshold = 1
                    flush-interval-ms = 50
                }
                http {
                    max-retries = 0
                }
            }
            """,
            targetServer.url("/prometheus").toString(),
            forwardServer.url("/ingest").toString()
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
