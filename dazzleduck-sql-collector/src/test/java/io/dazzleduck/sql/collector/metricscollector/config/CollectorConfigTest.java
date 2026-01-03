package io.dazzleduck.sql.collector.metricscollector.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.collector.metricscollector.CollectorProperties;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CollectorConfig - HOCON configuration loading.
 */
class CollectorConfigTest {

    private File tempConfigFile;

    @BeforeEach
    void setUp() throws Exception {
        // Clear any cached config from previous tests
        ConfigFactory.invalidateCaches();
    }

    @AfterEach
    void tearDown() {
        if (tempConfigFile != null && tempConfigFile.exists()) {
            tempConfigFile.delete();
        }
        ConfigFactory.invalidateCaches();
    }

    @Test
    @DisplayName("Should load default configuration from classpath")
    void loadDefaultConfig() {
        CollectorConfig config = new CollectorConfig();

        assertTrue(config.isEnabled());
        assertNotNull(config.getTargets());
        assertFalse(config.getTargets().isEmpty());
        assertNotNull(config.getServerUrl());
        assertTrue(config.getScrapeIntervalMs() > 0);
        assertTrue(config.getFlushThreshold() > 0);
        assertTrue(config.getFlushIntervalMs() > 0);
    }

    @Test
    @DisplayName("Should load configuration from external file")
    void loadExternalConfig() throws Exception {
        String hoconContent = """
            collector {
                enabled = true
                targets = ["/custom/prometheus"]
                target-prefix = "http://custom-server:9090"
                server-url = "http://remote:8080/custom-ingest"
                path = "custom_metrics"
                scrape-interval-ms = 30000
                tailing {
                    flush-threshold = 200
                    flush-interval-ms = 10000
                    max-buffer-size = 50000
                }
                http {
                    connection-timeout-ms = 10000
                    read-timeout-ms = 20000
                    max-retries = 5
                    retry-delay-ms = 2000
                }
                identity {
                    id = "custom-collector"
                    name = "Custom Collector"
                    host = "custom-host"
                }
            }
            """;

        tempConfigFile = createTempConfigFile(hoconContent);
        CollectorConfig config = new CollectorConfig(tempConfigFile.getAbsolutePath());

        assertTrue(config.isEnabled());
        assertEquals(List.of("/custom/prometheus"), config.getTargets());
        assertEquals("http://custom-server:9090", config.getTargetPrefix());
        assertEquals("http://remote:8080/custom-ingest", config.getServerUrl());
        assertEquals("custom_metrics", config.getPath());
        assertEquals(30000, config.getScrapeIntervalMs());
        assertEquals(200, config.getFlushThreshold());
        assertEquals(10000, config.getFlushIntervalMs());
        assertEquals(50000, config.getMaxBufferSize());
        assertEquals(10000, config.getConnectionTimeoutMs());
        assertEquals(20000, config.getReadTimeoutMs());
        assertEquals(5, config.getMaxRetries());
        assertEquals(2000, config.getRetryDelayMs());
        assertEquals("custom-collector", config.getCollectorId());
        assertEquals("Custom Collector", config.getCollectorName());
        assertEquals("custom-host", config.getCollectorHost());
    }

    @Test
    @DisplayName("Should handle missing external config file gracefully")
    void handleMissingExternalFile() {
        // Should not throw, just use defaults
        CollectorConfig config = new CollectorConfig("/non/existent/path.conf");

        // Should fall back to defaults from classpath
        assertNotNull(config.getServerUrl());
    }

    @Test
    @DisplayName("Should convert to CollectorProperties correctly")
    void convertToProperties() throws Exception {
        String hoconContent = """
            collector {
                enabled = true
                targets = ["/test/metrics", "/test/prometheus"]
                target-prefix = "http://localhost:8080"
                server-url = "http://server:8081/ingest"
                path = "test_path"
                scrape-interval-ms = 5000
                tailing {
                    flush-threshold = 50
                    flush-interval-ms = 2000
                    max-buffer-size = 5000
                }
                http {
                    connection-timeout-ms = 3000
                    read-timeout-ms = 6000
                    max-retries = 2
                    retry-delay-ms = 500
                }
                identity {
                    id = "test-id"
                    name = "Test Name"
                    host = "test-host"
                }
            }
            """;

        tempConfigFile = createTempConfigFile(hoconContent);
        CollectorConfig config = new CollectorConfig(tempConfigFile.getAbsolutePath());
        CollectorProperties props = config.toProperties();

        assertTrue(props.isEnabled());
        assertEquals(List.of("/test/metrics", "/test/prometheus"), props.getTargets());
        assertEquals("http://localhost:8080", props.getTargetPrefix());
        assertEquals("http://server:8081/ingest", props.getServerUrl());
        assertEquals("test_path", props.getPath());
        assertEquals(5000, props.getScrapeIntervalMs());
        assertEquals(50, props.getFlushThreshold());
        assertEquals(2000, props.getFlushIntervalMs());
        assertEquals(5000, props.getMaxBufferSize());
        assertEquals(3000, props.getConnectionTimeoutMs());
        assertEquals(6000, props.getReadTimeoutMs());
        assertEquals(2, props.getMaxRetries());
        assertEquals(500, props.getRetryDelayMs());
        assertEquals("test-id", props.getCollectorId());
        assertEquals("Test Name", props.getCollectorName());
        assertEquals("test-host", props.getCollectorHost());
    }

    @Test
    @DisplayName("Should use fallback values for missing config values")
    void useFallbackForMissingValues() throws Exception {
        // External file only specifies some values
        String hoconContent = """
            collector {
                enabled = true
                targets = ["/metrics"]
            }
            """;

        tempConfigFile = createTempConfigFile(hoconContent);
        CollectorConfig config = new CollectorConfig(tempConfigFile.getAbsolutePath());

        // Should use values from classpath application.conf as fallback
        // These are the values defined in src/test/resources/application.conf
        assertTrue(config.isEnabled());
        assertEquals(List.of("/metrics"), config.getTargets()); // From external file
        assertNotNull(config.getTargetPrefix()); // Falls back to classpath or default
        assertNotNull(config.getServerUrl());
        assertTrue(config.getScrapeIntervalMs() > 0);
        assertTrue(config.getFlushThreshold() > 0);
        assertTrue(config.getFlushIntervalMs() > 0);
        assertTrue(config.getMaxBufferSize() > 0);
    }

    @Test
    @DisplayName("Should handle disabled collector")
    void handleDisabledCollector() throws Exception {
        String hoconContent = """
            collector {
                enabled = false
            }
            """;

        tempConfigFile = createTempConfigFile(hoconContent);
        CollectorConfig config = new CollectorConfig(tempConfigFile.getAbsolutePath());

        assertFalse(config.isEnabled());
    }

    @Test
    @DisplayName("Should support multiple targets")
    void supportMultipleTargets() throws Exception {
        String hoconContent = """
            collector {
                enabled = true
                targets = [
                    "http://service1:8080/prometheus",
                    "http://service2:8080/prometheus",
                    "http://service3:8080/prometheus"
                ]
            }
            """;

        tempConfigFile = createTempConfigFile(hoconContent);
        CollectorConfig config = new CollectorConfig(tempConfigFile.getAbsolutePath());

        assertEquals(3, config.getTargets().size());
        assertTrue(config.getTargets().contains("http://service1:8080/prometheus"));
        assertTrue(config.getTargets().contains("http://service2:8080/prometheus"));
        assertTrue(config.getTargets().contains("http://service3:8080/prometheus"));
    }

    @Test
    @DisplayName("Should create from Config object")
    void createFromConfigObject() {
        String hoconString = """
            collector {
                enabled = true
                targets = ["/test"]
                server-url = "http://test:8080/ingest"
            }
            """;

        Config config = ConfigFactory.parseString(hoconString);
        CollectorConfig collectorConfig = new CollectorConfig(config);

        assertTrue(collectorConfig.isEnabled());
        assertEquals(List.of("/test"), collectorConfig.getTargets());
        assertEquals("http://test:8080/ingest", collectorConfig.getServerUrl());
    }

    @Test
    @DisplayName("Should have meaningful toString")
    void toStringTest() throws Exception {
        String hoconContent = """
            collector {
                enabled = true
                targets = ["/metrics"]
                server-url = "http://localhost:8081/ingest"
            }
            """;

        tempConfigFile = createTempConfigFile(hoconContent);
        CollectorConfig config = new CollectorConfig(tempConfigFile.getAbsolutePath());

        String str = config.toString();
        assertTrue(str.contains("enabled=true"));
        assertTrue(str.contains("serverUrl="));
    }

    @Test
    @DisplayName("Should allow system property overrides")
    void systemPropertyOverrides() throws Exception {
        String hoconContent = """
            collector {
                enabled = true
                targets = ["/original"]
                server-url = "http://original:8080/ingest"
            }
            """;

        tempConfigFile = createTempConfigFile(hoconContent);

        // Set system property override
        System.setProperty("collector.server-url", "http://override:9090/custom");
        try {
            ConfigFactory.invalidateCaches();
            CollectorConfig config = new CollectorConfig(tempConfigFile.getAbsolutePath());

            // System property should override file config
            assertEquals("http://override:9090/custom", config.getServerUrl());
        } finally {
            System.clearProperty("collector.server-url");
            ConfigFactory.invalidateCaches();
        }
    }

    private File createTempConfigFile(String content) throws Exception {
        File file = File.createTempFile("test-config-", ".conf");
        file.deleteOnExit();
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
        }
        return file;
    }
}
