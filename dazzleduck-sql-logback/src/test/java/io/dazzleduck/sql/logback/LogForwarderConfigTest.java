package io.dazzleduck.sql.logback;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class LogForwarderConfigTest {

    @Test
    void builder_shouldUseDefaults() {
        LogForwarderConfig config = LogForwarderConfig.builder().build();

        assertEquals("default-app", config.getApplicationId());
        assertEquals("DefaultApplication", config.getApplicationName());
        // Verify hostname is auto-detected (not null or empty)
        assertNotNull(config.getApplicationHost());
        assertFalse(config.getApplicationHost().isEmpty());
        assertEquals("http://localhost:8081", config.getBaseUrl());
        assertEquals("admin", config.getUsername());
        assertEquals("admin", config.getPassword());
        assertEquals("logs", config.getTargetPath());
        assertEquals(Duration.ofSeconds(3), config.getHttpClientTimeout());
        assertEquals(10000, config.getMaxBufferSize());
        assertEquals(Duration.ofSeconds(5), config.getPollInterval());
        assertEquals(1024 * 1024, config.getMinBatchSize());
        assertEquals(Duration.ofSeconds(2), config.getMaxSendInterval());
        assertEquals(10 * 1024 * 1024, config.getMaxInMemorySize());
        assertEquals(1024 * 1024 * 1024L, config.getMaxOnDiskSize());
        assertTrue(config.isEnabled());
    }

    @Test
    void builder_shouldSetCustomValues() {
        LogForwarderConfig config = LogForwarderConfig.builder()
                .applicationId("my-app")
                .applicationName("MyApp")
                .applicationHost("my-host")
                .baseUrl("http://custom:9000")
                .username("user")
                .password("pass")
                .targetPath("custom-logs")
                .httpClientTimeout(Duration.ofSeconds(10))
                .maxBufferSize(5000)
                .pollInterval(Duration.ofSeconds(30))
                .minBatchSize(512 * 1024)
                .maxSendInterval(Duration.ofSeconds(5))
                .maxInMemorySize(50 * 1024 * 1024)
                .maxOnDiskSize(5 * 1024 * 1024 * 1024L)
                .enabled(false)
                .build();

        assertEquals("my-app", config.getApplicationId());
        assertEquals("MyApp", config.getApplicationName());
        assertEquals("my-host", config.getApplicationHost());
        assertEquals("http://custom:9000", config.getBaseUrl());
        assertEquals("user", config.getUsername());
        assertEquals("pass", config.getPassword());
        assertEquals("custom-logs", config.getTargetPath());
        assertEquals(Duration.ofSeconds(10), config.getHttpClientTimeout());
        assertEquals(5000, config.getMaxBufferSize());
        assertEquals(Duration.ofSeconds(30), config.getPollInterval());
        assertEquals(512 * 1024, config.getMinBatchSize());
        assertEquals(Duration.ofSeconds(5), config.getMaxSendInterval());
        assertEquals(50 * 1024 * 1024, config.getMaxInMemorySize());
        assertEquals(5 * 1024 * 1024 * 1024L, config.getMaxOnDiskSize());
        assertFalse(config.isEnabled());
    }

    @Test
    void builder_shouldRejectNullApplicationId() {
        assertThrows(NullPointerException.class, () ->
                LogForwarderConfig.builder().applicationId(null)
        );
    }

    @Test
    void builder_shouldRejectNullApplicationName() {
        assertThrows(NullPointerException.class, () ->
                LogForwarderConfig.builder().applicationName(null)
        );
    }

    @Test
    void builder_shouldRejectNullBaseUrl() {
        assertThrows(NullPointerException.class, () ->
                LogForwarderConfig.builder().baseUrl(null)
        );
    }

    @Test
    void builder_shouldRejectNonPositiveMaxBufferSize() {
        assertThrows(IllegalArgumentException.class, () ->
                LogForwarderConfig.builder().maxBufferSize(0)
        );

        assertThrows(IllegalArgumentException.class, () ->
                LogForwarderConfig.builder().maxBufferSize(-1)
        );
    }

    @Test
    void builder_shouldRejectNonPositiveMinBatchSize() {
        assertThrows(IllegalArgumentException.class, () ->
                LogForwarderConfig.builder().minBatchSize(0)
        );

        assertThrows(IllegalArgumentException.class, () ->
                LogForwarderConfig.builder().minBatchSize(-100)
        );
    }

    @Test
    void builder_shouldRejectNonPositiveMaxInMemorySize() {
        assertThrows(IllegalArgumentException.class, () ->
                LogForwarderConfig.builder().maxInMemorySize(0)
        );
    }

    @Test
    void builder_shouldRejectNonPositiveMaxOnDiskSize() {
        assertThrows(IllegalArgumentException.class, () ->
                LogForwarderConfig.builder().maxOnDiskSize(0)
        );
    }

    @Test
    void builder_shouldChainMethods() {
        LogForwarderConfig config = LogForwarderConfig.builder()
                .applicationId("id")
                .applicationName("name")
                .applicationHost("host")
                .baseUrl("http://test:8080")
                .username("u")
                .password("p")
                .targetPath("path")
                .build();

        assertNotNull(config);
        assertEquals("id", config.getApplicationId());
        assertEquals("name", config.getApplicationName());
    }
}
