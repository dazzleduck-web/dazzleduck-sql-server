package io.dazzleduck.sql.logback;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class LogForwarderConfigTest {

    @Test
    void builder_shouldUseDefaults() {
        LogForwarderConfig config = LogForwarderConfig.builder().build();

        assertEquals("http://localhost:8081", config.baseUrl());
        assertEquals("admin", config.username());
        assertEquals("admin", config.password());
        assertEquals("log", config.ingestionQueue());
        assertEquals(Duration.ofSeconds(30), config.httpClientTimeout()); // Updated from 3s to 30s
        assertEquals(10000, config.maxBufferSize());
        assertEquals(Duration.ofSeconds(5), config.pollInterval());
        assertEquals(1024 * 1024, config.minBatchSize());
        assertEquals(Duration.ofSeconds(2), config.maxSendInterval());
        assertEquals(10 * 1024 * 1024, config.maxInMemorySize());
        assertEquals(1024 * 1024 * 1024L, config.maxOnDiskSize());
        assertTrue(config.enabled());
    }

    @Test
    void builder_shouldSetCustomValues() {
        LogForwarderConfig config = LogForwarderConfig.builder()
                .baseUrl("http://custom:9000")
                .username("user")
                .password("pass")
                .ingestionQueue("custom-logs")
                .httpClientTimeout(Duration.ofSeconds(10))
                .maxBufferSize(5000)
                .pollInterval(Duration.ofSeconds(30))
                .minBatchSize(512 * 1024)
                .maxSendInterval(Duration.ofSeconds(5))
                .maxInMemorySize(50 * 1024 * 1024)
                .maxOnDiskSize(5 * 1024 * 1024 * 1024L)
                .enabled(false)
                .build();

        assertEquals("http://custom:9000", config.baseUrl());
        assertEquals("user", config.username());
        assertEquals("pass", config.password());
        assertEquals("custom-logs", config.ingestionQueue());
        assertEquals(Duration.ofSeconds(10), config.httpClientTimeout());
        assertEquals(5000, config.maxBufferSize());
        assertEquals(Duration.ofSeconds(30), config.pollInterval());
        assertEquals(512 * 1024, config.minBatchSize());
        assertEquals(Duration.ofSeconds(5), config.maxSendInterval());
        assertEquals(50 * 1024 * 1024, config.maxInMemorySize());
        assertEquals(5 * 1024 * 1024 * 1024L, config.maxOnDiskSize());
        assertFalse(config.enabled());
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
                .baseUrl("http://test:8080")
                .username("u")
                .password("p")
                .ingestionQueue("path")
                .build();

        assertNotNull(config);
        assertEquals("http://test:8080", config.baseUrl());
        assertEquals("u", config.username());
    }
}
