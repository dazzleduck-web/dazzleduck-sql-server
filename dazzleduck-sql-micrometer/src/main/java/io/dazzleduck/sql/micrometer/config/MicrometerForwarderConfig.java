package io.dazzleduck.sql.micrometer.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for MicrometerForwarder.
 * Use the builder pattern for flexible configuration.
 */
public record MicrometerForwarderConfig(
        // HTTP settings
        String baseUrl,
        String username,
        String password,
        String jwt,
        Map<String,String> claims,
        String ingestionQueue,
        Duration httpClientTimeout,

        // Micrometer step interval
        Duration stepInterval,

        // Sender settings
        long minBatchSize,
        long maxBatchSize,
        Duration maxSendInterval,
        long maxInMemorySize,
        long maxOnDiskSize,
        int retryCount,
        long retryIntervalMillis,
        List<String> partitionBy,

        // Feature flags
        boolean enabled,

        // System metrics flag
        boolean publishSystemMetrics
) {
    public MicrometerForwarderConfig {
        Objects.requireNonNull(baseUrl, "baseUrl must not be null");
        Objects.requireNonNull(ingestionQueue, "ingestionQueue must not be null");
        Objects.requireNonNull(httpClientTimeout, "httpClientTimeout must not be null");
        Objects.requireNonNull(stepInterval, "stepInterval must not be null");
        Objects.requireNonNull(maxSendInterval, "maxSendInterval must not be null");
        if (jwt == null) {
            Objects.requireNonNull(username, "username must not be null when jwt is not provided");
            Objects.requireNonNull(password, "password must not be null when jwt is not provided");
        }
        partitionBy = List.copyOf(partitionBy);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String baseUrl = "http://localhost:8081";
        private String username = "admin";
        private String password = "admin";
        private String jwt = null;
        private Map<String,String> claims = Map.of();
        private String ingestionQueue = "metrics";
        private Duration httpClientTimeout = Duration.ofSeconds(3);
        private Duration stepInterval = Duration.ofSeconds(10);
        private long minBatchSize = 1024 * 1024; // 1 MB
        private long maxBatchSize = 10 * 1024 * 1024; // 10 MB
        private Duration maxSendInterval = Duration.ofSeconds(2);
        private long maxInMemorySize = 10 * 1024 * 1024; // 10 MB
        private long maxOnDiskSize = 1024 * 1024 * 1024L; // 1 GB
        private int retryCount = 3;
        private long retryIntervalMillis = 1000; // 1 second
        private List<String> partitionBy = List.of();
        private boolean enabled = true;

        // System metrics default - enabled by default
        private boolean publishSystemMetrics = true;

        private Builder() {
        }

        public Builder baseUrl(String baseUrl) {
            this.baseUrl = Objects.requireNonNull(baseUrl);
            return this;
        }

        public Builder username(String username) {
            this.username = Objects.requireNonNull(username);
            return this;
        }

        public Builder password(String password) {
            this.password = Objects.requireNonNull(password);
            return this;
        }

        public Builder jwt(String jwt) {
            this.jwt = Objects.requireNonNull(jwt);
            return this;
        }

        public Builder claims(Map<String, String> claims) {
            this.claims = Objects.requireNonNull(claims);
            return this;
        }

        public Builder ingestionQueue(String ingestionQueue) {
            this.ingestionQueue = Objects.requireNonNull(ingestionQueue);
            return this;
        }

        public Builder httpClientTimeout(Duration httpClientTimeout) {
            this.httpClientTimeout = Objects.requireNonNull(httpClientTimeout);
            return this;
        }

        public Builder stepInterval(Duration stepInterval) {
            this.stepInterval = Objects.requireNonNull(stepInterval);
            return this;
        }

        public Builder minBatchSize(long minBatchSize) {
            if (minBatchSize <= 0) throw new IllegalArgumentException("minBatchSize must be positive");
            this.minBatchSize = minBatchSize;
            return this;
        }

        public Builder maxBatchSize(long maxBatchSize) {
            if (maxBatchSize <= 0) throw new IllegalArgumentException("maxBatchSize must be positive");
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder maxSendInterval(Duration maxSendInterval) {
            this.maxSendInterval = Objects.requireNonNull(maxSendInterval);
            return this;
        }

        public Builder maxInMemorySize(long maxInMemorySize) {
            if (maxInMemorySize <= 0) throw new IllegalArgumentException("maxInMemorySize must be positive");
            this.maxInMemorySize = maxInMemorySize;
            return this;
        }

        public Builder maxOnDiskSize(long maxOnDiskSize) {
            if (maxOnDiskSize <= 0) throw new IllegalArgumentException("maxOnDiskSize must be positive");
            this.maxOnDiskSize = maxOnDiskSize;
            return this;
        }

        public Builder retryCount(int retryCount) {
            if (retryCount < 0) throw new IllegalArgumentException("retryCount must be non-negative");
            this.retryCount = retryCount;
            return this;
        }

        public Builder retryIntervalMillis(long retryIntervalMillis) {
            if (retryIntervalMillis < 0) throw new IllegalArgumentException("retryIntervalMillis must be non-negative");
            this.retryIntervalMillis = retryIntervalMillis;
            return this;
        }

        public Builder partitionBy(List<String> partitionBy) {
            this.partitionBy = Objects.requireNonNull(partitionBy);
            return this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder publishSystemMetrics(boolean publishSystemMetrics) {
            this.publishSystemMetrics = publishSystemMetrics;
            return this;
        }

        public MicrometerForwarderConfig build() {
            return new MicrometerForwarderConfig(
                    baseUrl,
                    username,
                    password,
                    jwt,
                    claims,
                    ingestionQueue,
                    httpClientTimeout,
                    stepInterval,
                    minBatchSize,
                    maxBatchSize,
                    maxSendInterval,
                    maxInMemorySize,
                    maxOnDiskSize,
                    retryCount,
                    retryIntervalMillis,
                    partitionBy,
                    enabled,
                    publishSystemMetrics
            );
        }
    }
}
