package io.dazzleduck.sql.logback;

import java.time.Duration;
import java.util.*;

/**
 * Configuration for LogForwarder.
 * Use the builder pattern for flexible configuration.
 */
public final class LogForwarderConfig {
    // HTTP settings
    private final String baseUrl;
    private final String username;
    private final String password;
    private final Map<String, String> claims;
    private final String ingestionQueue;
    private final Duration httpClientTimeout;

    // Buffer settings
    private final int maxBufferSize;
    private final Duration pollInterval;

    // Sender settings
    private final long minBatchSize;
    private final long maxBatchSize;
    private final Duration maxSendInterval;
    private final long maxInMemorySize;
    private final long maxOnDiskSize;
    private final int retryCount;
    private final long retryIntervalMillis;
    private final List<String> project;
    private final List<String> partitionBy;

    // Feature flags
    private final boolean enabled;

    public LogForwarderConfig(
            String baseUrl,
            String username,
            String password,
            Map<String, String> claims,
            String ingestionQueue,
            Duration httpClientTimeout,
            int maxBufferSize,
            Duration pollInterval,
            long minBatchSize,
            long maxBatchSize,
            Duration maxSendInterval,
            long maxInMemorySize,
            long maxOnDiskSize,
            int retryCount,
            long retryIntervalMillis,
            List<String> project,
            List<String> partitionBy,
            boolean enabled) {
        Objects.requireNonNull(baseUrl, "baseUrl must not be null");
        Objects.requireNonNull(username, "username must not be null");
        Objects.requireNonNull(password, "password must not be null");
        Objects.requireNonNull(ingestionQueue, "ingestionQueue must not be null");
        Objects.requireNonNull(httpClientTimeout, "httpClientTimeout must not be null");
        Objects.requireNonNull(pollInterval, "pollInterval must not be null");
        Objects.requireNonNull(maxSendInterval, "maxSendInterval must not be null");

        this.baseUrl = baseUrl;
        this.username = username;
        this.password = password;
        this.claims = claims;
        this.ingestionQueue = ingestionQueue;
        this.httpClientTimeout = httpClientTimeout;
        this.maxBufferSize = maxBufferSize;
        this.pollInterval = pollInterval;
        this.minBatchSize = minBatchSize;
        this.maxBatchSize = maxBatchSize;
        this.maxSendInterval = maxSendInterval;
        this.maxInMemorySize = maxInMemorySize;
        this.maxOnDiskSize = maxOnDiskSize;
        this.retryCount = retryCount;
        this.retryIntervalMillis = retryIntervalMillis;
        this.project = Collections.unmodifiableList(new ArrayList<>(project));
        this.partitionBy = Collections.unmodifiableList(new ArrayList<>(partitionBy));
        this.enabled = enabled;
    }

    public String baseUrl() {
        return baseUrl;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public Map<String, String> claims() {
        return claims;
    }

    public String ingestionQueue() {
        return ingestionQueue;
    }

    public Duration httpClientTimeout() {
        return httpClientTimeout;
    }

    public int maxBufferSize() {
        return maxBufferSize;
    }

    public Duration pollInterval() {
        return pollInterval;
    }

    public long minBatchSize() {
        return minBatchSize;
    }

    public long maxBatchSize() {
        return maxBatchSize;
    }

    public Duration maxSendInterval() {
        return maxSendInterval;
    }

    public long maxInMemorySize() {
        return maxInMemorySize;
    }

    public long maxOnDiskSize() {
        return maxOnDiskSize;
    }

    public int retryCount() {
        return retryCount;
    }

    public long retryIntervalMillis() {
        return retryIntervalMillis;
    }

    public List<String> project() {
        return project;
    }

    public List<String> partitionBy() {
        return partitionBy;
    }

    public boolean enabled() {
        return enabled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogForwarderConfig that = (LogForwarderConfig) o;
        return maxBufferSize == that.maxBufferSize &&
               minBatchSize == that.minBatchSize &&
               maxBatchSize == that.maxBatchSize &&
               maxInMemorySize == that.maxInMemorySize &&
               maxOnDiskSize == that.maxOnDiskSize &&
               retryCount == that.retryCount &&
               retryIntervalMillis == that.retryIntervalMillis &&
               enabled == that.enabled &&
               Objects.equals(baseUrl, that.baseUrl) &&
               Objects.equals(username, that.username) &&
               Objects.equals(password, that.password) &&
               Objects.equals(ingestionQueue, that.ingestionQueue) &&
               Objects.equals(httpClientTimeout, that.httpClientTimeout) &&
               Objects.equals(pollInterval, that.pollInterval) &&
               Objects.equals(maxSendInterval, that.maxSendInterval) &&
               Objects.equals(project, that.project) &&
               Objects.equals(partitionBy, that.partitionBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseUrl, username, password, ingestionQueue, httpClientTimeout,
                maxBufferSize, pollInterval, minBatchSize, maxBatchSize, maxSendInterval,
                maxInMemorySize, maxOnDiskSize, retryCount, retryIntervalMillis,
                project, partitionBy, enabled);
    }

    @Override
    public String toString() {
        return "LogForwarderConfig[" +
               "baseUrl=" + baseUrl +
               ", username=" + username +
               ", password=***" +
               ", claims=" + claims +
               ", ingestionQueue=" + ingestionQueue +
               ", httpClientTimeout=" + httpClientTimeout +
               ", maxBufferSize=" + maxBufferSize +
               ", pollInterval=" + pollInterval +
               ", minBatchSize=" + minBatchSize +
               ", maxBatchSize=" + maxBatchSize +
               ", maxSendInterval=" + maxSendInterval +
               ", maxInMemorySize=" + maxInMemorySize +
               ", maxOnDiskSize=" + maxOnDiskSize +
               ", retryCount=" + retryCount +
               ", retryIntervalMillis=" + retryIntervalMillis +
               ", project=" + project +
               ", partitionBy=" + partitionBy +
               ", enabled=" + enabled +
               "]";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String baseUrl = "http://localhost:8081";
        private String username = "admin";
        private String password = "admin";
        private Map<String, String> claims = Map.of();
        private String ingestionQueue = "log";
        private Duration httpClientTimeout = Duration.ofSeconds(30); // Increased from 3s to 30s for ingestion
        private int maxBufferSize = 10000;
        private Duration pollInterval = Duration.ofSeconds(5);
        private long minBatchSize = 1024 * 1024; // 1 MB
        private long maxBatchSize = 10 * 1024 * 1024; // 10 MB
        private Duration maxSendInterval = Duration.ofSeconds(2);
        private long maxInMemorySize = 10 * 1024 * 1024; // 10 MB
        private long maxOnDiskSize = 1024 * 1024 * 1024L; // 1 GB
        private int retryCount = 3;
        private long retryIntervalMillis = 1000; // 1 second
        private List<String> project = Collections.emptyList();
        private List<String> partitionBy = Collections.emptyList();
        private boolean enabled = true;

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

        public Builder ingestionQueue(String ingestionQueue) {
            this.ingestionQueue = Objects.requireNonNull(ingestionQueue);
            return this;
        }

        public Builder httpClientTimeout(Duration httpClientTimeout) {
            this.httpClientTimeout = Objects.requireNonNull(httpClientTimeout);
            return this;
        }

        public Builder maxBufferSize(int maxBufferSize) {
            if (maxBufferSize <= 0) throw new IllegalArgumentException("maxBufferSize must be positive");
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public Builder pollInterval(Duration pollInterval) {
            this.pollInterval = Objects.requireNonNull(pollInterval);
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

        public Builder project(List<String> project) {
            this.project = Objects.requireNonNull(project);
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

        public Builder claims(Map<String, String> claims) {
            this.claims = Objects.requireNonNull(claims);
            return this;
        }

        public LogForwarderConfig build() {
            return new LogForwarderConfig(
                    baseUrl,
                    username,
                    password,
                    claims,
                    ingestionQueue,
                    httpClientTimeout,
                    maxBufferSize,
                    pollInterval,
                    minBatchSize,
                    maxBatchSize,
                    maxSendInterval,
                    maxInMemorySize,
                    maxOnDiskSize,
                    retryCount,
                    retryIntervalMillis,
                    project,
                    partitionBy,
                    enabled
            );
        }
    }
}
