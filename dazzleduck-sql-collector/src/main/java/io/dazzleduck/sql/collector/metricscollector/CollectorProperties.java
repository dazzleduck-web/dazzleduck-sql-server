package io.dazzleduck.sql.collector.metricscollector;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration properties for the metrics collector module.
 *
 * This is a standalone, portable metrics collector that:
 * - Scrapes Prometheus metrics from configured endpoints
 * - Buffers metrics with tailing mode
 * - Forwards to a remote server in Apache Arrow format
 *
 * Configuration example (application.yml):
 *
 * collector:
 *   enabled: true
 *   targets:
 *     - <a href="http://localhost:8080/actuator/prometheus">...</a>
 *     - <a href="http://localhost:8081/actuator/prometheus">...</a>
 *   target-prefix: http://localhost  # Optional: prefix for all targets
 *   server-url: <a href="http://remote-server:8080/ingest">...</a>
 *   path: scraped_metrics
 *   scrape-interval-ms: 15000
 *   flush-threshold: 100
 *   flush-interval-ms: 5000
 */
public class CollectorProperties {

    private boolean enabled = false;

    /**
     * List of target endpoints to scrape metrics from.
     * Example: <a href="http://localhost:8080/actuator/prometheus">...</a>
     */
    private List<String> targets = new ArrayList<>();

    /**
     * Optional prefix for target URLs.
     * If set, targets can be specified as relative paths.
     * Example: prefix = "<a href="http://localhost:8080">...</a>", target = "/actuator/prometheus"
     */
    private String targetPrefix = "";

    /**
     * Remote server URL to forward scraped metrics.
     */
    private String serverUrl = "http://localhost:8081/ingest";

    /**
     * Path parameter appended to server URL.
     */
    private String path = "scraped_metrics";

    /**
     * Scrape interval in milliseconds.
     */
    private int scrapeIntervalMs = 15000;

    /**
     * Flush when buffer reaches this size (tailing mode).
     */
    private int flushThreshold = 100;

    /**
     * Max time in ms before flushing buffer (tailing mode).
     */
    private int flushIntervalMs = 5000;

    /**
     * Maximum metrics to buffer before dropping oldest.
     */
    private int maxBufferSize = 10000;

    /**
     * HTTP connection timeout in milliseconds.
     */
    private int connectionTimeoutMs = 5000;

    /**
     * HTTP read timeout in milliseconds.
     */
    private int readTimeoutMs = 10000;

    /**
     * Max retry attempts for failed forwards.
     */
    private int maxRetries = 3;

    /**
     * Initial retry delay in milliseconds (doubles each retry).
     */
    private int retryDelayMs = 1000;

    /**
     * Unique identifier for this collector instance.
     */
    private String collectorId = "metrics-collector";

    /**
     * Human-readable name for this collector.
     */
    private String collectorName = "Metrics Collector";

    /**
     * Host identifier (defaults to hostname if empty).
     */
    private String collectorHost = "";

    // Getters and Setters

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<String> getTargets() {
        return targets;
    }

    public void setTargets(List<String> targets) {
        this.targets = targets;
    }

    public String getTargetPrefix() {
        return targetPrefix;
    }

    public void setTargetPrefix(String targetPrefix) {
        this.targetPrefix = targetPrefix;
    }

    public String getServerUrl() {
        return serverUrl;
    }

    public void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getScrapeIntervalMs() {
        return scrapeIntervalMs;
    }

    public void setScrapeIntervalMs(int scrapeIntervalMs) {
        this.scrapeIntervalMs = scrapeIntervalMs;
    }

    public int getFlushThreshold() {
        return flushThreshold;
    }

    public void setFlushThreshold(int flushThreshold) {
        this.flushThreshold = flushThreshold;
    }

    public int getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public void setFlushIntervalMs(int flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public void setReadTimeoutMs(int readTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public int getRetryDelayMs() {
        return retryDelayMs;
    }

    public void setRetryDelayMs(int retryDelayMs) {
        this.retryDelayMs = retryDelayMs;
    }

    public String getCollectorId() {
        return collectorId;
    }

    public void setCollectorId(String collectorId) {
        this.collectorId = collectorId;
    }

    public String getCollectorName() {
        return collectorName;
    }

    public void setCollectorName(String collectorName) {
        this.collectorName = collectorName;
    }

    public String getCollectorHost() {
        return collectorHost;
    }

    public void setCollectorHost(String collectorHost) {
        this.collectorHost = collectorHost;
    }

    /**
     * Get resolved target URLs (applies prefix if set).
     */
    public List<String> getResolvedTargets() {
        if (targetPrefix == null || targetPrefix.isEmpty()) {
            return targets;
        }
        List<String> resolved = new ArrayList<>();
        for (String target : targets) {
            if (target.startsWith("http://") || target.startsWith("https://")) {
                resolved.add(target);
            } else {
                String prefix = targetPrefix.endsWith("/") ? targetPrefix.substring(0, targetPrefix.length() - 1) : targetPrefix;
                String path = target.startsWith("/") ? target : "/" + target;
                resolved.add(prefix + path);
            }
        }
        return resolved;
    }
}
