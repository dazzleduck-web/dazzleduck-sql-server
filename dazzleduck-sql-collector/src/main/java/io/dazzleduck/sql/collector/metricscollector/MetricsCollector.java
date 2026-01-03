package io.dazzleduck.sql.collector.metricscollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main metrics collector with tailing support.
 *
 * This is a standalone, portable module that:
 * 1. Scrapes Prometheus metrics from configured endpoints at regular intervals
 * 2. Buffers metrics in memory
 * 3. Flushes to remote server when threshold or interval is reached (tailing mode)
 *
 * Usage:
 * <pre>
 * CollectorProperties props = new CollectorProperties();
 * props.setEnabled(true);
 * props.setTargets(List.of("<a href="http://localhost:8080/actuator/prometheus">...</a>"));
 * props.setServerUrl("http://remote:8081/ingest");
 *
 * MetricsCollector collector = new MetricsCollector(props);
 * collector.start();
 *
 * // On shutdown:
 * collector.stop();
 * </pre>
 */
public class MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);

    private final CollectorProperties properties;
    private final MetricsBuffer buffer;
    private final MetricsScraper scraper;
    private final MetricsForwarder forwarder;

    private final ScheduledExecutorService scraperScheduler;
    private final ScheduledExecutorService forwarderScheduler;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile long lastFlushTime = System.currentTimeMillis();

    public MetricsCollector(CollectorProperties properties) {
        this.properties = properties;
        this.buffer = new MetricsBuffer(properties.getMaxBufferSize());
        this.scraper = new MetricsScraper(properties, buffer);
        this.forwarder = new MetricsForwarder(properties);

        // Separate thread for scraping
        this.scraperScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-scraper");
            t.setDaemon(true);
            return t;
        });

        // Separate thread for forwarding (tailing)
        this.forwarderScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-forwarder-tailer");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Start the collector.
     */
    public void start() {
        if (!properties.isEnabled()) {
            log.info("Metrics collector is disabled");
            return;
        }

        if (properties.getTargets().isEmpty()) {
            log.warn("Metrics collector enabled but no targets configured");
            return;
        }

        if (running.compareAndSet(false, true)) {
            // Start scraper - runs at fixed intervals
            scraperScheduler.scheduleAtFixedRate(
                this::scrape,
                0,
                properties.getScrapeIntervalMs(),
                TimeUnit.MILLISECONDS
            );

            // Start forwarder tailer - checks every 100ms for flush conditions
            forwarderScheduler.scheduleAtFixedRate(
                this::checkAndFlush,
                100,
                100,
                TimeUnit.MILLISECONDS
            );

            log.info("Metrics collector started: targets={}, scrapeInterval={}ms, flushThreshold={}, flushInterval={}ms, server={}",
                properties.getResolvedTargets().size(),
                properties.getScrapeIntervalMs(),
                properties.getFlushThreshold(),
                properties.getFlushIntervalMs(),
                properties.getServerUrl());
        }
    }

    /**
     * Stop the collector gracefully.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            // Final flush before shutdown
            flushBuffer();

            shutdownScheduler(scraperScheduler, "scraper");
            shutdownScheduler(forwarderScheduler, "forwarder");

            log.info("Metrics collector stopped");
        }
    }

    private void shutdownScheduler(ScheduledExecutorService scheduler, String name) {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
                log.warn("{} scheduler did not terminate gracefully", name);
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Scrape all targets and add metrics to buffer.
     */
    private void scrape() {
        try {
            int scraped = scraper.scrapeAllTargets();
            if (scraped > 0) {
                log.debug("Scraped {} metrics from {} targets",
                    scraped, properties.getResolvedTargets().size());
            }
        } catch (Exception e) {
            log.error("Error during scrape cycle: {}", e.getMessage());
        }
    }

    /**
     * Check buffer and flush if threshold reached OR interval elapsed (tailing mode).
     */
    private void checkAndFlush() {
        if (!running.get()) {
            return;
        }

        if (buffer.isEmpty()) {
            return;
        }

        int bufferSize = buffer.getSize();
        long timeSinceLastFlush = System.currentTimeMillis() - lastFlushTime;

        // Flush if threshold reached OR interval elapsed
        boolean thresholdReached = bufferSize >= properties.getFlushThreshold();
        boolean intervalElapsed = timeSinceLastFlush >= properties.getFlushIntervalMs() && bufferSize > 0;

        if (thresholdReached || intervalElapsed) {
            if (thresholdReached) {
                log.debug("Flush triggered: buffer size {} reached threshold {}",
                    bufferSize, properties.getFlushThreshold());
            } else {
                log.debug("Flush triggered: interval {}ms elapsed with {} items",
                    timeSinceLastFlush, bufferSize);
            }
            flushBuffer();
        }
    }

    /**
     * Flush all metrics from buffer to remote server.
     */
    private void flushBuffer() {
        List<CollectedMetric> metrics = buffer.drain();

        if (metrics.isEmpty()) {
            return;
        }

        lastFlushTime = System.currentTimeMillis();

        boolean success = forwarder.sendMetrics(metrics);
        if (!success) {
            buffer.returnForRetry(metrics);
            log.warn("Forwarding failed, {} metrics returned to buffer", metrics.size());
        }
    }

    /**
     * Check if collector is running.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Get current buffer size.
     */
    public int getBufferSize() {
        return buffer.getSize();
    }

    /**
     * Get metrics sent count.
     */
    public long getMetricsSentCount() {
        return forwarder.getMetricsSentCount();
    }

    /**
     * Get metrics dropped count.
     */
    public long getMetricsDroppedCount() {
        return forwarder.getMetricsDroppedCount();
    }

    /**
     * Get the properties for this collector.
     */
    public CollectorProperties getProperties() {
        return properties;
    }

    /**
     * Get the buffer (for testing or advanced use).
     */
    public MetricsBuffer getBuffer() {
        return buffer;
    }

    /**
     * Get the scraper (for testing or advanced use).
     */
    public MetricsScraper getScraper() {
        return scraper;
    }

    /**
     * Get the forwarder (for testing or advanced use).
     */
    public MetricsForwarder getForwarder() {
        return forwarder;
    }
}
