package io.dazzleduck.sql.micrometer.service;

import io.dazzleduck.sql.micrometer.config.ArrowRegistryConfig;
import io.dazzleduck.sql.micrometer.server.ArrowReceiverServer;
import io.dazzleduck.sql.micrometer.util.ArrowFileWriterUtil;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Custom Micrometer registry that exports metrics to Arrow format,
 * optionally writes to file, and posts over HTTP.
 */
public class ArrowMicroMeterRegistry extends StepMeterRegistry {
    private static final Logger log = LoggerFactory.getLogger(ArrowMicroMeterRegistry.class);

    private final ArrowRegistryConfig arrowConfig;
    private final String endpoint;
    private final java.net.http.HttpClient httpClient;
    private final Duration httpTimeout;
    private final boolean testMode;
    private final String outputPath;
    private final ArrowReceiverServer receiver;

    private ArrowMicroMeterRegistry(
            ArrowRegistryConfig config,
            Clock clock,
            ThreadFactory threadFactory,
            HttpClient httpClient,
            String endpoint,
            Duration httpTimeout,
            String outputPath,
            boolean testMode,
            ArrowReceiverServer receiver
    ) {
        super(config, clock);
        this.arrowConfig = Objects.requireNonNull(config, "config");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.httpClient = (httpClient != null) ? httpClient : HttpClient.newHttpClient();
        this.httpTimeout = (httpTimeout != null) ? httpTimeout : Duration.ofSeconds(20);
        this.outputPath = outputPath;
        this.testMode = testMode;
        this.receiver = receiver;

        this.config().namingConvention(NamingConvention.dot);

        if (!testMode) {
            start(threadFactory);
        }
    }

    @Override
    protected void publish() {
        if (!arrowConfig.enabled()) return;

        List<Meter> meters = new ArrayList<>(getMeters());
        if (meters.isEmpty()) {
            log.debug("No meters to publish");
            return;
        }

        try {
            byte[] payload = ArrowFileWriterUtil.convertMetersToArrowBytes(meters);

            // Write to file if configured
            if (outputPath != null && !outputPath.isBlank()) {
                ArrowFileWriterUtil.writeMetersToFile(meters, outputPath);
                log.info("Wrote {} meters to Arrow file: {}", meters.size(), outputPath);
            }

            // Optionally post to HTTP endpoint
            if (endpoint != null && !endpoint.isBlank()) {
                if (receiver != null && !testMode) {
                    receiver.start();
                }

                int status = ArrowHttpPoster.postBytes(httpClient, payload, endpoint, httpTimeout);
                log.info("Published {} meters to {} (HTTP {})", meters.size(), endpoint, status);

                if (receiver != null && !testMode) {
                    receiver.stop(0);
                }
            }

        } catch (Exception e) {
            log.error("Error publishing Arrow metrics", e);
        }
    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.SECONDS;
    }

    // -------- Builder ----------
    public static class Builder {
        private ArrowRegistryConfig config = (k) -> null;
        private Clock clock = Clock.SYSTEM;
        private ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "arrow-metrics-publisher");
            t.setDaemon(true);
            return t;
        };
        private java.net.http.HttpClient httpClient;
        private String endpoint;
        private Duration httpTimeout = Duration.ofSeconds(20);
        private String outputPath;
        private boolean testMode = false;
        private ArrowReceiverServer receiver;

        public Builder config(ArrowRegistryConfig cfg) { this.config = cfg; return this; }
        public Builder clock(Clock c) { this.clock = c; return this; }
        public Builder threadFactory(ThreadFactory tf) { this.threadFactory = tf; return this; }
        public Builder httpClient(java.net.http.HttpClient client) { this.httpClient = client; return this; }
        public Builder endpoint(String endpoint) { this.endpoint = endpoint; return this; }
        public Builder httpTimeout(Duration timeout) { this.httpTimeout = timeout; return this; }
        public Builder outputPath(String path) { this.outputPath = path; return this; }
        public Builder testMode(boolean t) { this.testMode = t; return this; }
        public Builder receiver(ArrowReceiverServer receiver) { this.receiver = receiver; return this; }

        public ArrowMicroMeterRegistry build() {
            if (endpoint == null) throw new IllegalStateException("endpoint is required");
            return new ArrowMicroMeterRegistry(
                    config, clock, threadFactory, httpClient,
                    endpoint, httpTimeout, outputPath, testMode, receiver
            );
        }
    }
}
