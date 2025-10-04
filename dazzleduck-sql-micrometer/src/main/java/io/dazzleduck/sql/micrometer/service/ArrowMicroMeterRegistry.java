package io.dazzleduck.sql.micrometer.service;

import io.dazzleduck.sql.micrometer.config.ArrowRegistryConfig;
import io.dazzleduck.sql.micrometer.server.ArrowReceiverServer;
import io.dazzleduck.sql.micrometer.util.ArrowFileWriterUtil;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ArrowMicroMeterRegistry extends StepMeterRegistry {
    private static final Logger log = LoggerFactory.getLogger(ArrowMicroMeterRegistry.class);
    private final ArrowRegistryConfig arrowConfig;
    private final String endpoint;
    private final java.net.http.HttpClient httpClient;
    private final Duration httpTimeout;
    private final boolean testMode;

    private ArrowMicroMeterRegistry(
            ArrowRegistryConfig config,
            Clock clock,
            ThreadFactory threadFactory,
            java.net.http.HttpClient httpClient,
            String endpoint,
            Duration httpTimeout,
            boolean testMode) {
        super(config, clock);
        this.arrowConfig = config;
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.httpClient = (httpClient != null) ? httpClient : java.net.http.HttpClient.newHttpClient();
        this.httpTimeout = (httpTimeout != null) ? httpTimeout : Duration.ofSeconds(10);
        this.testMode = testMode;

        this.config().namingConvention(NamingConvention.dot);

        if (!testMode) {
            start(threadFactory); // only start publisher thread in production
        }
    }

    @Override
    protected void publish() {
        if (!arrowConfig.enabled()) return;

        try {
            byte[] payload = ArrowFileWriterUtil.convertMetersToArrowBytes(new ArrayList<>(getMeters()));

            String outputPath = arrowConfig.get("arrow.outputFile");
            if (outputPath != null && !outputPath.isEmpty()) {
                ArrowFileWriterUtil.writeMetersToFile(new ArrayList<>(getMeters()), outputPath);
                log.info("Arrow metrics written to file: {}", outputPath);
            }

            ArrowReceiverServer receiver = new ArrowReceiverServer(8080);
            receiver.start();
            int status = ArrowHttpPoster.postBytes(httpClient, payload, endpoint, httpTimeout);
            log.info("Published {} meters to {} (HTTP {})", getMeters().size(), endpoint, status);
            receiver.stop(0);
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
        private Duration httpTimeout = Duration.ofSeconds(10);
        private boolean testMode = false;

        public Builder config(ArrowRegistryConfig cfg) { this.config = cfg; return this; }
        public Builder clock(Clock c) { this.clock = c; return this; }
        public Builder threadFactory(ThreadFactory tf) { this.threadFactory = tf; return this; }
        public Builder httpClient(java.net.http.HttpClient client) { this.httpClient = client; return this; }
        public Builder endpoint(String endpoint) { this.endpoint = endpoint; return this; }
        public Builder httpTimeout(Duration timeout) { this.httpTimeout = timeout; return this; }
        public Builder testMode(boolean t) { this.testMode = t; return this; }

        public ArrowMicroMeterRegistry build() {
            if (endpoint == null) throw new IllegalStateException("endpoint is required");
            return new ArrowMicroMeterRegistry(config, clock, threadFactory, httpClient, endpoint, httpTimeout, testMode);
        }
    }
}
