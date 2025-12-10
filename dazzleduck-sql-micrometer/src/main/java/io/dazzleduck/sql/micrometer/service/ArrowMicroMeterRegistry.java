package io.dazzleduck.sql.micrometer.service;

import io.dazzleduck.sql.common.ingestion.FlightSender;
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
    private final String applicationId;
    private final String applicationName;
    private final String host;
    private final FlightSender flightSender;

    private ArrowMicroMeterRegistry(
            ArrowRegistryConfig config,
            Clock clock,
            ThreadFactory threadFactory,
            HttpClient httpClient,
            String endpoint,
            Duration httpTimeout,
            String outputPath,
            boolean testMode,
            ArrowReceiverServer receiver,
            String applicationId,
            String applicationName,
            String host,
            FlightSender flightSender
    ) {
        super(config, clock);
        this.arrowConfig = Objects.requireNonNull(config, "config");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.httpClient = (httpClient != null) ? httpClient : HttpClient.newHttpClient();
        this.httpTimeout = (httpTimeout != null) ? httpTimeout : Duration.ofSeconds(20);
        this.outputPath = outputPath;
        this.testMode = testMode;
        this.receiver = receiver;
        this.applicationId = applicationId;
        this.applicationName = applicationName;
        this.host = host;
        this.flightSender = flightSender;
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
            byte[] payload = ArrowFileWriterUtil.convertMetersToArrowBytes(meters, applicationId, applicationName, host);

            // Write to file if configured
            if (outputPath != null && !outputPath.isBlank()) {
                ArrowFileWriterUtil.writeMetersToFile(meters, outputPath, applicationId, applicationName, host);
                log.info("Wrote {} meters to Arrow file: {}", meters.size(), outputPath);
            }

            if (flightSender != null) {
                flightSender.enqueue(payload);
                log.info("Published {} meters async via FlightSender → {}", meters.size(), endpoint);
                return;
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
        private String applicationId;
        private String applicationName;
        private String host;

        public Builder config(ArrowRegistryConfig cfg) { this.config = cfg; return this; }
        public Builder clock(Clock c) { this.clock = c; return this; }
        public Builder threadFactory(ThreadFactory tf) { this.threadFactory = tf; return this; }
        public Builder httpClient(java.net.http.HttpClient client) { this.httpClient = client; return this; }
        public Builder endpoint(String endpoint) { this.endpoint = endpoint; return this; }
        public Builder httpTimeout(Duration timeout) { this.httpTimeout = timeout; return this; }
        public Builder outputPath(String path) { this.outputPath = path; return this; }
        public Builder testMode(boolean t) { this.testMode = t; return this; }
        public Builder receiver(ArrowReceiverServer receiver) { this.receiver = receiver; return this; }
        public Builder applicationId(String id) { this.applicationId = id; return this; }
        public Builder applicationName(String name) { this.applicationName = name; return this; }
        public Builder host(String host) { this.host = host; return this; }
        private FlightSender flightSender;

        public Builder flightSender(FlightSender fs) {
            this.flightSender = fs;
            return this;
        }

        public ArrowMicroMeterRegistry build() {
            if (endpoint == null)
                throw new IllegalStateException("endpoint is required");

            if (flightSender != null) {
                flightSender.setIngestEndpoint(endpoint);
            }

            return new ArrowMicroMeterRegistry(
                    config, clock, threadFactory, httpClient,
                    endpoint, httpTimeout, outputPath, testMode,
                    receiver, applicationId, applicationName, host, flightSender
            );
        }
    }
}
