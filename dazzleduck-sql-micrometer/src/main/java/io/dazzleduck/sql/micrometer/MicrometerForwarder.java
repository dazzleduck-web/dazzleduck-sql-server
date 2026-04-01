package io.dazzleduck.sql.micrometer;

import io.dazzleduck.sql.client.HttpArrowProducer;
import io.dazzleduck.sql.micrometer.config.MicrometerForwarderConfig;
import io.dazzleduck.sql.micrometer.service.ArrowMicroMeterRegistry;
import io.dazzleduck.sql.micrometer.util.ArrowMetricSchema;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Forwards Micrometer metrics to a remote server via HTTP using Arrow format.
 * This is the main entry point for using the micrometer forwarder library.
 *
 * <p>Usage example:</p>
 * <pre>{@code
 * MicrometerForwarderConfig config = MicrometerForwarderConfig.builder()
 *     .baseUrl("http://localhost:8081")
 *     .username("admin")
 *     .password("admin")
 *     .ingestionQueue("metrics")
 *     .build();
 *
 * MicrometerForwarder forwarder = MicrometerForwarder.createAndStart(config);
 *
 * // Use the registry to record metrics
 * MeterRegistry registry = forwarder.getRegistry();
 * Counter counter = registry.counter("my.counter");
 * counter.increment();
 *
 * // When done, close the forwarder
 * forwarder.close();
 * }</pre>
 */
public final class MicrometerForwarder implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(MicrometerForwarder.class);

    private final MicrometerForwarderConfig config;
    private final Clock clock;
    private final CompositeMeterRegistry registry;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Created lazily in start()
    private HttpArrowProducer httpProducer;
    private ArrowMicroMeterRegistry arrowRegistry;

    /**
     * Create a MicrometerForwarder with the given configuration.
     *
     * @param config Configuration for the forwarder
     */
    public MicrometerForwarder(MicrometerForwarderConfig config) {
        this(config, Clock.SYSTEM);
    }

    /**
     * Create a MicrometerForwarder with the given configuration and clock.
     * Useful for testing with a mock clock.
     *
     * @param config Configuration for the forwarder
     * @param clock Clock to use for timing
     */
    public MicrometerForwarder(MicrometerForwarderConfig config, Clock clock) {
        this.config = config;
        this.clock = clock;

        // Create composite registry (ArrowMicroMeterRegistry will be added in start())
        this.registry = new CompositeMeterRegistry();

        logger.info("MicrometerForwarder initialized with baseUrl={}, ingestionQueue={}, stepInterval={}",
                config.baseUrl(), config.ingestionQueue(), config.stepInterval());
    }

    /**
     * Start the metric forwarding process.
     * Metrics will be published at the configured step interval.
     */
    public void start() {
        if (!config.enabled()) {
            logger.info("MicrometerForwarder is disabled, not starting");
            return;
        }

        if (closed.get()) {
            throw new IllegalStateException("MicrometerForwarder has been closed");
        }

        if (started.compareAndSet(false, true)) {
            // Create HttpArrowProducer
            this.httpProducer = new HttpArrowProducer(
                    ArrowMetricSchema.SCHEMA,
                    config.baseUrl(),
                    config.username(),
                    config.password(),
                    config.claims(),
                    config.ingestionQueue(),
                    config.httpClientTimeout(),
                    config.minBatchSize(),
                    config.maxBatchSize(),
                    config.maxSendInterval(),
                    config.retryCount(),
                    config.retryIntervalMillis(),
                    config.partitionBy(),
                    config.maxInMemorySize(),
                    config.maxOnDiskSize(),
                    java.time.Clock.systemUTC()
            );

            // Create ArrowMicroMeterRegistry
            this.arrowRegistry = new ArrowMicroMeterRegistry(
                    httpProducer,
                    clock,
                    config.stepInterval()
            );

            // Add to composite registry
            this.registry.add(arrowRegistry);

            // Bind system metrics if configured
            if (config.publishSystemMetrics()) {
                new ProcessorMetrics().bindTo(this.registry);
                logger.info("Bound CPU metrics (system.cpu.*) to registry");

                new JvmMemoryMetrics().bindTo(this.registry);
                logger.info("Bound JVM memory metrics (jvm.memory.*) to registry");

                new JvmGcMetrics().bindTo(this.registry);
                logger.info("Bound JVM GC metrics (jvm.gc.*) to registry");
            }

            arrowRegistry.start(runnable -> {
                Thread thread = new Thread(runnable, "micrometer-forwarder");
                thread.setDaemon(true);
                return thread;
            });

            logger.info("MicrometerForwarder started");
        }
    }

    /**
     * Check if the forwarder is currently running.
     */
    public boolean isRunning() {
        return started.get() && !closed.get();
    }

    /**
     * Check if the forwarder is enabled.
     */
    public boolean isEnabled() {
        return config.enabled();
    }

    /**
     * Get the configuration.
     */
    public MicrometerForwarderConfig getConfig() {
        return config;
    }

    /**
     * Get the meter registry.
     */
    public CompositeMeterRegistry getRegistry() {
        return registry;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing MicrometerForwarder...");

            if (arrowRegistry != null) {
                try {
                    arrowRegistry.close();
                } catch (Exception e) {
                    logger.error("Error closing ArrowMicroMeterRegistry", e);
                }
            }

            if (httpProducer != null) {
                try {
                    httpProducer.close();
                } catch (Exception e) {
                    logger.error("Error closing HttpArrowProducer", e);
                }
            }

            logger.info("MicrometerForwarder closed");
        }
    }

    /**
     * Create and start a MicrometerForwarder with the given configuration.
     * Convenience method for simple usage.
     *
     * @param config Configuration for the forwarder
     * @return A started MicrometerForwarder instance
     */
    public static MicrometerForwarder createAndStart(MicrometerForwarderConfig config) {
        MicrometerForwarder forwarder = new MicrometerForwarder(config);
        forwarder.start();
        return forwarder;
    }
}
