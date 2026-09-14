package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.otel.collector.auth.JwtServerInterceptor;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.dazzleduck.sql.otel.collector.health.CollectorHealth;
import io.dazzleduck.sql.otel.collector.health.CollectorHealthStatus;
import io.dazzleduck.sql.otel.collector.health.HealthServer;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Comparator;
import java.util.ArrayList;
import java.nio.file.Files;
import io.dazzleduck.sql.common.ConfigConstants;
import java.net.InetAddress;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of the OTLP gRPC server.
 *
 * <p>Queues are not pre-created at startup. Each request is resolved straight through the
 * {@link IngestionHandler}, which is the single registry: it is the source of truth for which
 * queues exist ({@link IngestionHandler#getKnownQueues()}) and where each writes
 * ({@link IngestionHandler#getTargetPath(String)}), and it lazily creates / caches / evicts the
 * underlying {@code ParquetIngestionQueue} per ID. So queues added to or removed from a dynamic
 * registry (e.g. {@code DynamicDuckLakeIngestionTaskFactoryProvider}) are routable/rejected
 * without a restart. The three signal services share one {@link #flushScheduler} for time-based
 * flushes. Every request must carry the {@code x-dd-ingestion-queue} JWT claim — there is no
 * default fallback. JWT authentication is the only supported mode.
 */
public class OtelCollectorServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(OtelCollectorServer.class);

    private final CollectorProperties props;
    private Server grpcServer;
    private IngestionHandler handler;
    private ScheduledExecutorService flushScheduler;
    private OtelLogService logService;
    private OtelTraceService traceService;
    private OtelMetricsService metricsService;
    private OtelCollectorMetrics collectorMetrics;
    private CollectorHealth health;
    private HealthServer healthServer;
    // Scratch directories created by start(); tracked so close() can remove them even when
    // startup fails after creating some but before the corresponding service exists.
    private final List<Path> scratchDirs = new ArrayList<>();
    private boolean started = false;
    private boolean closed = false;

    public OtelCollectorServer(CollectorProperties props) {
        this.props = props;
    }

    public void start() throws IOException {
        try {
            // Resolved once, before anything is allocated: this validates and creates operator
            // config, which is a startup concern rather than a per-service one. Doing it here
            // means a bad temp_write_location fails before the meter registry, health reporter
            // and flush scheduler are built, and the three services share one checked directory
            // instead of each re-creating and re-validating it.
            Path tempWriteDir = ConfigConstants.getTempWriteDir(props.getTempWriteLocation());

            handler = props.getIngestionHandler();
            var ingestionConfig = props.getIngestionConfig();

            // No startup seeding: the handler is the single registry. Queues are created lazily on
            // first use of a known queue and evicted when the handler stops reporting them.
            // All queues share one flush scheduler; an evicted queue's stray flush is a no-op.
            setupCommonTags(props.getMeterRegistry(), props.getServiceName());
            collectorMetrics = new OtelCollectorMetrics(props.getMeterRegistry());
            health = new CollectorHealth(
                    () -> handler.getKnownQueues().size(),
                    // Sourced from collectorMetrics, not handler.getQueueStats(): the latter only
                    // sums currently-cached queues, so it drops a queue's contribution the instant
                    // it's evicted or the cache is cleared on drain — wrong for a counter queried
                    // right up until the collector reports DOWN. collectorMetrics' total is a plain
                    // LongAdder bumped once per successfully written batch, independent of queue
                    // lifecycle.
                    collectorMetrics::getTotalBatchesProcessed);
            flushScheduler = Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "otel-flush-scheduler");
                t.setDaemon(true);
                return t;
            });

            // Directories are created here rather than in the service constructors: constructor
            // I/O forced an IOException on every caller and left a window where the allocator
            // existed but could never be closed if creation failed.
            logService     = new OtelLogService(
                    createScratchDir(tempWriteDir, OtelLogService.SCRATCH_PREFIX),
                    handler, ingestionConfig, flushScheduler, collectorMetrics);
            traceService   = new OtelTraceService(
                    createScratchDir(tempWriteDir, OtelTraceService.SCRATCH_PREFIX),
                    handler, ingestionConfig, flushScheduler, collectorMetrics);
            metricsService = new OtelMetricsService(
                    createScratchDir(tempWriteDir, OtelMetricsService.SCRATCH_PREFIX),
                    handler, ingestionConfig, flushScheduler, collectorMetrics);

            if (!"jwt".equals(props.getAuthentication())) {
                throw new IllegalStateException(
                        "Unsupported authentication mode: '" + props.getAuthentication() + "'. Only 'jwt' is supported.");
            }
            if (props.getSecretKey() == null || props.getSecretKey().isEmpty()) {
                throw new IllegalStateException("otel_collector.secret_key is required");
            }
            var secretKey = Validator.fromBase64String(props.getSecretKey());
            var userHashMap = new java.util.HashMap<String, byte[]>();
            props.getUsers().forEach((u, p) -> userHashMap.put(u, Validator.hash(p)));

            var builder = NettyServerBuilder
                    .forPort(props.getGrpcPort())
                    .addService(logService)
                    .addService(traceService)
                    .addService(metricsService);
            builder.intercept(new JwtServerInterceptor(secretKey, userHashMap,
                    props.getJwtExpiration(), props.getLoginUrl(), props.isVerifySignature()));

            if (props.getLoginUrl() != null) {
                log.info("JWT authentication enabled with login delegation to {}", props.getLoginUrl());
            } else {
                log.info("JWT authentication enabled for {} user(s)", userHashMap.size());
            }

            grpcServer = builder.build().start();

            healthServer = new HealthServer(props.getHealthPort(), health, props.getGrpcPort());
            healthServer.start();
            health.transitionTo(CollectorHealthStatus.HEALTHY);
            started = true;

            log.info("OTLP gRPC server started on port {} — known queues: {} (writers created lazily on first use)",
                    props.getGrpcPort(), handler.getKnownQueues());
        } catch (Exception e) {
            close();
            if (e instanceof IOException ioe) throw ioe;
            if (e instanceof RuntimeException re) throw re;
            throw new IOException("Server startup failed", e);
        }
    }

    /**
     * Creates one service's private scratch directory under the validated {@code tempWriteDir} and
     * records it for cleanup. Package-private so the naming can be tested without a running
     * server.
     */
    Path createScratchDir(Path tempWriteDir, String prefix) throws IOException {
        Path dir = Files.createTempDirectory(tempWriteDir, prefix);
        scratchDirs.add(dir);
        log.info("Arrow scratch directory: {}", dir);
        return dir;
    }

    /** Removes a scratch directory and anything still staged in it. Best-effort, never throws. */
    private static void deleteScratchDir(Path dir) {
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                  .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
        } catch (IOException e) {
            log.warn("Could not remove scratch directory {}", dir, e);
        }
    }

    private static void setupCommonTags(MeterRegistry registry, String serviceName) {
        String hostname = System.getenv("HOSTNAME");
        if (hostname == null || hostname.isBlank()) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                hostname = "unknown";
            }
        }
        String containerId = System.getenv().getOrDefault("CONTAINER_ID", "unknown");
        registry.config().commonTags(
                "service.name", serviceName,
                "host.name", hostname,
                "container.id", containerId);
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    @Override
    public void close() {
        // Idempotent: a JVM shutdown hook and an explicit close() can race (and tests close twice),
        // and re-running would re-enter MAINTENANCE and sleep the grace period again. First caller wins.
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        // Enter MAINTENANCE and give readiness probes/load balancers a grace period to notice the
        // 503 and stop routing new traffic before the gRPC server actually stops accepting calls.
        // Skipped on a failed-startup cleanup path (healthServer was never started).
        if (started) {
            health.transitionTo(CollectorHealthStatus.MAINTENANCE);
            sleepForGracePeriod();
        }
        if (grpcServer != null) {
            log.info("Shutting down gRPC server...");
            grpcServer.shutdown();
            // Await termination with the flush scheduler still running: an already-accepted call
            // only gets its response once its batch is written, and the time-based flush
            // (max_delay) fires within this window, so in-flight calls can complete normally
            // before we force-cancel anything past the timeout.
            try {
                if (!grpcServer.awaitTermination(10, TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        // Flush and close all queues the handler owns, then stop the shared flush scheduler. The
        // drain bound (IngestionHandler.DEFAULT_DRAIN_TIMEOUT) is distinct from the LB-drain
        // shutdownGracePeriod: it bounds the actual data flush and returns as soon as writes
        // complete, so it only limits how long a stalled write backend can delay shutdown.
        if (handler != null) {
            try { handler.closeQueues(IngestionHandler.DEFAULT_DRAIN_TIMEOUT); } catch (Exception e) { log.warn("Error closing ingestion queues", e); }
        }
        if (flushScheduler != null) {
            flushScheduler.shutdownNow();
        }
        closeQuietly("logService",       logService);
        closeQuietly("traceService",     traceService);
        closeQuietly("metricsService",   metricsService);
        // After the services are closed, so nothing is still staging batches into them.
        scratchDirs.forEach(OtelCollectorServer::deleteScratchDir);
        scratchDirs.clear();
        closeQuietly("collectorMetrics", collectorMetrics);
        if (started) {
            health.transitionTo(CollectorHealthStatus.DOWN);
        }
        closeQuietly("healthServer", healthServer);
        log.info("OtelCollectorServer stopped.");
    }

    private void sleepForGracePeriod() {
        // Small, bounded LB-drain window: stay in MAINTENANCE (503) long enough for readiness probes
        // to stop routing before the gRPC server stops. Independent of max_delay — in-flight batches
        // are flushed by the awaitTermination window in close(). ZERO skips the wait (e.g. tests).
        Duration grace = props.getShutdownGracePeriod();
        long graceMs = grace == null ? 0 : grace.toMillis();
        if (graceMs <= 0) {
            return;
        }
        log.info("Entering MAINTENANCE — draining for up to {}ms before shutdown", graceMs);
        try {
            Thread.sleep(graceMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Current health status; {@code MAINTENANCE} before {@link #start()} completes successfully. */
    public CollectorHealthStatus getHealthStatus() {
        return health == null ? CollectorHealthStatus.MAINTENANCE : health.getStatus();
    }

    private void closeQuietly(String name, Closeable c) {
        if (c != null) {
            try { c.close(); } catch (Exception e) { log.warn("Error closing {}", name, e); }
        }
    }
}
