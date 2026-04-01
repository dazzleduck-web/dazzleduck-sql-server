package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.otel.collector.auth.JwtServerInterceptor;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of the OTLP gRPC server and signal writers for logs, traces, and metrics.
 */
public class OtelCollectorServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(OtelCollectorServer.class);

    private final CollectorProperties props;
    private Server grpcServer;
    private SignalWriter logWriter;
    private SignalWriter traceWriter;
    private SignalWriter metricsWriter;

    public OtelCollectorServer(CollectorProperties props) {
        this.props = props;
    }

    public void start() throws IOException {
        String startupScript = props.getStartupScript();
        if (startupScript != null && !startupScript.isBlank()) {
            log.info("Executing startup script");
            ConnectionPool.executeOnSingleton(startupScript);
        }

        logWriter     = new SignalWriter("logs",    props.getLogIngestionConfig(),    props.getLogIngestionTaskFactory());
        traceWriter   = new SignalWriter("traces",  props.getTraceIngestionConfig(),  props.getTraceIngestionTaskFactory());
        metricsWriter = new SignalWriter("metrics", props.getMetricIngestionConfig(), props.getMetricIngestionTaskFactory());

        setupCommonTags(props.getMeterRegistry(), props.getServiceName());
        OtelCollectorMetrics metrics = new OtelCollectorMetrics(props.getMeterRegistry(),
                props.getLogIngestionConfig().maxDelayMs(),
                props.getTraceIngestionConfig().maxDelayMs(),
                props.getMetricIngestionConfig().maxDelayMs());
        metrics.registerWriter("logs",    logWriter);
        metrics.registerWriter("traces",  traceWriter);
        metrics.registerWriter("metrics", metricsWriter);

        var builder = NettyServerBuilder
                .forPort(props.getGrpcPort())
                .addService(new OtelLogService(logWriter, metrics))
                .addService(new OtelTraceService(traceWriter, metrics))
                .addService(new OtelMetricsService(metricsWriter, metrics));

        if (!"jwt".equals(props.getAuthentication())) {
            throw new IllegalStateException("Unsupported authentication mode: " + props.getAuthentication() + ". Only 'jwt' is supported.");
        }
        if (props.getSecretKey() == null || props.getSecretKey().isEmpty()) {
            throw new IllegalStateException("otel_collector.secret_key is required");
        }
        var secretKey = Validator.fromBase64String(props.getSecretKey());
        var userHashMap = new java.util.HashMap<String, byte[]>();
        props.getUsers().forEach((u, p) -> userHashMap.put(u, Validator.hash(p)));
        builder.intercept(new JwtServerInterceptor(secretKey, userHashMap, props.getJwtExpiration(), props.getLoginUrl()));
        if (props.getLoginUrl() != null) {
            log.info("JWT authentication enabled with login delegation to {}", props.getLoginUrl());
        } else {
            log.info("JWT authentication enabled for {} user(s)", userHashMap.size());
        }

        grpcServer = builder.build().start();

        log.info("OTLP gRPC server started on port {} — logs={}, traces={}, metrics={}",
                props.getGrpcPort(),
                props.getLogIngestionConfig().outputPath(),
                props.getTraceIngestionConfig().outputPath(),
                props.getMetricIngestionConfig().outputPath());
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
        if (grpcServer != null) {
            log.info("Shutting down gRPC server...");
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(10, TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (logWriter != null) logWriter.close();
        if (traceWriter != null) traceWriter.close();
        if (metricsWriter != null) metricsWriter.close();
        log.info("OtelCollectorServer stopped.");
    }
}
