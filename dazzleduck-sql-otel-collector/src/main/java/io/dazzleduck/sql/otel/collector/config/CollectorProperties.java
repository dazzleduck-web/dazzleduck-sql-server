package io.dazzleduck.sql.otel.collector.config;

import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.NOOPIngestionTaskFactoryProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectorProperties {

    private int grpcPort = 4317;
    private SignalIngestionConfig logIngestionConfig =
            new SignalIngestionConfig("./otel-logs", List.of(), null, 1_048_576L, 5000L);
    private SignalIngestionConfig traceIngestionConfig =
            new SignalIngestionConfig("./otel-traces", List.of(), null, 1_048_576L, 5000L);
    private SignalIngestionConfig metricIngestionConfig =
            new SignalIngestionConfig("./otel-metrics", List.of(), null, 1_048_576L, 5000L);
    private IngestionHandler logIngestionHandler =
            new NOOPIngestionTaskFactoryProvider("./otel-logs").getIngestionTaskFactory();
    private IngestionHandler traceIngestionHandler =
            new NOOPIngestionTaskFactoryProvider("./otel-traces").getIngestionTaskFactory();
    private IngestionHandler metricIngestionHandler =
            new NOOPIngestionTaskFactoryProvider("./otel-metrics").getIngestionTaskFactory();
    private String startupScript = "INSTALL arrow FROM community; LOAD arrow;";
    private String serviceName = "open-telemetry-collector";
    private String authentication = "jwt";
    private String secretKey = null;
    private String loginUrl = null;
    private Map<String, String> users = new HashMap<>();
    private Duration jwtExpiration = Duration.ofHours(1);
    private MeterRegistry meterRegistry = new SimpleMeterRegistry();

    public int getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(int grpcPort) {
        this.grpcPort = grpcPort;
    }

    public SignalIngestionConfig getLogIngestionConfig() {
        return logIngestionConfig;
    }

    public void setLogIngestionConfig(SignalIngestionConfig logIngestionConfig) {
        this.logIngestionConfig = logIngestionConfig;
    }

    public SignalIngestionConfig getTraceIngestionConfig() {
        return traceIngestionConfig;
    }

    public void setTraceIngestionConfig(SignalIngestionConfig traceIngestionConfig) {
        this.traceIngestionConfig = traceIngestionConfig;
    }

    public SignalIngestionConfig getMetricIngestionConfig() {
        return metricIngestionConfig;
    }

    public void setMetricIngestionConfig(SignalIngestionConfig metricIngestionConfig) {
        this.metricIngestionConfig = metricIngestionConfig;
    }

    public IngestionHandler getLogIngestionTaskFactory() {
        return logIngestionHandler;
    }

    public void setLogIngestionTaskFactory(IngestionHandler logIngestionHandler) {
        this.logIngestionHandler = logIngestionHandler;
    }

    public IngestionHandler getTraceIngestionTaskFactory() {
        return traceIngestionHandler;
    }

    public void setTraceIngestionTaskFactory(IngestionHandler traceIngestionHandler) {
        this.traceIngestionHandler = traceIngestionHandler;
    }

    public IngestionHandler getMetricIngestionTaskFactory() {
        return metricIngestionHandler;
    }

    public void setMetricIngestionTaskFactory(IngestionHandler metricIngestionHandler) {
        this.metricIngestionHandler = metricIngestionHandler;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getAuthentication() {
        return authentication;
    }

    public void setAuthentication(String authentication) {
        this.authentication = authentication;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getLoginUrl() {
        return loginUrl;
    }

    public void setLoginUrl(String loginUrl) {
        this.loginUrl = loginUrl;
    }

    public Map<String, String> getUsers() {
        return users;
    }

    public void setUsers(Map<String, String> users) {
        this.users = users;
    }

    public Duration getJwtExpiration() {
        return jwtExpiration;
    }

    public void setJwtExpiration(Duration jwtExpiration) {
        this.jwtExpiration = jwtExpiration;
    }

    public String getStartupScript() {
        return startupScript;
    }

    public void setStartupScript(String startupScript) {
        this.startupScript = startupScript;
    }

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public void setMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
}
