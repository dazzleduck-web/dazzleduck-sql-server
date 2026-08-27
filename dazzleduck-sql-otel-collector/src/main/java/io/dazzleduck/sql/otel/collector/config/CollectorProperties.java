package io.dazzleduck.sql.otel.collector.config;

import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.NOOPIngestionTaskFactoryProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class CollectorProperties {

    private int grpcPort = 4317;
    private int healthPort = 8081;
    // Absolute time to stay in MAINTENANCE (serving 503s so the load balancer stops routing) before
    // stopping the gRPC server. Deliberately a small, bounded LB-drain window independent of the
    // ingestion max_delay — in-flight batches are flushed by the awaitTermination window in close(),
    // not by this sleep. Set to ZERO to skip the wait entirely (e.g. in tests).
    private Duration shutdownGracePeriod = Duration.ofSeconds(2);
    private IngestionHandler ingestionHandler =
            new NOOPIngestionTaskFactoryProvider("./otel-output").getIngestionHandler();
    private IngestionConfig ingestionConfig = new IngestionConfig(
            1_048_576L, IngestionConfig.DEFAULT_MAX_BUCKET_SIZE, IngestionConfig.DEFAULT_MAX_BATCHES,
            IngestionConfig.DEFAULT_MAX_PENDING_WRITE, Duration.ofSeconds(5),
            IngestionConfig.DEFAULT_CONFIG_REFRESH);
    private String startupScript = "INSTALL arrow FROM community; LOAD arrow;";
    private String serviceName = "open-telemetry-collector";
    // "jwt" is the only supported authentication mode.
    private String authentication = "jwt";
    private String secretKey = null;
    private String loginUrl = null;
    // Cluster this collector serves; null disables the x-dd-cluster claim check.
    private String cluster = null;
    private Map<String, String> users = new HashMap<>();
    private Duration jwtExpiration = Duration.ofHours(1);
    private MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private boolean verifySignature = true;

    public int getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(int grpcPort) {
        this.grpcPort = grpcPort;
    }

    public int getHealthPort() {
        return healthPort;
    }

    public void setHealthPort(int healthPort) {
        this.healthPort = healthPort;
    }

    public Duration getShutdownGracePeriod() {
        return shutdownGracePeriod;
    }

    public void setShutdownGracePeriod(Duration shutdownGracePeriod) {
        this.shutdownGracePeriod = shutdownGracePeriod;
    }

    public IngestionHandler getIngestionHandler() {
        return ingestionHandler;
    }

    public void setIngestionHandler(IngestionHandler ingestionHandler) {
        this.ingestionHandler = ingestionHandler;
    }

    public IngestionConfig getIngestionConfig() {
        return ingestionConfig;
    }

    public void setIngestionConfig(IngestionConfig ingestionConfig) {
        this.ingestionConfig = ingestionConfig;
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

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
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

    public boolean isVerifySignature() {
        return verifySignature;
    }

    public void setVerifySignature(boolean verifySignature) {
        this.verifySignature = verifySignature;
    }
}
