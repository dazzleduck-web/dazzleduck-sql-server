package io.dazzleduck.sql.micrometer.metrics;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.dazzleduck.sql.common.ingestion.FlightSender;
import io.dazzleduck.sql.micrometer.config.ArrowRegistryConfig;
import io.dazzleduck.sql.micrometer.service.ArrowMicroMeterRegistry;
import io.dazzleduck.sql.micrometer.service.MetricsFlightSender;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public final class MetricsRegistryFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsRegistryFactory.class);

    private static volatile FlightSender sender;

    public static MeterRegistry create() {

        // ------------------------------
        // Read configuration
        // ------------------------------
        Config dazzConfig = ConfigFactory.load().getConfig("dazzleduck_micrometer");

        String applicationId   = dazzConfig.getString("application_id");
        String applicationName = dazzConfig.getString("application_name");

        String host            = dazzConfig.getString("host");
        int grpcPort           = dazzConfig.getInt("port");
        String httpEndpoint    = dazzConfig.getString("http_endpoint");

        boolean httpEnabled    = dazzConfig.getBoolean("http.enabled");
        boolean grpcEnabled    = dazzConfig.getBoolean("grpc.enabled");

        LOG.info("Micrometer config loaded: http={}, grpcEnabled={}, grpcUrl=grpc://{}:{}",
                httpEndpoint, grpcEnabled, host, grpcPort);

        // ------------------------------
        // Build ArrowRegistryConfig
        // ------------------------------
        ArrowRegistryConfig regCfg = new ArrowRegistryConfig() {
            @Override
            public String get(String key) {
                return switch (key) {
                    case "arrow.enabled"  -> "true";
                    case "arrow.endpoint" -> httpEndpoint;
                    default -> null;
                };
            }

            @Override
            public String uri() {
                return httpEndpoint;
            }
        };

        // ------------------------------
        // Lazy init of the FlightSender
        // ------------------------------
        if (sender == null) {
            synchronized (MetricsRegistryFactory.class) {
                if (sender == null) {

                    MetricsFlightSender s = new MetricsFlightSender();

                    // Enable gRPC mode
                    if (grpcEnabled) {
                        s.enableGrpc(host, grpcPort);
                    }

                    // Enable HTTP mode
                    if (httpEnabled) {
                        s.enableHttp(httpEndpoint);
                    }
                    sender = s;

                    // Background queue worker thread
                    ((FlightSender.AbstractFlightSender) sender).start();

                    LOG.info("MetricsFlightSender thread started successfully");
                }
            }
        }

        ArrowMicroMeterRegistry arrow =
                new ArrowMicroMeterRegistry.Builder()
                        .config(regCfg)
                        .flightSender(sender)
                        .endpoint(httpEndpoint)
                        .httpTimeout(Duration.ofMinutes(2))
                        .applicationId(applicationId)
                        .applicationName(applicationName)
                        .host(host)
                        .clock(Clock.SYSTEM)
                        .build();

        LOG.info("Arrow MicroMeter Registry initialized");
        return arrow;
    }
}
