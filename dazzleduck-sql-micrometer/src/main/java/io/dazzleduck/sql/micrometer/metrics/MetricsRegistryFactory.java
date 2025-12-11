package io.dazzleduck.sql.micrometer.metrics;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.micrometer.config.ArrowRegistryConfig;
import io.dazzleduck.sql.micrometer.service.ArrowMicroMeterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.time.Duration;

public class MetricsRegistryFactory {

    public static MeterRegistry create() {
        Config dazzConfig = ConfigFactory.load().getConfig("dazzleduck_micrometer");

        String applicationId = dazzConfig.getString("application_id");
        String applicationName = dazzConfig.getString("application_name");
        String host = dazzConfig.getString("host");
        ArrowRegistryConfig config = new ArrowRegistryConfig() {
            @Override
            public String get(String key) {
                return switch (key) {
                    case "arrow.enabled"     -> "true";
                    case "arrow.endpoint"    -> "http://localhost:8080/ingest?path=metrics";
                    default -> null;
                };
            }
        };

        ArrowMicroMeterRegistry arrow =
                new ArrowMicroMeterRegistry.Builder()
                        .config(config)
                        .endpoint(config.uri())
                        .httpTimeout(Duration.ofMinutes(2))
                        .applicationId(applicationId)
                        .applicationName(applicationName)
                        .host(host)
                        .clock(Clock.SYSTEM)
                        .build();

        CompositeMeterRegistry composite = new CompositeMeterRegistry();
        composite.add(arrow);
        return composite;
    }
}
