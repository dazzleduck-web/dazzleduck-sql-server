package io.dazzleduck.sql.micrometer.metrics;

import io.dazzleduck.sql.micrometer.config.ArrowRegistryConfig;
import io.dazzleduck.sql.micrometer.service.ArrowMicroMeterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.time.Duration;

public class MetricsRegistryFactory {

    public static MeterRegistry create() {

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
                        .httpTimeout(Duration.ofSeconds(20))
                        .clock(Clock.SYSTEM)
                        .build();

        CompositeMeterRegistry composite = new CompositeMeterRegistry();
        composite.add(arrow);

        return composite;
    }
}
