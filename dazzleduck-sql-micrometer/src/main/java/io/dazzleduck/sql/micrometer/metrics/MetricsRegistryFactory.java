package io.dazzleduck.sql.micrometer.metrics;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.HttpSender;
import io.dazzleduck.sql.micrometer.util.ArrowMetricSchema;
import io.dazzleduck.sql.micrometer.service.ArrowMicroMeterRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.time.Duration;

public final class MetricsRegistryFactory {

    private static final Config config = ConfigFactory.load().getConfig("dazzleduck_micrometer");

    private MetricsRegistryFactory() {}

    public static MeterRegistry create() {

        Config http = config.getConfig("http");

        HttpSender sender = new HttpSender(
                ArrowMetricSchema.SCHEMA,
                http.getString("base_url"),
                http.getString("username"),
                http.getString("password"),
                http.getString("target_path"),
                Duration.ofMillis(http.getLong("http_client_timeout_ms")),
                config.getLong("min_batch_size"),
                Duration.ofMillis(config.getLong("max_send_interval_ms")),
                config.getInt("retry_count"),
                config.getLong("retry_interval_ms"),
                config.getStringList("transformations"),
                config.getStringList("partition_by"),
                config.getLong("max_in_memory_bytes"),
                config.getLong("max_on_disk_bytes")
        );

        ArrowMicroMeterRegistry arrow = new ArrowMicroMeterRegistry(
                        sender,
                        Clock.SYSTEM,
                        Duration.ofMillis(config.getLong("max_send_interval_ms")),
                        config.getString("application_id"),
                        config.getString("application_name"),
                        config.getString("application_host")
                );

        CompositeMeterRegistry composite = new CompositeMeterRegistry();
        composite.add(arrow);
        return composite;
    }
}
