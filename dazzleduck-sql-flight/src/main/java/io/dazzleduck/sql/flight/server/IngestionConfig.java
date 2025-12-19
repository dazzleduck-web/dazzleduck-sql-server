package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;

import java.time.Duration;

public record IngestionConfig(long minBucketSize, Duration maxDelay) {

    public static IngestionConfig fromConfig(Config config){
        return new IngestionConfig(config.getLong("min_bucket_size"),
                Duration.ofMillis(config.getLong("max_delay_ms")));
    }
}
