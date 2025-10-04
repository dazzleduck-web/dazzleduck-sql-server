package io.dazzleduck.sql.micrometer.config;

import io.micrometer.core.instrument.step.StepRegistryConfig;

import java.time.Duration;

public interface ArrowRegistryConfig extends StepRegistryConfig {

    @Override
    default String prefix() {
        return "arrow";
    }

    @Override
    default Duration step() {
        return Duration.ofSeconds(10);
    }

    default boolean enabled() {
        String v = get(prefix() + ".enabled");
        return v == null || Boolean.parseBoolean(v);
    }

    default String uri() {
        return get(prefix() + ".uri");
    }
}
