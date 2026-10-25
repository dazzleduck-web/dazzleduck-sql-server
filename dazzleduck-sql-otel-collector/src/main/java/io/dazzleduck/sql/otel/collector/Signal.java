package io.dazzleduck.sql.otel.collector;

/**
 * An OTLP signal, as the attribute type registry records it: one collector serves all three, and
 * each writes a different schema, so the same attribute name can carry different types in each.
 *
 * <p>{@link #wireName()} is what goes in the registry table's {@code signal} column.
 */
public enum Signal {
    LOGS("logs"),
    TRACES("traces"),
    METRICS("metrics");

    private final String wireName;

    Signal(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }
}
