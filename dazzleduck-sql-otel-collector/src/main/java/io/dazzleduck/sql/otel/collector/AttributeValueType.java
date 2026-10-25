package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.AnyValue;

/**
 * The OTLP {@code AnyValue} kind an attribute arrived with, as the attribute type registry records
 * it. This is the kind of the <i>value</i>, not the column it lands in: {@link #ARRAY} and
 * {@link #KVLIST} are kept apart here although both are written to {@code attributes_json}, and
 * {@link #EMPTY} has no typed column at all. {@link AttributeType} is the column view of the same
 * routing.
 *
 * <p>{@link #wireName()} is what goes in the registry table's {@code value_type} column.
 */
enum AttributeValueType {
    STRING("string"),
    INT("int"),
    DOUBLE("double"),
    BOOL("bool"),
    BYTES("bytes"),
    ARRAY("array"),
    KVLIST("kvlist"),
    /**
     * An attribute whose value is not set. It has no typed column, so {@link AttributeMapsWriter}
     * keeps it in the string map as an empty string whatever {@code string_copy} says — and it is
     * recorded, rather than skipped, because a name that only ever arrives unset is exactly the
     * case a reader cannot otherwise explain.
     */
    EMPTY("empty");

    private final String wireName;

    AttributeValueType(String wireName) {
        this.wireName = wireName;
    }

    String wireName() {
        return wireName;
    }

    /** Indexed by {@code ValueCase.ordinal()}, so the lookup is an array read, not a switch. */
    private static final AttributeValueType[] BY_CASE = byCase();

    private static AttributeValueType[] byCase() {
        var cases = AnyValue.ValueCase.values();
        var byCase = new AttributeValueType[cases.length];
        for (AnyValue.ValueCase valueCase : cases) {
            byCase[valueCase.ordinal()] = switch (valueCase) {
                case STRING_VALUE -> STRING;
                case INT_VALUE -> INT;
                case DOUBLE_VALUE -> DOUBLE;
                case BOOL_VALUE -> BOOL;
                case BYTES_VALUE -> BYTES;
                case ARRAY_VALUE -> ARRAY;
                case KVLIST_VALUE -> KVLIST;
                case VALUE_NOT_SET -> EMPTY;
            };
        }
        return byCase;
    }

    /**
     * The type for one attribute's value kind. The table covers every case of this build's
     * {@code AnyValue}, so a case added by a later OTLP release fails the switch above at compile
     * time rather than being silently mapped.
     */
    static AttributeValueType of(AnyValue.ValueCase valueCase) {
        return BY_CASE[valueCase.ordinal()];
    }
}
