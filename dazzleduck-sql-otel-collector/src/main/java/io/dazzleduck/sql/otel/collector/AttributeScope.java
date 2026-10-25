package io.dazzleduck.sql.otel.collector;

/**
 * Which attribute set of a record an attribute belongs to. One record carries up to four: its own
 * attributes, its resource's, and — for a span — those of each of its events and links. The same
 * name can mean different things in different scopes, so the attribute type registry keeps them
 * apart (see {@link AttributeTypeRegistry}).
 *
 * <p>{@link #wireName()} is what goes in the registry table's {@code attribute_scope} column.
 */
enum AttributeScope {
    ATTRIBUTE("attribute"),
    RESOURCE("resource"),
    EVENT("event"),
    LINK("link");

    private final String wireName;

    AttributeScope(String wireName) {
        this.wireName = wireName;
    }

    String wireName() {
        return wireName;
    }
}
