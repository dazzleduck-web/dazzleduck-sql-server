package io.dazzleduck.sql.otel.collector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link AttributeTypeCache} holds the same information as a set of mappings would, in a shape that
 * survives a process running thousands of streams. These tests pin down both halves of that: that
 * it answers exactly as a set would, and that what it stores per mapping is what the design claims.
 */
public class AttributeTypeCacheTest {

    private static AttributeTypeRegistry.Key key(String queue, Signal signal, AttributeScope scope,
                                                 String name, AttributeValueType type) {
        return new AttributeTypeRegistry.Key(queue, signal, scope, name, type);
    }

    private static AttributeTypeRegistry.Key logKey(String name, AttributeValueType type) {
        return key("logs", Signal.LOGS, AttributeScope.ATTRIBUTE, name, type);
    }

    @Test
    void aMappingIsAddedOnceAndThenFound() {
        var cache = new AttributeTypeCache();
        assertFalse(cache.contains(logKey("x", AttributeValueType.STRING)));
        assertTrue(cache.add(logKey("x", AttributeValueType.STRING)), "added");
        assertTrue(cache.contains(logKey("x", AttributeValueType.STRING)));
        assertFalse(cache.add(logKey("x", AttributeValueType.STRING)), "already there");
        assertEquals(1, cache.size());
    }

    /** Every part of the key is distinguishing — a mask bit or a map entry, but never conflated. */
    @Test
    void mappingsDifferingInAnyPartOfTheKeyAreDistinct() {
        var cache = new AttributeTypeCache();
        cache.add(logKey("x", AttributeValueType.STRING));
        assertFalse(cache.contains(logKey("x", AttributeValueType.INT)), "another type");
        assertFalse(cache.contains(logKey("y", AttributeValueType.STRING)), "another name");
        assertFalse(cache.contains(key("logs", Signal.LOGS, AttributeScope.RESOURCE, "x",
                AttributeValueType.STRING)), "another scope");
        assertFalse(cache.contains(key("logs", Signal.TRACES, AttributeScope.ATTRIBUTE, "x",
                AttributeValueType.STRING)), "another signal");
        assertFalse(cache.contains(key("other", Signal.LOGS, AttributeScope.ATTRIBUTE, "x",
                AttributeValueType.STRING)), "another stream");
    }

    /** Every (scope, type) pair has its own bit, and all of them fit one name's mask. */
    @Test
    void everyScopeAndTypeCombinationOfOneNameIsHeldAtOnce() {
        var cache = new AttributeTypeCache();
        int expected = 0;
        for (AttributeScope scope : AttributeScope.values()) {
            for (AttributeValueType type : AttributeValueType.values()) {
                assertTrue(cache.add(key("logs", Signal.LOGS, scope, "x", type)), scope + "/" + type);
                expected++;
            }
        }
        assertEquals(expected, cache.size());
        assertEquals(1, cache.nameCount(), "all of them under one name");
        for (AttributeScope scope : AttributeScope.values()) {
            for (AttributeValueType type : AttributeValueType.values()) {
                assertTrue(cache.contains(key("logs", Signal.LOGS, scope, "x", type)), scope + "/" + type);
            }
        }
    }

    /**
     * The claim the design rests on: a name shared by many streams is stored once. The cache is
     * given the name as a distinct {@code String} instance each time, as a request would, and must
     * still end with one.
     */
    @Test
    void aNameSharedByManyStreamsIsStoredOnce() {
        var cache = new AttributeTypeCache();
        for (int stream = 0; stream < 1_000; stream++) {
            cache.add(key("stream-" + stream, Signal.LOGS, AttributeScope.ATTRIBUTE,
                    new String("service.name"), AttributeValueType.STRING));
        }
        assertEquals(1_000, cache.size());
        assertEquals(1, cache.nameCount(), "one canonical instance for all thousand streams");
    }

    /** Counted per queue as well as in total, which is what lets one stream be capped alone. */
    @Test
    void countsMappingsPerQueueAsWellAsInTotal() {
        var cache = new AttributeTypeCache();
        cache.add(key("noisy", Signal.LOGS, AttributeScope.ATTRIBUTE, "a", AttributeValueType.STRING));
        cache.add(key("noisy", Signal.LOGS, AttributeScope.ATTRIBUTE, "b", AttributeValueType.STRING));
        // A second type of a held name is a new mapping for the queue too, bit or not.
        cache.add(key("noisy", Signal.LOGS, AttributeScope.ATTRIBUTE, "a", AttributeValueType.INT));
        // Another signal of the same queue counts against the same queue.
        cache.add(key("noisy", Signal.TRACES, AttributeScope.ATTRIBUTE, "a", AttributeValueType.STRING));
        cache.add(key("quiet", Signal.LOGS, AttributeScope.ATTRIBUTE, "a", AttributeValueType.STRING));

        assertEquals(5, cache.size());
        assertEquals(4, cache.sizeOf("noisy"), "across its signals, scopes and types");
        assertEquals(1, cache.sizeOf("quiet"));
        assertEquals(0, cache.sizeOf("never-seen"));

        // A duplicate is not counted twice, for the queue any more than in total.
        cache.add(key("quiet", Signal.LOGS, AttributeScope.ATTRIBUTE, "a", AttributeValueType.STRING));
        assertEquals(1, cache.sizeOf("quiet"));
        assertEquals(5, cache.size());
    }

    /** Further types of a name a stream already holds cost no entry at all, only a bit. */
    @Test
    void aSecondTypeOfAKnownNameAddsNoEntry() {
        var cache = new AttributeTypeCache();
        cache.add(logKey("x", AttributeValueType.STRING));
        assertEquals(1, cache.nameCount());
        cache.add(logKey("x", AttributeValueType.INT));
        cache.add(logKey("x", AttributeValueType.DOUBLE));
        assertEquals(3, cache.size());
        assertEquals(1, cache.nameCount());
    }
}
