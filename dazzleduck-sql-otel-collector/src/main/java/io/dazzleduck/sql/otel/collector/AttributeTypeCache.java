package io.dazzleduck.sql.otel.collector;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What {@link AttributeTypeRegistry} already knows is recorded, held so that a process running
 * thousands of streams does not spend its heap on it.
 *
 * <p>WHY NOT A SET OF MAPPINGS. The obvious structure is one set entry per
 * {@code (queue, signal, scope, name, type)}. At a thousand streams with a few hundred attribute
 * names each that is hundreds of thousands of entries, and every one of them costs a map node, a
 * key object of five references, and — because the queue id arrives as a fresh {@code String} on
 * each request — potentially its own copy of the stream name. It also grows again each time an
 * attribute appears in another scope or with another type.
 *
 * <p>So the same information is held as a bitmask instead:
 * <ul>
 *   <li>one entry per <b>stream and signal</b> ({@link Stream}), of which there are at most three
 *       per stream, holding that stream's names;</li>
 *   <li>one entry per <b>attribute name</b> within it, whose value is an {@code int} with one bit
 *       per {@code (scope, type)} pair — four scopes by eight types is exactly 32 bits. Every
 *       further scope or type of a name it already holds is a bit, not an entry;</li>
 *   <li>the name itself is canonicalised once in {@link #names} and that one instance is shared by
 *       every stream that sees it, so a name common to a thousand streams is stored once.</li>
 * </ul>
 * A recorded mapping therefore costs a map node and a counter the first time its name appears in a
 * stream, and nothing after that.
 *
 * <p>Thread-safe, and the counter of distinct mappings is exact: a bit is counted by whichever
 * thread sets it.
 */
final class AttributeTypeCache {

    /** One stream's one signal — the unit the names below are grouped under. */
    private record Stream(String queue, Signal signal) {}

    private static final int TYPES = AttributeValueType.values().length;

    static {
        int combinations = AttributeScope.values().length * TYPES;
        if (combinations > Integer.SIZE) {
            throw new ExceptionInInitializerError(
                    "a (scope, type) bitmask no longer fits in an int: " + combinations + " combinations");
        }
    }

    /** stream and signal -> canonical attribute name -> bitmask of the (scope, type) pairs seen. */
    private final ConcurrentHashMap<Stream, ConcurrentHashMap<String, AtomicInteger>> byStream =
            new ConcurrentHashMap<>();
    /** One instance of each attribute name, shared by every stream that records it. */
    private final ConcurrentHashMap<String, String> names = new ConcurrentHashMap<>();
    /** Distinct mappings held — bits set, not entries. */
    private final AtomicInteger mappings = new AtomicInteger();
    /** The same count per queue, so one stream's cardinality can be capped on its own. */
    private final ConcurrentHashMap<String, AtomicInteger> byQueue = new ConcurrentHashMap<>();

    private static int bit(AttributeScope scope, AttributeValueType type) {
        return 1 << (scope.ordinal() * TYPES + type.ordinal());
    }

    boolean contains(AttributeTypeRegistry.Key key) {
        var forStream = byStream.get(new Stream(key.queue(), key.signal()));
        if (forStream == null) {
            return false;
        }
        AtomicInteger mask = forStream.get(key.name());
        return mask != null && (mask.get() & bit(key.scope(), key.type())) != 0;
    }

    /** Records {@code key}; returns true when it was not there before. */
    boolean add(AttributeTypeRegistry.Key key) {
        var forStream = byStream.computeIfAbsent(new Stream(key.queue(), key.signal()),
                stream -> new ConcurrentHashMap<>());
        // The canonical instance, so one name shared by many streams is one String.
        String name = names.computeIfAbsent(key.name(), n -> n);
        AtomicInteger mask = forStream.computeIfAbsent(name, n -> new AtomicInteger());
        int bit = bit(key.scope(), key.type());
        int current;
        do {
            current = mask.get();
            if ((current & bit) != 0) {
                return false;
            }
        } while (!mask.compareAndSet(current, current | bit));
        mappings.incrementAndGet();
        byQueue.computeIfAbsent(key.queue(), q -> new AtomicInteger()).incrementAndGet();
        return true;
    }

    /** Distinct mappings held. */
    int size() {
        return mappings.get();
    }

    /** Distinct mappings held for one queue, across its signals. */
    int sizeOf(String queue) {
        AtomicInteger count = byQueue.get(queue);
        return count == null ? 0 : count.get();
    }

    /** Distinct attribute names held, across all streams. */
    int nameCount() {
        return names.size();
    }
}
