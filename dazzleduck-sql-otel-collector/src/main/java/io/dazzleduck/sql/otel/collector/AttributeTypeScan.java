package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The distinct attribute-name-to-type mappings one batch contains, collected while the batch is
 * written and handed to {@link AttributeTypeRegistry} afterwards.
 *
 * <p>WHY IT IS SHAPED LIKE THIS. The collector is CPU-bound in the per-attribute loop of
 * {@link AttributeMapsWriter}, and the obvious implementation — hash each key and probe a set,
 * once per attribute per record — is the one measurement has already rejected in this exact loop:
 * memoising encoded keys by {@code HashMap.computeIfAbsent} cost 11.8% of collector Java CPU,
 * because protobuf materialises a fresh {@code String} per message so the cached
 * {@code String.hashCode} never carries over between records (the note in {@link MapColumnWriter}
 * has the numbers). Nothing here hashes a key on the common path.
 *
 * <p>Instead each attribute set keeps a {@link Cursor} holding the previous row's key list, as the
 * {@code String} references protobuf already handed out, and that row's value kinds. A row whose
 * keys and kinds match the previous row's, in the same order, is skipped outright — and OTLP
 * producers emit the same attribute layout record after record, so in a steady stream every row
 * after the first is. A row that differs is walked once and its mappings added to this set. Every
 * mapping in a batch is therefore collected: a row is either collected itself or identical to the
 * row before it, which was.
 *
 * <p>The comparison itself is a {@code String.equals} per attribute — a length check and an
 * intrinsified byte compare over the short Latin-1 names OTLP uses, with no allocation, no hash
 * and no map probe.
 *
 * <p>NOT thread-safe: one instance per batch, with one {@link Cursor} per attribute set, which is
 * how the batch writers use it.
 */
final class AttributeTypeScan {

    /** One attribute name seen in one scope with one value kind. */
    record Mapping(AttributeScope scope, String name, AttributeValueType type) {}

    private final Set<Mapping> mappings = new HashSet<>();

    /** A cursor for one attribute set of this batch — the record's, its resource's, an event's. */
    Cursor cursor(AttributeScope scope) {
        return new Cursor(scope);
    }

    /**
     * {@code scan.cursor(scope)}, or null when {@code scan} is null — the shape the batch writers
     * need, since a null scan means the registry is off and nothing is collected.
     */
    static Cursor cursorOf(AttributeTypeScan scan, AttributeScope scope) {
        return scan == null ? null : scan.cursor(scope);
    }

    /** The mappings this batch contains, deduplicated. */
    Set<Mapping> mappings() {
        return mappings;
    }

    boolean isEmpty() {
        return mappings.isEmpty();
    }

    /**
     * Tracks one attribute set across the rows of a batch. Created by
     * {@link AttributeTypeScan#cursor}; {@link #observe} is called once per row.
     */
    final class Cursor {

        private static final String[] NO_KEYS = new String[0];
        private static final int[] NO_CASES = new int[0];

        private final AttributeScope scope;
        // The previous row's list itself, for the identity check below.
        private List<KeyValue> last;
        // The previous row's layout: its keys as protobuf's own String instances (never copied)
        // and its value kinds as ValueCase ordinals. Both are reused across rows, growing only
        // when a row is wider than any before it. count is -1 until the first row is seen, so the
        // first row never matches an empty layout.
        private String[] keys = NO_KEYS;
        private int[] cases = NO_CASES;
        private int count = -1;

        private Cursor(AttributeScope scope) {
            this.scope = scope;
        }

        /** Collects row {@code kvList}'s mappings unless its layout repeats the previous row's. */
        void observe(List<KeyValue> kvList) {
            // The same list object again needs no comparison at all, and that is the common case
            // for a resource attribute set: one Resource message covers every record of an export,
            // and protobuf hands out the same list for it each time. A record with no attributes
            // gets the shared empty list, which matches the same way.
            if (kvList == last) {
                return;
            }
            int size = kvList.size();
            if (size == count && repeatsPreviousRow(kvList, size)) {
                last = kvList;
                return;
            }
            collect(kvList, size);
        }

        private boolean repeatsPreviousRow(List<KeyValue> kvList, int size) {
            for (int i = 0; i < size; i++) {
                KeyValue kv = kvList.get(i);
                if (cases[i] != kv.getValue().getValueCase().ordinal() || !keys[i].equals(kv.getKey())) {
                    return false;
                }
            }
            return true;
        }

        private void collect(List<KeyValue> kvList, int size) {
            if (keys.length < size) {
                keys = new String[size];
                cases = new int[size];
            }
            for (int i = 0; i < size; i++) {
                KeyValue kv = kvList.get(i);
                AnyValue.ValueCase valueCase = kv.getValue().getValueCase();
                String key = kv.getKey();
                keys[i] = key;
                cases[i] = valueCase.ordinal();
                mappings.add(new Mapping(scope, key, AttributeValueType.of(valueCase)));
            }
            count = size;
            last = kvList;
        }
    }
}
