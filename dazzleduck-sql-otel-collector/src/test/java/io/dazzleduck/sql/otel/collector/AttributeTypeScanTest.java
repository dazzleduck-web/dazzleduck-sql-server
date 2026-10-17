package io.dazzleduck.sql.otel.collector;

import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link AttributeTypeScan} skips a row whose attribute layout repeats the previous row's, which is
 * what keeps detection off the per-attribute hot path. These tests pin down that it still collects
 * every mapping a batch contains — the property that skip relies on: a row is either collected
 * itself or identical to the row before it, which was.
 */
public class AttributeTypeScanTest {

    private static KeyValue kv(String key, AnyValue value) {
        return KeyValue.newBuilder().setKey(key).setValue(value).build();
    }

    private static AnyValue str(String v) {
        return AnyValue.newBuilder().setStringValue(v).build();
    }

    private static AnyValue num(long v) {
        return AnyValue.newBuilder().setIntValue(v).build();
    }

    /** The scan's mappings as sorted {@code scope/name/type} text, so assertions read as data. */
    private static String mappings(AttributeTypeScan scan) {
        List<String> rendered = new ArrayList<>();
        for (AttributeTypeScan.Mapping m : scan.mappings()) {
            rendered.add(m.scope().wireName() + "/" + m.name() + "/" + m.type().wireName());
        }
        return String.join(" ", rendered.stream().sorted().toList());
    }

    @Test
    void everyOtlpValueKindIsRecordedUnderItsOwnName() {
        var scan = new AttributeTypeScan();
        scan.cursor(AttributeScope.ATTRIBUTE).observe(List.of(
                kv("s", str("text")),
                kv("n", num(42)),
                kv("d", AnyValue.newBuilder().setDoubleValue(1.5).build()),
                kv("f", AnyValue.newBuilder().setBoolValue(true).build()),
                kv("b", AnyValue.newBuilder().setBytesValue(ByteString.copyFrom(new byte[]{1})).build()),
                kv("arr", AnyValue.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addValues(AnyValue.newBuilder().setIntValue(1))).build()),
                kv("obj", AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder()
                        .addValues(kv("k", num(1)))).build()),
                kv("unset", AnyValue.newBuilder().build())));
        // array and kvlist stay apart although both are written to attributes_json, and an
        // attribute with no value set is recorded rather than skipped.
        assertEquals("attribute/arr/array attribute/b/bytes attribute/d/double attribute/f/bool"
                        + " attribute/n/int attribute/obj/kvlist attribute/s/string attribute/unset/empty",
                mappings(scan));
    }

    @Test
    void aRowRepeatingThePreviousLayoutAddsNothingNew() {
        var scan = new AttributeTypeScan();
        var cursor = scan.cursor(AttributeScope.ATTRIBUTE);
        for (int row = 0; row < 100; row++) {
            // Keys as distinct String instances with equal content, which is what parsing OTLP off
            // the wire produces: protobuf decodes a fresh String per message, so the comparison has
            // to be by content. A literal here would be interned and let reference equality pass.
            cursor.observe(List.of(kv(new String("host"), str("h")), kv(new String("port"), num(8080))));
        }
        assertEquals("attribute/host/string attribute/port/int", mappings(scan));
    }

    @Test
    void aTypeChangeWithinOneBatchIsRecordedForBothTypes() {
        var scan = new AttributeTypeScan();
        var cursor = scan.cursor(AttributeScope.ATTRIBUTE);
        cursor.observe(List.of(kv("x", str("1"))));
        cursor.observe(List.of(kv("x", num(1))));
        assertEquals("attribute/x/int attribute/x/string", mappings(scan));
    }

    /**
     * The same key list in a different order is a different layout, so the row is walked again —
     * and a mapping that only ever appears in such a row is still collected.
     */
    @Test
    void aLayoutThatDiffersOnlyInOrderIsStillWalked() {
        var scan = new AttributeTypeScan();
        var cursor = scan.cursor(AttributeScope.ATTRIBUTE);
        cursor.observe(List.of(kv("a", str("1")), kv("b", num(2))));
        cursor.observe(List.of(kv("b", num(2)), kv("a", str("1"))));
        cursor.observe(List.of(kv("b", num(2)), kv("c", str("3"))));
        assertEquals("attribute/a/string attribute/b/int attribute/c/string", mappings(scan));
    }

    /** Alternating layouts never match their predecessor, so neither is ever missed. */
    @Test
    void alternatingLayoutsAreBothCollected() {
        var scan = new AttributeTypeScan();
        var cursor = scan.cursor(AttributeScope.ATTRIBUTE);
        for (int row = 0; row < 10; row++) {
            cursor.observe(row % 2 == 0 ? List.of(kv("x", str("1"))) : List.of(kv("y", num(1))));
        }
        assertEquals("attribute/x/string attribute/y/int", mappings(scan));
    }

    /** A narrower row after a wider one: the reused key array must not report a stale match. */
    @Test
    void aNarrowerRowAfterAWiderOneIsWalked() {
        var scan = new AttributeTypeScan();
        var cursor = scan.cursor(AttributeScope.ATTRIBUTE);
        cursor.observe(List.of(kv("a", str("1")), kv("b", str("2")), kv("c", str("3"))));
        cursor.observe(List.of(kv("a", str("1"))));
        cursor.observe(List.of(kv("a", str("1")), kv("b", str("2")), kv("c", str("3"))));
        cursor.observe(List.of());
        assertEquals("attribute/a/string attribute/b/string attribute/c/string", mappings(scan));
    }

    /** One scan serves every attribute set of a batch, and the scopes stay apart. */
    @Test
    void eachScopeKeepsItsOwnNames() {
        var scan = new AttributeTypeScan();
        scan.cursor(AttributeScope.ATTRIBUTE).observe(List.of(kv("x", str("1"))));
        scan.cursor(AttributeScope.RESOURCE).observe(List.of(kv("x", num(1))));
        scan.cursor(AttributeScope.EVENT).observe(List.of(kv("x", str("1"))));
        scan.cursor(AttributeScope.LINK).observe(List.of(kv("x", str("1"))));
        assertEquals("attribute/x/string event/x/string link/x/string resource/x/int", mappings(scan));
    }

    /** Each attribute set has its own cursor, so one set's layout cannot mask another's row. */
    @Test
    void oneSetsRepeatedLayoutDoesNotHideAnothers() {
        var scan = new AttributeTypeScan();
        var attributes = scan.cursor(AttributeScope.ATTRIBUTE);
        var resource = scan.cursor(AttributeScope.RESOURCE);
        for (int row = 0; row < 5; row++) {
            attributes.observe(List.of(kv("same", str("v"))));
            resource.observe(List.of(kv("same", str("v"))));
        }
        assertEquals("attribute/same/string resource/same/string", mappings(scan));
    }

    @Test
    void anEmptyBatchHasNoMappings() {
        var scan = new AttributeTypeScan();
        scan.cursor(AttributeScope.ATTRIBUTE).observe(List.of());
        assertEquals("", mappings(scan));
        assertTrue(scan.isEmpty());
    }
}
