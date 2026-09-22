package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Guards {@link MapColumnWriter}, which replaced the {@code UnionMapWriter} loop in the three
 * batch writers for performance.
 *
 * <p>This is a DIFFERENTIAL test on purpose. The new writer maintains the map's offset buffer and
 * the entry struct's validity bits by hand, so the failure mode is not an exception — it is
 * silently misaligned data, where row N gets row N+1's attributes. Asserting against the old
 * writer's output catches that; asserting "it didn't throw" would not. Before this class the
 * attribute-map path had no test coverage at all.
 */
class MapColumnWriterTest {

    /** Rows deliberately vary in size, including empty ones at the start, middle and end. */
    private static List<LogEntry> fixture() {
        List<LogEntry> entries = new ArrayList<>();
        entries.add(entry(List.of(), List.of()));                       // empty map, first row
        entries.add(entry(List.of(kv("a", "1")), List.of(kv("r", "x"))));
        entries.add(entry(List.of(kv("a", "2"), kv("b", "y"), kv("c", "z")), List.of()));
        entries.add(entry(List.of(), List.of(kv("r", "x"))));           // empty map, middle
        entries.add(entry(List.of(kv("n", 42L), kv("f", true)), null));  // non-string AnyValues
        entries.add(entry(List.of(kv("a", "3")), List.of()));            // repeated key 'a'
        entries.add(entry(List.of(), List.of()));                       // empty map, last row
        return entries;
    }

    @Test
    void producesTheSameArrowAsTheWriterApiItReplaced() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot actual = VectorSchemaRoot.create(OtelLogSchema.SCHEMA, allocator);
             VectorSchemaRoot expected = VectorSchemaRoot.create(OtelLogSchema.SCHEMA, allocator)) {

            List<LogEntry> entries = fixture();
            LogRecordBatchWriter.write(entries, actual);
            writeMapsTheOldWay(entries, expected);

            for (int col : new int[]{OtelLogSchema.COL_ATTRIBUTES, OtelLogSchema.COL_RESOURCE_ATTRIBUTES}) {
                MapVector a = (MapVector) actual.getVector(col);
                MapVector e = (MapVector) expected.getVector(col);
                assertEquals(e.getValueCount(), a.getValueCount(), "value count, column " + col);
                for (int row = 0; row < entries.size(); row++) {
                    assertEquals(String.valueOf(e.getObject(row)), String.valueOf(a.getObject(row)),
                            "column " + col + ", row " + row);
                }
            }
        }
    }

    /** Independently of the old writer: the contents are what the fixture actually specified. */
    @Test
    void writesTheExpectedEntries() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(OtelLogSchema.SCHEMA, allocator)) {

            LogRecordBatchWriter.write(fixture(), root);
            MapVector attrs = (MapVector) root.getVector(OtelLogSchema.COL_ATTRIBUTES);

            assertEquals("", render(attrs, 0));
            assertEquals("a=1", render(attrs, 1));
            assertEquals("a=2,b=y,c=z", render(attrs, 2));
            assertEquals("", render(attrs, 3));
            assertEquals("n=42,f=true", render(attrs, 4));
            assertEquals("a=3", render(attrs, 5));
            assertEquals("", render(attrs, 6));

            MapVector res = (MapVector) root.getVector(OtelLogSchema.COL_RESOURCE_ATTRIBUTES);
            assertEquals("r=x", render(res, 1));
            assertEquals("", render(res, 4));  // null Resource
        }
    }

    /** Renders one row's map as {@code k=v,k=v}, so assertions do not depend on Arrow's toString. */
    private static String render(MapVector vector, int row) {
        StringBuilder sb = new StringBuilder();
        for (Object o : (List<?>) vector.getObject(row)) {
            java.util.Map<?, ?> entry = (java.util.Map<?, ?>) o;
            if (sb.length() > 0) sb.append(',');
            sb.append(entry.get("key")).append('=').append(entry.get("value"));
        }
        return sb.toString();
    }

    /** Verbatim copy of the loop MapColumnWriter replaced, kept here as the reference output. */
    private static void writeMapsTheOldWay(List<LogEntry> entries, VectorSchemaRoot root) {
        root.allocateNew();
        MapVector attributesVec = (MapVector) root.getVector(OtelLogSchema.COL_ATTRIBUTES);
        MapVector resourceAttributesVec = (MapVector) root.getVector(OtelLogSchema.COL_RESOURCE_ATTRIBUTES);
        UnionMapWriter attrWriter = attributesVec.getWriter();
        UnionMapWriter resAttrWriter = resourceAttributesVec.getWriter();

        for (int i = 0; i < entries.size(); i++) {
            LogEntry entry = entries.get(i);
            Resource resource = entry.resource();
            writeMap(attrWriter, i, entry.record().getAttributesList());
            writeMap(resAttrWriter, i, resource != null ? resource.getAttributesList() : List.of());
        }
        root.setRowCount(entries.size());
    }

    private static void writeMap(UnionMapWriter writer, int index, List<KeyValue> kvList) {
        writer.setPosition(index);
        writer.startMap();
        for (KeyValue kv : kvList) {
            OtelSchemaFields.writeEntry(writer, kv.getKey(), LogRecordConverter.anyValueToString(kv.getValue()));
        }
        writer.endMap();
    }

    private static LogEntry entry(List<KeyValue> attrs, List<KeyValue> resourceAttrs) {
        LogRecord record = LogRecord.newBuilder()
                .setTimeUnixNano(1_000_000_000L)
                .addAllAttributes(attrs)
                .build();
        Resource resource = resourceAttrs == null
                ? null
                : Resource.newBuilder().addAllAttributes(resourceAttrs).build();
        return new LogEntry(record, resource, null);
    }

    private static KeyValue kv(String k, String v) {
        return kv(k, AnyValue.newBuilder().setStringValue(v).build());
    }

    private static KeyValue kv(String k, long v) {
        return kv(k, AnyValue.newBuilder().setIntValue(v).build());
    }

    private static KeyValue kv(String k, boolean v) {
        return kv(k, AnyValue.newBuilder().setBoolValue(v).build());
    }

    private static KeyValue kv(String k, AnyValue v) {
        return KeyValue.newBuilder().setKey(k).setValue(v).build();
    }
}
