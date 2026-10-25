package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.otel.collector.ArrowColumns.BigInts;
import io.dazzleduck.sql.otel.collector.ArrowColumns.Bits;
import io.dazzleduck.sql.otel.collector.ArrowColumns.Offsets;
import io.dazzleduck.sql.otel.collector.ArrowColumns.VarWidth;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.List;

import static io.dazzleduck.sql.otel.collector.MapColumnWriter.utf8;

/**
 * Writes a span's {@code events} or {@code links} column — a {@code LIST<STRUCT<...>>} with two
 * scalar fields and the six attribute maps — through {@link ArrowColumns}, the same buffered path
 * as the top-level attribute maps (see {@link AttributeMapsWriter} for why). It replaced Arrow's
 * {@code UnionListWriter}/{@code NullableStructWriter} API, which paid the per-value dispatch,
 * hash lookups and type promotion that {@link MapColumnWriter} documents, once for each of the six
 * maps of every event and link.
 *
 * <p>Layout, unchanged from the writer API: a span with no events (links) has a null list; every
 * element struct is non-null; its attribute maps are all null when it has no attributes. Usage as
 * {@link AttributeMapsWriter}: {@link #write} every span in order, then {@link #finish} BEFORE
 * {@code root.setRowCount}. NOT thread-safe; one instance per column per batch.
 *
 * @param <T> {@link Span.Event} or {@link Span.Link}
 */
abstract sealed class SpanListWriter<T> permits SpanListWriter.Events, SpanListWriter.Links {

    private final ListVector list;
    private final StructVector struct;
    private final Bits listValidity = new Bits();
    private final Offsets listOffsets = new Offsets();
    private final AttributeMapsWriter attributes;
    private int rows;
    private int elements;

    private SpanListWriter(ListVector list, boolean stringCopy, AttributeTypeScan.Cursor scan) {
        this.list = list;
        this.struct = (StructVector) list.getDataVector();
        this.attributes = AttributeMapsWriter.of(struct, stringCopy, scan);
    }

    /** @param scan collects the events' attribute name/type mappings, or null to collect nothing */
    static SpanListWriter<Span.Event> events(VectorSchemaRoot root, boolean stringCopy,
                                             AttributeTypeScan.Cursor scan) {
        return new Events((ListVector) root.getVector(OtelTraceSchema.COL_EVENTS), stringCopy, scan);
    }

    /** @param scan collects the links' attribute name/type mappings, or null to collect nothing */
    static SpanListWriter<Span.Link> links(VectorSchemaRoot root, boolean stringCopy,
                                           AttributeTypeScan.Cursor scan) {
        return new Links((ListVector) root.getVector(OtelTraceSchema.COL_LINKS), stringCopy, scan);
    }

    /** Buffers span {@code row}'s elements; rows must arrive in order from 0 with no gaps. */
    void write(int row, List<T> items) {
        if (row != rows) {
            throw new IllegalStateException("span rows must be written in order: expected " + rows + ", got " + row);
        }
        int count = items.size();
        for (int i = 0; i < count; i++) {
            T item = items.get(i);
            writeFields(item);
            List<KeyValue> kvList = attributes(item);
            if (kvList.isEmpty()) {
                attributes.writeNull(elements);
            } else {
                attributes.write(elements, kvList);
            }
            elements++;
        }
        listValidity.add(count > 0);
        listOffsets.add(elements);
        rows++;
    }

    /** Loads the list, its element struct, the struct's scalar fields and its attribute maps. */
    void finish(int rowCount) {
        if (rowCount != rows) {
            throw new IllegalStateException("wrote " + rows + " spans' " + list.getName() + ", batch has " + rowCount);
        }
        ArrowColumns.load(list, rows, listValidity.nullCount(), listValidity::validity, listOffsets::buffer);
        ArrowColumns.load(struct, elements, 0, BufferAllocator::getEmpty);
        loadFields(struct);
        attributes.finish(elements);
    }

    /** Buffers the element's scalar fields. */
    abstract void writeFields(T item);

    abstract List<KeyValue> attributes(T item);

    /** Loads the scalar fields' buffers into the element struct's children. */
    abstract void loadFields(StructVector struct);

    private static byte[] utf8OrNull(String s) {
        return s == null ? null : utf8(s);
    }

    /** {@code events}: {@code name} (VARCHAR, null when empty), {@code time_ms} (BIGINT, null when unset). */
    static final class Events extends SpanListWriter<Span.Event> {
        private final VarWidth names = new VarWidth();
        private final BigInts times = new BigInts();

        private Events(ListVector list, boolean stringCopy, AttributeTypeScan.Cursor scan) {
            super(list, stringCopy, scan);
        }

        @Override
        void writeFields(Span.Event event) {
            names.add(utf8OrNull(LogRecordConverter.emptyToNull(event.getName())));
            if (event.getTimeUnixNano() > 0) {
                times.add(event.getTimeUnixNano() / 1_000_000L);
            } else {
                times.addNull();
            }
        }

        @Override
        List<KeyValue> attributes(Span.Event event) {
            return event.getAttributesList();
        }

        @Override
        void loadFields(StructVector struct) {
            names.load(struct.getChild("name", FieldVector.class));
            times.load(struct.getChild("time_ms", FieldVector.class));
        }
    }

    /** {@code links}: {@code trace_id} and {@code span_id} (VARCHAR hex, null when empty). */
    static final class Links extends SpanListWriter<Span.Link> {
        private final VarWidth traceIds = new VarWidth();
        private final VarWidth spanIds = new VarWidth();

        private Links(ListVector list, boolean stringCopy, AttributeTypeScan.Cursor scan) {
            super(list, stringCopy, scan);
        }

        @Override
        void writeFields(Span.Link link) {
            traceIds.add(utf8OrNull(SpanBatchWriter.hexOrNull(link.getTraceId().toByteArray())));
            spanIds.add(utf8OrNull(SpanBatchWriter.hexOrNull(link.getSpanId().toByteArray())));
        }

        @Override
        List<KeyValue> attributes(Span.Link link) {
            return link.getAttributesList();
        }

        @Override
        void loadFields(StructVector struct) {
            traceIds.load(struct.getChild("trace_id", FieldVector.class));
            spanIds.load(struct.getChild("span_id", FieldVector.class));
        }
    }
}
