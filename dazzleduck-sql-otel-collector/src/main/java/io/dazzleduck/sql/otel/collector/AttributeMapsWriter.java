package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.otel.collector.ArrowColumns.Bits;
import io.dazzleduck.sql.otel.collector.ArrowColumns.Bytes;
import io.dazzleduck.sql.otel.collector.ArrowColumns.Longs;
import io.dazzleduck.sql.otel.collector.ArrowColumns.Offsets;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.List;

import static io.dazzleduck.sql.otel.collector.MapColumnWriter.utf8;

/**
 * Writes one OTLP attribute set into its six map columns, routing each attribute by its
 * {@code AnyValue} kind (see {@link AttributeType}):
 * <ul>
 *   <li>{@code attributes} — string values, and with {@code stringCopy} every other value too,
 *       rendered as text exactly as before the typed maps existed;</li>
 *   <li>{@code attributes_int}, {@code _double}, {@code _bool}, {@code _bytes} — the native value;</li>
 *   <li>{@code attributes_json} — arrays and key/value lists as typed JSON.</li>
 * </ul>
 * A row written with {@link #write} has a map in all six columns, empty when it has no attribute
 * of that kind; a row written with {@link #writeNull} (a span event or link with no attributes)
 * is null in all six. An attribute with no value set has no typed column, so it is kept in the
 * string map as an empty string whatever {@code stringCopy} says.
 *
 * <p>WHY IT BUFFERS THE BATCH. The collector is CPU-bound in Java, and with ~30-60 attributes per
 * record this is its hottest loop. Writing each entry into the Arrow vectors costs several
 * bounds-checked buffer operations per entry ({@code setIndexDefined}, then {@code setSafe} on the
 * key and on the value, each checking capacity, filling offset holes and touching a validity
 * bitmap), plus a {@code startNewValue}/{@code endValue} pair per row of each map. With six maps
 * per attribute set that bookkeeping, not the data, was the cost: written entry by entry through
 * {@link MapColumnWriter}, the six maps made the whole batch writer 24-37% slower than the single
 * string map they replaced. So the batch is accumulated in plain heap arrays already in Arrow's
 * layout — offsets through a little-endian {@code VarHandle} view, bytes by
 * {@code System.arraycopy}, see {@link ArrowColumns} — and {@link #finish} hands each Arrow buffer
 * over in ONE copy through {@code FieldVector.loadFieldBuffers}, the entry point the IPC reader
 * uses. Entries, keys and values are never null, so their validity bitmaps are all-valid, which
 * {@code loadFieldBuffers} builds itself from a null count of 0.
 *
 * <p>Usage: {@link #write} or {@link #writeNull} every row, in order from 0, then {@link #finish}
 * once — for a top-level attribute set BEFORE {@code root.setRowCount}. NOT thread-safe; one
 * instance per attribute set per batch.
 */
final class AttributeMapsWriter {

    private final boolean stringCopy;
    /** Collects this attribute set's name/type mappings, or null when the registry is off. */
    private final AttributeTypeScan.Cursor scan;
    private final MapBuilder strings;
    private final MapBuilder ints;
    private final MapBuilder doubles;
    private final MapBuilder bools;
    private final MapBuilder bytes;
    private final MapBuilder json;
    private int rows;

    private static final byte[] TRUE = {'t', 'r', 'u', 'e'};
    private static final byte[] FALSE = {'f', 'a', 'l', 's', 'e'};

    /**
     * @param stringBytes starting size of the string map's key and value buffers, which hold most
     *                    of a batch (see {@link ArrowColumns#initialBytes}); the typed maps hold
     *                    little and start at {@link ArrowColumns#DEFAULT_BYTES}
     */
    private AttributeMapsWriter(MapVector strings, List<MapVector> typed, boolean stringCopy, int stringBytes,
                                AttributeTypeScan.Cursor scan) {
        this.stringCopy = stringCopy;
        this.scan = scan;
        this.strings = new MapBuilder(AttributeType.STRING, strings, stringBytes);
        this.ints    = new MapBuilder(AttributeType.INT,    typed.get(0), ArrowColumns.DEFAULT_BYTES);
        this.doubles = new MapBuilder(AttributeType.DOUBLE, typed.get(1), ArrowColumns.DEFAULT_BYTES);
        this.bools   = new MapBuilder(AttributeType.BOOL,   typed.get(2), ArrowColumns.DEFAULT_BYTES);
        this.bytes   = new MapBuilder(AttributeType.BYTES,  typed.get(3), ArrowColumns.DEFAULT_BYTES);
        this.json    = new MapBuilder(AttributeType.JSON,   typed.get(4), ArrowColumns.DEFAULT_BYTES);
    }

    /**
     * A top-level attribute set of {@code root}.
     *
     * @param stringColumn index of the string map
     * @param typedStart   index of the first of the five typed maps, in {@link AttributeType#TYPED} order
     * @param stringCopy   also write every non-string value, as text, to the string map
     * @param expectedRows the batch's row count, to size the buffers (see {@link ArrowColumns#initialBytes})
     * @param scan         collects this set's name/type mappings, or null to collect nothing
     */
    static AttributeMapsWriter of(VectorSchemaRoot root, int stringColumn, int typedStart, boolean stringCopy,
                                  int expectedRows, AttributeTypeScan.Cursor scan) {
        return new AttributeMapsWriter(
                (MapVector) root.getVector(stringColumn),
                List.of((MapVector) root.getVector(typedStart),
                        (MapVector) root.getVector(typedStart + 1),
                        (MapVector) root.getVector(typedStart + 2),
                        (MapVector) root.getVector(typedStart + 3),
                        (MapVector) root.getVector(typedStart + 4)),
                stringCopy, ArrowColumns.initialBytes(expectedRows), scan);
    }

    /** The {@code attributes} set of a struct — a span event or link — one row per struct value. */
    static AttributeMapsWriter of(StructVector struct, boolean stringCopy, AttributeTypeScan.Cursor scan) {
        return new AttributeMapsWriter(
                struct.getChild("attributes", MapVector.class),
                AttributeType.TYPED.stream()
                        .map(type -> struct.getChild("attributes" + type.suffix(), MapVector.class))
                        .toList(),
                stringCopy, ArrowColumns.DEFAULT_BYTES, scan);
    }

    /** Buffers row {@code row}'s attributes; rows must arrive in order from 0 with no gaps. */
    void write(int row, List<KeyValue> kvList) {
        checkRow(row);
        // Before the write loop, so that loop stays exactly as it was: one branch per row when the
        // registry is off, and a layout comparison against the previous row when it is on — never
        // per-attribute hashing. See AttributeTypeScan.
        if (scan != null) {
            scan.observe(kvList);
        }
        int count = kvList.size();
        // Indexed, not for-each: protobuf's list is RandomAccess (see MapColumnWriter).
        for (int i = 0; i < count; i++) {
            KeyValue kv = kvList.get(i);
            AnyValue value = kv.getValue();
            byte[] key = utf8(kv.getKey());
            switch (value.getValueCase()) {
                case STRING_VALUE -> strings.putVar(key, utf8(value.getStringValue()));
                case INT_VALUE -> {
                    ints.putLong(key, value.getIntValue());
                    copy(key, value);
                }
                case DOUBLE_VALUE -> {
                    doubles.putLong(key, Double.doubleToRawLongBits(value.getDoubleValue()));
                    copy(key, value);
                }
                case BOOL_VALUE -> {
                    bools.putBool(key, value.getBoolValue());
                    copy(key, value);
                }
                case BYTES_VALUE -> {
                    bytes.putVar(key, value.getBytesValue().toByteArray());
                    copy(key, value);
                }
                case ARRAY_VALUE, KVLIST_VALUE -> {
                    byte[] text = utf8(LogRecordConverter.anyValueToJson(value));
                    json.putVar(key, text);
                    if (stringCopy) strings.putVar(key, text);
                }
                default -> strings.putVar(key, utf8(LogRecordConverter.anyValueToString(value)));
            }
        }
        endRow(true);
    }

    /** Writes row {@code row} as null in all six maps. */
    void writeNull(int row) {
        checkRow(row);
        endRow(false);
    }

    /**
     * Loads the buffered batch into the six map vectors. {@code rowCount} is the number of rows
     * the vectors are about to be given (the batch's row count, or the struct's value count); a
     * mismatch would misalign the maps against every other column, so it fails instead.
     */
    void finish(int rowCount) {
        if (rowCount != rows) {
            throw new IllegalStateException("wrote attributes for " + rows + " rows, batch has " + rowCount);
        }
        strings.load();
        ints.load();
        doubles.load();
        bools.load();
        bytes.load();
        json.load();
    }

    private void checkRow(int row) {
        if (row != rows) {
            throw new IllegalStateException("attribute rows must be written in order: expected " + rows + ", got " + row);
        }
    }

    private void endRow(boolean present) {
        strings.endRow(present);
        ints.endRow(present);
        doubles.endRow(present);
        bools.endRow(present);
        bytes.endRow(present);
        json.endRow(present);
        rows++;
    }

    /**
     * The string copy of a non-string value: the text {@link LogRecordConverter#anyValueToString}
     * gives, written straight into the buffer — no intermediate String and no UTF-8 encode, which
     * together were ~6% of collector Java CPU with the copy on.
     */
    private void copy(byte[] key, AnyValue value) {
        if (!stringCopy) return;
        switch (value.getValueCase()) {
            case INT_VALUE -> strings.putDecimal(key, value.getIntValue());
            case DOUBLE_VALUE -> strings.putAscii(key, Double.toString(value.getDoubleValue()));
            case BOOL_VALUE -> strings.putVar(key, value.getBoolValue() ? TRUE : FALSE);
            case BYTES_VALUE -> strings.putHex(key, value.getBytesValue().toByteArray());
            default -> strings.putVar(key, utf8(LogRecordConverter.anyValueToString(value)));
        }
    }

    /** One map column's batch: row validity and offsets, keys, and values of the map's type. */
    private static final class MapBuilder {

        private final AttributeType type;
        private final MapVector vector;

        private final Bits rowValidity = new Bits();
        private final Offsets rowOffsets = new Offsets();
        private int entries;
        private final Offsets keyOffsets;
        private final Bytes keyData;
        // String, bytes and json values: offsets + data. Int and double: longs. Bool: bits.
        private final Offsets valueOffsets;
        private final Bytes valueData;
        private final Longs longs;
        private final Bits booleans;

        MapBuilder(AttributeType type, MapVector vector, int initialBytes) {
            this.type = type;
            this.vector = vector;
            int initialEntries = Math.max(ArrowColumns.DEFAULT_ENTRIES, initialBytes / ArrowColumns.BYTES_PER_ENTRY);
            this.keyOffsets = new Offsets(initialEntries);
            this.keyData = new Bytes(initialBytes);
            boolean variableWidth = type == AttributeType.STRING || type == AttributeType.BYTES || type == AttributeType.JSON;
            this.valueOffsets = variableWidth ? new Offsets(initialEntries) : null;
            this.valueData = variableWidth ? new Bytes(initialBytes) : null;
            this.longs = type == AttributeType.INT || type == AttributeType.DOUBLE ? new Longs() : null;
            this.booleans = type == AttributeType.BOOL ? new Bits() : null;
        }

        void putVar(byte[] key, byte[] value) {
            putKey(key);
            valueData.add(value);
            valueOffsets.add(valueData.size());
        }

        void putDecimal(byte[] key, long value) {
            putKey(key);
            valueData.addDecimal(value);
            valueOffsets.add(valueData.size());
        }

        void putAscii(byte[] key, String value) {
            putKey(key);
            valueData.addAscii(value);
            valueOffsets.add(valueData.size());
        }

        void putHex(byte[] key, byte[] value) {
            putKey(key);
            valueData.addHex(value);
            valueOffsets.add(valueData.size());
        }

        void putLong(byte[] key, long value) {
            putKey(key);
            longs.add(value);
        }

        void putBool(byte[] key, boolean value) {
            putKey(key);
            booleans.add(value);
        }

        private void putKey(byte[] key) {
            keyData.add(key);
            keyOffsets.add(keyData.size());
            entries++;
        }

        void endRow(boolean present) {
            rowValidity.add(present);
            rowOffsets.add(entries);
        }

        /** Hands every buffer to its vector: the map, its entry struct, the key and the value. */
        void load() {
            StructVector struct = (StructVector) vector.getDataVector();
            FieldVector keyVector = (FieldVector) struct.getChild(MapVector.KEY_NAME);
            FieldVector valueVector = (FieldVector) struct.getChild(MapVector.VALUE_NAME);

            ArrowColumns.load(vector, rowValidity.count(), rowValidity.nullCount(),
                    rowValidity::validity, rowOffsets::buffer);
            ArrowColumns.load(struct, entries, 0, BufferAllocator::getEmpty);
            ArrowColumns.load(keyVector, entries, 0, BufferAllocator::getEmpty, keyOffsets::buffer, keyData::buffer);
            switch (type) {
                case INT, DOUBLE -> ArrowColumns.load(valueVector, entries, 0, BufferAllocator::getEmpty, longs::buffer);
                case BOOL -> ArrowColumns.load(valueVector, entries, 0, BufferAllocator::getEmpty, booleans::values);
                default -> ArrowColumns.load(valueVector, entries, 0,
                        BufferAllocator::getEmpty, valueOffsets::buffer, valueData::buffer);
            }
        }
    }
}
