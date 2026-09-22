package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.KeyValue;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Writes an OTLP attribute map column by addressing the {@link MapVector}'s child vectors
 * directly, instead of going through {@code UnionMapWriter}.
 *
 * <p>WHY THIS EXISTS. A JFR profile of the benchmark collector at 15.5k rec/s put <b>69.7% of
 * all Java CPU</b> inside the {@code UnionMapWriter} loop this replaces — and essentially none
 * of it in data movement. The payload is two short strings per entry; the rest was dispatch.
 * Per entry the writer API paid:
 * <ul>
 *   <li>{@code NullableStructWriter.varChar("key"/"value")} → a {@code HashMap} lookup keyed by
 *       a string literal, twice;</li>
 *   <li>{@code StructWriter.start()} → {@code NullableStructWriter.setPosition()} → allocate an
 *       iterator over the field {@code HashMap} and walk it;</li>
 *   <li>{@code PromotableWriter} dispatch, to resolve a type the schema already fixes;</li>
 *   <li>{@code VarCharWriterImpl.writeVarChar(String)} → {@code Text.set()} → {@code
 *       Text.encode()}, whose {@code ThreadLocal<CharsetEncoder>} lookup <i>alone</i> was 12.2%
 *       of Java CPU, plus a {@code CharBuffer} and a {@code ByteBuffer} allocated per string;</li>
 *   <li>{@code setValueCount()} on <i>every</i> value write, recomputing buffer indices.</li>
 * </ul>
 * At ~30–60 attributes per record that is roughly a million writer calls per second which do no
 * useful work. Measured non-causes, recorded so nobody re-spends the time: buffer reallocation
 * was 0.5%, {@code anyValueToString} 0.0%, GC ~1.2% of wall, and all scalar columns together 1.0%.
 *
 * <p>CORRECTNESS. The bookkeeping mirrors {@code UnionMapWriter} exactly, verified against the
 * Arrow 19.0.0 bytecode rather than inferred: {@code startMap()} is {@link
 * MapVector#startNewValue(int)}; {@code endMap()} fixes up {@code offset[row+1]}, which is what
 * {@link MapVector#endValue(int, int)} does given {@code startNewValue} seeded it; {@code
 * startEntry()} is {@link StructVector#setIndexDefined(int)}; {@code endEntry()} just advances
 * the entry cursor. Every buffer-growth path is still Arrow's own — {@code startNewValue} grows
 * the offset and validity buffers, {@code setIndexDefined} grows the struct validity buffer, and
 * {@code setSafe} grows the varchar buffers — so this never writes into memory it has not sized.
 *
 * <p>The caller still finishes the batch with {@code root.setRowCount(n)} exactly as before;
 * that propagates the value count down to the struct and its key/value children.
 *
 * <p>NOT thread-safe, and rows must be written in increasing order. One instance per vector per
 * batch, which is how the batch writers use it.
 */
final class MapColumnWriter {

    private final MapVector vector;
    private final StructVector entries;
    private final VarCharVector keyVector;
    private final VarCharVector valueVector;

    /*
     * NO KEY CACHE, DELIBERATELY — this was measured, not assumed.
     *
     * The obvious optimisation is a Map<String,byte[]> memoising the encoded attribute keys, since
     * the same ~30-60 names repeat on every record. It was implemented and profiled, and it LOST:
     * HashMap.computeIfAbsent plus String.hashCode came to 395 samples, 11.8% of Java CPU, making
     * it the fourth-largest item in the whole collector. The reason is that protobuf materialises a
     * fresh String per message, so String's cached hashCode never carries over between records and
     * every lookup rehashes the full key — against which String.getBytes(UTF_8) is intrinsified and
     * near-free for the short Latin-1 keys OTLP uses. Do not reintroduce it without a measurement.
     */

    /** Cursor into the child key/value vectors, running across every row of the batch. */
    private int entryIndex;

    private MapColumnWriter(MapVector vector) {
        this.vector = vector;
        this.entries = (StructVector) vector.getDataVector();
        this.keyVector = (VarCharVector) entries.getChild(MapVector.KEY_NAME);
        this.valueVector = (VarCharVector) entries.getChild(MapVector.VALUE_NAME);
    }

    static MapColumnWriter of(MapVector vector) {
        return new MapColumnWriter(vector);
    }

    /**
     * Writes one row's map. An empty {@code kvList} yields an empty — not null — map, which is
     * what the {@code startMap()}/{@code endMap()} pair it replaces produced.
     */
    void write(int row, List<KeyValue> kvList) {
        vector.startNewValue(row);
        int count = kvList.size();
        // Indexed, not for-each: protobuf's list is RandomAccess, and the iterator allocation
        // showed up in the profile at 1.1% of the map-write path.
        for (int i = 0; i < count; i++) {
            KeyValue kv = kvList.get(i);
            entries.setIndexDefined(entryIndex);
            keyVector.setSafe(entryIndex, utf8(kv.getKey()));
            String value = LogRecordConverter.anyValueToString(kv.getValue());
            if (value == null) {
                valueVector.setNull(entryIndex);
            } else {
                valueVector.setSafe(entryIndex, utf8(value));
            }
            entryIndex++;
        }
        vector.endValue(row, count);
    }

    /**
     * Writes one row from already-encoded entries, for a map whose contents repeat across rows.
     *
     * <p>The caller encodes once and passes the same arrays for every row — which is the whole
     * point for the JWT claims column, where one export request carries exactly one token and so
     * every row of the batch gets an identical map. {@code values} entries may be null.
     */
    void writeEncoded(int row, byte[][] keys, byte[][] values) {
        vector.startNewValue(row);
        for (int i = 0; i < keys.length; i++) {
            entries.setIndexDefined(entryIndex);
            keyVector.setSafe(entryIndex, keys[i]);
            if (values[i] == null) {
                valueVector.setNull(entryIndex);
            } else {
                valueVector.setSafe(entryIndex, values[i]);
            }
            entryIndex++;
        }
        vector.endValue(row, keys.length);
    }

    /** UTF-8 bytes for a value that will be written to a map column. */
    static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
