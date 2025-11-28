package io.dazzleduck.sql.micrometer.util;

import io.dazzleduck.sql.commons.types.JavaRow;
import io.dazzleduck.sql.commons.types.VectorSchemaRootWriter;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to export Micrometer Meters into Apache Arrow format.
 * Uses {@link VectorSchemaRootWriter} and {@link JavaRow} for schema-driven, type-safe writing.
 */
public final class ArrowFileWriterUtil {
    private static final Logger log = LoggerFactory.getLogger(ArrowFileWriterUtil.class);

    // Prebuilt schema reused across calls (thread-safe & immutable)
    private static final Schema METER_SCHEMA = buildMeterSchema();

    private ArrowFileWriterUtil() {
    }

    /**
     * Writes meters to an Arrow IPC file on disk.
     */
    public static void writeMetersToFile(List<Meter> meters, String filePath) throws IOException {
        if (meters == null || meters.isEmpty()) {
            log.debug("No meters to write to file: {}", filePath);
            return;
        }
        Objects.requireNonNull(filePath, "filePath must not be null");
        try (BufferAllocator allocator = new RootAllocator();
             FileOutputStream fos = new FileOutputStream(filePath)) {
            writeMetersInternal(allocator, meters, fos);
            log.info("Successfully wrote {} meters to Arrow file: {}", meters.size(), filePath);
        }
    }

    /**
     * Converts meters into Arrow IPC bytes (in-memory).
     */
    public static byte[] convertMetersToArrowBytes(List<Meter> meters) throws IOException {
        if (meters == null || meters.isEmpty()) {
            log.debug("No meters to convert to Arrow bytes");
            return new byte[0];
        }

        try (BufferAllocator allocator = new RootAllocator();
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writeMetersInternal(allocator, meters, out);
            return out.toByteArray();
        }
    }

    private static void writeMetersInternal(BufferAllocator allocator, List<Meter> meters, OutputStream os) throws IOException {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(METER_SCHEMA, allocator);
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, os)) {

            writer.start();
            var vectorWriter = VectorSchemaRootWriter.of(METER_SCHEMA);

            // Sort meters for deterministic order
            List<Meter> sortedMeters = meters.stream()
                    .sorted(Comparator
                            .comparing((Meter m) -> m.getId().getType().name())
                            .thenComparing(m -> m.getId().getName()))
                    .toList();

            List<JavaRow> rows = new ArrayList<>(sortedMeters.size());
            for (Meter meter : sortedMeters) {
                rows.add(toRow(meter));
            }

            vectorWriter.writeToVector(rows.toArray(JavaRow[]::new), root);
            writer.writeBatch();
            writer.end();
        }
    }

    /**
     * Converts a single Micrometer Meter to a JavaRow (Arrow record).
     */
    private static JavaRow toRow(Meter meter) {
        Meter.Id id = meter.getId();

        Map<String, String> tagsMap = new LinkedHashMap<>();
        for (Tag t : id.getTags()) {
            tagsMap.put(t.getKey(), t.getValue());
        }

        double value = Double.NaN, min = Double.NaN, max = Double.NaN, mean = Double.NaN;

        try {
            switch (meter) {
                case Counter c -> value = c.count();
                case Gauge g -> value = g.value();
                case Timer tmr -> {
                    HistogramSnapshot snap = tmr.takeSnapshot();
                    value = tmr.count();
                    Double totalTime = snap.total(TimeUnit.SECONDS);
                    max = snap.max(TimeUnit.SECONDS);
                    mean = snap.mean(TimeUnit.SECONDS);

                }
                case DistributionSummary ds -> {
                    value = ds.count();
                    min = 0.0;
                    max = ds.max();
                    mean = ds.mean();
                }
                case LongTaskTimer ltt -> {
                    value = ltt.activeTasks();
                    double totalTime = ltt.duration(TimeUnit.SECONDS);
                    max = totalTime;
                    if (value > 0) mean = totalTime / value;
                }
                case FunctionCounter fc -> value = fc.count();
                case FunctionTimer ft -> {
                    double count = ft.count();
                    double totalTime = ft.totalTime(TimeUnit.SECONDS);
                    value = count;
                    min = 0.0;
                    max = totalTime;
                    if (count > 0) mean = totalTime / count;
                }
                default -> {
                    // Generic fallback for unknown Meter implementations
                    Iterable<Measurement> measurements = meter.measure();
                    double total = 0;
                    int count = 0;
                    for (Measurement m : measurements) {
                        total += m.getValue();
                        count++;
                    }
                    if (count > 0) {
                        value = total;
                        mean = total / count;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error collecting value for meter: {}", id.getName(), e);
        }

        return new JavaRow(new Object[]{
                id.getName(),
                id.getType().name().toLowerCase(),
                tagsMap,
                !Double.isNaN(value) ? value : 0,
                !Double.isNaN(min) ? min : 0,
                !Double.isNaN(max) ? max : 0,
                !Double.isNaN(mean) ? mean : 0
        });
    }

    // -------------------------------------------------------------------------
    // Schema Definition
    // -------------------------------------------------------------------------

    private static Schema buildMeterSchema() {
        List<Field> fields = new ArrayList<>();

        fields.add(new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null));
        fields.add(new Field("type", FieldType.notNullable(new ArrowType.Utf8()), null));

        // tags: Map<String, String>
        Field keyField = new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null);
        Field valField = new Field("value", FieldType.nullable(new ArrowType.Utf8()), null);
        Field entriesStruct = new Field("entries", FieldType.notNullable(new ArrowType.Struct()),
                Arrays.asList(keyField, valField));
        Field tagsField = new Field("tags", FieldType.notNullable(new ArrowType.Map(false)),
                Collections.singletonList(entriesStruct));
        fields.add(tagsField);

        // Numeric fields
        fields.add(new Field("value", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        fields.add(new Field("min", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        fields.add(new Field("max", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        fields.add(new Field("mean", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));

        return new Schema(fields);
    }
}