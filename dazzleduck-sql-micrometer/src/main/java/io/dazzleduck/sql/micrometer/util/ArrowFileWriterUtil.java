package io.dazzleduck.sql.micrometer.util;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class ArrowFileWriterUtil {
    private static final Logger log = LoggerFactory.getLogger(ArrowFileWriterUtil.class);

    // Write meters to Arrow IPC file
    public static void writeMetersToFile(List<Meter> meters, String filePath) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             FileOutputStream fos = new FileOutputStream(filePath)) {
            writeMeters(allocator, meters, fos);
        }
    }

    // Convert meters to Arrow IPC byte[]
    public static byte[] convertMetersToArrowBytes(List<Meter> meters) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writeMeters(allocator, meters, out);
            return out.toByteArray();
        }
    }

    private static void writeMeters(BufferAllocator allocator, List<Meter> meters, java.io.OutputStream os) throws IOException {

        // <<<--- Define schema --->>>
        List<Field> schemaFields = new ArrayList<>();
        schemaFields.add(new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null));
        schemaFields.add(new Field("type", FieldType.notNullable(new ArrowType.Utf8()), null));

        // Map<String,String>
        Field keyField = new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null);
        Field valField = new Field("value", FieldType.nullable(new ArrowType.Utf8()), null);
        Field entriesStruct = new Field("entries", FieldType.notNullable(new ArrowType.Struct()), Arrays.asList(keyField, valField));
        Field tagsField = new Field("tags", FieldType.notNullable(new ArrowType.Map(false)), Collections.singletonList(entriesStruct));
        schemaFields.add(tagsField);

        schemaFields.add(new Field("value", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        schemaFields.add(new Field("min", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        schemaFields.add(new Field("max", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        schemaFields.add(new Field("mean", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));

        Schema schema = new Schema(schemaFields);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, os)) {

            writer.start();

            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            VarCharVector typeVec = (VarCharVector) root.getVector("type");
            MapVector tagsVec = (MapVector) root.getVector("tags");
            Float8Vector valueVec = (Float8Vector) root.getVector("value");
            Float8Vector minVec = (Float8Vector) root.getVector("min");
            Float8Vector maxVec = (Float8Vector) root.getVector("max");
            Float8Vector meanVec = (Float8Vector) root.getVector("mean");

            // mapWriter for tags using UnionMapWriter
            UnionMapWriter mapWriter = tagsVec.getWriter();

            // Sort meters deterministically
            List<Meter> sortedMeters = meters.stream()
                    .sorted(Comparator.comparing((Meter m) -> m.getId().getType().name())
                            .thenComparing(m -> m.getId().getName()))
                    .toList();

            int row = 0;
            for (Meter meter : sortedMeters) {
                Meter.Id id = meter.getId();
                setString(nameVec, row, id.getName());
                setString(typeVec, row, id.getType().name().toLowerCase());

                Map<String, String> tagsMap = new LinkedHashMap<>();
                for (Tag t : id.getTags()) tagsMap.put(t.getKey(), t.getValue());

                double value = Double.NaN, min = Double.NaN, max = Double.NaN, mean = Double.NaN;

                // Handle all meter types
                if (meter instanceof Counter) {
                    value = ((Counter) meter).count();
                } else if (meter instanceof Gauge) {
                    try {
                        value = ((Gauge) meter).value();
                    } catch (Exception e) {
                        log.error("Error publishing Arrow metrics for Gauge", e);
                    }
                } else if (meter instanceof Timer tmr) {
                    HistogramSnapshot snap = tmr.takeSnapshot();
                    value = snap.total(TimeUnit.SECONDS);
                    double[] vals = Arrays.stream(snap.percentileValues())
                            .mapToDouble(v -> v.value(TimeUnit.SECONDS)).toArray();
                    if (vals.length > 0) {
                        min = Arrays.stream(vals).min().orElse(Double.NaN);
                        max = Arrays.stream(vals).max().orElse(Double.NaN);
                        mean = Arrays.stream(vals).average().orElse(Double.NaN);
                    }
                } else if (meter instanceof DistributionSummary ds) {
                    value = ds.count();
                    min = 0.0;
                    max = ds.max();
                    mean = ds.mean();
                } else if (meter instanceof LongTaskTimer ltt) {
                    value = ltt.activeTasks();
                    double totalTime = ltt.duration(TimeUnit.SECONDS);
                    max = totalTime;
                    if (value > 0) mean = totalTime / value;
                } else if (meter instanceof FunctionCounter fc) {
                    value = fc.count();
                } else if (meter instanceof FunctionTimer ft) {
                    double count = ft.count();
                    double totalTime = ft.totalTime(TimeUnit.SECONDS);
                    value = count;
                    min = 0.0;
                    max = totalTime;
                    if (count > 0) mean = totalTime / count;
                } else {
                    // Generic fallback
                    Iterable<Measurement> measurements = meter.measure();
                    double total = 0;
                    int cnt = 0;
                    for (Measurement m : measurements) {
                        total += m.getValue();
                        cnt++;
                    }
                    if (cnt > 0) {
                        value = total;
                        mean = total / cnt;
                    }
                }

                // Set numeric vectors (respect nulls)
                if (!Double.isNaN(value)) valueVec.setSafe(row, value);
                else valueVec.setNull(row);
                if (!Double.isNaN(min)) minVec.setSafe(row, min);
                else minVec.setNull(row);
                if (!Double.isNaN(max)) maxVec.setSafe(row, max);
                else maxVec.setNull(row);
                if (!Double.isNaN(mean)) meanVec.setSafe(row, mean);
                else meanVec.setNull(row);

                // --- Write tags map for this row ---
                mapWriter.setPosition(row);
                mapWriter.startMap();
                for (Map.Entry<String, String> entry : tagsMap.entrySet()) {
                    mapWriter.startEntry();
                    byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    mapWriter.key().varChar().writeVarChar(Arrays.toString(keyBytes));
                    if (entry.getValue() != null) {
                        byte[] valueBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
                        mapWriter.value().varChar().writeVarChar(Arrays.toString(valueBytes));
                    } else {
                        mapWriter.value().varChar().writeNull();
                    }
                    mapWriter.endEntry();
                }
                mapWriter.endMap();

                row++;
            }

            root.setRowCount(row);
            root.getVector("name").setValueCount(row);
            root.getVector("type").setValueCount(row);
            root.getVector("tags").setValueCount(row);
            root.getVector("value").setValueCount(row);
            root.getVector("min").setValueCount(row);
            root.getVector("max").setValueCount(row);
            root.getVector("mean").setValueCount(row);

            writer.writeBatch();
            writer.end();
        }
    }

    private static void setString(VarCharVector vector, int index, String value) {
        if (value != null) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            vector.setSafe(index, bytes);
        } else {
            vector.setNull(index);
        }
    }
}