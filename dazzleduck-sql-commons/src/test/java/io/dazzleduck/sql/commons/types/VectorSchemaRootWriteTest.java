package io.dazzleduck.sql.commons.types;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.apache.arrow.vector.types.pojo.Field;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

public class VectorSchemaRootWriteTest {

    @Test
    public void testList() {
        var row1 = new JavaRow(new Object[]{
                List.of(1, 2, 3),
                List.of("one", "two")
        });
        var row2 = new JavaRow(new Object[]{
                List.of(7, 8, 9),
                List.of("four", "three")
        });
        JavaRow[] testRows = {row1, row2};
        Field intList = new Field("myIntList", FieldType.nullable(new ArrowType.List()), List.of(new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        Field varCharList = new Field("myCharList", FieldType.nullable(new ArrowType.List()), List.of(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)));

        Schema schema = new Schema(List.of(intList, varCharList));
        var vectorSchemaRootWriter = VectorSchemaRootWriter.of(schema);
        try (var allocator = new RootAllocator()) {
            try (var root = VectorSchemaRoot.create(schema, allocator)) {
                vectorSchemaRootWriter.writeToVector(testRows, root);
                System.out.println(root.contentToTSVString());
            }
        }
    }

    @Test
    public void testMap() {
        var row1 = new JavaRow(new Object[]{Map.of("one", 1, "two", 2)});
        var row2 = new JavaRow(new Object[]{Map.of("six", 6, "seven", 7)});
        JavaRow[] testRows = {row1, row2};
        Field entriesField = new Field(
                "entries",
                FieldType.notNullable(new ArrowType.Struct()),
                List.of(
                        new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                        new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)
                )
        );
        Field mapField = new Field(
                "myMap",
                FieldType.nullable(new ArrowType.Map(false)),
                List.of(entriesField)
        );
        Schema schema = new Schema(List.of(mapField));
        var vectorSchemaRootWriter = VectorSchemaRootWriter.of(schema);
        try (var allocator = new RootAllocator()) {
            try (var root = VectorSchemaRoot.create(schema, allocator)) {
                vectorSchemaRootWriter.writeToVector(testRows, root);
                System.out.println(root.contentToTSVString());
            }
        }
    }

    @Test
    public void testSimple() {
        var row1 = new JavaRow(new Object[]{
                1,
                12L,
                12.01,
                "one",
                List.of(1, 2, 3),
                List.of("one", "two"),
                Map.of("one", 1, "two", 2)
        });
        var row2 = new JavaRow(new Object[]{
                2,
                121L,
                1.01,
                "two",
                List.of(7, 8, 9),
                List.of("four", "three"),
                Map.of("six", 6, "seven", 7)
        });
        JavaRow[] testRows = {row1, row2};
        var intField = new Field("int", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        var bigIntField = new Field("bigInt", FieldType.notNullable(new ArrowType.Int(64, true)), null);
        var floatField = new Field("float", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
        var varCharField = new Field("varChar", FieldType.notNullable(new ArrowType.Utf8()), null);
        var intList = new Field("myIntList", FieldType.nullable(new ArrowType.List()), List.of(new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        var varCharList = new Field("myCharList", FieldType.nullable(new ArrowType.List()), List.of(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)));
        var entriesFiled = new Field(
                "entries",
                FieldType.notNullable(new ArrowType.Struct()),
                List.of(new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                        new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)
                )
        );
        var mapField = new Field(
                "myMap",
                FieldType.nullable(new ArrowType.Map(false)), // keys not sorted
                List.of(entriesFiled)
        );
        Schema schema = new Schema(List.of(intField, bigIntField, floatField, varCharField, intList, varCharList, mapField));

        try (var allocator = new RootAllocator()) {
            var vectorSchemaRootWriter = VectorSchemaRootWriter.of(schema);
            try (var root = VectorSchemaRoot.create(schema, allocator)) {
                vectorSchemaRootWriter.writeToVector(testRows, root);
                System.out.println(root.contentToTSVString());
            }
        }
    }

    @Test
    public void testOfFactory() {
        // --- Build schema with primitive, list, and map ---
        Field name = new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.notNullable(new ArrowType.Int(32, true)), null);

        // List<Int> field
        FieldType intType = new FieldType(true, new ArrowType.Int(32, true), null);
        Field listChild = new Field("intCol", intType, null);
        Field points = new Field("points", FieldType.notNullable(new ArrowType.List()), Collections.singletonList(listChild));

        // Map<Utf8, Int> field
        Field keyField = new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null);
        Field valueField = new Field("value", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Field mapStruct = new Field("entries", FieldType.notNullable(new ArrowType.Struct()), asList(keyField, valueField));
        Field mapField = new Field("scores", FieldType.notNullable(new ArrowType.Map(false)), Collections.singletonList(mapStruct));
        Field timeField = new Field("time", FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null);
        Field dateMilliField = new Field("dateMilli", FieldType.notNullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);
        Field dateDayField = new Field("dateDay", FieldType.notNullable(new ArrowType.Date(DateUnit.DAY)), null);
        Field decimal128Field = new Field("decimal128", FieldType.notNullable(new ArrowType.Decimal(38, 10, 128)), null);
        Field decimal256Field = new Field("decimal256", FieldType.notNullable(new ArrowType.Decimal(76, 20, 256)), null);
        Schema schema = new Schema(asList(name, age, points, mapField, timeField, dateMilliField, dateDayField, decimal128Field, decimal256Field));
        // --- Create rows ---
        JavaRow row1 = new JavaRow(new Object[]{
                "John", 25,
                List.of(10, 20, 30),
                Map.of("math", 90, "english", 85),
                Instant.now().toEpochMilli(),                 // time (Timestamp)
                LocalDate.now().atStartOfDay(ZoneOffset.UTC)  // date (DateMilli)
                        .toInstant().toEpochMilli(),
                (int) LocalDate.now().toEpochDay(),
                new BigDecimal("12345.6708623331"),       // decimal128
                new BigDecimal("1234567890.12345678901234567890") // decimal256
        });

        JavaRow row2 = new JavaRow(new Object[]{
                "David", 30,
                List.of(40, 50),
                Map.of("math", 75, "english", 95),
                Instant.now().toEpochMilli(),                 // time (Timestamp)
                LocalDate.now().atStartOfDay(ZoneOffset.UTC)  // date (DateMilli)
                        .toInstant().toEpochMilli(),
                (int) LocalDate.now().minusDays(1).toEpochDay(),
                new BigDecimal("98765.4307500009"),
                new BigDecimal("9876543210.98765432109876543210")
        });

        JavaRow[] rows = {row1, row2};

        // --- Create VectorSchemaRoot ---
        try (var allocator = new RootAllocator()) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                // --- Use the factory ---
                VectorSchemaRootWriter writer = VectorSchemaRootWriter.of(schema);
                // --- Write rows ---
                writer.writeToVector(rows, root);
                // --- Assertions ---
                assertEquals(2, root.getRowCount());
            }
        }
    }
}
