package io.dazzleduck.sql.micrometer.util;


import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.*;

import java.util.List;

public final class ArrowMetricSchema {

    private ArrowMetricSchema() {}

    public static final Schema SCHEMA = new Schema(List.of(
            new Field("timestamp", FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
            new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
            new Field("type", FieldType.notNullable(new ArrowType.Utf8()), null),

            // tags: Map<String, String>
            new Field("tags", FieldType.notNullable(new ArrowType.Map(false)), List.of(new Field("entries", FieldType.notNullable(new ArrowType.Struct()), List.of(new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null), new Field("value", FieldType.nullable(new ArrowType.Utf8()), null))))),

            new Field("value", fp(), null),
            new Field("min", fp(), null),
            new Field("max", fp(), null),
            new Field("mean", fp(), null)
    ));

    private static FieldType fp() {
        return FieldType.nullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
        );
    }
}
