package io.dazzleduck.sql.otel.collector;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Wide-table Arrow schema for all OTLP metric types (GAUGE, SUM, HISTOGRAM,
 * EXPONENTIAL_HISTOGRAM, SUMMARY). Columns not applicable to a metric type are null.
 */
public class OtelMetricSchema {

    public static final int COL_NAME = 0;
    public static final int COL_DESCRIPTION = 1;
    public static final int COL_UNIT = 2;
    public static final int COL_METRIC_TYPE = 3;
    public static final int COL_START_TIME_MS = 4;
    public static final int COL_TIME_MS = 5;
    public static final int COL_ATTRIBUTES = 6;
    public static final int COL_RESOURCE_ATTRIBUTES = 7;
    public static final int COL_SCOPE_NAME = 8;
    public static final int COL_SCOPE_VERSION = 9;
    public static final int COL_VALUE_DOUBLE = 10;
    public static final int COL_VALUE_INT = 11;
    public static final int COL_COUNT = 12;
    public static final int COL_SUM = 13;
    public static final int COL_BUCKET_COUNTS = 14;
    public static final int COL_EXPLICIT_BOUNDS = 15;
    public static final int COL_QUANTILE_VALUES = 16;
    public static final int COL_IS_MONOTONIC = 17;
    public static final int COL_AGGREGATION_TEMPORALITY = 18;
    /** First of the five typed {@code attributes_*} maps, in {@link AttributeType#TYPED} order. */
    public static final int COL_TYPED_ATTRIBUTES = 19;
    /** First of the five typed {@code resource_attributes_*} maps. */
    public static final int COL_TYPED_RESOURCE_ATTRIBUTES = 24;

    public static final Schema SCHEMA = new Schema(OtelSchemaFields.withTypedAttributes(List.of(
            new Field("name",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("description",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("unit",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("metric_type",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("start_time_ms",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, OtelSchemaFields.UTC)), null),
            new Field("time_ms",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, OtelSchemaFields.UTC)), null),
            OtelSchemaFields.mapField("attributes"),
            OtelSchemaFields.mapField("resource_attributes"),
            new Field("scope_name",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("scope_version",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("value_double",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
            new Field("value_int",
                    FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("count",
                    FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("sum",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
            OtelSchemaFields.listField("bucket_counts", new ArrowType.Int(64, true)),
            OtelSchemaFields.listField("explicit_bounds", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            quantileValuesField(),
            new Field("is_monotonic",
                    FieldType.nullable(new ArrowType.Bool()), null),
            new Field("aggregation_temporality",
                    FieldType.nullable(new ArrowType.Utf8()), null)
    ), "attributes", "resource_attributes"));

    private static Field quantileValuesField() {
        Field qvStruct = new Field("quantile_value",
                FieldType.nullable(new ArrowType.Struct()),
                List.of(
                        new Field("quantile", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                        new Field("value", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
                ));
        return new Field("quantile_values", FieldType.nullable(new ArrowType.List()), List.of(qvStruct));
    }

}
