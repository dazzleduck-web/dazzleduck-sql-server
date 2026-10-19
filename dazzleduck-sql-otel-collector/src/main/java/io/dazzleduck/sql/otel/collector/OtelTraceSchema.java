package io.dazzleduck.sql.otel.collector;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Arrow schema for OTLP spans, flattened from the 3-level OTLP hierarchy
 * (ResourceSpans → ScopeSpans → Span).
 */
public class OtelTraceSchema {

    public static final int COL_TRACE_ID = 0;
    public static final int COL_SPAN_ID = 1;
    public static final int COL_PARENT_SPAN_ID = 2;
    public static final int COL_NAME = 3;
    public static final int COL_KIND = 4;
    public static final int COL_START_TIME_MS = 5;
    public static final int COL_END_TIME_MS = 6;
    public static final int COL_DURATION_MS = 7;
    public static final int COL_STATUS_CODE = 8;
    public static final int COL_STATUS_MESSAGE = 9;
    public static final int COL_ATTRIBUTES = 10;
    public static final int COL_RESOURCE_ATTRIBUTES = 11;
    public static final int COL_SCOPE_NAME = 12;
    public static final int COL_SCOPE_VERSION = 13;
    public static final int COL_EVENTS = 14;
    public static final int COL_LINKS = 15;
    /** First of the five typed {@code attributes_*} maps, in {@link AttributeType#TYPED} order. */
    public static final int COL_TYPED_ATTRIBUTES = 16;
    /** First of the five typed {@code resource_attributes_*} maps. */
    public static final int COL_TYPED_RESOURCE_ATTRIBUTES = 21;

    public static final Schema SCHEMA = new Schema(OtelSchemaFields.withTypedAttributes(List.of(
            new Field("trace_id",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("span_id",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("parent_span_id",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("name",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("kind",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("start_time_ms",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, OtelSchemaFields.UTC)), null),
            new Field("end_time_ms",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, OtelSchemaFields.UTC)), null),
            new Field("duration_ms",
                    FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("status_code",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("status_message",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            OtelSchemaFields.mapField("attributes"),
            OtelSchemaFields.mapField("resource_attributes"),
            new Field("scope_name",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("scope_version",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            eventListField(),
            linkListField()
    ), "attributes", "resource_attributes"));

    private static Field eventListField() {
        Field eventStruct = new Field("event",
                FieldType.nullable(new ArrowType.Struct()),
                OtelSchemaFields.withTypedAttributes(List.of(
                        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("time_ms", FieldType.nullable(new ArrowType.Int(64, true)), null),
                        OtelSchemaFields.mapField("attributes")
                ), "attributes"));
        return new Field("events", FieldType.nullable(new ArrowType.List()), List.of(eventStruct));
    }

    private static Field linkListField() {
        Field linkStruct = new Field("link",
                FieldType.nullable(new ArrowType.Struct()),
                OtelSchemaFields.withTypedAttributes(List.of(
                        new Field("trace_id", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("span_id", FieldType.nullable(new ArrowType.Utf8()), null),
                        OtelSchemaFields.mapField("attributes")
                ), "attributes"));
        return new Field("links", FieldType.nullable(new ArrowType.List()), List.of(linkStruct));
    }

}
