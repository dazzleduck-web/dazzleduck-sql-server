package io.dazzleduck.sql.otel.collector;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Arrow schema for OTLP log records, flattened from the 3-level OTLP hierarchy
 * (ResourceLogs → ScopeLogs → LogRecord).
 */
public class OtelLogSchema {

    // Column indices
    public static final int COL_TIMESTAMP = 0;
    public static final int COL_OBSERVED_TIMESTAMP = 1;
    public static final int COL_SEVERITY_NUMBER = 2;
    public static final int COL_SEVERITY_TEXT = 3;
    public static final int COL_BODY = 4;
    public static final int COL_TRACE_ID = 5;
    public static final int COL_SPAN_ID = 6;
    public static final int COL_FLAGS = 7;
    public static final int COL_EVENT_NAME = 8;
    public static final int COL_ATTRIBUTES = 9;
    public static final int COL_RESOURCE_ATTRIBUTES = 10;
    public static final int COL_SCOPE_NAME = 11;
    public static final int COL_SCOPE_VERSION = 12;
    /** First of the five typed {@code attributes_*} maps, in {@link AttributeType#TYPED} order. */
    public static final int COL_TYPED_ATTRIBUTES = 13;
    /** First of the five typed {@code resource_attributes_*} maps. */
    public static final int COL_TYPED_RESOURCE_ATTRIBUTES = 18;

    public static final Schema SCHEMA = new Schema(OtelSchemaFields.withTypedAttributes(List.of(
            // LogRecord fields
            new Field("timestamp",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, OtelSchemaFields.UTC)), null),
            new Field("observed_timestamp",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, OtelSchemaFields.UTC)), null),
            new Field("severity_number",
                    FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("severity_text",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("body",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("trace_id",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("span_id",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("flags",
                    FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("event_name",
                    FieldType.nullable(new ArrowType.Utf8()), null),

            // LogRecord.attributes: Map<String, String>; typed companions are appended at the end
            OtelSchemaFields.mapField("attributes"),

            // Resource.attributes: Map<String, String>
            OtelSchemaFields.mapField("resource_attributes"),

            // InstrumentationScope fields
            new Field("scope_name",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("scope_version",
                    FieldType.nullable(new ArrowType.Utf8()), null)
    ), "attributes", "resource_attributes"));

}
