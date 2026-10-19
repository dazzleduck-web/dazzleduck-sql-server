package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared Arrow field builder helpers used across otel signal schemas.
 */
class OtelSchemaFields {

    /**
     * Time zone of every timestamp column. OTLP times are instants (nanoseconds since the Unix epoch), so they are
     * written as UTC-adjusted timestamps, which DuckDB reads as {@code TIMESTAMP WITH TIME ZONE}. Without a zone they
     * would read as a plain {@code TIMESTAMP}: a wall-clock value that is UTC only by convention, against which
     * {@code now() - INTERVAL ...} (a {@code TIMESTAMPTZ}) forces a cast of the column and stops filter pushdown.
     */
    static final String UTC = "UTC";

    private static final Map<Schema, Schema> WITH_CLAIMS = new ConcurrentHashMap<>();

    /**
     * Returns {@code base} with the {@link IngestionHandler#CLAIMS_COLUMN} map column appended
     * LAST, so the base schema's column indices — and therefore the per-signal batch writers —
     * stay valid on either variant. Memoized per base schema.
     */
    static Schema withClaimsColumn(Schema base) {
        return WITH_CLAIMS.computeIfAbsent(base, b -> {
            List<Field> fields = new ArrayList<>(b.getFields());
            fields.add(mapField(IngestionHandler.CLAIMS_COLUMN));
            return new Schema(fields);
        });
    }

    static void writeEntry(BaseWriter.MapWriter writer, String key, String value) {
        writer.startEntry();
        ((BaseWriter.ListWriter) writer.key()).varChar().writeVarChar(key);
        if (value != null) {
            ((BaseWriter.ListWriter) writer.value()).varChar().writeVarChar(value);
        } else {
            ((BaseWriter.ListWriter) writer.value()).varChar().writeNull();
        }
        writer.endEntry();
    }

    static Field mapField(String name) {
        return mapField(name, new ArrowType.Utf8());
    }

    /** A {@code MAP(VARCHAR, valueType)} column; values are nullable. */
    static Field mapField(String name, ArrowType valueType) {
        return mapField(name, valueType, null);
    }

    /** As {@link #mapField(String, ArrowType)}, with {@code valueMetadata} on the value field. */
    static Field mapField(String name, ArrowType valueType, Map<String, String> valueMetadata) {
        return new Field(name,
                FieldType.nullable(new ArrowType.Map(false)),
                List.of(new Field("entries",
                        FieldType.notNullable(new ArrowType.Struct()),
                        List.of(
                                new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                                new Field("value", new FieldType(true, valueType, null, valueMetadata), null)
                        )
                ))
        );
    }

    /**
     * The typed companions of the {@code prefix} attribute map, in {@link AttributeType} order:
     * {@code prefix_int}, {@code prefix_double}, {@code prefix_bool}, {@code prefix_bytes} and
     * {@code prefix_json}. The plain {@code prefix} map (strings) is not included.
     */
    static List<Field> typedAttributeFields(String prefix) {
        List<Field> fields = new ArrayList<>(AttributeType.TYPED.size());
        for (AttributeType type : AttributeType.TYPED) {
            fields.add(mapField(prefix + type.suffix(), type.arrowType(), type.metadata()));
        }
        return fields;
    }

    /** {@code head}, then the typed attribute maps for each of {@code prefixes}, in order. */
    static List<Field> withTypedAttributes(List<Field> head, String... prefixes) {
        List<Field> fields = new ArrayList<>(head);
        for (String prefix : prefixes) {
            fields.addAll(typedAttributeFields(prefix));
        }
        return fields;
    }

    static Field listField(String name, ArrowType elementType) {
        return new Field(name,
                FieldType.nullable(new ArrowType.List()),
                List.of(new Field("item", FieldType.nullable(elementType), null)));
    }
}
