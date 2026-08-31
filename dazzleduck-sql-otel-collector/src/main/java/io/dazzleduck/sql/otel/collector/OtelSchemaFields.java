package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared Arrow field builder helpers used across otel signal schemas.
 */
class OtelSchemaFields {

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

    /** Pre-encoded variant — a plain byte copy, no per-call UTF-8 encoding. */
    static void writeEntry(BaseWriter.MapWriter writer, Text key, Text value) {
        writer.startEntry();
        ((BaseWriter.ListWriter) writer.key()).varChar().writeVarChar(key);
        ((BaseWriter.ListWriter) writer.value()).varChar().writeVarChar(value);
        writer.endEntry();
    }

    static Field mapField(String name) {
        return new Field(name,
                FieldType.nullable(new ArrowType.Map(false)),
                List.of(new Field("entries",
                        FieldType.notNullable(new ArrowType.Struct()),
                        List.of(
                                new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                                new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)
                        )
                ))
        );
    }

    static Field listField(String name, ArrowType elementType) {
        return new Field(name,
                FieldType.nullable(new ArrowType.List()),
                List.of(new Field("item", FieldType.nullable(elementType), null)));
    }
}
