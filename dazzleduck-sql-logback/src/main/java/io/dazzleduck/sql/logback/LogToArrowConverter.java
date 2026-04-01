package io.dazzleduck.sql.logback;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Provides the Arrow schema for log entries forwarded by the logback appender.
 *
 * <p>Schema fields (in order):
 * <ol>
 *   <li>sequence_number BIGINT</li>
 *   <li>timestamp TIMESTAMP(ms)</li>
 *   <li>level VARCHAR</li>
 *   <li>logger VARCHAR</li>
 *   <li>thread VARCHAR</li>
 *   <li>message VARCHAR</li>
 *   <li>mdc MAP(VARCHAR, VARCHAR)</li>
 *   <li>throwable VARCHAR</li>
 *   <li>marker LIST(VARCHAR)</li>
 *   <li>key_value_pairs MAP(VARCHAR, VARCHAR)</li>
 *   <li>caller_class VARCHAR</li>
 *   <li>caller_method VARCHAR</li>
 *   <li>caller_file VARCHAR</li>
 *   <li>caller_line INT</li>
 * </ol>
 */
public final class LogToArrowConverter {

    private static final Schema SCHEMA = buildSchema();

    public Schema getSchema() {
        return SCHEMA;
    }

    private static Field mapField(String name) {
        return new Field(name, FieldType.nullable(new ArrowType.Map(false)),
                List.of(
                        new Field("entries", FieldType.notNullable(new ArrowType.Struct()),
                                List.of(
                                        new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                                        new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)
                                )
                        )
                )
        );
    }

    private static Schema buildSchema() {
        return new Schema(List.of(
                new Field("sequence_number", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("timestamp", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
                new Field("level", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("logger", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("thread", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("message", FieldType.nullable(new ArrowType.Utf8()), null),
                mapField("mdc"),
                new Field("throwable", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("marker", FieldType.nullable(new ArrowType.List()),
                        List.of(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null))),
                mapField("key_value_pairs"),
                new Field("caller_class", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("caller_method", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("caller_file", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("caller_line", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
    }
}
