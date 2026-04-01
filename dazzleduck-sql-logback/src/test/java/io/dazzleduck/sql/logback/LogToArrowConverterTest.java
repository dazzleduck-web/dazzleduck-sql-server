package io.dazzleduck.sql.logback;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LogToArrowConverterTest {

    private final LogToArrowConverter converter = new LogToArrowConverter();

    @Test
    void getSchema_shouldReturnValidSchema() {
        Schema schema = converter.getSchema();

        assertNotNull(schema);
        assertEquals(14, schema.getFields().size());
        assertNotNull(schema.findField("sequence_number"));
        assertNotNull(schema.findField("timestamp"));
        assertNotNull(schema.findField("level"));
        assertNotNull(schema.findField("logger"));
        assertNotNull(schema.findField("thread"));
        assertNotNull(schema.findField("message"));
        assertNotNull(schema.findField("mdc"));
        assertNotNull(schema.findField("throwable"));
        assertNotNull(schema.findField("marker"));
        assertNotNull(schema.findField("key_value_pairs"));
        assertNotNull(schema.findField("caller_class"));
        assertNotNull(schema.findField("caller_method"));
        assertNotNull(schema.findField("caller_file"));
        assertNotNull(schema.findField("caller_line"));
    }

    @Test
    void getSchema_markerShouldBeListType() {
        Field markerField = converter.getSchema().findField("marker");
        assertNotNull(markerField);
        assertInstanceOf(ArrowType.List.class, markerField.getType());
        assertEquals(1, markerField.getChildren().size());
        assertInstanceOf(ArrowType.Utf8.class, markerField.getChildren().get(0).getType());
    }

    @Test
    void getSchema_mdcAndKeyValuePairsShouldBeMapType() {
        Field mdcField = converter.getSchema().findField("mdc");
        Field kvpField = converter.getSchema().findField("key_value_pairs");
        assertInstanceOf(ArrowType.Map.class, mdcField.getType());
        assertInstanceOf(ArrowType.Map.class, kvpField.getType());
    }

    @Test
    void getSchema_sequenceNumberShouldBeBigInt() {
        Field seqField = converter.getSchema().findField("sequence_number");
        ArrowType.Int intType = (ArrowType.Int) seqField.getType();
        assertEquals(64, intType.getBitWidth());
    }

    @Test
    void getSchema_callerLineShouldBeInt32() {
        Field lineField = converter.getSchema().findField("caller_line");
        ArrowType.Int intType = (ArrowType.Int) lineField.getType();
        assertEquals(32, intType.getBitWidth());
    }

    @Test
    void getSchema_shouldReturnSameInstanceEachTime() {
        assertSame(converter.getSchema(), converter.getSchema());
    }
}
