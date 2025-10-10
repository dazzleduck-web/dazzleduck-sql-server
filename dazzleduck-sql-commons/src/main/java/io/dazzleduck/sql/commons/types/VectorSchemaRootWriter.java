package io.dazzleduck.sql.commons.types;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;


public class VectorSchemaRootWriter {

    @SuppressWarnings("rawtypes")
    private final VectorWriter[] functions;
    private final Schema schema;

    public VectorSchemaRootWriter(Schema schema,
                                  @SuppressWarnings("rawtypes") VectorWriter... functions) {
        this.functions = functions;
        this.schema = schema;
    }

    public VectorSchemaRoot writeToVector(JavaRow[] rows, VectorSchemaRoot root) {
        root.allocateNew();
        for (int i = 0; i < rows.length; i++) {
            for (int j = 0; j < functions.length; j++) {
                var function = functions[j];
                var vector = root.getVector(j);
                //noinspection unchecked
                function.write(vector, i, rows[i].get(j));
            }
        }
        root.setRowCount(rows.length);
        return root;
    }

    public static VectorSchemaRootWriter of(Schema schema) {
        List<VectorWriter<?>> listOfFunctions = new ArrayList<>();
        for (var field : schema.getFields()) {
            listOfFunctions.add(createWriter(field));
        }
        return new VectorSchemaRootWriter(schema, listOfFunctions.toArray(new VectorWriter[0]));
    }

    private static VectorWriter<?> createWriter(Field field) {
        var type = field.getType();
        // ---------- Primitive types ----------
        if (type instanceof ArrowType.Int intType) {
            if (intType.getBitWidth() == 32) return new VectorWriter.IntVectorWriter();
            else if (intType.getBitWidth() == 64) return new VectorWriter.BigVectorWriter();
            else throw new UnsupportedOperationException("Unsupported int bit width: " + intType.getBitWidth());
        } else if (type instanceof ArrowType.FloatingPoint) {
            return new VectorWriter.FloatVectorWriter();
        } else if (type instanceof ArrowType.Utf8) {
            return new VectorWriter.VarCharVectorWriter();
        } else if (type instanceof ArrowType.Timestamp) {
            return new VectorWriter.TimeStampMilliTZVectorWriter();
        } else if (type instanceof ArrowType.Date dateType) {
            if (dateType.getUnit() == DateUnit.DAY) return new VectorWriter.DateDayVectorWriter();
            else return new VectorWriter.DateMilliVectorVectorWriter();
        }  else if (type instanceof ArrowType.Decimal decimalType) {
            if (decimalType.getBitWidth() == 128) return new VectorWriter.DecimalVectorWriter();
            else if (decimalType.getBitWidth() == 256) return new VectorWriter.Decimal256VectorWriter();
            else throw new UnsupportedOperationException("Unsupported decimal bit width: " + decimalType.getBitWidth());
        }
        // ---------- List type ----------
        else if (type instanceof ArrowType.List) {
            var elementField = field.getChildren().get(0);
            var elementType = elementField.getType();

            ElementWriteFunction elementFunc;

            if (elementType instanceof ArrowType.Int intType) {
                elementFunc = intType.getBitWidth() == 32 ? ElementWriteFunction.INT : ElementWriteFunction.BIGINT;
            } else if (elementType instanceof ArrowType.FloatingPoint) {
                elementFunc = ElementWriteFunction.DOUBLE;
            } else if (elementType instanceof ArrowType.Utf8) {
                elementFunc = ElementWriteFunction.VARCHAR;
            } else {
                throw new UnsupportedOperationException("Unsupported List element type: " + elementType);
            }

            return new VectorWriter.ListVectorWriter(elementFunc);
        }

        // ---------- Map type ----------
        else if (type instanceof ArrowType.Map) {
            var structField = field.getChildren().get(0); // Struct inside Map
            var keyField = structField.getChildren().get(0);
            var valueField = structField.getChildren().get(1);

            // Key function
            ElementWriteFunction keyFunc;
            var keyType = keyField.getType();
            if (keyType instanceof ArrowType.Int intType) {
                keyFunc = intType.getBitWidth() == 32 ? ElementWriteFunction.INT : ElementWriteFunction.BIGINT;
            } else if (keyType instanceof ArrowType.Utf8) {
                keyFunc = ElementWriteFunction.VARCHAR;
            } else {
                throw new UnsupportedOperationException("Unsupported Map key type: " + keyType);
            }

            // Value function
            ElementWriteFunction valueFunc;
            var valueType = valueField.getType();
            if (valueType instanceof ArrowType.Int intType) {
                valueFunc = intType.getBitWidth() == 32 ? ElementWriteFunction.INT : ElementWriteFunction.BIGINT;
            } else if (valueType instanceof ArrowType.FloatingPoint) {
                valueFunc = ElementWriteFunction.DOUBLE;
            } else if (valueType instanceof ArrowType.Utf8) {
                valueFunc = ElementWriteFunction.VARCHAR;
            } else {
                throw new UnsupportedOperationException("Unsupported Map value type: " + valueType);
            }
            return new VectorWriter.MapVectorWriter(keyFunc, valueFunc);
        }
        else {
            throw new UnsupportedOperationException("Unsupported ArrowType: " + type);
        }
    }
}
