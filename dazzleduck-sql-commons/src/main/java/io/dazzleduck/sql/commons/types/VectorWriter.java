package io.dazzleduck.sql.commons.types;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.holders.Decimal256Holder;
import org.apache.arrow.vector.holders.DecimalHolder;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;


public interface VectorWriter<V> {
    void write(V listVector, int index, Object value);


    class VarCharVectorWriter implements VectorWriter<VarCharVector> {
        @Override
        public void write(VarCharVector varCharVector, int index, Object value) {
            if (value == null) {
                varCharVector.setNull(index);
                return;
            }
            var v = (String) value;
            varCharVector.set(index, v.getBytes(StandardCharsets.UTF_8));
        }
    }

    class IntVectorWriter implements VectorWriter<IntVector> {
        @Override
        public void write(IntVector intVector, int index, Object value) {
            if (value == null) {
                intVector.setNull(index);
                return;
            }
            var v = (Integer) value;
            intVector.set(index, v);
        }
    }

    class BigVectorWriter implements VectorWriter<BigIntVector> {
        @Override
        public void write(BigIntVector bigIntVector, int index, Object value) {
            if (value == null) {
                bigIntVector.setNull(index);
                return;
            }
            var v = (Long) value;
            bigIntVector.set(index, v);
        }
    }

    class FloatVectorWriter implements VectorWriter<Float8Vector> {
        @Override
        public void write(Float8Vector float8Vector, int index, Object value) {
            if (value == null) {
                float8Vector.setNull(index);
                return;
            }
            var v = (Double) value;
            float8Vector.set(index, v);
        }
    }

    class TimeStampMilliTZVectorWriter implements VectorWriter<TimeStampMilliTZVector> {
        @Override
        public void write(TimeStampMilliTZVector timeStampMilliTZVector, int index, Object value) {
            if (value == null) {
                timeStampMilliTZVector.setNull(index);
                return;
            }
            var v = (Long) value;
            timeStampMilliTZVector.set(index, v);
        }
    }

    class DecimalVectorWriter implements VectorWriter<DecimalVector> {
        @Override
        public void write(DecimalVector decimalVector, int index, Object value) {
            if (value == null) {
                decimalVector.setNull(index);
                return;
            }
            var v = (BigDecimal) value;
            decimalVector.set(index, v);
        }
    }

    class Decimal256VectorWriter implements VectorWriter<Decimal256Vector> {
        @Override
        public void write(Decimal256Vector decimal256Vector, int index, Object value) {
            if (value == null) {
                decimal256Vector.setNull(index);
                return;
            }
            var v = (BigDecimal) value;
            decimal256Vector.set(index, v);
        }
    }

    class DateMilliVectorVectorWriter implements VectorWriter<DateMilliVector> {
        @Override
        public void write(DateMilliVector dateMilliVector, int index, Object value) {
            if (value == null) {
                dateMilliVector.setNull(index);
                return;
            }
            var v = (Long) value;
            dateMilliVector.set(index, v);
        }
    }

    class DateDayVectorWriter implements VectorWriter<DateDayVector> {
        @Override
        public void write(DateDayVector dateDayVector, int index, Object value) {
            if (value == null) {
                dateDayVector.setNull(index);
                return;
            }
            var v = (Integer) value;
            dateDayVector.set(index, v);
        }
    }

    class ListVectorWriter implements VectorWriter<ListVector> {
        private final ElementWriteFunction elementWriteFunction;

        public ListVectorWriter(ElementWriteFunction elementWriteFunction) {
            this.elementWriteFunction = elementWriteFunction;
        }

        public void write(ListVector listVector, int index, Object value) {
            UnionListWriter writer = listVector.getWriter();
            if (value == null) {
                writer.writeNull();
                return;
            }
            writer.setPosition(index);
            writer.startList();
            @SuppressWarnings("unchecked")
            var v = (List<Object>) value;
            for (Object object : v) {
                elementWriteFunction.apply(writer, object);
            }
            writer.endList();
        }
    }

    class MapVectorWriter implements VectorWriter<MapVector> {
        private final ElementWriteFunction keyWrite;
        private final ElementWriteFunction valueWrite;

        public MapVectorWriter(ElementWriteFunction keyWrite, ElementWriteFunction valueWrite) {
            this.keyWrite = keyWrite;
            this.valueWrite = valueWrite;
        }

        @Override
        public void write(MapVector mapVector, int index, Object value) {
            UnionMapWriter mapWriter = new UnionMapWriter(mapVector);
            mapWriter.setPosition(index);
            mapWriter.startMap();
            @SuppressWarnings("unchecked")
            var m = (Map<Object, Object>) value;
            for (var e : m.entrySet()) {
                mapWriter.startEntry();
                keyWrite.apply(mapWriter.key(), e.getKey());
                valueWrite.apply(mapWriter.value(), e.getValue());
                mapWriter.endEntry();
            }
            mapWriter.endMap();
        }
    }
}