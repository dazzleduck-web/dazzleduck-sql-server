package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Writes a batch of OTLP metrics directly into an Arrow VectorSchemaRoot,
 * bypassing the JavaRow intermediate representation.
 * One MetricEntry may produce multiple rows (one per data point).
 */
public class MetricBatchWriter {

    public static void write(List<MetricEntry> entries, VectorSchemaRoot root) {
        root.allocateNew();

        VarCharVector nameVec           = (VarCharVector)        root.getVector(OtelMetricSchema.COL_NAME);
        VarCharVector descVec           = (VarCharVector)        root.getVector(OtelMetricSchema.COL_DESCRIPTION);
        VarCharVector unitVec           = (VarCharVector)        root.getVector(OtelMetricSchema.COL_UNIT);
        VarCharVector typeVec           = (VarCharVector)        root.getVector(OtelMetricSchema.COL_METRIC_TYPE);
        TimeStampMilliVector startVec   = (TimeStampMilliVector) root.getVector(OtelMetricSchema.COL_START_TIME_MS);
        TimeStampMilliVector timeVec    = (TimeStampMilliVector) root.getVector(OtelMetricSchema.COL_TIME_MS);
        MapVector attributesVec         = (MapVector)            root.getVector(OtelMetricSchema.COL_ATTRIBUTES);
        MapVector resourceAttributesVec = (MapVector)            root.getVector(OtelMetricSchema.COL_RESOURCE_ATTRIBUTES);
        VarCharVector scopeNameVec      = (VarCharVector)        root.getVector(OtelMetricSchema.COL_SCOPE_NAME);
        VarCharVector scopeVersionVec   = (VarCharVector)        root.getVector(OtelMetricSchema.COL_SCOPE_VERSION);
        Float8Vector valueDoubleVec     = (Float8Vector)         root.getVector(OtelMetricSchema.COL_VALUE_DOUBLE);
        BigIntVector valueIntVec        = (BigIntVector)         root.getVector(OtelMetricSchema.COL_VALUE_INT);
        BigIntVector countVec           = (BigIntVector)         root.getVector(OtelMetricSchema.COL_COUNT);
        Float8Vector sumVec             = (Float8Vector)         root.getVector(OtelMetricSchema.COL_SUM);
        ListVector bucketCountsVec      = (ListVector)           root.getVector(OtelMetricSchema.COL_BUCKET_COUNTS);
        ListVector explicitBoundsVec    = (ListVector)           root.getVector(OtelMetricSchema.COL_EXPLICIT_BOUNDS);
        ListVector quantileValuesVec    = (ListVector)           root.getVector(OtelMetricSchema.COL_QUANTILE_VALUES);
        BitVector isMonotonicVec        = (BitVector)            root.getVector(OtelMetricSchema.COL_IS_MONOTONIC);
        VarCharVector aggTempVec        = (VarCharVector)        root.getVector(OtelMetricSchema.COL_AGGREGATION_TEMPORALITY);

        UnionMapWriter attrWriter    = attributesVec.getWriter();
        UnionMapWriter resAttrWriter = resourceAttributesVec.getWriter();

        int row = 0;
        for (MetricEntry entry : entries) {
            Metric metric = entry.metric();
            Resource resource = entry.resource();
            InstrumentationScope scope = entry.scope();

            switch (metric.getDataCase()) {
                case GAUGE -> {
                    for (var dp : metric.getGauge().getDataPointsList()) {
                        writeBase(row, metric, resource, scope, "GAUGE",
                                dp.getStartTimeUnixNano(), dp.getTimeUnixNano(),
                                dp.getAttributesList(),
                                nameVec, descVec, unitVec, typeVec, startVec, timeVec,
                                attrWriter, resAttrWriter, scopeNameVec, scopeVersionVec);
                        writeNumberValue(row, dp, valueDoubleVec, valueIntVec);
                        isMonotonicVec.setNull(row);
                        aggTempVec.setNull(row);
                        nullListCols(row, countVec, sumVec, bucketCountsVec, explicitBoundsVec, quantileValuesVec);
                        row++;
                    }
                }
                case SUM -> {
                    var sum = metric.getSum();
                    String temporality = stripAggPrefix(sum.getAggregationTemporality().name());
                    for (var dp : sum.getDataPointsList()) {
                        writeBase(row, metric, resource, scope, "SUM",
                                dp.getStartTimeUnixNano(), dp.getTimeUnixNano(),
                                dp.getAttributesList(),
                                nameVec, descVec, unitVec, typeVec, startVec, timeVec,
                                attrWriter, resAttrWriter, scopeNameVec, scopeVersionVec);
                        writeNumberValue(row, dp, valueDoubleVec, valueIntVec);
                        isMonotonicVec.setSafe(row, sum.getIsMonotonic() ? 1 : 0);
                        writeVarChar(aggTempVec, row, temporality);
                        nullListCols(row, countVec, sumVec, bucketCountsVec, explicitBoundsVec, quantileValuesVec);
                        row++;
                    }
                }
                case HISTOGRAM -> {
                    var hist = metric.getHistogram();
                    String temporality = stripAggPrefix(hist.getAggregationTemporality().name());
                    for (var dp : hist.getDataPointsList()) {
                        writeBase(row, metric, resource, scope, "HISTOGRAM",
                                dp.getStartTimeUnixNano(), dp.getTimeUnixNano(),
                                dp.getAttributesList(),
                                nameVec, descVec, unitVec, typeVec, startVec, timeVec,
                                attrWriter, resAttrWriter, scopeNameVec, scopeVersionVec);
                        writeHistogram(row, dp, countVec, sumVec, bucketCountsVec, explicitBoundsVec);
                        writeVarChar(aggTempVec, row, temporality);
                        valueDoubleVec.setNull(row);
                        valueIntVec.setNull(row);
                        isMonotonicVec.setNull(row);
                        quantileValuesVec.setNull(row);
                        row++;
                    }
                }
                case EXPONENTIAL_HISTOGRAM -> {
                    var expHist = metric.getExponentialHistogram();
                    String temporality = stripAggPrefix(expHist.getAggregationTemporality().name());
                    for (var dp : expHist.getDataPointsList()) {
                        writeBase(row, metric, resource, scope, "EXPONENTIAL_HISTOGRAM",
                                dp.getStartTimeUnixNano(), dp.getTimeUnixNano(),
                                dp.getAttributesList(),
                                nameVec, descVec, unitVec, typeVec, startVec, timeVec,
                                attrWriter, resAttrWriter, scopeNameVec, scopeVersionVec);
                        countVec.setSafe(row, dp.getCount());
                        if (dp.hasSum()) sumVec.setSafe(row, dp.getSum()); else sumVec.setNull(row);
                        writeVarChar(aggTempVec, row, temporality);
                        valueDoubleVec.setNull(row);
                        valueIntVec.setNull(row);
                        isMonotonicVec.setNull(row);
                        bucketCountsVec.setNull(row);
                        explicitBoundsVec.setNull(row);
                        quantileValuesVec.setNull(row);
                        row++;
                    }
                }
                case SUMMARY -> {
                    for (var dp : metric.getSummary().getDataPointsList()) {
                        writeBase(row, metric, resource, scope, "SUMMARY",
                                dp.getStartTimeUnixNano(), dp.getTimeUnixNano(),
                                dp.getAttributesList(),
                                nameVec, descVec, unitVec, typeVec, startVec, timeVec,
                                attrWriter, resAttrWriter, scopeNameVec, scopeVersionVec);
                        countVec.setSafe(row, dp.getCount());
                        sumVec.setSafe(row, dp.getSum());
                        writeQuantileValues(row, dp, quantileValuesVec);
                        valueDoubleVec.setNull(row);
                        valueIntVec.setNull(row);
                        isMonotonicVec.setNull(row);
                        aggTempVec.setNull(row);
                        bucketCountsVec.setNull(row);
                        explicitBoundsVec.setNull(row);
                        row++;
                    }
                }
                default -> {} // DATA_NOT_SET — ignore
            }
        }

        root.setRowCount(row);
    }

    private static void writeBase(int row, Metric metric, Resource resource, InstrumentationScope scope,
                                  String metricType, long startNanos, long timeNanos,
                                  List<KeyValue> attributes,
                                  VarCharVector nameVec, VarCharVector descVec, VarCharVector unitVec,
                                  VarCharVector typeVec, TimeStampMilliVector startVec, TimeStampMilliVector timeVec,
                                  UnionMapWriter attrWriter, UnionMapWriter resAttrWriter,
                                  VarCharVector scopeNameVec, VarCharVector scopeVersionVec) {
        writeVarChar(nameVec, row, metric.getName());
        writeVarChar(descVec, row, LogRecordConverter.emptyToNull(metric.getDescription()));
        writeVarChar(unitVec, row, LogRecordConverter.emptyToNull(metric.getUnit()));
        writeVarChar(typeVec, row, metricType);
        if (startNanos > 0) startVec.setSafe(row, startNanos / 1_000_000L); else startVec.setNull(row);
        if (timeNanos  > 0) timeVec.setSafe(row, timeNanos  / 1_000_000L);  else timeVec.setNull(row);
        writeMap(attrWriter, row, attributes);
        writeMap(resAttrWriter, row, resource != null ? resource.getAttributesList() : List.of());
        if (scope != null) {
            writeVarChar(scopeNameVec, row, LogRecordConverter.emptyToNull(scope.getName()));
            writeVarChar(scopeVersionVec, row, LogRecordConverter.emptyToNull(scope.getVersion()));
        } else {
            scopeNameVec.setNull(row);
            scopeVersionVec.setNull(row);
        }
    }

    private static void writeNumberValue(int row, NumberDataPoint dp,
                                         Float8Vector valueDoubleVec, BigIntVector valueIntVec) {
        switch (dp.getValueCase()) {
            case AS_DOUBLE -> { valueDoubleVec.setSafe(row, dp.getAsDouble()); valueIntVec.setNull(row); }
            case AS_INT    -> { valueIntVec.setSafe(row, dp.getAsInt());       valueDoubleVec.setNull(row); }
            default        -> { valueDoubleVec.setNull(row); valueIntVec.setNull(row); }
        }
    }

    private static void writeHistogram(int row, HistogramDataPoint dp,
                                        BigIntVector countVec, Float8Vector sumVec,
                                        ListVector bucketCountsVec, ListVector explicitBoundsVec) {
        countVec.setSafe(row, dp.getCount());
        if (dp.hasSum()) sumVec.setSafe(row, dp.getSum()); else sumVec.setNull(row);

        List<Long> buckets = dp.getBucketCountsList();
        if (buckets.isEmpty()) {
            bucketCountsVec.setNull(row);
        } else {
            BaseWriter.ListWriter bw = bucketCountsVec.getWriter();
            bw.setPosition(row);
            bw.startList();
            for (long b : buckets) bw.bigInt().writeBigInt(b);
            bw.endList();
        }

        List<Double> bounds = dp.getExplicitBoundsList();
        if (bounds.isEmpty()) {
            explicitBoundsVec.setNull(row);
        } else {
            BaseWriter.ListWriter ew = explicitBoundsVec.getWriter();
            ew.setPosition(row);
            ew.startList();
            for (double b : bounds) ew.float8().writeFloat8(b);
            ew.endList();
        }
    }

    private static void writeQuantileValues(int row, SummaryDataPoint dp, ListVector quantileValuesVec) {
        if (dp.getQuantileValuesCount() == 0) {
            quantileValuesVec.setNull(row);
            return;
        }
        BaseWriter.ListWriter lw = quantileValuesVec.getWriter();
        lw.setPosition(row);
        lw.startList();
        for (var qv : dp.getQuantileValuesList()) {
            BaseWriter.StructWriter sw = lw.struct();
            sw.start();
            sw.float8("quantile").writeFloat8(qv.getQuantile());
            sw.float8("value").writeFloat8(qv.getValue());
            sw.end();
        }
        lw.endList();
    }

    private static void nullListCols(int row, BigIntVector countVec, Float8Vector sumVec,
                                     ListVector bucketCountsVec, ListVector explicitBoundsVec,
                                     ListVector quantileValuesVec) {
        countVec.setNull(row);
        sumVec.setNull(row);
        bucketCountsVec.setNull(row);
        explicitBoundsVec.setNull(row);
        quantileValuesVec.setNull(row);
    }

    private static void writeMap(UnionMapWriter writer, int index, List<KeyValue> kvList) {
        writer.setPosition(index);
        writer.startMap();
        for (KeyValue kv : kvList) {
            OtelSchemaFields.writeEntry(writer, kv.getKey(), LogRecordConverter.anyValueToString(kv.getValue()));
        }
        writer.endMap();
    }

    private static void writeVarChar(VarCharVector vec, int index, String value) {
        if (value == null) {
            vec.setNull(index);
        } else {
            vec.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String stripAggPrefix(String name) {
        String prefix = "AGGREGATION_TEMPORALITY_";
        return name.startsWith(prefix) ? name.substring(prefix.length()) : name;
    }
}
