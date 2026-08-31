package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;

/**
 * Writes a batch of OTLP log records directly into an Arrow VectorSchemaRoot,
 * bypassing the JavaRow intermediate representation.
 */
public class LogRecordBatchWriter {

    private static final HexFormat HEX = HexFormat.of();

    public static void write(List<LogEntry> entries, VectorSchemaRoot root) {
        root.allocateNew();

        TimeStampMilliVector tsVec      = (TimeStampMilliVector) root.getVector(OtelLogSchema.COL_TIMESTAMP);
        TimeStampMilliVector obsTsVec   = (TimeStampMilliVector) root.getVector(OtelLogSchema.COL_OBSERVED_TIMESTAMP);
        IntVector severityNumVec        = (IntVector)            root.getVector(OtelLogSchema.COL_SEVERITY_NUMBER);
        VarCharVector severityTextVec   = (VarCharVector)        root.getVector(OtelLogSchema.COL_SEVERITY_TEXT);
        VarCharVector bodyVec           = (VarCharVector)        root.getVector(OtelLogSchema.COL_BODY);
        VarCharVector traceIdVec        = (VarCharVector)        root.getVector(OtelLogSchema.COL_TRACE_ID);
        VarCharVector spanIdVec         = (VarCharVector)        root.getVector(OtelLogSchema.COL_SPAN_ID);
        IntVector flagsVec              = (IntVector)            root.getVector(OtelLogSchema.COL_FLAGS);
        VarCharVector eventNameVec      = (VarCharVector)        root.getVector(OtelLogSchema.COL_EVENT_NAME);
        MapVector attributesVec         = (MapVector)            root.getVector(OtelLogSchema.COL_ATTRIBUTES);
        MapVector resourceAttributesVec = (MapVector)            root.getVector(OtelLogSchema.COL_RESOURCE_ATTRIBUTES);
        VarCharVector scopeNameVec      = (VarCharVector)        root.getVector(OtelLogSchema.COL_SCOPE_NAME);
        VarCharVector scopeVersionVec   = (VarCharVector)        root.getVector(OtelLogSchema.COL_SCOPE_VERSION);

        UnionMapWriter attrWriter    = attributesVec.getWriter();
        UnionMapWriter resAttrWriter = resourceAttributesVec.getWriter();

        for (int i = 0; i < entries.size(); i++) {
            LogEntry entry = entries.get(i);
            LogRecord record = entry.record();
            Resource resource = entry.resource();
            InstrumentationScope scope = entry.scope();

            if (record.getTimeUnixNano() > 0) {
                tsVec.setSafe(i, record.getTimeUnixNano() / 1_000_000L);
            } else {
                tsVec.setNull(i);
            }
            if (record.getObservedTimeUnixNano() > 0) {
                obsTsVec.setSafe(i, record.getObservedTimeUnixNano() / 1_000_000L);
            } else {
                obsTsVec.setNull(i);
            }

            severityNumVec.setSafe(i, record.getSeverityNumberValue());
            writeVarChar(severityTextVec, i, LogRecordConverter.emptyToNull(record.getSeverityText()));
            writeVarChar(bodyVec, i, record.hasBody() ? LogRecordConverter.anyValueToString(record.getBody()) : null);

            byte[] traceId = record.getTraceId().toByteArray();
            writeVarChar(traceIdVec, i, traceId.length > 0 ? HEX.formatHex(traceId) : null);

            byte[] spanId = record.getSpanId().toByteArray();
            writeVarChar(spanIdVec, i, spanId.length > 0 ? HEX.formatHex(spanId) : null);

            flagsVec.setSafe(i, (int) (record.getFlags() & 0xFFFFFFFFL));
            eventNameVec.setNull(i);

            writeMap(attrWriter, i, record.getAttributesList());
            writeMap(resAttrWriter, i, resource != null ? resource.getAttributesList() : List.of());

            if (scope != null) {
                writeVarChar(scopeNameVec, i, LogRecordConverter.emptyToNull(scope.getName()));
                writeVarChar(scopeVersionVec, i, LogRecordConverter.emptyToNull(scope.getVersion()));
            } else {
                scopeNameVec.setNull(i);
                scopeVersionVec.setNull(i);
            }
        }

        root.setRowCount(entries.size());
    }

    private static void writeVarChar(VarCharVector vec, int index, String value) {
        if (value == null) {
            vec.setNull(index);
        } else {
            vec.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static void writeMap(UnionMapWriter writer, int index, List<KeyValue> kvList) {
        writer.setPosition(index);
        writer.startMap();
        for (KeyValue kv : kvList) {
            OtelSchemaFields.writeEntry(writer, kv.getKey(), LogRecordConverter.anyValueToString(kv.getValue()));
        }
        writer.endMap();
    }
}
