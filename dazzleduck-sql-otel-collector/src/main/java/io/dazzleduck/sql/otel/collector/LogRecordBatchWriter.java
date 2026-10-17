package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

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
        write(entries, root, true);
    }

    public static void write(List<LogEntry> entries, VectorSchemaRoot root, boolean stringCopy) {
        write(entries, root, stringCopy, null);
    }

    /**
     * @param stringCopy also write every non-string attribute value, as text, to the
     *                   {@code attributes} / {@code resource_attributes} string maps
     * @param scan       collects the batch's attribute name/type mappings for
     *                   {@link AttributeTypeRegistry}, or null to collect nothing
     */
    public static void write(List<LogEntry> entries, VectorSchemaRoot root, boolean stringCopy,
                             AttributeTypeScan scan) {
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
        VarCharVector scopeNameVec      = (VarCharVector)        root.getVector(OtelLogSchema.COL_SCOPE_NAME);
        VarCharVector scopeVersionVec   = (VarCharVector)        root.getVector(OtelLogSchema.COL_SCOPE_VERSION);

        AttributeMapsWriter attrWriter = AttributeMapsWriter.of(root,
                OtelLogSchema.COL_ATTRIBUTES, OtelLogSchema.COL_TYPED_ATTRIBUTES, stringCopy, entries.size(),
                AttributeTypeScan.cursorOf(scan, AttributeScope.ATTRIBUTE));
        AttributeMapsWriter resAttrWriter = AttributeMapsWriter.of(root,
                OtelLogSchema.COL_RESOURCE_ATTRIBUTES, OtelLogSchema.COL_TYPED_RESOURCE_ATTRIBUTES, stringCopy,
                entries.size(), AttributeTypeScan.cursorOf(scan, AttributeScope.RESOURCE));

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

            attrWriter.write(i, record.getAttributesList());
            resAttrWriter.write(i, resource != null ? resource.getAttributesList() : List.of());

            if (scope != null) {
                writeVarChar(scopeNameVec, i, LogRecordConverter.emptyToNull(scope.getName()));
                writeVarChar(scopeVersionVec, i, LogRecordConverter.emptyToNull(scope.getVersion()));
            } else {
                scopeNameVec.setNull(i);
                scopeVersionVec.setNull(i);
            }
        }

        attrWriter.finish(entries.size());
        resAttrWriter.finish(entries.size());
        root.setRowCount(entries.size());
    }

    private static void writeVarChar(VarCharVector vec, int index, String value) {
        if (value == null) {
            vec.setNull(index);
        } else {
            vec.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
