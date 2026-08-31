package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;

/**
 * Writes a batch of OTLP spans directly into an Arrow VectorSchemaRoot,
 * bypassing the JavaRow intermediate representation.
 */
public class SpanBatchWriter {

    private static final HexFormat HEX = HexFormat.of();

    public static void write(List<SpanEntry> entries, VectorSchemaRoot root) {
        root.allocateNew();

        VarCharVector traceIdVec        = (VarCharVector)        root.getVector(OtelTraceSchema.COL_TRACE_ID);
        VarCharVector spanIdVec         = (VarCharVector)        root.getVector(OtelTraceSchema.COL_SPAN_ID);
        VarCharVector parentSpanIdVec   = (VarCharVector)        root.getVector(OtelTraceSchema.COL_PARENT_SPAN_ID);
        VarCharVector nameVec           = (VarCharVector)        root.getVector(OtelTraceSchema.COL_NAME);
        VarCharVector kindVec           = (VarCharVector)        root.getVector(OtelTraceSchema.COL_KIND);
        TimeStampMilliVector startVec   = (TimeStampMilliVector) root.getVector(OtelTraceSchema.COL_START_TIME_MS);
        TimeStampMilliVector endVec     = (TimeStampMilliVector) root.getVector(OtelTraceSchema.COL_END_TIME_MS);
        BigIntVector durationVec        = (BigIntVector)         root.getVector(OtelTraceSchema.COL_DURATION_MS);
        VarCharVector statusCodeVec     = (VarCharVector)        root.getVector(OtelTraceSchema.COL_STATUS_CODE);
        VarCharVector statusMsgVec      = (VarCharVector)        root.getVector(OtelTraceSchema.COL_STATUS_MESSAGE);
        MapVector attributesVec         = (MapVector)            root.getVector(OtelTraceSchema.COL_ATTRIBUTES);
        MapVector resourceAttributesVec = (MapVector)            root.getVector(OtelTraceSchema.COL_RESOURCE_ATTRIBUTES);
        VarCharVector scopeNameVec      = (VarCharVector)        root.getVector(OtelTraceSchema.COL_SCOPE_NAME);
        VarCharVector scopeVersionVec   = (VarCharVector)        root.getVector(OtelTraceSchema.COL_SCOPE_VERSION);
        ListVector eventsVec            = (ListVector)           root.getVector(OtelTraceSchema.COL_EVENTS);
        ListVector linksVec             = (ListVector)           root.getVector(OtelTraceSchema.COL_LINKS);

        UnionMapWriter attrWriter    = attributesVec.getWriter();
        UnionMapWriter resAttrWriter = resourceAttributesVec.getWriter();

        for (int i = 0; i < entries.size(); i++) {
            SpanEntry entry = entries.get(i);
            Span span = entry.span();
            Resource resource = entry.resource();
            InstrumentationScope scope = entry.scope();

            writeVarChar(traceIdVec, i, hexOrNull(span.getTraceId().toByteArray()));
            writeVarChar(spanIdVec, i, hexOrNull(span.getSpanId().toByteArray()));
            writeVarChar(parentSpanIdVec, i, hexOrNull(span.getParentSpanId().toByteArray()));
            writeVarChar(nameVec, i, LogRecordConverter.emptyToNull(span.getName()));

            String kind = span.getKind().name();
            if (kind.startsWith("SPAN_KIND_")) kind = kind.substring("SPAN_KIND_".length());
            writeVarChar(kindVec, i, kind);

            long startNanos = span.getStartTimeUnixNano();
            long endNanos   = span.getEndTimeUnixNano();
            long startMs = startNanos > 0 ? startNanos / 1_000_000L : 0L;
            long endMs   = endNanos   > 0 ? endNanos   / 1_000_000L : 0L;
            if (startNanos > 0) startVec.setSafe(i, startMs); else startVec.setNull(i);
            if (endNanos   > 0) endVec.setSafe(i, endMs);     else endVec.setNull(i);
            if (startNanos > 0 && endNanos > 0) durationVec.setSafe(i, endMs - startMs);
            else durationVec.setNull(i);

            Status status = span.getStatus();
            String statusCode = status.getCode().name();
            if (statusCode.startsWith("STATUS_CODE_")) statusCode = statusCode.substring("STATUS_CODE_".length());
            writeVarChar(statusCodeVec, i, statusCode);
            writeVarChar(statusMsgVec, i, LogRecordConverter.emptyToNull(status.getMessage()));

            writeMap(attrWriter, i, span.getAttributesList());
            writeMap(resAttrWriter, i, resource != null ? resource.getAttributesList() : List.of());

            if (scope != null) {
                writeVarChar(scopeNameVec, i, LogRecordConverter.emptyToNull(scope.getName()));
                writeVarChar(scopeVersionVec, i, LogRecordConverter.emptyToNull(scope.getVersion()));
            } else {
                scopeNameVec.setNull(i);
                scopeVersionVec.setNull(i);
            }

            writeEvents(eventsVec.getWriter(), i, span);
            writeLinks(linksVec.getWriter(), i, span);
        }

        root.setRowCount(entries.size());
    }

    private static void writeEvents(BaseWriter.ListWriter writer, int index, Span span) {
        writer.setPosition(index);
        if (span.getEventsCount() == 0) {
            writer.writeNull();
            return;
        }
        writer.startList();
        for (Span.Event e : span.getEventsList()) {
            BaseWriter.StructWriter sw = writer.struct();
            sw.start();
            writeStructVarChar(sw, "name", LogRecordConverter.emptyToNull(e.getName()));
            if (e.getTimeUnixNano() > 0) {
                sw.bigInt("time_ms").writeBigInt(e.getTimeUnixNano() / 1_000_000L);
            } else {
                sw.bigInt("time_ms").writeNull();
            }
            writeStructMap(sw, "attributes", e.getAttributesList());
            sw.end();
        }
        writer.endList();
    }

    private static void writeLinks(BaseWriter.ListWriter writer, int index, Span span) {
        writer.setPosition(index);
        if (span.getLinksCount() == 0) {
            writer.writeNull();
            return;
        }
        writer.startList();
        for (Span.Link l : span.getLinksList()) {
            BaseWriter.StructWriter sw = writer.struct();
            sw.start();
            writeStructVarChar(sw, "trace_id", hexOrNull(l.getTraceId().toByteArray()));
            writeStructVarChar(sw, "span_id", hexOrNull(l.getSpanId().toByteArray()));
            writeStructMap(sw, "attributes", l.getAttributesList());
            sw.end();
        }
        writer.endList();
    }

    private static void writeStructVarChar(BaseWriter.StructWriter sw, String name, String value) {
        if (value == null) {
            sw.varChar(name).writeNull();
        } else {
            sw.varChar(name).writeVarChar(value);
        }
    }

    private static void writeStructMap(BaseWriter.StructWriter sw, String name, List<KeyValue> kvList) {
        BaseWriter.MapWriter mw = sw.map(name, false);
        if (kvList.isEmpty()) {
            mw.writeNull();
            return;
        }
        mw.startMap();
        for (KeyValue kv : kvList) {
            OtelSchemaFields.writeEntry(mw, kv.getKey(), LogRecordConverter.anyValueToString(kv.getValue()));
        }
        mw.endMap();
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

    private static String hexOrNull(byte[] bytes) {
        return bytes.length > 0 ? HEX.formatHex(bytes) : null;
    }
}
