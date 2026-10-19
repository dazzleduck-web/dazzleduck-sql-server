package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

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
        write(entries, root, true);
    }

    /**
     * @param stringCopy also write every non-string attribute value, as text, to the string
     *                   attribute maps — the span's, its resource's, and its events' and links'
     */
    public static void write(List<SpanEntry> entries, VectorSchemaRoot root, boolean stringCopy) {
        root.allocateNew();

        VarCharVector traceIdVec        = (VarCharVector)        root.getVector(OtelTraceSchema.COL_TRACE_ID);
        VarCharVector spanIdVec         = (VarCharVector)        root.getVector(OtelTraceSchema.COL_SPAN_ID);
        VarCharVector parentSpanIdVec   = (VarCharVector)        root.getVector(OtelTraceSchema.COL_PARENT_SPAN_ID);
        VarCharVector nameVec           = (VarCharVector)        root.getVector(OtelTraceSchema.COL_NAME);
        VarCharVector kindVec           = (VarCharVector)        root.getVector(OtelTraceSchema.COL_KIND);
        TimeStampMilliTZVector startVec   = (TimeStampMilliTZVector) root.getVector(OtelTraceSchema.COL_START_TIME_MS);
        TimeStampMilliTZVector endVec     = (TimeStampMilliTZVector) root.getVector(OtelTraceSchema.COL_END_TIME_MS);
        BigIntVector durationVec        = (BigIntVector)         root.getVector(OtelTraceSchema.COL_DURATION_MS);
        VarCharVector statusCodeVec     = (VarCharVector)        root.getVector(OtelTraceSchema.COL_STATUS_CODE);
        VarCharVector statusMsgVec      = (VarCharVector)        root.getVector(OtelTraceSchema.COL_STATUS_MESSAGE);
        VarCharVector scopeNameVec      = (VarCharVector)        root.getVector(OtelTraceSchema.COL_SCOPE_NAME);
        VarCharVector scopeVersionVec   = (VarCharVector)        root.getVector(OtelTraceSchema.COL_SCOPE_VERSION);

        AttributeMapsWriter attrWriter = AttributeMapsWriter.of(root,
                OtelTraceSchema.COL_ATTRIBUTES, OtelTraceSchema.COL_TYPED_ATTRIBUTES, stringCopy, entries.size());
        AttributeMapsWriter resAttrWriter = AttributeMapsWriter.of(root,
                OtelTraceSchema.COL_RESOURCE_ATTRIBUTES, OtelTraceSchema.COL_TYPED_RESOURCE_ATTRIBUTES, stringCopy, entries.size());
        SpanListWriter<Span.Event> eventsWriter = SpanListWriter.events(root, stringCopy);
        SpanListWriter<Span.Link> linksWriter = SpanListWriter.links(root, stringCopy);

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

            attrWriter.write(i, span.getAttributesList());
            resAttrWriter.write(i, resource != null ? resource.getAttributesList() : List.of());

            if (scope != null) {
                writeVarChar(scopeNameVec, i, LogRecordConverter.emptyToNull(scope.getName()));
                writeVarChar(scopeVersionVec, i, LogRecordConverter.emptyToNull(scope.getVersion()));
            } else {
                scopeNameVec.setNull(i);
                scopeVersionVec.setNull(i);
            }

            eventsWriter.write(i, span.getEventsList());
            linksWriter.write(i, span.getLinksList());
        }

        attrWriter.finish(entries.size());
        resAttrWriter.finish(entries.size());
        eventsWriter.finish(entries.size());
        linksWriter.finish(entries.size());
        root.setRowCount(entries.size());
    }

    private static void writeVarChar(VarCharVector vec, int index, String value) {
        if (value == null) {
            vec.setNull(index);
        } else {
            vec.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    static String hexOrNull(byte[] bytes) {
        return bytes.length > 0 ? HEX.formatHex(bytes) : null;
    }
}
