package io.dazzleduck.sql.compaction;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.common.Value;
import io.opentelemetry.api.common.ValueType;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.logs.data.Body;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.resources.Resource;

import java.util.Collection;

/**
 * The single point every exported log record passes through: masks credentials (see
 * {@link LogRedaction}) in the body and in every string attribute, which includes
 * {@code exception.message} and {@code exception.stacktrace} with its "Caused by" messages. Console
 * output is not affected; only what leaves the process is.
 */
final class RedactingLogRecordExporter implements LogRecordExporter {

    private final LogRecordExporter delegate;

    RedactingLogRecordExporter(LogRecordExporter delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletableResultCode export(Collection<LogRecordData> logs) {
        return delegate.export(logs.stream().<LogRecordData>map(Redacted::new).toList());
    }

    @Override
    public CompletableResultCode flush() {
        return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
        return delegate.shutdown();
    }

    @SuppressWarnings("deprecation") // getBody(): still part of LogRecordData, used by older marshalers
    private record Redacted(LogRecordData record) implements LogRecordData {

        @Override
        public Value<?> getBodyValue() {
            Value<?> body = record.getBodyValue();
            return body != null && body.getType() == ValueType.STRING
                    ? Value.of(LogRedaction.redact((String) body.getValue()))
                    : body;
        }

        @Override
        public Body getBody() {
            Body body = record.getBody();
            return body.getType() == Body.Type.STRING ? Body.string(LogRedaction.redact(body.asString())) : body;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Attributes getAttributes() {
            AttributesBuilder redacted = Attributes.builder();
            record.getAttributes().forEach((key, value) -> {
                if (key.getType() == AttributeType.STRING) {
                    redacted.put((AttributeKey<String>) key, LogRedaction.redact((String) value));
                } else {
                    redacted.put((AttributeKey<Object>) key, value);
                }
            });
            return redacted.build();
        }

        @Override
        public Resource getResource() {
            return record.getResource();
        }

        @Override
        public InstrumentationScopeInfo getInstrumentationScopeInfo() {
            return record.getInstrumentationScopeInfo();
        }

        @Override
        public long getTimestampEpochNanos() {
            return record.getTimestampEpochNanos();
        }

        @Override
        public long getObservedTimestampEpochNanos() {
            return record.getObservedTimestampEpochNanos();
        }

        @Override
        public SpanContext getSpanContext() {
            return record.getSpanContext();
        }

        @Override
        public Severity getSeverity() {
            return record.getSeverity();
        }

        @Override
        public String getSeverityText() {
            return record.getSeverityText();
        }

        @Override
        public int getTotalAttributeCount() {
            return record.getTotalAttributeCount();
        }

        @Override
        public String getEventName() {
            return record.getEventName();
        }
    }
}
