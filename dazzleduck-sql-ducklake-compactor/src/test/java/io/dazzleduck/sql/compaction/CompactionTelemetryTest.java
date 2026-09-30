package io.dazzleduck.sql.compaction;

import ch.qos.logback.classic.LoggerContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises the log-export path without a collector: the OTLP exporter is swapped for an in-memory
 * one, and the real Logback root logger is inspected before and after to prove the appender is
 * attached only while export is on.
 */
class CompactionTelemetryTest {

    private static final String SERVICE = "compactor-under-test";

    private static Config metricsOff() {
        return ConfigFactory.parseString("""
                enabled = false
                endpoint = "http://localhost:4317"
                export_interval = 1 minute
                request_timeout = 10 seconds
                service_name = "%s"
                """.formatted(SERVICE));
    }

    private static Config logsOff() {
        return ConfigFactory.parseString("enabled = false");
    }

    private static Config logsOn(String token, String level) {
        return ConfigFactory.parseString("""
                enabled = true
                endpoint = "http://localhost:4317"
                request_timeout = 10 seconds
                token = "%s"
                level = %s
                """.formatted(token, level));
    }

    private static int rootAppenderCount() {
        Iterator<?> it = ((LoggerContext) LoggerFactory.getILoggerFactory())
                .getLogger(Logger.ROOT_LOGGER_NAME).iteratorForAppenders();
        int n = 0;
        while (it.hasNext()) {
            it.next();
            n++;
        }
        return n;
    }

    /** Three-method interface, so a hand-rolled one beats pulling in opentelemetry-sdk-testing. */
    static final class InMemoryExporter implements LogRecordExporter {
        final List<LogRecordData> records = new CopyOnWriteArrayList<>();

        /** The service's own "Exporting logs ..." line is exported too; tests look at their logger only. */
        List<LogRecordData> from(String loggerName) {
            return records.stream()
                    .filter(r -> loggerName.equals(r.getInstrumentationScopeInfo().getName()))
                    .toList();
        }

        @Override
        public CompletableResultCode export(Collection<LogRecordData> logs) {
            records.addAll(logs);
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }
    }

    @Test
    void bothDisabledLeavesLogbackUntouched() {
        int before = rootAppenderCount();
        try (CompactionTelemetry telemetry = CompactionTelemetry.create(metricsOff(), logsOff())) {
            assertNull(telemetry.sdk());
            assertNull(telemetry.appender());
            assertInstanceOf(LoggingMeterRegistry.class, telemetry.registry());
            assertEquals(before, rootAppenderCount());
        }
    }

    @Test
    void logsEnabledWithoutTokenFailsFastBeforeAttaching() {
        int before = rootAppenderCount();
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> CompactionTelemetry.create(metricsOff(), logsOn("", "INFO")));
        assertTrue(e.getMessage().contains("DD_LOGS_OTLP_TOKEN"), e.getMessage());
        assertEquals(before, rootAppenderCount());
    }

    @Test
    void invalidLevelIsRejectedBeforeAnyExporterIsBuilt() {
        int before = rootAppenderCount();
        for (String level : List.of("LOUD", "OFF", "ALL")) {
            assertThrows(IllegalArgumentException.class, () -> CompactionTelemetry.create(
                    metricsOff(), logsOn("tok", level), c -> fail("exporter built for level " + level)));
        }
        assertEquals(before, rootAppenderCount());
    }

    @Test
    void endpointAndTimeoutFallBackToTheMetricsValues() {
        Config metrics = metricsOff().withValue("endpoint",
                com.typesafe.config.ConfigValueFactory.fromAnyRef("http://collector:4317"));
        Config logs = ConfigFactory.parseString("enabled = true, token = tok, level = info");
        Config[] seen = new Config[1];
        try (CompactionTelemetry ignored = CompactionTelemetry.create(metrics, logs, c -> {
            seen[0] = c;
            return new InMemoryExporter();
        })) {
            assertEquals("http://collector:4317", seen[0].getString("endpoint"));
            assertEquals(metrics.getDuration("request_timeout"), seen[0].getDuration("request_timeout"));
        }
    }

    @Test
    void slf4jLinesArriveAsLogRecordsAboveTheThreshold() {
        int before = rootAppenderCount();
        InMemoryExporter exporter = new InMemoryExporter();
        Logger log = LoggerFactory.getLogger("compaction.test");

        try (CompactionTelemetry telemetry = CompactionTelemetry.create(
                metricsOff(), logsOn("tok", "INFO"), c -> exporter)) {
            assertEquals(before + 1, rootAppenderCount());
            assertInstanceOf(LoggingMeterRegistry.class, telemetry.registry());

            log.info("hello {}", "world");
            log.debug("hidden");
            telemetry.sdk().getSdkLoggerProvider().forceFlush().join(5, TimeUnit.SECONDS);

            List<LogRecordData> mine = exporter.from("compaction.test");
            assertEquals(1, mine.size(), () -> "records: " + exporter.records);
            LogRecordData record = mine.get(0);
            assertEquals("hello world", record.getBodyValue().asString());
            assertEquals(Severity.INFO, record.getSeverity());
            assertEquals("INFO", record.getSeverityText());
            assertEquals("compaction.test", record.getInstrumentationScopeInfo().getName());
            assertEquals(SERVICE, record.getResource().getAttribute(AttributeKey.stringKey("service.name")));
            assertNotNull(record.getAttributes().get(AttributeKey.stringKey("thread.name")));
        }

        assertEquals(before, rootAppenderCount());
        log.info("after close");
        assertEquals(1, exporter.from("compaction.test").size());
    }

    @Test
    void exceptionIsCapturedAsAttributes() {
        InMemoryExporter exporter = new InMemoryExporter();
        try (CompactionTelemetry telemetry = CompactionTelemetry.create(
                metricsOff(), logsOn("tok", "WARN"), c -> exporter)) {
            LoggerFactory.getLogger("compaction.test").error("boom", new IllegalStateException("tier blew up"));
            telemetry.sdk().getSdkLoggerProvider().forceFlush().join(5, TimeUnit.SECONDS);

            List<LogRecordData> mine = exporter.from("compaction.test");
            assertEquals(1, mine.size(), () -> "records: " + exporter.records);
            LogRecordData record = mine.get(0);
            assertEquals(Severity.ERROR, record.getSeverity());
            assertEquals(IllegalStateException.class.getName(),
                    record.getAttributes().get(AttributeKey.stringKey("exception.type")));
            assertEquals("tier blew up", record.getAttributes().get(AttributeKey.stringKey("exception.message")));
            String stack = record.getAttributes().get(AttributeKey.stringKey("exception.stacktrace"));
            assertNotNull(stack);
            assertTrue(stack.contains("CompactionTelemetryTest"), stack);
        }
    }

    @Test
    void credentialsInAFailedStartupAreMaskedBeforeExport() {
        // The shape of a failed startup: ConnectionPool wraps the failing statement, DuckDB's cause
        // echoes the connection string. Both reach exception.message / exception.stacktrace.
        var cause = new java.sql.SQLException("IO Error: Unable to connect to Postgres at \"host=db user=svc"
                + " password=FAKE_PG_PASSWORD\": Connection refused");
        var failure = new RuntimeException("Failed to execute on singleton connection: ATTACH"
                + " 'ducklake:postgres:host=db user=svc password=FAKE_PG_PASSWORD' AS lake", cause);
        InMemoryExporter exporter = new InMemoryExporter();
        try (CompactionTelemetry telemetry = CompactionTelemetry.create(
                metricsOff(), logsOn("tok", "INFO"), c -> exporter)) {
            LoggerFactory.getLogger("compaction.test").error("Startup failed: {}", failure.getMessage(), failure);
            telemetry.sdk().getSdkLoggerProvider().forceFlush().join(5, TimeUnit.SECONDS);

            List<LogRecordData> mine = exporter.from("compaction.test");
            assertEquals(1, mine.size(), () -> "records: " + exporter.records);
            LogRecordData record = mine.get(0);
            assertFalse(record.getBodyValue().asString().contains("FAKE_PG_PASSWORD"), record.getBodyValue().asString());
            record.getAttributes().forEach((key, value) ->
                    assertFalse(String.valueOf(value).contains("FAKE_PG_PASSWORD"), key + " = " + value));
            // still useful: the error and its cause are there, only the credential is masked
            String stack = record.getAttributes().get(AttributeKey.stringKey("exception.stacktrace"));
            assertTrue(stack.contains("Unable to connect to Postgres") && stack.contains("password=***"), stack);
        }
    }

    @Test
    void theSameTokenForLogsAndMetricsIsRefused() {
        int before = rootAppenderCount();
        Config metrics = metricsOff().withValue("enabled", com.typesafe.config.ConfigValueFactory.fromAnyRef(true))
                .withValue("token", com.typesafe.config.ConfigValueFactory.fromAnyRef("shared-token"));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> CompactionTelemetry.create(
                metrics, logsOn("Bearer shared-token", "INFO"), c -> fail("exporter built")));
        assertTrue(e.getMessage().contains("same as metrics.token"), e.getMessage());
        assertEquals(before, rootAppenderCount());
    }
}
