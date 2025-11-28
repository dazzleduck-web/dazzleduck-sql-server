package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.model.FlightMetricsSnapshot;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class MicroMeterFlightRecorderTest {

    private MeterRegistry registry;
    private MicroMeterFlightRecorder recorder;

    @BeforeEach
    void setup() {
        registry = new SimpleMeterRegistry();
        recorder = new MicroMeterFlightRecorder(registry, "producer1");
    }

    private Counter counter(String metric) {
        return registry.find("dazzleduck.flight." + metric + ".count")
                .tag("producer", "producer1")
                .counter();
    }

    private Timer timer(String metric) {
        return registry.find("dazzleduck.flight." + metric + ".timer")
                .tag("producer", "producer1")
                .timer();
    }

    @Test
    void testRecordStatementCancel() {
        recorder.recordStatementCancel();

        Counter c = counter("cancel_statement");
        assertNotNull(c);
        assertEquals(1.0, c.count());
    }

    @Test
    void testRecordPreparedStatementCancel() {
        recorder.recordPreparedStatementCancel();

        Counter c = counter("cancel_prepared_statement");
        assertNotNull(c);
        assertEquals(1.0, c.count());
    }

    @Test
    void testStartAndEndStreamStatement() {
        recorder.startStreamStatement();
        recorder.endStreamStatement();

        assertEquals(1.0, counter("stream_statement").count());
        assertEquals(1.0, counter("stream_statement_completed").count());
        assertEquals(1, timer("stream_statement").count());
    }

    @Test
    void testStreamStatementTimerRecord() {
        recorder.startStreamStatement();
        // simulate 10ms long operation
        try { Thread.sleep(10); } catch (Exception ignored) {}
        recorder.endStreamStatement();

        Timer t = timer("stream_statement");

        assertEquals(1, t.count());
        assertTrue(t.totalTime(TimeUnit.MILLISECONDS) >= 9);
    }

    @Test
    void testStartAndEndPreparedStatement() {
        recorder.startStreamPreparedStatement();
        recorder.endStreamPreparedStatement();

        assertEquals(1.0, counter("stream_prepared_statement").count());
        assertEquals(1.0, counter("stream_prepared_statement_completed").count());
        assertEquals(1, timer("stream_prepared_statement").count());
    }

    @Test
    void testPreparedStatementTimerRecord() {
        recorder.startStreamPreparedStatement();
        try { Thread.sleep(5); } catch (Exception ignored) {}
        recorder.endStreamPreparedStatement();

        Timer t = timer("stream_prepared_statement");

        assertEquals(1, t.count());
        assertTrue(t.totalTime(TimeUnit.MILLISECONDS) >= 4);
    }


    @Test
    void testStartAndEndBulkIngest() {
        recorder.startBulkIngest();
        recorder.endBulkIngest();

        assertEquals(1.0, counter("bulk_ingest").count());
        assertEquals(1.0, counter("bulk_ingest_completed").count());
        assertEquals(1, timer("bulk_ingest").count());
    }

    @Test
    void testBulkIngestTimerRecord() {
        recorder.startBulkIngest();
        try { Thread.sleep(7); } catch (Exception ignored) {}
        recorder.endBulkIngest();

        Timer t = timer("bulk_ingest");

        assertEquals(1, t.count());
        assertTrue(t.totalTime(TimeUnit.MILLISECONDS) >= 6);
    }

    @Test
    void testRecordGetStreamPreparedStatementDoesNotThrow() {
        recorder.recordGetStreamPreparedStatement(123);
    }

    @Test
    void testSnapshotRunningCounts() {

        // start but don't end -> running = 1
        recorder.startStreamStatement();
        recorder.startStreamPreparedStatement();
        recorder.startBulkIngest();

        FlightMetricsSnapshot snap = recorder.snapshot();

        assertEquals(1, snap.runningStatements());
        assertEquals(1, snap.runningPrepared());
        assertEquals(1, snap.runningBulkIngest());
    }

    @Test
    void testStartTimeCaptured() {
        long before = System.currentTimeMillis();
        MicroMeterFlightRecorder r2 = new MicroMeterFlightRecorder(registry, "p1");
        long after = System.currentTimeMillis();

        long recorded = r2.snapshot().startTimeMs();

        assertTrue(recorded >= before && recorded <= after);
    }
}
