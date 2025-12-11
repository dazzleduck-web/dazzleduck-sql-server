package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.FlightRecorder;
import io.dazzleduck.sql.flight.model.FlightMetricsSnapshot;

import java.util.concurrent.atomic.AtomicLong;

public class NOOPFlightRecorder implements FlightRecorder {

    // Atomic counters to track metrics in a thread-safe manner
    private final AtomicLong statementCancelCount = new AtomicLong();
    private final AtomicLong preparedStatementCancelCount = new AtomicLong();
    private final AtomicLong statementTimeoutCount = new AtomicLong();
    private final AtomicLong preparedStatementTimeoutCount = new AtomicLong();
    private final AtomicLong completedStatements = new AtomicLong();
    private final AtomicLong failedStatements = new AtomicLong();
    private final AtomicLong completedPreparedStatements = new AtomicLong();
    private final AtomicLong failedPreparedStatements = new AtomicLong();
    private final AtomicLong completedBulkIngest = new AtomicLong();
    private final AtomicLong failedBulkIngest = new AtomicLong();
    private final AtomicLong bytesIn = new AtomicLong();
    private final AtomicLong bytesOut = new AtomicLong();

    @Override
    public void recordStatementCancel() {
        statementCancelCount.incrementAndGet();
    }

    @Override
    public void recordPreparedStatementCancel() {
        preparedStatementCancelCount.incrementAndGet();
    }

    @Override
    public void recordStatementTimeout() {
        statementTimeoutCount.incrementAndGet();
    }

    @Override
    public void recordPreparedStatementTimeout() {
        preparedStatementTimeoutCount.incrementAndGet();
    }

    @Override
    public void startStreamStatement() {
        // Logic for when a stream statement starts (e.g., increment active count)
    }

    @Override
    public void endStreamStatement() {
        completedStatements.incrementAndGet();
    }

    @Override
    public void errorStreamStatement() {
        failedStatements.incrementAndGet();
    }

    @Override
    public void errorStreamPreparedStatement() {
        failedPreparedStatements.incrementAndGet();
    }

    @Override
    public void startStreamPreparedStatement() {
        // Logic for when a prepared stream starts
    }

    @Override
    public void endStreamPreparedStatement() {
        completedPreparedStatements.incrementAndGet();
    }

    @Override
    public void errorPreparedStreamStatement() {
        // Assuming this tracks the same category of error as errorStreamPreparedStatement
        failedPreparedStatements.incrementAndGet();
    }

    @Override
    public void startBulkIngest() {
        // Logic for when bulk ingest starts
    }

    @Override
    public void endBulkIngest() {
        completedBulkIngest.incrementAndGet();
    }

    @Override
    public void errorBulkIngest() {
        failedBulkIngest.incrementAndGet();
    }

    @Override
    public void recordGetStreamPreparedStatement(long networkSize) {
        if (networkSize > 0) {
            bytesIn.addAndGet(networkSize);
        }
    }

    @Override
    public void recordGetStreamStatement(long size) {
        if (size > 0) {
            bytesIn.addAndGet(size);
        }
    }

    @Override
    public double getBytesOut() {
        return bytesOut.get();
    }

    @Override
    public double getBytesIn() {
        return bytesIn.get();
    }

    @Override
    public long getCompletedStatements() {
        return completedStatements.get();
    }

    @Override
    public long getCancelledPreparedStatements() {
        return preparedStatementCancelCount.get();
    }

    @Override
    public long getCancelledStatements() {
        return statementCancelCount.get();
    }

    @Override
    public long getCompletedPreparedStatements() {
        return completedPreparedStatements.get();
    }

    @Override
    public long getCompletedBulkIngest() {
        return completedBulkIngest.get();
    }
}