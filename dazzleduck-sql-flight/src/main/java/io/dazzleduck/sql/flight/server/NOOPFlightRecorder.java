package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.FlightRecorder;
import io.dazzleduck.sql.flight.model.FlightMetricsSnapshot;

public class NOOPFlightRecorder implements FlightRecorder {
    @Override
    public void recordStatementCancel() {

    }

    @Override
    public void recordPreparedStatementCancel() {

    }

    @Override
    public void recordStatementTimeout() {

    }

    @Override
    public void recordPreparedStatementTimeout() {

    }

    @Override
    public void startStreamStatement() {

    }

    @Override
    public void endStreamStatement() {

    }

    @Override
    public void errorStreamStatement() {

    }

    @Override
    public void errorStreamPreparedStatement() {

    }

    @Override
    public void startStreamPreparedStatement() {

    }

    @Override
    public void endStreamPreparedStatement() {

    }

    @Override
    public void errorPreparedStreamStatement() {

    }

    @Override
    public void startBulkIngest() {

    }

    @Override
    public void endBulkIngest() {

    }

    @Override
    public void errorBulkIngest() {

    }

    @Override
    public void recordGetStreamPreparedStatement(long networkSize) {

    }

    @Override
    public void recordGetStreamStatement(long size) {

    }

    @Override
    public FlightMetricsSnapshot snapshot() {
        return null;
    }

    @Override
    public double getBytesOut() {
        return 0;
    }

    @Override
    public double getBytesIn() {
        return 0;
    }

    @Override
    public long getCompletedStatements() {
        return 0;
    }

    @Override
    public long getCancelledPreparedStatements() {
        return 0;
    }

    @Override
    public long getCancelledStatements() {
        return 0;
    }

    @Override
    public long getCompletedPreparedStatements() {
        return 0;
    }

    @Override
    public long getCompletedBulkIngest() {
        return 0;
    }
}