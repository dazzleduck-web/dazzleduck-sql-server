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
    public void startStreamStatement() {

    }

    @Override
    public void endStreamStatement() {

    }

    @Override
    public void startStreamPreparedStatement() {

    }

    @Override
    public void endStreamPreparedStatement() {

    }

    @Override
    public void startBulkIngest() {

    }

    @Override
    public void endBulkIngest() {

    }
    @Override
    public void recordGetStreamPreparedStatement(long networkSize) {

    }

    @Override
    public FlightMetricsSnapshot snapshot() {
        return null;
    }
}