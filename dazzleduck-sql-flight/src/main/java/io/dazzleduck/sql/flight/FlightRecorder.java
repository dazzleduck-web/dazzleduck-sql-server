package io.dazzleduck.sql.flight;

import io.dazzleduck.sql.flight.model.FlightMetricsSnapshot;

public interface FlightRecorder {
    void recordStatementCancel();

    void recordPreparedStatementCancel();

    void startStreamStatement();

    void endStreamStatement();

    void startStreamPreparedStatement();

    void endStreamPreparedStatement();

    void startBulkIngest();

    void endBulkIngest();

    void recordGetStreamPreparedStatement(long size);

    FlightMetricsSnapshot snapshot();


    // Add other methods which needs to be recorded
}

