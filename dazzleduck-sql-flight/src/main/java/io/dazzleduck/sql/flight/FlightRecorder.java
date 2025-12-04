package io.dazzleduck.sql.flight;

import io.dazzleduck.sql.flight.model.FlightMetricsSnapshot;

public interface FlightRecorder {
    void recordStatementCancel();

    void recordPreparedStatementCancel();

    void recordStatementTimeout();

    void recordPreparedStatementTimeout();

    void startStreamStatement();

    default void startStream(boolean isPreparedStatement) {
        if (isPreparedStatement) {
            startStreamPreparedStatement();
        } else {
            startStreamStatement();
        }
    }

    default void endStream(boolean isPreparedStatement) {
        if (isPreparedStatement) {
            endStreamPreparedStatement();
        } else {
            endStreamStatement();
        }
    }

    default void errorStream(boolean isPreparedStatement) {
        if (isPreparedStatement) {
            errorStreamPreparedStatement();
        } else {
            errorStreamStatement();
        }
    }

    void endStreamStatement();

    void errorStreamStatement();

    void errorStreamPreparedStatement();

    void startStreamPreparedStatement();

    void endStreamPreparedStatement();

    void errorPreparedStreamStatement();

    void startBulkIngest();

    void endBulkIngest();

    void errorBulkIngest();

    void recordGetStreamPreparedStatement(long size);

    default void recordGetStream(boolean preparedStatement, long size) {
        if(preparedStatement) {
            recordGetStreamPreparedStatement(size);
        } else {
            recordGetStreamStatement(size);
        }
    }

    void recordGetStreamStatement(long size);

    FlightMetricsSnapshot snapshot();

    double getBytesOut();

    double getBytesIn();

    long getCompletedStatements();

    long getCancelledPreparedStatements();

    long getCancelledStatements();

    long getCompletedPreparedStatements();

    long getCompletedBulkIngest();


    // Add other methods which needs to be recorded
}

