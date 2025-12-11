package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.model.RunningStatementInfo;

import java.time.Instant;
import java.util.List;

public interface SqlProducerMBean {
    long getRunningStatements();
    long getOpenPreparedStatement();
    long getRunningPreparedStatements();
    double getBytesOut();
    double getBytesIn();
    Instant getStartTime();
    long getCompletedStatements();
    long getCompletedPreparedStatements();
    long getCompletedBulkIngest();
    long getCancelledStatements();
    long getCancelledPreparedStatements();
    List<RunningStatementInfo> getRunningStatementDetails();
    List<RunningStatementInfo> getOpenPreparedStatementDetails();
    List<RunningStatementInfo> getRunningBulkIngestDetails();

}
