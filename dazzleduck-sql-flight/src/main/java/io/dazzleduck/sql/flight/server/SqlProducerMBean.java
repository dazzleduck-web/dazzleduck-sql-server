package io.dazzleduck.sql.flight.server;

import java.time.Instant;

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
}
