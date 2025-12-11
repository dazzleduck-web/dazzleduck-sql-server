package io.dazzleduck.sql.flight.model;

import java.time.Instant;

public record RunningStatementInfo(String user, String statementId, Instant startInstant, String query, String action,
                                   Instant endInstant) {

    /**
     * Convenience constructor:
     * running = true  → "RUNNING"
     * start == null   → "OPEN"
     * else            → "COMPLETED"
     */
    public RunningStatementInfo(String user, String statementId, Instant startInstant, String query, boolean running, Object endInstant) {

        this(user, statementId, startInstant, query, running ? "RUNNING" : (startInstant == null ? "OPEN" : "COMPLETED"), (endInstant instanceof Instant ei) ? ei : null);
    }
}
