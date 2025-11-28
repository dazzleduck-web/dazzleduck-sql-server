package io.dazzleduck.sql.flight.model;

import java.time.Duration;
import java.time.Instant;

public record RunningStatementInfo(
        String user,
        String statementId,
        Instant startInstant,
        String query,             // per-statement
        String action,            // per-statement
        Instant endInstant        // per-statement
) {

    // Canonical compact constructor
    public RunningStatementInfo(String user, String statementId, String query) {
        this(user, statementId, Instant.now(), query, "RUNNING", null);
    }

    // Copy-like setters (records are immutable, so return a new instance)
    public RunningStatementInfo withQuery(String newQuery) {
        return new RunningStatementInfo(
                user, statementId, startInstant, newQuery, action, endInstant
        );
    }

    public RunningStatementInfo asCompleted() {
        return new RunningStatementInfo(
                user, statementId, startInstant, query, "COMPLETED", Instant.now()
        );
    }

    public RunningStatementInfo asFailed() {
        return new RunningStatementInfo(
                user, statementId, startInstant, query, "FAILED", Instant.now()
        );
    }

    // Duration helpers
    public long durationMs() {
        Instant end = (endInstant != null) ? endInstant : Instant.now();
        return Duration.between(startInstant, end).toMillis();
    }

    public Duration duration() {
        Instant end = (endInstant != null) ? endInstant : Instant.now();
        return Duration.between(startInstant, end);
    }
}
