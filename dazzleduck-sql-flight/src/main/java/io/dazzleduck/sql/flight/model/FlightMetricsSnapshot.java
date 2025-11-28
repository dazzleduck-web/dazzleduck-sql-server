package io.dazzleduck.sql.flight.model;

public record FlightMetricsSnapshot(

        // ---- Application / Global ----
        long startTimeMs,

        // ---- Live gauges ----
        long runningStatements,
        long runningPrepared,
        long runningBulkIngest,

        // ---- Counters ----
        long completedStatements,
        long completedPrepared,
        long completedBulkIngest,

        long cancelledStatements,
        long cancelledPrepared

) {}
