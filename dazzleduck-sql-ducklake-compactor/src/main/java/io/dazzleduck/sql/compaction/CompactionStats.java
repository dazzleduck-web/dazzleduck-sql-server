package io.dazzleduck.sql.compaction;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public record CompactionStats(
        Instant serviceStart,
        Map<String, DatabaseStats> databases) {

    /** Compaction counts are successes only; {@code lastSuccessTime} is null until one completes. */
    public record DatabaseStats(
            long totalMinorCompactions,
            long totalMajorCompactions,
            long totalFailedCycles,
            long totalFilesCompacted,
            Instant lastSuccessTime,
            Instant nextExecutionTime,
            long currentSmallFiles,
            long currentMediumFiles,
            long currentTotalFiles) {

        /**
         * Fills in the one field {@link CompactionState} cannot know, so the nine-component
         * constructor is spelled out in exactly one place.
         */
        public DatabaseStats withNextExecutionTime(Instant next) {
            return new DatabaseStats(totalMinorCompactions, totalMajorCompactions, totalFailedCycles,
                    totalFilesCompacted, lastSuccessTime, next,
                    currentSmallFiles, currentMediumFiles, currentTotalFiles);
        }
    }

    public Duration uptime() {
        return Duration.between(serviceStart, Instant.now());
    }
}
