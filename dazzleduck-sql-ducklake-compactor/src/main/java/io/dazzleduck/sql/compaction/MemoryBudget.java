package io.dazzleduck.sql.compaction;

import java.util.ArrayList;
import java.util.List;

/**
 * How the process's memory is split among the compactor's DuckDB instances, each of which would
 * otherwise default to about 80% of the machine's RAM. Shares are of the process's memory (the
 * container's limit, else the machine's) and are divided among the databases, since every database
 * has its own instances. Together they leave about a third for the JVM and for what DuckDB allocates
 * outside its {@code memory_limit} (glibc arenas, see MALLOC_ARENA_MAX).
 *
 * <ul>
 *   <li>Tier instances: {@link DuckDbTierCompactor#MEMORY_SHARE} (50%), split by {@code memory_share}</li>
 *   <li>Housekeeping (delete-file rewrite, expiry, cleanup): {@link #HOUSEKEEPING} (8%)</li>
 *   <li>Data retention: {@link #RETENTION} (8%)</li>
 *   <li>The file-count gauges: {@link #FILE_COUNTS} (2%)</li>
 * </ul>
 *
 * An instance whose own settings set a {@code memory_limit} keeps it.
 */
final class MemoryBudget {

    static final double HOUSEKEEPING = 0.08;
    static final double RETENTION = 0.08;
    static final double FILE_COUNTS = 0.02;
    /** No instance is given less. */
    static final long MINIMUM_BYTES = 64L << 20;

    private MemoryBudget() {
    }

    /** {@code share} of {@code totalBytes} divided among the databases, or -1 when the total is unknown. */
    static long limit(double share, int databases, long totalBytes) {
        if (totalBytes <= 0) {
            return -1;
        }
        return Math.max(MINIMUM_BYTES, (long) (share * totalBytes / Math.max(1, databases)));
    }

    /**
     * The settings with a {@code SET memory_limit} for {@code share} first, unless they set one or the
     * total is unknown.
     */
    static List<String> withDefaultLimit(List<String> settings, double share, int databases, long totalBytes) {
        long limit = limit(share, databases, totalBytes);
        if (limit <= 0 || ResourceSampler.memoryLimitBytes(settings) >= 0) {
            return settings;
        }
        List<String> with = new ArrayList<>(settings.size() + 1);
        with.add("SET memory_limit = '" + limit + " bytes'");
        with.addAll(settings);
        return List.copyOf(with);
    }
}
