package io.dazzleduck.sql.compaction;

import java.util.ArrayList;
import java.util.List;

/**
 * How the process's memory is split among the JVM and the compactor's DuckDB instances, each of
 * which would otherwise default to about 80% of the machine's RAM.
 *
 * <p>The process's memory (the container's limit, else the machine's) is first reduced by the JVM:
 * its maximum heap, at most {@link #JVM_HEAP_MAX_SHARE} of the total (the images cap it with
 * {@code MaxRAMPercentage}), plus {@link #JVM_OVERHEAD_BYTES} for metaspace, code cache and thread
 * stacks. What is left, the DuckDB pool, is split by share among the instances and divided among the
 * databases, since every database has its own instances:
 *
 * <ul>
 *   <li>Tier instances: {@link #TIERS} (70%), split by {@code memory_share}</li>
 *   <li>Housekeeping (delete-file rewrite, expiry, cleanup): {@link #HOUSEKEEPING} (12%)</li>
 *   <li>Data retention: {@link #RETENTION} (12%)</li>
 *   <li>The file-count gauges: {@link #FILE_COUNTS} (4%)</li>
 * </ul>
 *
 * An instance's slice holds its {@code memory_limit} and what it allocates outside it: freed memory
 * DuckDB's allocator keeps (up to its 64 MB flush threshold, see RawConnections), glibc arenas
 * (MALLOC_ARENA_MAX) and fragmentation, budgeted as {@link #OUTSIDE_LIMIT_SHARE} of the limit plus
 * {@link #OUTSIDE_LIMIT_FIXED_BYTES}. Before, the instances' limits took 68% of the process's memory
 * and left the rest for both the JVM (whose default heap is a quarter of it) and that memory; a
 * 2 GiB container measured about 1.2 GB above a 322 MB limit. The allowance is a starting value:
 * calibrate it with the per-call RSS readings.
 *
 * <p>An instance whose own settings set a {@code memory_limit} keeps it; {@link #overcommit} checks
 * that the limits, their allowances and the JVM still fit the process's memory.
 */
final class MemoryBudget {

    static final double TIERS = 0.70;
    static final double HOUSEKEEPING = 0.12;
    static final double RETENTION = 0.12;
    static final double FILE_COUNTS = 0.04;
    /** No instance is given less. */
    static final long MINIMUM_BYTES = 64L << 20;
    /** The most of the process's memory the JVM's heap is reserved for. */
    static final double JVM_HEAP_MAX_SHARE = 0.20;
    /** The JVM's memory besides its heap: metaspace, code cache, thread stacks, GC structures. */
    static final long JVM_OVERHEAD_BYTES = 128L << 20;
    /** What a DuckDB instance allocates outside its memory_limit, as a share of the limit ... */
    static final double OUTSIDE_LIMIT_SHARE = 0.25;
    /** ... plus this: the allocator's flush threshold, the freed memory it keeps between tasks. */
    static final long OUTSIDE_LIMIT_FIXED_BYTES = 64L << 20;

    private MemoryBudget() {
    }

    /** The memory the JVM is budgeted: its maximum heap, at most a share of the total, and its overhead. */
    static long jvmReserveBytes(long totalBytes, long maxHeapBytes) {
        long heap = maxHeapBytes <= 0 || maxHeapBytes == Long.MAX_VALUE ? 0 : maxHeapBytes;
        if (totalBytes > 0) {
            heap = Math.min(heap, (long) (JVM_HEAP_MAX_SHARE * totalBytes));
        }
        return heap + JVM_OVERHEAD_BYTES;
    }

    /** The process's memory left for the DuckDB instances once the JVM is reserved, or -1 when unknown. */
    static long duckDbPoolBytes(long totalBytes) {
        if (totalBytes <= 0) {
            return -1;
        }
        return Math.max(0, totalBytes - jvmReserveBytes(totalBytes, Runtime.getRuntime().maxMemory()));
    }

    /** What an instance with this memory_limit is budgeted in all: the limit and what it uses outside it. */
    static long footprintBytes(long memoryLimitBytes) {
        return (long) (memoryLimitBytes * (1 + OUTSIDE_LIMIT_SHARE)) + OUTSIDE_LIMIT_FIXED_BYTES;
    }

    /**
     * The memory_limit for {@code share} of the DuckDB pool divided among the databases, so that the
     * limit and what the instance allocates outside it fit that slice; -1 when the pool is unknown.
     */
    static long limit(double share, int databases, long poolBytes) {
        if (poolBytes <= 0) {
            return -1;
        }
        long slice = (long) (share * poolBytes / Math.max(1, databases));
        return Math.max(MINIMUM_BYTES, (long) ((slice - OUTSIDE_LIMIT_FIXED_BYTES) / (1 + OUTSIDE_LIMIT_SHARE)));
    }

    /**
     * The settings with a {@code SET memory_limit} for {@code share} first, unless they set one or the
     * pool is unknown.
     */
    static List<String> withDefaultLimit(List<String> settings, double share, int databases, long poolBytes) {
        long limit = limit(share, databases, poolBytes);
        if (limit <= 0 || ResourceSampler.memoryLimitBytes(settings) >= 0) {
            return settings;
        }
        List<String> with = new ArrayList<>(settings.size() + 1);
        with.add("SET memory_limit = '" + limit + " bytes'");
        with.addAll(settings);
        return List.copyOf(with);
    }

    /**
     * How far the JVM and every DuckDB instance the config opens (each with its limit, from its
     * settings or the default, and its allowance outside it) exceed the process's memory, in bytes;
     * 0 when they fit or the memory is unknown. Explicit {@code memory_limit} settings are where this
     * goes wrong: the defaults always fit.
     */
    static long overcommit(CompactionConfig config, long totalBytes, long maxHeapBytes) {
        if (totalBytes <= 0) {
            return 0;
        }
        long pool = Math.max(0, totalBytes - jvmReserveBytes(totalBytes, maxHeapBytes));
        int databases = Math.max(1, config.databases().size());
        long perDatabase = 0;
        for (CompactionTier tier : config.tiers()) {
            if (tier.enabled()) {
                perDatabase += footprintBytes(limitOf(tier.connectionSettings(), TIERS * tier.memoryShare(), databases, pool));
            }
        }
        perDatabase += footprintBytes(limitOf(config.housekeepingConnectionSettings(), HOUSEKEEPING, databases, pool));
        if (config.dataRetention().enabled()) {
            perDatabase += footprintBytes(limitOf(config.dataRetention().connectionSettings(), RETENTION, databases, pool));
        }
        perDatabase += footprintBytes(limit(FILE_COUNTS, databases, pool));
        long planned = jvmReserveBytes(totalBytes, maxHeapBytes) + perDatabase * databases;
        return Math.max(0, planned - totalBytes);
    }

    private static long limitOf(List<String> settings, double share, int databases, long pool) {
        long explicit = ResourceSampler.memoryLimitBytes(settings);
        return explicit >= 0 ? explicit : limit(share, databases, pool);
    }
}
