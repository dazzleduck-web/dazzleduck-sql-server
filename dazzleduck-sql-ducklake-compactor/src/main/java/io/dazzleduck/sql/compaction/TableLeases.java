package io.dazzleduck.sql.compaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * One job at a time per table, across tiers and jobs: merges, the data-retention {@code DELETE} and
 * the delete-file rewrite (DYNAMIC_COMPACTION_SPEC.md, "Table leases"). Without it a merge and a
 * retention {@code DELETE} on the same files would conflict.
 *
 * <p>The tier loops only {@link #tryAcquire} and skip a busy table for the pass. Retention and the
 * rewrite {@link #acquire} and wait; while one waits, {@link #tryAcquire} refuses the table too, so a
 * busy table cannot keep the lease from them. In-process only: one compactor per catalog.
 */
public final class TableLeases {

    private final Set<String> busy = new HashSet<>();
    private final Map<String, Integer> waiting = new HashMap<>();

    /** The key of a table: its catalog, schema and name, lower-cased (DuckDB names are case-insensitive). */
    public static String key(String catalog, String schema, String table) {
        return (catalog + "." + schema + "." + table).toLowerCase(Locale.ROOT);
    }

    /** Takes the lease if the table is free and nobody waits for it. */
    public synchronized boolean tryAcquire(String key) {
        if (busy.contains(key) || waiting.containsKey(key)) {
            return false;
        }
        busy.add(key);
        return true;
    }

    /** Waits until the table is free, then takes the lease. */
    public synchronized void acquire(String key) throws InterruptedException {
        waiting.merge(key, 1, Integer::sum);
        try {
            while (busy.contains(key)) {
                wait();
            }
            busy.add(key);
        } finally {
            waiting.computeIfPresent(key, (k, n) -> n == 1 ? null : n - 1);
        }
    }

    public synchronized void release(String key) {
        busy.remove(key);
        notifyAll();
    }

    synchronized boolean isBusy(String key) {
        return busy.contains(key);
    }

    synchronized boolean hasWaiter(String key) {
        return waiting.containsKey(key);
    }
}
