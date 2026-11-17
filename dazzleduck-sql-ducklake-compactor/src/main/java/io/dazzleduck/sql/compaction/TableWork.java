package io.dazzleduck.sql.compaction;

import java.time.Instant;

/**
 * One table the discovery query listed for a tier: it has a merge group past the tier's admission
 * threshold. Counts cover the eligible files in the table's groups of at least two files, which are
 * the files a call can merge: in the band, below the target, without deletes.
 *
 * @param filesWaiting    eligible files in groups of at least two
 * @param bytesWaiting    their total size
 * @param oldestWaiting   the earliest second-oldest file of any such group: the oldest file that has
 *                        something to merge with. A group's oldest file is usually the leftover of
 *                        the last merge, so timing it would report a healthy table as stale
 * @param busyPartitions  groups with a file committed in the last {@link CallSizing#BUSY_WINDOW}
 * @param sorted          whether the table has a sort key; a sorted merge is cheaper per file
 */
public record TableWork(
        String schema,
        String table,
        long filesWaiting,
        long bytesWaiting,
        Instant oldestWaiting,
        int busyPartitions,
        boolean sorted) {

    /** {@code schema.table}, as it appears in logs, telemetry and metric tags. */
    public String name() {
        return schema + "." + table;
    }

    /** The lease key: catalog, schema and table, lower-cased since DuckDB names are case-insensitive. */
    public String leaseKey(String database) {
        return TableLeases.key(database, schema, table);
    }

    public long averageFileSize() {
        return filesWaiting == 0 ? 0 : bytesWaiting / filesWaiting;
    }
}
