package io.dazzleduck.sql.compaction;

import java.io.Closeable;
import java.io.IOException;

public interface DataRetention extends Closeable {

    /** What one retention cycle did for a database. */
    record Outcome(int tables, long rowsDeleted, int failedTables) {
    }

    /**
     * Deletes the rows older than each policy's retention from the database's tables. A table that
     * fails is counted in {@link Outcome#failedTables()} and does not stop the others.
     *
     * @throws Exception when the policy table itself cannot be read
     */
    Outcome enforce(String database) throws Exception;

    @Override
    default void close() throws IOException {
    }
}
