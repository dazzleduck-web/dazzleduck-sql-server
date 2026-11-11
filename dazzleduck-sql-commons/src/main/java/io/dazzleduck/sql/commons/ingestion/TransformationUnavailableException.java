package io.dazzleduck.sql.commons.ingestion;

/**
 * A queue's view-based transformation cannot be resolved — e.g. the view was dropped, renamed, or
 * replaced by a body that no longer reads the queue's {@code input_table} exactly once.
 *
 * <p>Scoped to one queue, like {@link UnsupportedPartitionTransformException}:
 * {@link DuckLakeIngestionHandler} records it in that queue's state so the queue's writes fail with
 * this message (rather than writing rows untransformed or with a stale transformation), while every
 * other queue keeps ingesting. The view is re-read on every refresh, so fixing it recovers the queue.
 */
public class TransformationUnavailableException extends IllegalStateException {

    public TransformationUnavailableException(String message) {
        super(message);
    }
}
