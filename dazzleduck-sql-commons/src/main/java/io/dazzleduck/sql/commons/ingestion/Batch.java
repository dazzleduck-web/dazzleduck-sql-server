package io.dazzleduck.sql.commons.ingestion;

import java.time.Instant;

/**
 * One unit of input handed to a queue. Several batches are merged into a {@link Bucket} and written
 * by one COPY, so anything carried here is unioned across the bucket.
 *
 * @param sidecarRows rows the producer derived from this batch, to be appended beside the target
 *                    table in the transaction that registers the batch's files
 *                    ({@link SidecarRows}); null when the producer has none, which is the usual case
 */
public record Batch<T>(String[] sortOrder,
                       String[] partitionBy,
                       T record,
                       String producerId,
                       long producerBatchId,
                       long totalSize,
                       String format,
                       Instant receivedTime,
                       SidecarRows sidecarRows) {

    /** A batch with no sidecar rows. */
    public Batch(String[] sortOrder, String[] partitionBy, T record, String producerId, long producerBatchId,
                 long totalSize, String format, Instant receivedTime) {
        this(sortOrder, partitionBy, record, producerId, producerBatchId, totalSize, format, receivedTime, null);
    }
}
