package io.dazzleduck.sql.commons.ingestion;


import java.util.concurrent.Future;

public interface BulkIngestQueueInterface<T, R> extends AutoCloseable, IngestionStatsMBean {
    Future<R> addToQueue(Batch<T> batch);

    void write(WriteTask<T, R> writeTask);

}
