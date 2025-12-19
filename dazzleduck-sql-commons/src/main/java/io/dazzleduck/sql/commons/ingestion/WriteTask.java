package io.dazzleduck.sql.commons.ingestion;

import java.time.Instant;

public record WriteTask<T, R>(long taskId, Instant startTime, Bucket<T, R> bucket) {
    public long size() {
        return bucket.size();
    }
}
