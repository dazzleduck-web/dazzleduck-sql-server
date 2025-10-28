package io.dazzleduck.sql.flight.ingestion;

import io.dazzleduck.sql.commons.ingestion.Batch;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public record IngestionParameters(String completePath, String format, String[] partitions, String[] transformations,
                                  String[] sortOrder, String producerId, Long producerBatchId,
                                  Map<String, String> parameters) {
    public static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(1);

    public static final long DEFAULT_MAX_BUCKET_SIZE = 16 * 1024 * 1024;

    public Batch<String> constructBatch(long size, String tempFile) {
        return new Batch<>(
                sortOrder,
                transformations,
                partitions,
                tempFile,
                producerId,
                producerBatchId,
                size,
                format,
                Instant.now()
        );
    }
}
