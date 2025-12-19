package io.dazzleduck.sql.commons.ingestion;


public record Stats(String identifier,
                    long totalWriteBytes,
                    long totalWriteBatches,
                    long totalWriteBuckets,
                    long timeSpendWriting,
                    long scheduledWriteBatches,
                    long scheduledWriteBuckets
                    ) {
}
