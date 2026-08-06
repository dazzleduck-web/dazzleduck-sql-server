package io.dazzleduck.sql.commons.ingestion;


public record Stats(String identifier,
                    long totalWriteBytes,
                    long totalWriteBatches,
                    long totalWriteBuckets,
                    long timeSpentWriting,
                    long pendingBatches,
                    long pendingBuckets,
                    long failedWriteBytes,
                    long failedWriteBatches,
                    long failedWriteBuckets,
                    long producerIdEvictions
                    ) {
}
