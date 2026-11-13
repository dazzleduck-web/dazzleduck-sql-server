package io.dazzleduck.sql.commons.ingestion;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class used once the data in ingested to let consumer know about it
 * @param queueName Generally this is the base Directory
 * @param ingestionBatchId Ingestion batchId
 * @param applicationId Application which is responsible for Ingestion
 * @param maxProducerIds Id of the producer
 * @param filesCreated This will be populated when the  files are written in the s3
 * @param sidecarFiles the files written for each of the queue's sidecars, by sidecar table; a
 *                     sidecar whose query returned no rows for the batch has no entry. Empty when
 *                     the queue has no sidecars.
 */
public record IngestionResult(String queueName, long ingestionBatchId, String applicationId, Map<String, Long> maxProducerIds,
                              long rowCount, List<String> filesCreated, String query, Map<String, List<String>> sidecarFiles) {

    public IngestionResult {
        sidecarFiles = sidecarFiles == null ? Map.of() : sidecarFiles;
    }

    public IngestionResult(String queueName, long ingestionBatchId, String applicationId, Map<String, Long> maxProducerIds,
                           long rowCount, List<String> filesCreated, String query) {
        this(queueName, ingestionBatchId, applicationId, maxProducerIds, rowCount, filesCreated, query, Map.of());
    }

    IngestionResult(String queueName, long ingestionBatchId, String applicationId, Map<String, Long> maxProducerIds, long rowCount, List<String> filesCreated) {
        this(queueName, ingestionBatchId, applicationId, maxProducerIds, rowCount, filesCreated, null);
    }

    /** Every file the batch wrote: the base files, then each sidecar's. */
    public List<String> allFiles() {
        List<String> all = new ArrayList<>(filesCreated == null ? List.of() : filesCreated);
        sidecarFiles.values().forEach(all::addAll);
        return all;
    }
}
