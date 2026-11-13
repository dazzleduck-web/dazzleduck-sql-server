package io.dazzleduck.sql.commons.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

/**
 * Class used once the data in ingested to let consumer know about it
 * @param queueName Generally this is the base Directory
 * @param ingestionBatchId Ingestion batchId
 * @param applicationId Application which is responsible for Ingestion
 * @param maxProducerIds Id of the producer
 * @param filesCreated This will be populated when the  files are written in the s3
 */
public record IngestionResult(String queueName, long ingestionBatchId, String applicationId, Map<String, Long> maxProducerIds, long rowCount, List<String> filesCreated, String query) {
    IngestionResult(String queueName, long ingestionBatchId, String applicationId, Map<String, Long> maxProducerIds, long rowCount, List<String> filesCreated) {
        this(queueName, ingestionBatchId, applicationId, maxProducerIds, rowCount, filesCreated, null);
    }
}
