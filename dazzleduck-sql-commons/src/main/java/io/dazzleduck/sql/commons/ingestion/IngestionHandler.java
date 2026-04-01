package io.dazzleduck.sql.commons.ingestion;

public interface IngestionHandler {
    PostIngestionTask createPostIngestionTask(IngestionResult ingestionResult);

    String getTargetPath(String queueId);

    default String getTransformation(String queueId) {
        return null;
    }
}
