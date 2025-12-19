package io.dazzleduck.sql.commons.ingestion;

import java.time.Duration;

public interface IngestionStatsMBean {


    String identifier();
    Stats getStats();
}
