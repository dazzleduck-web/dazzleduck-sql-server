package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;

/**
 * No-operation implementation of PostIngestionTaskFactoryProvider.
 * Returns a factory that creates NOOP tasks which do nothing.
 */
public class NOOPPostIngestionTaskFactoryProvider implements PostIngestionTaskFactoryProvider {

    private Config config;
    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public PostIngestionTaskFactory getPostIngestionTaskFactory() {
        return ingestionResult -> PostIngestionTask.NOOP;
    }
}
