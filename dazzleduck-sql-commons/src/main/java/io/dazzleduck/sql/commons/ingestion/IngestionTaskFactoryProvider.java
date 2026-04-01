package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.config.ConfigBasedProvider;

public interface IngestionTaskFactoryProvider extends ConfigBasedProvider {

    void setConfig(Config config);

    IngestionHandler getIngestionTaskFactory();

    /**
     * Validates provider configuration eagerly at server startup.
     * Called after {@link #getIngestionTaskFactory()} so the factory is already built.
     * Implementations should throw {@link IllegalArgumentException} on misconfiguration.
     */
    default void validate() {}

}
