package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ingestion watermarks were removed: a queue mapping config that still sets a {@code watermark_*}
 * key in {@code additional_parameters} fails when it is loaded, at startup, rather than have its
 * watermark table silently stop receiving rows.
 */
class WatermarkKeysRejectedTest {

    private static DuckLakeIngestionTaskFactoryProvider provider(String mapping) {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString("ingestion_queue_table_mapping = [ " + mapping + " ]"));
        return provider;
    }

    @Test
    void aConfigWithWatermarkKeysFailsAtStartup() {
        var provider = provider("""
                { ingestion_queue = "q1", catalog = "lake", schema = "main", table = "logs",
                  additional_parameters {
                    watermark_table = "ingest_watermark"
                    watermark_timestamp_column = "ts"
                    other = "x"
                  } }
                """);
        var e = assertThrows(IllegalArgumentException.class, provider::validate);
        assertTrue(e.getMessage().contains("q1"), e.getMessage());
        assertTrue(e.getMessage().contains("[watermark_table, watermark_timestamp_column]"),
                () -> "names every watermark key and no other: " + e.getMessage());
    }

    @Test
    void evenOnAMappingWithoutADuckLakeTable() {
        var provider = provider("""
                { ingestion_queue = "q1", additional_parameters { watermark_table = "ingest_watermark" } }
                """);
        assertThrows(IllegalArgumentException.class, provider::validate);
    }

    @Test
    void otherAdditionalParametersStillLoad() {
        var provider = provider("""
                { ingestion_queue = "q1", catalog = "lake", schema = "main", table = "logs",
                  additional_parameters { other = "x" } }
                """);
        assertEquals(Map.of("other", "x"), provider.loadMappings().get("q1").additionalParameters());
    }
}
