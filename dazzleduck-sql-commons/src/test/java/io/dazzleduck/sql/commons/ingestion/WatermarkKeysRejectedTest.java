package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ingestion watermarks were removed: a queue mapping that still sets a {@code watermark*} key in
 * {@code additional_parameters} fails when it is built, so a configured one fails at startup,
 * rather than have its watermark table silently stop receiving rows.
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
    void aNestedOrDifferentlyCasedKeyIsCaughtToo() {
        assertThrows(IllegalArgumentException.class, () -> provider("""
                { ingestion_queue = "q1", catalog = "lake", schema = "main", table = "logs",
                  additional_parameters { watermark { table = "ingest_watermark" } } }
                """).validate());
        assertThrows(IllegalArgumentException.class, () -> provider("""
                { ingestion_queue = "q1", catalog = "lake", schema = "main", table = "logs",
                  additional_parameters { WATERMARK_TABLE = "ingest_watermark" } }
                """).validate());
    }

    @Test
    void aMappingBuiltInCodeIsRejectedToo() {
        assertThrows(IllegalArgumentException.class, () -> new QueueIdToTableMapping(
                "q1", "lake", "main", "logs", Map.of("watermark_table", "w"), null, null, null));
    }

    @Test
    void aNonObjectAdditionalParametersOnASkippedMappingStillLoads() {
        var provider = provider("""
                { ingestion_queue = "q1", output_path = "/tmp/q1", additional_parameters = "x" }
                """);
        assertEquals(Map.of(), provider.loadMappings(), "skipped as before: it has no DuckLake table");
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
