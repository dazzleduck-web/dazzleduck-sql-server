package io.dazzleduck.sql.commons.ingestion;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ingestion watermarks were removed: a queue mapping that still sets a {@code watermark_*} key in
 * {@code additional_parameters} fails when it is loaded, rather than have its watermark table
 * silently stop receiving rows.
 */
class WatermarkKeysRejectedTest {

    @Test
    void aMappingWithWatermarkKeysFailsToLoad() {
        var e = assertThrows(IllegalArgumentException.class, () -> new QueueIdToTableMapping(
                "q1", "lake", "main", "logs",
                Map.of("watermark_table", "ingest_watermark", "watermark_timestamp_column", "ts", "other", "x"),
                null, null, null));
        assertTrue(e.getMessage().contains("q1"), e.getMessage());
        assertTrue(e.getMessage().contains("[watermark_table, watermark_timestamp_column]"),
                () -> "names every watermark key and no other: " + e.getMessage());
    }

    @Test
    void otherAdditionalParametersStillLoad() {
        var mapping = new QueueIdToTableMapping("q1", "lake", "main", "logs", Map.of("other", "x"), null, null, null);
        assertEquals(Map.of("other", "x"), mapping.additionalParameters());
        assertDoesNotThrow(() -> new QueueIdToTableMapping("q2", "lake", "main", "logs", null, null, null, null));
    }
}
