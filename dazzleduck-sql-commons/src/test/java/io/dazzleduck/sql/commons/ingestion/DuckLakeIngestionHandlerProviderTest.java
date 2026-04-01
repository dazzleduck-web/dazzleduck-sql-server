package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DuckLakeIngestionHandlerProviderTest {

    // -------------------------------------------------------------------------
    // Valid transformations
    // -------------------------------------------------------------------------

    @Test
    public void testValidTransformationSelectStar() {
        // Simplest valid form — pass-through
        assertDoesNotThrow(() ->
            DuckLakeIngestionTaskFactoryProvider.validateTransformation("q", "SELECT * FROM __this"));
    }

    @Test
    public void testValidTransformationColumnSubset() {
        assertDoesNotThrow(() ->
            DuckLakeIngestionTaskFactoryProvider.validateTransformation("q", "SELECT id, value FROM __this"));
    }

    @Test
    public void testValidTransformationWithDerivedColumn() {
        assertDoesNotThrow(() ->
            DuckLakeIngestionTaskFactoryProvider.validateTransformation("q",
                "SELECT id, value * 2 AS doubled_value FROM __this"));
    }

    @Test
    public void testValidTransformationWithFilter() {
        assertDoesNotThrow(() ->
            DuckLakeIngestionTaskFactoryProvider.validateTransformation("q",
                "SELECT * FROM __this WHERE id >= 50"));
    }

    @Test
    public void testValidTransformationWithFunctionCall() {
        assertDoesNotThrow(() ->
            DuckLakeIngestionTaskFactoryProvider.validateTransformation("q",
                "SELECT id, upper(category) AS category FROM __this"));
    }

    // -------------------------------------------------------------------------
    // Missing __this reference
    // -------------------------------------------------------------------------

    @Test
    public void testInvalidTransformationMissingThisTable() {
        var ex = assertThrows(IllegalArgumentException.class, () ->
            DuckLakeIngestionTaskFactoryProvider.validateTransformation("log",
                "SELECT * FROM some_other_table"));

        assertTrue(ex.getMessage().contains("__this"),
            "Error should mention __this, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("log"),
            "Error should mention the queue name, got: " + ex.getMessage());
    }

    @Test
    public void testInvalidTransformationReferencesWrongTable() {
        var ex = assertThrows(IllegalArgumentException.class, () ->
            DuckLakeIngestionTaskFactoryProvider.validateTransformation("metrics",
                "SELECT id FROM source_data"));

        assertTrue(ex.getMessage().contains("__this"));
        assertTrue(ex.getMessage().contains("metrics"));
    }

    // -------------------------------------------------------------------------
    // Unparseable SQL
    // -------------------------------------------------------------------------

    @Test
    public void testInvalidTransformationUnparseableSql() {
        var ex = assertThrows(IllegalArgumentException.class, () ->
            DuckLakeIngestionTaskFactoryProvider.validateTransformation("q",
                "THIS IS NOT SQL"));

        assertTrue(ex.getMessage().contains("q"),
            "Error should mention the queue name, got: " + ex.getMessage());
    }

    @Test
    public void testInvalidTransformationIncompleteSql() {
        assertThrows(IllegalArgumentException.class, () ->
            DuckLakeIngestionTaskFactoryProvider.validateTransformation("q",
                "SELECT FROM"));
    }

    // -------------------------------------------------------------------------
    // validate() — explicit startup entry point
    // -------------------------------------------------------------------------

    private static DuckLakeIngestionTaskFactoryProvider providerFromHocon(String hocon) {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString(hocon));
        return provider;
    }

    @Test
    public void testValidatePassesWithNoMapping() {
        var provider = providerFromHocon("ingestion_path = \"/tmp\"");
        assertDoesNotThrow(provider::validate);
    }

    @Test
    public void testValidatePassesWithValidTransformation() {
        var provider = providerFromHocon("""
            ingestion_queue_table_mapping = [{
                ingestion_queue = "log"
                catalog = "loglake"
                schema  = "main"
                table   = "log"
                transformation = "SELECT id, ts FROM __this"
            }]
            """);
        assertDoesNotThrow(provider::validate);
    }

    @Test
    public void testValidatePassesWithNoTransformation() {
        var provider = providerFromHocon("""
            ingestion_queue_table_mapping = [{
                ingestion_queue = "log"
                catalog = "loglake"
                schema  = "main"
                table   = "log"
            }]
            """);
        assertDoesNotThrow(provider::validate);
    }

    @Test
    public void testValidateFailsWithInvalidTransformation() {
        var provider = providerFromHocon("""
            ingestion_queue_table_mapping = [{
                ingestion_queue = "log"
                catalog = "loglake"
                schema  = "main"
                table   = "log"
                transformation = "SELECT * FROM wrong_table"
            }]
            """);
        var ex = assertThrows(IllegalArgumentException.class, provider::validate);
        assertTrue(ex.getMessage().contains("__this"));
        assertTrue(ex.getMessage().contains("log"));
    }

    @Test
    public void testValidateChecksAllQueues() {
        // First queue is valid, second is invalid — validate() must catch the second
        var provider = providerFromHocon("""
            ingestion_queue_table_mapping = [
                {
                    ingestion_queue = "events"
                    catalog = "loglake"
                    schema  = "main"
                    table   = "events"
                    transformation = "SELECT * FROM __this"
                },
                {
                    ingestion_queue = "metrics"
                    catalog = "loglake"
                    schema  = "main"
                    table   = "metrics"
                    transformation = "SELECT * FROM wrong_table"
                }
            ]
            """);
        var ex = assertThrows(IllegalArgumentException.class, provider::validate);
        assertTrue(ex.getMessage().contains("metrics"));
    }
}
