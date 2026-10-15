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

    @Test
    public void testValidTransformationWithCte() {
        // __this is referenced only inside the CTE body, not in the outer FROM
        assertDoesNotThrow(() ->
            DuckLakeIngestionTaskFactoryProvider.validateTransformation("q",
                "WITH parsed AS (SELECT *, upper(category) AS cat FROM __this) SELECT id, cat FROM parsed"));
    }

    @Test
    public void testValidTransformationWithChainedCtes() {
        assertDoesNotThrow(() ->
            DuckLakeIngestionTaskFactoryProvider.validateTransformation("q",
                "WITH a AS (SELECT id, value FROM __this), b AS (SELECT id, value * 2 AS v2 FROM a) "
                    + "SELECT * FROM b WHERE v2 > 10"));
    }

    // -------------------------------------------------------------------------
    // Missing __this reference
    // -------------------------------------------------------------------------

    @Test
    public void testInvalidTransformationCteNotReadingThis() {
        var ex = assertThrows(IllegalArgumentException.class, () ->
            DuckLakeIngestionTaskFactoryProvider.validateTransformation("log",
                "WITH x AS (SELECT * FROM some_other_table) SELECT * FROM x"));

        assertTrue(ex.getMessage().contains("__this"),
            "Error should mention __this, got: " + ex.getMessage());
    }

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
    public void testValidatePassesWithCteTransformation() {
        // A transformation starting with WITH is a full query and must not get the
        // "SELECT *, <expr> FROM __this" shorthand wrapping.
        var provider = providerFromHocon("""
            ingestion_queue_table_mapping = [{
                ingestion_queue = "log"
                catalog = "loglake"
                schema  = "main"
                table   = "log"
                transformation = "WITH t AS (SELECT id, ts FROM __this) SELECT * FROM t"
            }]
            """);
        assertDoesNotThrow(provider::validate);
    }

    @Test
    public void testShorthandOnKeywordPrefixedColumnIsStillWrapped() {
        // with_flag / selected_at start with WITH / SELECT but are expressions, not full queries.
        var provider = providerFromHocon("""
            ingestion_queue_table_mapping = [
                { ingestion_queue = "a", catalog = "loglake", schema = "main", table = "a",
                  transformation = "with_flag AS f" },
                { ingestion_queue = "b", catalog = "loglake", schema = "main", table = "b",
                  transformation = "selected_at AS s" },
                { ingestion_queue = "c", catalog = "loglake", schema = "main", table = "c",
                  transformation = "  with t AS (SELECT * FROM __this) SELECT * FROM t" }
            ]
            """);
        var mappings = provider.loadMappings();
        assertEquals("SELECT *, with_flag AS f FROM __this", mappings.get("a").transformation());
        assertEquals("SELECT *, selected_at AS s FROM __this", mappings.get("b").transformation());
        assertEquals("  with t AS (SELECT * FROM __this) SELECT * FROM t", mappings.get("c").transformation());
        assertDoesNotThrow(provider::validate);
    }

    @Test
    public void testFullQueryNotStartingWithSelectOrWithIsNotWrapped() {
        // A leading comment (common in multi-line HOCON), a parenthesized query and FROM-first
        // syntax are full queries too; only a select-list expression is shorthand.
        var provider = providerFromHocon("""
            ingestion_queue_table_mapping = [
                { ingestion_queue = "a", catalog = "loglake", schema = "main", table = "a",
                  transformation = "-- parse once\\nWITH t AS (SELECT * FROM __this) SELECT * FROM t" },
                { ingestion_queue = "b", catalog = "loglake", schema = "main", table = "b",
                  transformation = "(SELECT id FROM __this)" },
                { ingestion_queue = "c", catalog = "loglake", schema = "main", table = "c",
                  transformation = "FROM __this SELECT id" },
                { ingestion_queue = "d", catalog = "loglake", schema = "main", table = "d",
                  transformation = "upper(level) AS lvl" }
            ]
            """);
        var mappings = provider.loadMappings();
        assertEquals("-- parse once\nWITH t AS (SELECT * FROM __this) SELECT * FROM t", mappings.get("a").transformation());
        assertEquals("(SELECT id FROM __this)", mappings.get("b").transformation());
        assertEquals("FROM __this SELECT id", mappings.get("c").transformation());
        assertEquals("SELECT *, upper(level) AS lvl FROM __this", mappings.get("d").transformation());
        assertDoesNotThrow(provider::validate);
    }

    @Test
    public void testShorthandWithTrailingCommentOrSemicolonIsWrappedValidly() {
        // Appended verbatim, a trailing comment swallowed " FROM __this" and a trailing ';' ended the
        // statement before it; both are trimmed before wrapping.
        var provider = providerFromHocon("""
            ingestion_queue_table_mapping = [
                { ingestion_queue = "a", catalog = "loglake", schema = "main", table = "a",
                  transformation = "upper(level) AS lvl -- uppercase the level" },
                { ingestion_queue = "b", catalog = "loglake", schema = "main", table = "b",
                  transformation = "upper(level) AS lvl;" }
            ]
            """);
        var mappings = provider.loadMappings();
        assertEquals("SELECT *, upper(level) AS lvl FROM __this", mappings.get("a").transformation());
        assertEquals("SELECT *, upper(level) AS lvl FROM __this", mappings.get("b").transformation());
        assertDoesNotThrow(provider::validate);
    }

    @Test
    public void testValidateRejectsMultipleStatements() {
        // Only the first statement used to be checked, and the write then failed on every batch.
        var provider = providerFromHocon("""
            ingestion_queue_table_mapping = [{
                ingestion_queue = "log", catalog = "loglake", schema = "main", table = "log"
                transformation = "SELECT * FROM __this; SELECT 1"
            }]
            """);
        var e = assertThrows(IllegalArgumentException.class, provider::validate);
        assertTrue(e.getMessage().contains("single statement"), e.getMessage());
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

    // -------------------------------------------------------------------------
    // Partitioning config
    // -------------------------------------------------------------------------

    @Test
    public void testLoadsPartitioningFromHocon() {
        var provider = providerFromHocon("""
            ingestion_queue_table_mapping = [{
                ingestion_queue = "events"
                catalog = "loglake"
                schema  = "main"
                table   = "events"
                num_partitions = 4
                partition_expression = "substr(source_ip, 1, 10)"
            }]
            """);
        var mapping = provider.loadMappings().get("events");
        assertNotNull(mapping);
        assertEquals(4, mapping.numPartitions());
        assertEquals("substr(source_ip, 1, 10)", mapping.partitionExpression());
        assertTrue(mapping.isPartitioned());
    }

    @Test
    public void testDefaultsToNoPartitioning() {
        var provider = providerFromHocon("""
            ingestion_queue_table_mapping = [{
                ingestion_queue = "events"
                catalog = "loglake"
                schema  = "main"
                table   = "events"
            }]
            """);
        var mapping = provider.loadMappings().get("events");
        assertEquals(1, mapping.numPartitions());
        assertNull(mapping.partitionExpression());
        assertFalse(mapping.isPartitioned());
    }

    @Test
    public void testPartitioningWithoutExpressionIsRejected() {
        var provider = providerFromHocon("""
            ingestion_queue_table_mapping = [{
                ingestion_queue = "events"
                catalog = "loglake"
                schema  = "main"
                table   = "events"
                num_partitions = 4
            }]
            """);
        var ex = assertThrows(IllegalArgumentException.class, provider::loadMappings);
        assertTrue(ex.getMessage().contains("partition_expression"), ex.getMessage());
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

    // -------------------------------------------------------------------------
    // Session variables on a mapping entry
    // -------------------------------------------------------------------------

    @Test
    public void testVariablesAreParsedFromTheMappingEntry() {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [{
                    ingestion_queue = "logs"
                    catalog = "loglake"
                    schema  = "main"
                    table   = "logs"
                    transformation = "SELECT *, getvariable('env') AS env FROM __this"
                    variables { env = "prod", tier = "hot" }
                    variables_view              = "loglake.main.v_logs_vars"
                    variables_key_column        = "config_key"
                    variables_expiration_column = "expires_at"
                }]
                """));
        var mapping = provider.loadMappings().get("logs");
        assertNotNull(mapping);
        var variables = mapping.variables();
        assertEquals("prod", variables.staticVariables().get("env"));
        assertEquals("hot", variables.staticVariables().get("tier"));
        assertTrue(variables.hasView());
        assertEquals("loglake.main.v_logs_vars", variables.view().relation());
        assertEquals("config_key", variables.view().keyColumn());
        assertEquals("value", variables.view().valueColumn(), "default value column");
        assertEquals("expires_at", variables.view().expirationColumn());
    }

    @Test
    public void testAMappingWithoutVariablesCarriesNone() {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [{
                    ingestion_queue = "logs"
                    catalog = "loglake"
                    schema  = "main"
                    table   = "logs"
                }]
                """));
        assertEquals(IngestionVariables.NONE, provider.loadMappings().get("logs").variables());
    }

    @Test
    public void testAMisconfiguredVariableFailsAtConfigLoad() {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [{
                    ingestion_queue = "logs"
                    catalog = "loglake"
                    schema  = "main"
                    table   = "logs"
                    variables { "not a name" = "x" }
                }]
                """));
        var ex = assertThrows(IllegalArgumentException.class, provider::loadMappings);
        assertTrue(ex.getMessage().contains("ingestion variable name"), ex.getMessage());
    }
}
