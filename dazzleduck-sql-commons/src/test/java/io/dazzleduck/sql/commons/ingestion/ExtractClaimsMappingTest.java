package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@code extract_claims} queue-mapping flag: record defaults, provider
 * config parsing, and the {@link DuckLakeIngestionHandler} lookup.
 */
class ExtractClaimsMappingTest {

    @TempDir
    Path tempDir;

    private static final String CATALOG = "extract_claims_lake";

    @BeforeAll
    static void loadExtensions() throws Exception {
        ConnectionPool.executeBatch(new String[]{"INSTALL ducklake", "LOAD ducklake"});
    }

    @AfterEach
    void detach() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH DATABASE IF EXISTS " + CATALOG);
        }
    }

    @Test
    void mapping_defaultsToFalse_andSurvivesWithInputSchema() {
        var mapping = new QueueIdToTableMapping("q", "cat", "main", "t", Map.of(), null);
        assertFalse(mapping.extractClaims(), "extract_claims must default to false");

        var enabled = mapping.withExtractClaims(true);
        assertTrue(enabled.extractClaims());
        assertTrue(enabled.withInputSchema("id BIGINT").extractClaims(),
                "withInputSchema must preserve extract_claims");
    }

    @Test
    void provider_parsesExtractClaimsPerMapping() {
        var config = ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [
                    { ingestion_queue = "with_claims",    catalog = "c", schema = "s", table = "t1", extract_claims = true }
                    { ingestion_queue = "without_claims", catalog = "c", schema = "s", table = "t2" }
                ]
                """);
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(config);
        Map<String, QueueIdToTableMapping> mappings = provider.loadMappings();

        assertTrue(mappings.get("with_claims").extractClaims());
        assertFalse(mappings.get("without_claims").extractClaims());
    }

    @Test
    void handler_looksUpFlagPerQueue() throws Exception {
        Path dataPath = tempDir.resolve("data");
        Files.createDirectories(dataPath);
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')"
                            .formatted(tempDir.resolve("catalog"), CATALOG, dataPath),
                    // Deliberately no 'claims' column: runs the missing-column warning path (not asserted).
                    "CREATE TABLE %s.main.events (id BIGINT)".formatted(CATALOG)
            });
        }

        var mapping = new QueueIdToTableMapping("q", CATALOG, "main", "events", Map.of(), null)
                .withExtractClaims(true);
        var handler = new DuckLakeIngestionHandler(Map.of("q", mapping));

        assertTrue(handler.extractClaims("q"));
        assertFalse(handler.extractClaims("unknown-queue"));
    }
}
