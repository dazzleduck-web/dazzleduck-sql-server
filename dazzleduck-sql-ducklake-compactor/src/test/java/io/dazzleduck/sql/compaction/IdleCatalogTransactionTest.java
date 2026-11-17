package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A merge that takes longer than the catalog's idle-in-transaction timeout commits: the patched
 * DuckLake (backport of duckdb/ducklake#1580) releases the catalog transaction while the merge
 * rewrites files. Before it, PostgreSQL closed the session that sat idle in the transaction during
 * the rewrite (the COMMIT_TIMEOUT the compactor sizes calls to avoid); a merge this small still
 * committed through DuckLake's retry on a new connection, so the test checks PostgreSQL's log for
 * the kill. Each row of the rewrite sleeps for a second against a 500 ms timeout. Runs on the patched
 * extension ({@link PatchedDuckLake}).
 */
class IdleCatalogTransactionTest {

    static final String CATALOG = "idle_lake";

    @TempDir
    static Path tempDir;
    static PostgreSQLContainer postgres;
    static String startupScript;

    @BeforeAll
    static void setUp() throws Exception {
        postgres = new PostgreSQLContainer("postgres:15-alpine")
                .withDatabaseName("ducklake").withUsername("duck").withPassword("duck");
        postgres.start();
        String connection = ("host=%s port=%d dbname=ducklake user=duck password=duck"
                + " options=''-c idle_in_transaction_session_timeout=500''").formatted(
                postgres.getHost(), postgres.getFirstMappedPort());
        startupScript = "LOAD '%s';\nATTACH 'ducklake:postgres:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0);"
                .formatted(PatchedDuckLake.extension(), connection, CATALOG, tempDir.resolve("data"));
    }

    @AfterAll
    static void tearDown() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    void aMergeLongerThanTheIdleTimeoutCommits() throws Exception {
        try (Connection c = RawConnections.open(startupScript, List.of()); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE %s.main.t (i INTEGER)".formatted(CATALOG));
            s.execute("INSERT INTO %s.main.t VALUES (1)".formatted(CATALOG));
            s.execute("INSERT INTO %s.main.t VALUES (2)".formatted(CATALOG));
            s.execute("ALTER TABLE %s.main.t SET SORTED BY (COALESCE(sleep_ms(1000), i))".formatted(CATALOG));
            try (ResultSet rs = s.executeQuery(("SELECT files_processed, files_created FROM ducklake_merge_adjacent_files("
                    + "'%s', 't', max_compacted_files => 4)").formatted(CATALOG))) {
                assertTrue(rs.next());
                assertEquals(2, rs.getLong(1));
                assertEquals(1, rs.getLong(2));
            }
            // On v1.5.6-dazzleduck.4 PostgreSQL killed the session mid-merge; DuckLake retried the commit on a
            // new connection, so a merge this small still committed, but a call can lose its transaction
            assertTrue(postgres.getLogs().lines().noneMatch(l -> l.contains("idle-in-transaction timeout")),
                    "the catalog session was not left idle in a transaction during the rewrite");
            try (ResultSet rs = s.executeQuery("SELECT count(*), sum(i) FROM %s.main.t".formatted(CATALOG))) {
                assertTrue(rs.next());
                assertEquals(2, rs.getLong(1));
                assertEquals(3, rs.getLong(2));
            }
        }
    }
}
