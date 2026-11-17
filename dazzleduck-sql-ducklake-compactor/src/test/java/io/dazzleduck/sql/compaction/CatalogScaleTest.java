package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Review of #544: with a PostgreSQL catalog of ~1.5M live files, minor's discovery ran out of its
 * 512 MB, because the postgres scanner copied every table's catalog rows into DuckDB. The catalog
 * rows here are generated straight into PostgreSQL (discovery reads only the catalog), a million
 * small files of one table beside a small one, and the compactor's catalog queries must run in an
 * instance of 64 MB.
 */
class CatalogScaleTest {

    static final String CATALOG = "scale_lake";
    static final long FAKE_FILES = 1_000_000;
    static final CompactionTier MINOR = CompactionTier.of("minor", 0, 8_000_000, 16_000_000, 12)
            .withConnectionSettings(List.of("SET memory_limit = '64MB'"));

    @TempDir
    static Path tempDir;
    static PostgreSQLContainer postgres;
    static String startupScript;

    @BeforeAll
    static void setUp() throws Exception {
        postgres = new PostgreSQLContainer("postgres:15-alpine")
                .withDatabaseName("ducklake").withUsername("duck").withPassword("duck");
        postgres.start();
        String connection = "host=%s port=%d dbname=ducklake user=duck password=duck".formatted(
                postgres.getHost(), postgres.getFirstMappedPort());
        startupScript = "INSTALL ducklake; LOAD ducklake;\nATTACH 'ducklake:postgres:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0);"
                .formatted(connection, CATALOG, tempDir.resolve("data"));
        try (Connection c = RawConnections.open(startupScript, List.of()); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE %s.main.big (id BIGINT)".formatted(CATALOG));
            s.execute("CREATE TABLE %s.main.small (id BIGINT)".formatted(CATALOG));
            for (int i = 0; i < 3; i++) {
                s.execute("INSERT INTO %s.main.big VALUES (%d)".formatted(CATALOG, i));
                s.execute("INSERT INTO %s.main.small VALUES (%d)".formatted(CATALOG, i));
            }
            // A million copies of one of big's file rows, with new ids, made inside PostgreSQL
            String copy = ("INSERT INTO ducklake_data_file SELECT (jsonb_populate_record(d, jsonb_build_object("
                    + "''data_file_id'', 100000000 + g, ''path'', ''fake-'' || g))).* FROM ducklake_data_file d,"
                    + " generate_series(1, %d) g WHERE d.data_file_id = (SELECT min(f.data_file_id)"
                    + " FROM ducklake_data_file f JOIN ducklake_table t USING (table_id) WHERE t.table_name = ''big'')")
                    .formatted(FAKE_FILES);
            s.execute("CALL postgres_execute('%s', '%s')".formatted(Sql.metadataCatalogName(CATALOG), copy));
        }
    }

    @AfterAll
    static void tearDown() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    void discoveryAndFileCountsReturnOnlyTheirResultFromAMillionFileCatalog() throws Exception {
        CompactionState state = new CompactionState(new SimpleMeterRegistry(), List.of(CATALOG), List.of("minor"));
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startupScript, state, 4, 1, -1)) {
            TierCompactor.Readiness readiness = compactor.prepare(CATALOG, MINOR);
            assertFalse(readiness.isRefused(), String.valueOf(readiness.refused()));
            List<TableWork> work = compactor.discover(CATALOG, MINOR);
            TableWork big = work.stream().filter(w -> w.table().equals("big")).findFirst().orElseThrow();
            assertEquals(FAKE_FILES + 3, big.filesWaiting());
            assertEquals(3, work.stream().filter(w -> w.table().equals("small")).findFirst().orElseThrow().filesWaiting());
        }
        try (Connection c = RawConnections.open(startupScript, List.of("SET memory_limit = '64MB'"));
             Statement s = c.createStatement();
             var rs = s.executeQuery(CatalogSql.viaPostgres(CATALOG, CompactionService.fileCountsSql(
                     Sql.identifier(CatalogSql.postgresSchema(c, CATALOG)), List.of(MINOR))))) {
            assertTrue(rs.next());
            assertEquals(FAKE_FILES + 6, rs.getLong("total_files"));
        }
    }

    @Test
    void theSameDiscoveryThroughThePostgresScannerDoesNotFitIn64Mb() throws Exception {
        // What the fix avoids: the DuckDB query copies the million rows into the instance
        try (Connection c = RawConnections.open(startupScript, List.of("SET memory_limit = '64MB'",
                "SET temp_directory = '" + tempDir.resolve("spill") + "'", "SET max_temp_directory_size = '0 bytes'"));
             Statement s = c.createStatement()) {
            var e = assertThrows(java.sql.SQLException.class,
                    () -> s.executeQuery(DuckDbTierCompactor.discoverySql(CATALOG, MINOR)).next());
            assertTrue(e.getMessage().contains("Out of Memory"), e.getMessage());
        }
    }
}
