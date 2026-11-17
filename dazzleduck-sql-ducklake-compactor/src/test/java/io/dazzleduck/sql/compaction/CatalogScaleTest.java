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
 * 512 MB, because the postgres scanner copied every table's catalog rows into DuckDB; and every
 * merge ran out of 307 MB, because DuckLake's file list aggregated the partition values of every
 * file in the catalog (fixed in the patched DuckLake, from duckdb/ducklake#1410 and #1707). The
 * catalog rows here are generated straight into PostgreSQL, a million small files of one
 * partitioned table beside a small one, and the compactor's catalog queries and a merge of the
 * small table must run in an instance of 64 MB. Runs on the patched extension ({@link PatchedDuckLake}).
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
        startupScript = "LOAD '%s';\nATTACH 'ducklake:postgres:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0);"
                .formatted(PatchedDuckLake.extension(), connection, CATALOG, tempDir.resolve("data"));
        try (Connection c = RawConnections.open(startupScript, List.of()); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE %s.main.big (id BIGINT)".formatted(CATALOG));
            s.execute("ALTER TABLE %s.main.big SET PARTITIONED BY (id)".formatted(CATALOG));
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
            // ... each with the partition value of that file
            String values = ("INSERT INTO ducklake_file_partition_value SELECT (jsonb_populate_record(v, jsonb_build_object("
                    + "''data_file_id'', 100000000 + g))).* FROM ducklake_file_partition_value v, generate_series(1, %d) g"
                    + " WHERE v.data_file_id = (SELECT min(f.data_file_id) FROM ducklake_data_file f"
                    + " JOIN ducklake_table t USING (table_id) WHERE t.table_name = ''big'')").formatted(FAKE_FILES);
            s.execute("CALL postgres_execute('%s', '%s')".formatted(Sql.metadataCatalogName(CATALOG), values));
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
            assertEquals(FAKE_FILES + 1, big.filesWaiting(), "the copies share the template's partition; "
                    + "the other two files are alone in theirs");
            assertEquals(3, work.stream().filter(w -> w.table().equals("small")).findFirst().orElseThrow().filesWaiting());
        }
        try (Connection c = RawConnections.open(startupScript, List.of("SET memory_limit = '64MB'"));
             Statement s = c.createStatement();
             var rs = s.executeQuery(CatalogSql.viaPostgres(CATALOG, CompactionService.fileCountsSql(
                     Sql.identifier(CatalogSql.postgresSchema(c, CATALOG).schema()), List.of(MINOR))))) {
            assertTrue(rs.next());
            assertEquals(FAKE_FILES + 6, rs.getLong("total_files"));
        }
    }

    @Test
    void aMergeOfTheSmallTableReadsOnlyItsOwnPartitionValues() throws Exception {
        // Without the backport, the file list of any merge aggregated the million partition values
        try (Connection c = RawConnections.open(startupScript, List.of("SET memory_limit = '64MB'", "SET threads = 2"));
             Statement s = c.createStatement();
             var rs = s.executeQuery("SELECT sum(files_processed), sum(files_created) FROM ducklake_merge_adjacent_files("
                     + "'%s', 'small', schema := 'main')".formatted(CATALOG))) {
            assertTrue(rs.next());
            assertEquals(3, rs.getLong(1));
            assertEquals(1, rs.getLong(2));
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
