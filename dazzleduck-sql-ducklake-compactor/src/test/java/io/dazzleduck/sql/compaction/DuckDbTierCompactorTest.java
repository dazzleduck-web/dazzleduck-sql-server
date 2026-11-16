package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The discovery query and the per-table call against a real DuckLake catalog (a local DuckDB file,
 * so only one instance may attach it at a time: the setup closes before the compactor opens).
 *
 * <p>The discovery query must list exactly what {@code ducklake_merge_adjacent_files} would merge:
 * groups by schema version, partition and partition values; no file with deletes; no table with
 * {@code auto_compact} off.
 */
class DuckDbTierCompactorTest {

    @TempDir
    Path tmp;

    private String startup;
    private CompactionState state;

    /** One band holding every file the test writes, at most 1 GB; a merge group never fills. */
    private static final CompactionTier ALL = CompactionTier.of("all", 0, 1_000_000_000L, 1_000_000_000L, 10);

    @BeforeEach
    void createCatalog() throws Exception {
        Path catalog = tmp.resolve("catalog.ducklake");
        Path data = Files.createDirectories(tmp.resolve("data"));
        // Newline after each ';' so RawConnections.splitStatements runs them individually; replay-safe.
        startup = "INSTALL ducklake;\nLOAD ducklake;\n"
                + "ATTACH 'ducklake:" + catalog + "' AS lake (DATA_PATH '" + data + "/');\n"
                + "CALL lake.set_option('data_inlining_row_limit', 0);\n";
        state = new CompactionState(new SimpleMeterRegistry(), List.of("lake"), List.of("all"));
        setup(
                "CREATE TABLE lake.main.t (id INTEGER, v VARCHAR)",
                insert("lake.main.t"), insert("lake.main.t"), insert("lake.main.t"), insert("lake.main.t"),

                "CREATE TABLE lake.main.parted (id INTEGER, p INTEGER)",
                "ALTER TABLE lake.main.parted SET PARTITIONED BY (p)",
                "INSERT INTO lake.main.parted SELECT i, 1 FROM range(100) t(i)",
                "INSERT INTO lake.main.parted SELECT i, 1 FROM range(100) t(i)",
                "INSERT INTO lake.main.parted SELECT i, 1 FROM range(100) t(i)",
                "INSERT INTO lake.main.parted SELECT i, 2 FROM range(100) t(i)",

                "CREATE TABLE lake.main.deleted (id INTEGER, v VARCHAR)",
                insert("lake.main.deleted"), insert("lake.main.deleted"),
                "DELETE FROM lake.main.deleted WHERE id = 5",

                "CREATE TABLE lake.main.off (id INTEGER, v VARCHAR)",
                insert("lake.main.off"), insert("lake.main.off"),
                "CALL lake.set_option('auto_compact', false, table_name => 'off')",

                "CREATE TABLE lake.main.sorted_t (id INTEGER, v VARCHAR)",
                "ALTER TABLE lake.main.sorted_t SET SORTED BY (id)",
                insert("lake.main.sorted_t"), insert("lake.main.sorted_t"),

                "CREATE TABLE lake.main.evolved (id INTEGER, v VARCHAR)",
                insert("lake.main.evolved"), insert("lake.main.evolved"),
                "ALTER TABLE lake.main.evolved ADD COLUMN w INTEGER",
                "INSERT INTO lake.main.evolved SELECT i, 'x', i FROM range(1, 1000) t(i)",

                "CREATE SCHEMA lake.other",
                "CREATE TABLE lake.other.t (id INTEGER, v VARCHAR)",
                insert("lake.other.t"), insert("lake.other.t"));
    }

    private static String insert(String table) {
        return "INSERT INTO " + table + " SELECT i, 'x' || i FROM range(1, 1000) t(i)";
    }

    /** Runs statements on a short-lived instance, closed before the compactor attaches the catalog. */
    private void setup(String... statements) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            for (String sql : startup.split(";\n")) {
                s.execute(sql);
            }
            for (String sql : statements) {
                s.execute(sql);
            }
        }
    }

    private Map<String, TableWork> discover(CompactionTier tier) throws Exception {
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 4)) {
            return compactor.discover("lake", tier).stream().collect(Collectors.toMap(TableWork::name, Function.identity()));
        }
    }

    private long liveFiles(String schema, String table) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            for (String sql : startup.split(";\n")) {
                s.execute(sql);
            }
            try (ResultSet rs = s.executeQuery(("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file d"
                    + " JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id)"
                    + " JOIN __ducklake_metadata_lake.ducklake_schema s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL"
                    + " WHERE d.end_snapshot IS NULL AND t.end_snapshot IS NULL AND s.schema_name = '%s' AND t.table_name = '%s'")
                    .formatted(schema, table))) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    @Test
    void discoveryListsWhatTheMergeWouldMerge() throws Exception {
        Map<String, TableWork> listed = discover(ALL);

        assertEquals(Map.of(
                        "main.t", 4L,
                        "main.parted", 3L,       // p=1's three files; p=2's single file has nothing to merge with
                        "main.sorted_t", 2L,
                        "main.evolved", 2L,      // the file written after ADD COLUMN is in its own schema version
                        "other.t", 2L),
                listed.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().filesWaiting())),
                "not main.deleted, whose other file has deletes, nor main.off, with auto_compact off");

        assertTrue(listed.get("main.sorted_t").sorted());
        assertFalse(listed.get("main.t").sorted());
        assertEquals(1, listed.get("main.parted").busyPartitions(), "one group, just written");
        assertTrue(listed.get("main.t").bytesWaiting() > 0);
        assertTrue(Duration.between(listed.get("main.t").oldestWaiting(), java.time.Instant.now()).toMinutes() < 5);
    }

    @Test
    void minFilesIsTheAdmissionThreshold() throws Exception {
        assertEquals(List.of("main.t"), List.copyOf(discover(ALL.withAdmission(4, Duration.ZERO)).keySet()),
                "only main.t has a group of four");
    }

    @Test
    void maxWaitAdmitsAGroupWhoseSecondOldestFileIsOldEnough() throws Exception {
        CompactionTier waits = ALL.withAdmission(100, Duration.ofSeconds(1));
        assertTrue(discover(waits).isEmpty(), "no group of 100, and nothing has waited a second yet");
        Thread.sleep(1_500);
        assertEquals(5, discover(waits).size(), "every table with a pair of files has now waited");
    }

    @Test
    void theBandFencesTheTier() throws Exception {
        // Every file here is well under 1 MB, so a band starting at 1 MB sees nothing.
        CompactionTier major = CompactionTier.of("major", 1_000_000, 64_000_000, 64_000_000, 16);
        assertTrue(discover(major).isEmpty());
    }

    @Test
    void aCallMergesOnlyItsTableUpToTheCap() throws Exception {
        Map<String, TableWork> listed = discover(ALL);
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 4)) {
            TierCompactor.MergeOutcome out = compactor.compact("lake", ALL, listed.get("main.t"), 10);
            assertEquals(4L, out.filesProcessed());
            assertEquals(1L, out.filesCreated());
            assertTrue(out.durationMergeMs() >= 0);

            // A second call finds a single merged file — nothing to merge -> no rows -> null.
            TierCompactor.MergeOutcome noop = compactor.compact("lake", ALL, listed.get("main.t"), 10);
            assertNull(noop.filesProcessed(), "nothing merged the second time");
            assertNull(noop.filesCreated());
        }
        assertEquals(1, liveFiles("main", "t"));
        assertEquals(2, liveFiles("other", "t"), "the table of the same name in another schema is untouched");
        assertEquals(4, liveFiles("main", "parted"), "other tables are untouched");
    }

    @Test
    void theTargetBoundsEachGroupAndTheCapCountsGroups() throws Exception {
        long fileSize;
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            for (String sql : startup.split(";\n")) {
                s.execute(sql);
            }
            try (ResultSet rs = s.executeQuery("SELECT max(file_size_bytes) FROM __ducklake_metadata_lake.ducklake_data_file"
                    + " JOIN __ducklake_metadata_lake.ducklake_table USING (table_id) WHERE table_name = 't'")) {
                rs.next();
                fileSize = rs.getLong(1);
            }
        }
        // A target of about two files: a group takes a file while its total is below the target,
        // so main.t's four files make two groups of two. A cap of 1 merges only the first.
        CompactionTier pairs = CompactionTier.of("pairs", 0, fileSize + 1, fileSize + 1, 10);
        TableWork t = discover(pairs).get("main.t");
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 4)) {
            TierCompactor.MergeOutcome out = compactor.compact("lake", pairs, t, 1);
            assertEquals(2L, out.filesProcessed(), "one group of two");
            assertEquals(1L, out.filesCreated());
        }
        assertEquals(3, liveFiles("main", "t"));
    }

    @Test
    void theSessionTargetIsSetAndReadBack() throws Exception {
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 4)) {
            TierCompactor.Readiness readiness = compactor.prepare("lake", ALL);
            assertFalse(readiness.isRefused(), String.valueOf(readiness.refused()));
            assertEquals(0, readiness.idleTxnTimeoutMs(), "a DuckDB-file catalog has no idle-in-transaction timeout");
            assertEquals(8, readiness.threads(), "2 x cpu_share 1.0 x 4 CPUs");
        }
        try (Connection c = RawConnections.open(startup, List.of())) {
            DuckDbTierCompactor.setTarget(c, ALL);
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_setting('ducklake_target_file_size')")) {
                rs.next();
                assertEquals("1000000000 bytes", rs.getString(1));
            }
        }
    }

    @Test
    void namesAreQuotedInTheCalls() {
        TableWork odd = new TableWork("my'schema", "it's", 2, 10, java.time.Instant.EPOCH, 0, false);
        assertEquals("CALL ducklake_merge_adjacent_files('lake', 'it''s', schema := 'my''schema', min_file_size := 0,"
                        + " max_file_size := 1000000000, max_compacted_files := 3)",
                DuckDbTierCompactor.mergeSql("lake", odd, ALL, 3));
    }
}
