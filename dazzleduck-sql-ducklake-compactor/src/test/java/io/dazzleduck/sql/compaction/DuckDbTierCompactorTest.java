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
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 4, 1, -1)) {
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
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 4, 1, -1)) {
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
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 4, 1, -1)) {
            TierCompactor.MergeOutcome out = compactor.compact("lake", pairs, t, 1);
            assertEquals(2L, out.filesProcessed(), "one group of two");
            assertEquals(1L, out.filesCreated());
        }
        assertEquals(3, liveFiles("main", "t"));
    }

    @Test
    void threadsAreCappedByMemoryAndAnExplicitThreadsSettingIsWhatGroupsAreSizedFor() throws Exception {
        // Review of #544: 16 CPUs and 2 GiB give minor 64 threads, whose buffers left no memory for a group
        CompactionTier minor = ALL.withMemoryShare(0.4);
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 16, 1, 2L << 30)) {
            TierCompactor.Readiness readiness = compactor.prepare("lake", minor);
            assertFalse(readiness.isRefused(), String.valueOf(readiness.refused()));
            assertEquals(20, readiness.threads(), "capped from 64 so the threads take at most half of a group's memory");
        }
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 16, 1, 2L << 30)) {
            TierCompactor.Readiness readiness = compactor.prepare("lake",
                    minor.withConnectionSettings(List.of("SET threads = 3")));
            assertEquals(3, readiness.threads(), "the instance's own setting, not the computed one");
        }
    }

    @Test
    void aTierWhoseSpillDirectoryIsNotWritableDoesNotStart(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        java.nio.file.Path file = java.nio.file.Files.createFile(dir.resolve("file"));
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 4, 1, -1,
                new SpillBudget(file, 10_000_000_000L))) {
            TierCompactor.Readiness readiness = compactor.prepare("lake", ALL);
            assertTrue(readiness.isRefused());
            assertTrue(readiness.refused().contains("not writable"), readiness.refused());
        }
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 4, 1, -1,
                new SpillBudget(dir.resolve("spill"), 10_000_000_000L))) {
            assertFalse(compactor.prepare("lake", ALL).isRefused());
            assertTrue(java.nio.file.Files.isDirectory(dir.resolve("spill").resolve("lake").resolve("tier-" + ALL.name())));
        }
    }

    @Test
    void theSessionTargetIsSetAndReadBack() throws Exception {
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 4, 1, -1)) {
            TierCompactor.Readiness readiness = compactor.prepare("lake", ALL);
            assertFalse(readiness.isRefused(), String.valueOf(readiness.refused()));
            assertEquals(0, readiness.idleTxnTimeoutMs(), "a DuckDB-file catalog has no idle-in-transaction timeout");
            assertEquals(16, readiness.threads(), "4 threads per CPU x 4 CPUs");
        }
        try (Connection c = RawConnections.open(startup, List.of())) {
            DuckDbTierCompactor.setTarget(c, ALL.targetFileSize());
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_setting('ducklake_target_file_size')")) {
                rs.next();
                assertEquals("1000000000 bytes", rs.getString(1));
            }
        }
    }

    @Test
    void mergedOutputsLeaveTheBandOrStayAsItsOneLeftover() throws Exception {
        // Review question on #542: with target = band max, a merge stops adding files once a group
        // reaches the target, and its output can come out a little smaller than its inputs, so it may
        // stay just under the band max. It then merges again with whatever shares its group, which
        // moves it out of the band. Repeated passes must converge: nothing left to list, at most one
        // file of the table left in the band.
        long fileSize = liveFileSize("t");
        CompactionTier band = CompactionTier.of("band", 0, 2 * fileSize, 2 * fileSize, 10);
        int passes = 0;
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 4, 1, -1)) {
            for (TableWork t = compactor.discover("lake", band).stream().filter(w -> w.name().equals("main.t"))
                    .findFirst().orElse(null); t != null; t = compactor.discover("lake", band).stream()
                    .filter(w -> w.name().equals("main.t")).findFirst().orElse(null)) {
                assertTrue(++passes <= 4, "merges must converge");
                compactor.compact("lake", band, t, 10);
            }
        }
        assertTrue(passes >= 1);
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            for (String sql : startup.split(";\n")) {
                s.execute(sql);
            }
            try (ResultSet rs = s.executeQuery(("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file d"
                    + " JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id)"
                    + " JOIN __ducklake_metadata_lake.ducklake_schema sc ON sc.schema_id = t.schema_id"
                    + " WHERE t.table_name = 't' AND sc.schema_name = 'main' AND t.end_snapshot IS NULL"
                    + " AND d.end_snapshot IS NULL AND d.file_size_bytes < %d").formatted(2 * fileSize))) {
                rs.next();
                assertTrue(rs.getLong(1) <= 1, "at most one leftover in the band, got " + rs.getLong(1));
            }
        }
    }

    private long liveFileSize(String table) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            for (String sql : startup.split(";\n")) {
                s.execute(sql);
            }
            try (ResultSet rs = s.executeQuery("SELECT max(file_size_bytes) FROM __ducklake_metadata_lake.ducklake_data_file"
                    + " JOIN __ducklake_metadata_lake.ducklake_table USING (table_id) WHERE table_name = '" + table + "'")) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    @Test
    void duckDbSizesAreParsedAsDuckDbWritesThem() {
        assertEquals(16_000_000L, DuckDbTierCompactor.parseDuckDbSize("16000000 bytes"));
        assertEquals(16_000_000L, DuckDbTierCompactor.parseDuckDbSize("16.0 MB"));
        assertEquals(16L * 1024 * 1024, DuckDbTierCompactor.parseDuckDbSize("16.0 MiB"));
        assertEquals(-1L, DuckDbTierCompactor.parseDuckDbSize("sixteen"));
    }

    @Test
    void threadsAndMemoryAreAProcessBudgetSplitAcrossDatabases() {
        CompactionTier minor = CompactionTier.of("minor", 0, 8_000_000, 16_000_000, 12).withMemoryShare(0.5);
        // Review on #542: merges of small files are round-trip bound, so every tier gets threads per CPU
        assertEquals(32, minor.threads(8, 1), "4 x 8 CPUs");
        assertEquals(11, minor.threads(8, 3), "three databases each run a minor instance: 32 / 3");
        assertEquals(2, minor.withThreadsPerCpu(2).threads(1, 1));
        assertEquals(1, minor.threads(1, 8), "at least one");

        DuckDbTierCompactor oneDb = new DuckDbTierCompactor(startup, state, 8, 1, 4_000_000_000L);
        // a 4 GB pool: the slice is 70% x memory_share / databases, less 64 MiB, over 1.25
        assertEquals(1_066_312_908L, oneDb.defaultMemoryLimitBytes(minor), 2, "(70% x 0.5 x 4 GB - 64 MiB) / 1.25");
        DuckDbTierCompactor twoDbs = new DuckDbTierCompactor(startup, state, 8, 2, 4_000_000_000L);
        assertEquals(506_312_908L, twoDbs.defaultMemoryLimitBytes(minor), 2);
        assertEquals(618_312_908L, oneDb.defaultMemoryLimitBytes(minor.withMemoryShare(0.3)), 2);
        assertEquals(-1L, oneDb.defaultMemoryLimitBytes(minor.withConnectionSettings(List.of("SET memory_limit='2GB'"))),
                "an explicit memory_limit wins");
        assertEquals(-1L, new DuckDbTierCompactor(startup, state, 8, 1, -1).defaultMemoryLimitBytes(minor),
                "unknown memory: DuckDB's default");
    }

    @Test
    void everyInstanceReturnsFreedMemoryAfterEachTask() throws Exception {
        // Review on #542: DuckDB's allocator otherwise keeps memory a merge freed, outside memory_limit
        try (Connection c = RawConnections.open(startup, List.of()); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT current_setting('allocator_background_threads'),"
                     + " current_setting('allocator_flush_threshold')")) {
            rs.next();
            assertTrue(rs.getBoolean(1));
            assertEquals(64_000_000L, DuckDbTierCompactor.parseDuckDbSize(rs.getString(2)), 1_000_000L);
        }
        try (Connection c = RawConnections.open(startup, List.of("SET allocator_flush_threshold = '256MB'"));
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT current_setting('allocator_flush_threshold')")) {
            rs.next();
            assertEquals(256_000_000L, DuckDbTierCompactor.parseDuckDbSize(rs.getString(1)), 1_000_000L,
                    "an instance's own settings win");
        }
    }

    @Test
    void theOtherInstancesGetASliceOfTheMemoryToo() {
        // Review on #542: housekeeping, retention and the file-count instance had DuckDB's default
        assertEquals(List.of("SET memory_limit = '138312908 bytes'", "SET enable_external_file_cache = false"),
                MemoryBudget.withDefaultLimit(List.of("SET enable_external_file_cache = false"),
                        MemoryBudget.HOUSEKEEPING, 2, 4_000_000_000L), "(12% of 4 GB / 2 - 64 MiB) / 1.25");
        assertEquals(List.of("SET memory_limit='1GB'"), MemoryBudget.withDefaultLimit(List.of("SET memory_limit='1GB'"),
                MemoryBudget.RETENTION, 1, 4_000_000_000L), "an explicit limit wins");
        assertEquals(List.of(), MemoryBudget.withDefaultLimit(List.of(), MemoryBudget.FILE_COUNTS, 1, -1),
                "unknown memory: DuckDB's default");
        assertEquals(MemoryBudget.MINIMUM_BYTES, MemoryBudget.limit(MemoryBudget.FILE_COUNTS, 8, 1_000_000_000L));
        double all = DuckDbTierCompactor.MEMORY_SHARE + MemoryBudget.HOUSEKEEPING + MemoryBudget.RETENTION
                + MemoryBudget.FILE_COUNTS;
        assertTrue(all <= 1, "the shares divide the pool left after the JVM: " + all);
    }

    @Test
    void theJvmAndTheMemoryOutsideEachLimitAreBudgeted() {
        // Review of #544: the instances took 68% of a 2 GiB container and left the rest for both the JVM
        // (a quarter of it by default) and DuckDB's memory outside memory_limit, measured at ~1.2 GB
        long total = 2L << 30;
        long mib = 1L << 20;
        assertEquals((long) (0.20 * total) + 128 * mib, MemoryBudget.jvmReserveBytes(total, 512 * mib),
                "a 512 MiB heap is reserved up to 20% of the memory, plus 128 MiB");
        assertEquals(300 * mib + 128 * mib, MemoryBudget.jvmReserveBytes(total, 300 * mib));
        assertEquals(128 * mib, MemoryBudget.jvmReserveBytes(total, Long.MAX_VALUE), "no heap limit reported");

        long pool = total - MemoryBudget.jvmReserveBytes(total, 300 * mib);
        for (double share : new double[]{MemoryBudget.TIERS * 0.4, MemoryBudget.TIERS * 0.3, MemoryBudget.HOUSEKEEPING}) {
            long limit = MemoryBudget.limit(share, 1, pool);
            assertTrue(MemoryBudget.footprintBytes(limit) <= share * pool + 1,
                    "the limit and what the instance uses outside it fit its slice");
        }

        CompactionTier minor = CompactionTier.of("minor", 0, 8_000_000, 16_000_000, 12).withMemoryShare(0.4);
        CompactionTier major = CompactionTier.of("major", 8_000_000, 64_000_000, 64_000_000, 16).withMemoryShare(0.3);
        CompactionTier mega = CompactionTier.of("mega", 64_000_000, 256_000_000, 256_000_000, 5).withMemoryShare(0.3);
        CompactionConfig defaults = new CompactionConfig(List.of("lake"), List.of(minor, major, mega),
                java.time.Duration.ofMinutes(5), java.time.Duration.ofMinutes(15), List.of(), 0, 10);
        assertEquals(0, MemoryBudget.overcommit(defaults, total, 300 * mib), "the default limits fit");

        CompactionConfig explicit = new CompactionConfig(List.of("lake"),
                List.of(minor.withConnectionSettings(List.of("SET memory_limit = '1100MB'")), major, mega),
                java.time.Duration.ofMinutes(5), java.time.Duration.ofMinutes(15), List.of(), 0, 10);
        assertTrue(MemoryBudget.overcommit(explicit, total, 300 * mib) > 0,
                "1100 MB for minor beside the default major and mega does not fit 2 GiB");
        assertEquals(0, MemoryBudget.overcommit(explicit, -1, 300 * mib), "unknown memory: no check");
        assertTrue(MemoryBudget.overcommit(defaults, total, 1024 * mib) > 0,
                "a heap raised to half the memory is reported, though the limits only reserve 20% for it");
    }

    @Test
    void aCatalogThatIsNotPostgresHasNoTimeoutEvenWhenItsBackendCannotBeRead() throws Exception {
        // Review on #542: a failure before the backend is known must not make the tier retry forever
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            assertEquals(0, DuckDbTierCompactor.idleTxnTimeoutMs(c, "not_attached", ALL));
            assertEquals(90_000, DuckDbTierCompactor.idleTxnTimeoutMs(c, "not_attached",
                    ALL.withConnectionSettings(List.of("SET idle_in_transaction_session_timeout = '90s'"))));
        }
    }

    @Test
    void theInstanceGetsItsMemoryLimitAndThreads() throws Exception {
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state, 4, 2, 4_000_000_000L)) {
            TierCompactor.Readiness readiness = compactor.prepare("lake", ALL);
            assertEquals(8, readiness.threads(), "4 threads per CPU x 4 CPUs / 2 databases");
            assertEquals(1_066_312_908L, readiness.memoryLimitBytes(), 2, "(70% of 4 GB / 2 databases - 64 MiB) / 1.25");
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
