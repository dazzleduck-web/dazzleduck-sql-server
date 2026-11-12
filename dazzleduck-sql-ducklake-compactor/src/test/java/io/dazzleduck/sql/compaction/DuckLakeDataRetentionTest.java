package io.dazzleduck.sql.compaction;

import com.typesafe.config.ConfigFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Data retention against a real DuckLake, using a DuckDB file as the catalog so no Postgres is
 * needed. A file catalog allows one attach at a time, so the setup and the checks each open their
 * own instance while the retention job's is closed.
 */
class DuckLakeDataRetentionTest {

    private static final Instant NOW = Instant.parse("2026-11-11T12:00:00Z");
    private static final String DB = "lake";

    @TempDir
    Path tmp;

    private String attach(Path catalog, Path data) {
        return "ATTACH 'ducklake:" + catalog + "' AS lake (DATA_PATH '" + data + "/')";
    }

    private String startupScript(Path catalog, Path data) {
        return "INSTALL ducklake;\nLOAD ducklake;\n" + attach(catalog, data) + ";\n";
    }

    private void run(Path catalog, Path data, String... statements) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
             Statement s = c.createStatement()) {
            s.execute("INSTALL ducklake");
            s.execute("LOAD ducklake");
            s.execute(attach(catalog, data));
            for (String sql : statements) {
                s.execute(sql);
            }
        }
    }

    private long count(Path catalog, Path data, String sql) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
             Statement s = c.createStatement()) {
            s.execute("LOAD ducklake");
            s.execute(attach(catalog, data));
            try (ResultSet rs = s.executeQuery(sql)) {
                assertTrue(rs.next());
                return rs.getLong(1);
            }
        }
    }

    @Test
    void deletesExpiredRowsOfEachPolicyTable() throws Exception {
        Path catalog = tmp.resolve("catalog.ducklake");
        Path data = Files.createDirectories(tmp.resolve("data"));
        run(catalog, data,
                "CALL lake.set_option('data_inlining_row_limit', 0)",
                "CREATE TABLE lake.main.events (id INTEGER, ts TIMESTAMP)",
                // each INSERT writes one file: 40 days old, 10 days old, and no time at all
                "INSERT INTO lake.main.events VALUES (1, TIMESTAMP '2026-10-02 12:00:00')",
                "INSERT INTO lake.main.events VALUES (2, TIMESTAMP '2026-11-01 12:00:00')",
                "INSERT INTO lake.main.events VALUES (3, NULL)",
                "CREATE TABLE lake.main.daily (d DATE, n INTEGER)",
                "INSERT INTO lake.main.daily VALUES (DATE '2026-10-01', 1)",
                "INSERT INTO lake.main.daily VALUES (DATE '2026-10-12', 2)",
                "INSERT INTO lake.main.daily VALUES (DATE '2026-11-01', 3)",
                "CREATE TABLE lake.main.tz_events (ts TIMESTAMPTZ)",
                "INSERT INTO lake.main.tz_events VALUES (TIMESTAMPTZ '2026-10-12 11:59:59+00')",
                "INSERT INTO lake.main.tz_events VALUES (TIMESTAMPTZ '2026-10-12 12:00:01+00')",
                "CREATE TABLE lake.main.kept (ts TIMESTAMP)",
                "CREATE TABLE lake.main.\"MixedCase\" (\"EventTime\" TIMESTAMP)",
                "INSERT INTO lake.main.\"MixedCase\" VALUES (TIMESTAMP '2000-01-01')",
                "INSERT INTO lake.main.\"MixedCase\" VALUES (TIMESTAMP '2026-11-10')",
                "INSERT INTO lake.main.kept VALUES (TIMESTAMP '2000-01-01')",
                "CREATE TABLE lake.main.broken (ts TIMESTAMP)",
                "INSERT INTO lake.main.broken VALUES (TIMESTAMP '2000-01-01')",
                "CREATE TABLE lake.main.retention_policy (table_name VARCHAR, time_column VARCHAR, retention VARCHAR)",
                "INSERT INTO lake.main.retention_policy VALUES "
                        + "('lake.main.events', 'ts', '30 days'),"
                        + "('lake.main.daily', 'd', '30 days'),"
                        + "('lake.main.tz_events', 'ts', '720h'),"
                        + "('lake.main.kept', 'ts', 'unlimited'),"
                        // listed twice: the longest retention, unlimited, wins
                        + "('lake.main.kept', 'ts', '1 day'),"
                        // names in another case than the table's
                        + "('LAKE.main.mixedcase', 'eventtime', '30 days'),"
                        // a column that does not exist: fails, the other tables still run
                        + "('lake.main.broken', 'no_such_column', '1 day'),"
                        // another database's table: not this cycle's
                        + "('other.main.events', 'ts', '1 day'),"
                        // rows that cannot be used are skipped
                        + "('lake.main.events; DROP TABLE lake.main.kept', 'ts', '1 day'),"
                        + "('lake.main.events', 'ts', 'soon')");

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        CompactionState state = new CompactionState(registry, List.of(DB), List.of());
        DataRetention.Outcome outcome;
        try (DuckLakeDataRetention retention = new DuckLakeDataRetention(startupScript(catalog, data), List.of(DB),
                "lake.main.retention_policy", List.of(), state, Clock.fixed(NOW, ZoneOffset.UTC))) {
            outcome = retention.enforce(DB);
        }

        assertEquals(5, outcome.tables(), "events, daily, tz_events, MixedCase and broken; kept is unlimited");
        assertEquals(1, outcome.failedTables(), "broken has no such column");
        assertEquals(4, outcome.rowsDeleted());

        assertEquals(2, count(catalog, data, "SELECT count(*) FROM lake.main.events"),
                "the 10-day-old row and the row without a time are kept");
        assertEquals(0, count(catalog, data, "SELECT count(*) FROM lake.main.events WHERE id = 1"));
        assertEquals(2, count(catalog, data, "SELECT count(*) FROM lake.main.daily"),
                "the cutoff's day (2026-10-12) and later are kept");
        assertEquals(1, count(catalog, data, "SELECT count(*) FROM lake.main.tz_events"),
                "only the row before now - 720h is deleted");
        assertEquals(1, count(catalog, data, "SELECT count(*) FROM lake.main.kept"));
        assertEquals(1, count(catalog, data, "SELECT count(*) FROM lake.main.\"MixedCase\""));
        assertEquals(1, count(catalog, data, "SELECT count(*) FROM lake.main.broken"));

        assertEquals(1.0, registry.get("ducklake.retention.rows_deleted")
                .tag("database", DB).tag("table", "lake.main.events").counter().count());
        assertEquals(0, count(catalog, data, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_delete_file"),
                "only whole files are deleted, so no delete file is written");
    }

    @Test
    void deletesOnlyWholeFilesBelowTheCutoff() throws Exception {
        Path catalog = tmp.resolve("catalog.ducklake");
        Path data = Files.createDirectories(tmp.resolve("data"));
        run(catalog, data,
                "CALL lake.set_option('data_inlining_row_limit', 0)",
                "CREATE TABLE lake.main.logs (ts TIMESTAMP)",
                // expired, below every other file
                "INSERT INTO lake.main.logs VALUES (TIMESTAMP '2026-08-01'), (TIMESTAMP '2026-08-02')",
                // straddles the cutoff (2026-10-12 12:00): kept whole, and the boundary drops to its minimum
                "INSERT INTO lake.main.logs VALUES (TIMESTAMP '2026-09-01'), (TIMESTAMP '2026-10-20')",
                // below the cutoff, but at or above the lowered boundary: kept this cycle
                "INSERT INTO lake.main.logs VALUES (TIMESTAMP '2026-09-15')",
                "CREATE TABLE lake.main.retention_policy (table_name VARCHAR, time_column VARCHAR, retention VARCHAR)",
                "INSERT INTO lake.main.retention_policy VALUES ('lake.main.logs', 'ts', '30 days')");

        CompactionState state = new CompactionState(new SimpleMeterRegistry(), List.of(DB), List.of());
        DataRetention.Outcome outcome;
        try (DuckLakeDataRetention retention = new DuckLakeDataRetention(startupScript(catalog, data), List.of(DB),
                "lake.main.retention_policy", List.of(), state, Clock.fixed(NOW, ZoneOffset.UTC))) {
            outcome = retention.enforce(DB);
        }

        assertEquals(0, outcome.failedTables());
        assertEquals(2, outcome.rowsDeleted(), "only the August file");
        assertEquals(3, count(catalog, data, "SELECT count(*) FROM lake.main.logs"),
                "the straddling file and the file between the boundary and the cutoff are kept");
        assertEquals(0, count(catalog, data,
                "SELECT count(*) FROM lake.main.logs WHERE ts BETWEEN TIMESTAMP '2026-08-01' AND TIMESTAMP '2026-08-02'"));
        assertEquals(0, count(catalog, data, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_delete_file"),
                "no file was partly deleted");
    }

    @Test
    void theBoundaryNeverSplitsAFile() {
        DuckLakeDataRetention.FileRange expired = new DuckLakeDataRetention.FileRange(10L, 20L, false, true);
        DuckLakeDataRetention.FileRange straddles = new DuckLakeDataRetention.FileRange(30L, 120L, false, true);
        DuckLakeDataRetention.FileRange fresh = new DuckLakeDataRetention.FileRange(150L, 200L, false, true);
        DuckLakeDataRetention.FileRange chained = new DuckLakeDataRetention.FileRange(25L, 40L, false, true);

        assertEquals(java.util.OptionalLong.of(100L),
                DuckLakeDataRetention.fileBoundary(List.of(expired, fresh), 100L), "nothing straddles the cutoff");
        assertEquals(java.util.OptionalLong.of(30L),
                DuckLakeDataRetention.fileBoundary(List.of(expired, straddles, fresh), 100L));
        assertEquals(java.util.OptionalLong.of(25L),
                DuckLakeDataRetention.fileBoundary(List.of(expired, straddles, chained), 100L),
                "lowering to 30 makes the 25-40 file straddle, so it drops again");
        assertTrue(DuckLakeDataRetention.fileBoundary(List.of(straddles, fresh), 100L).isEmpty(),
                "no file lies entirely below the boundary");
        assertTrue(DuckLakeDataRetention.fileBoundary(
                        List.of(new DuckLakeDataRetention.FileRange(10L, 20L, true, true)), 100L).isEmpty(),
                "a file with NULLs cannot be dropped whole, so it alone does not start a delete");
        assertEquals(java.util.OptionalLong.of(100L), DuckLakeDataRetention.fileBoundary(
                        List.of(expired, new DuckLakeDataRetention.FileRange(5L, 50L, true, true)), 100L),
                "a file with NULLs does not move the boundary, or it would hold retention back forever");
        assertEquals(java.util.OptionalLong.of(100L), DuckLakeDataRetention.fileBoundary(
                        List.of(expired, new DuckLakeDataRetention.FileRange(null, null, true, true)), 100L),
                "a file of NULLs only does not move the boundary");
    }

    @Test
    void aMissingPolicyTableFailsTheCycle() throws Exception {
        Path catalog = tmp.resolve("catalog.ducklake");
        Path data = Files.createDirectories(tmp.resolve("data"));
        run(catalog, data, "CREATE TABLE lake.main.t (ts TIMESTAMP)");

        CompactionState state = new CompactionState(new SimpleMeterRegistry(), List.of(DB), List.of());
        try (DuckLakeDataRetention retention = new DuckLakeDataRetention(startupScript(catalog, data), List.of(DB),
                "lake.main.no_such_policy", List.of(), state)) {
            assertThrows(Exception.class, () -> retention.enforce(DB));
        }
    }

    @Test
    void theServiceCountsAFailedTableOrCycleAsARetentionFailure() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        CompactionState state = new CompactionState(registry, List.of(DB), List.of());
        CompactionConfig config = new CompactionConfig(List.of(DB), List.of(), Duration.ofMinutes(5),
                Duration.ofMinutes(5), List.of(), 0, 10, false, null,
                new DataRetentionConfig(true, Duration.ofHours(1), "lake.main.retention_policy", List.of()));
        DataRetention[] next = {database -> new DataRetention.Outcome(2, 10, 0)};
        DataRetention delegating = database -> next[0].enforce(database);
        try (CompactionService service = new CompactionService(config, null, (db, tier) -> null,
                database -> { }, delegating, state, new CompactionRunLog(10))) {
            service.runRetention(DB);
            assertEquals(0, retentionFailures(registry));

            next[0] = database -> new DataRetention.Outcome(2, 10, 1);
            service.runRetention(DB);
            assertEquals(1, retentionFailures(registry));

            next[0] = database -> {
                throw new IllegalStateException("policy table unreadable");
            };
            service.runRetention(DB);
            assertEquals(2, retentionFailures(registry), "a cycle that throws is counted, not propagated");
        }
    }

    @Test
    void aRetentionJobNeedsRetentionEnabled() {
        CompactionConfig disabled = new CompactionConfig(List.of(DB), List.of(), Duration.ofMinutes(5),
                Duration.ofMinutes(5), List.of(), 0, 10);
        CompactionState state = new CompactionState(new SimpleMeterRegistry(), List.of(DB), List.of());
        assertThrows(IllegalArgumentException.class, () -> new CompactionService(disabled, null, (db, tier) -> null,
                database -> { }, database -> new DataRetention.Outcome(0, 0, 0), state, new CompactionRunLog(10)));
    }

    @Test
    void configIsOffByDefaultAndChecksThePolicyTable() {
        assertFalse(DataRetentionConfig.from(ConfigFactory.parseString("")).enabled());
        assertThrows(IllegalArgumentException.class, () -> DataRetentionConfig.from(
                ConfigFactory.parseString("data_retention { enabled = true }")));
        assertThrows(IllegalArgumentException.class, () -> DataRetentionConfig.from(ConfigFactory.parseString(
                "data_retention { enabled = true, policy_table = \"lake.main.p; DROP TABLE x\" }")));
        DataRetentionConfig config = DataRetentionConfig.from(ConfigFactory.parseString(
                "data_retention { enabled = true, frequency = 10 minutes, policy_table = lake.main.retention_policy }"));
        assertEquals(Duration.ofMinutes(10), config.frequency());
        assertEquals("lake.main.retention_policy", config.policyTable());
    }

    private static double retentionFailures(SimpleMeterRegistry registry) {
        return registry.get("ducklake.compaction.failures")
                .tag("database", DB).tag("type", CompactionState.RETENTION_KIND).functionCounter().count();
    }
}
