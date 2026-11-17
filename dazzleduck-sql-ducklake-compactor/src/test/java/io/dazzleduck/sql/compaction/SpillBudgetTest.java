package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Review on #542: an explicit spill directory per instance, created and checked when it opens. */
class SpillBudgetTest {

    @TempDir
    Path dir;

    @Test
    void eachInstanceGetsItsOwnDirectoryAndAShareOfTheBudget() throws Exception {
        SpillBudget spill = new SpillBudget(dir.resolve("spill"), 10_000_000_000L);
        List<String> settings = spill.apply("my lake", "tier-minor", 0.2, 2, List.of("SET threads = 4"));
        Path instance = dir.resolve("spill").resolve("my_lake").resolve("tier-minor");
        assertTrue(Files.isDirectory(instance), "created, missing parents too");
        assertEquals(List.of("SET temp_directory = '" + instance + "'",
                "SET max_temp_directory_size = '680000000 bytes'", "SET threads = 4"), settings,
                "68% of 20% of 10 GB over two databases");

        try (Connection c = RawConnections.open("", settings); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT current_setting('temp_directory')")) {
            rs.next();
            assertEquals(instance.toString(), rs.getString(1), "DuckDB takes the settings");
        }
        assertEquals(SpillBudget.MINIMUM_BYTES, spill.limit(0.0001, 1), "never below the minimum");
    }

    @Test
    void anInstanceThatSetsItsOwnDirectoryKeepsIt() throws Exception {
        SpillBudget spill = new SpillBudget(dir, 10_000_000_000L);
        List<String> own = List.of("SET temp_directory = '/data/spill'");
        assertSame(own, spill.apply("lake", "tier-minor", 0.2, 1, own));
        List<String> sized = spill.apply("lake", "tier-minor", 0.2, 1, List.of("SET max_temp_directory_size = '5GB'"));
        assertEquals(2, sized.size(), "its own size is kept, and no computed one added");
        assertTrue(sized.get(0).startsWith("SET temp_directory"), sized.get(0));
        assertSame(own, SpillBudget.NONE.apply("lake", "tier-minor", 0.2, 1, own));
    }

    @Test
    void anUnwritableDirectoryIsReportedWhenTheInstanceOpens() throws Exception {
        Path file = Files.createFile(dir.resolve("not-a-directory"));
        SpillBudget spill = new SpillBudget(file, 10_000_000_000L);
        SQLException e = assertThrows(SQLException.class, () -> spill.apply("lake", "tier-minor", 0.2, 1, List.of()));
        assertTrue(e.getMessage().contains("not writable"), e.getMessage());
    }

    @Test
    void theBudgetIsTheFreeSpaceOrTheCapWhicheverIsSmaller() {
        SpillBudget uncapped = SpillBudget.create(dir.toString(), -1);
        assertTrue(uncapped.budgetBytes() > 0, "the free space under the root");
        assertEquals(1_000_000L, SpillBudget.create(dir.toString(), 1_000_000L).budgetBytes(), "the cap");
        assertSame(SpillBudget.NONE, SpillBudget.create("", 1_000_000L));
    }

    @Test
    void underAConfiguredCapTheSizesNeverAddUpToMoreThanIt() {
        // Review of #544: six instances raised to 256 MiB went over a 1 GB cap, which evicts the pod
        SpillBudget capped = SpillBudget.create(dir.toString(), 1_000_000_000L);
        double[] shares = {0.5 * 0.4, 0.5 * 0.3, 0.5 * 0.3, MemoryBudget.HOUSEKEEPING, MemoryBudget.RETENTION,
                MemoryBudget.FILE_COUNTS};
        long total = 0;
        for (double share : shares) {
            total += capped.limit(share, 1);
        }
        assertTrue(total <= 1_000_000_000L, "the sizes add up to " + total);
        // Review of #544: with MemoryBudget's shares adding up to 98%, spill could fill nearly all the disk
        double all = MemoryBudget.TIERS + MemoryBudget.HOUSEKEEPING + MemoryBudget.RETENTION + MemoryBudget.FILE_COUNTS;
        assertTrue(SpillBudget.BUDGET_SHARE * all <= 0.7, "a third of the free space stays for the disk's other users");
        assertEquals(27_200_000L, capped.limit(MemoryBudget.FILE_COUNTS, 1), "no 256 MiB minimum under a cap: 68% of 4% of 1 GB");
        assertEquals(SpillBudget.MINIMUM_BYTES, new SpillBudget(dir, 1_000_000_000L).limit(MemoryBudget.FILE_COUNTS, 1),
                "a budget of the free space keeps the minimum");
    }

    @Test
    void spillFilesAnEarlierProcessLeftAreDeletedTheFirstTimeOnly() throws Exception {
        // Review of #544: a killed process leaves its spill files, which DuckDB removes only on its next spill
        SpillBudget spill = new SpillBudget(dir, 10_000_000_000L);
        Path instance = spill.directory("lake", "tier-minor");
        Files.createDirectories(instance);
        Path leftover = Files.write(instance.resolve("duckdb_temp_storage_DEFAULT-0.tmp"), new byte[1_000]);
        Path other = Files.write(instance.resolve("notes.txt"), new byte[10]);
        spill.apply("lake", "tier-minor", 0.2, 1, List.of());
        assertFalse(Files.exists(leftover), "deleted before the instance opens");
        assertTrue(Files.exists(other), "only DuckDB's spill files");

        // a later open in the same process (after the instance was replaced) leaves the directory alone
        Path current = Files.write(instance.resolve("duckdb_temp_storage_DEFAULT-0.tmp"), new byte[1_000]);
        spill.apply("lake", "tier-minor", 0.2, 1, List.of());
        assertTrue(Files.exists(current));
    }

    @Test
    void healthAlertsAgainstTheSizesGivenOutAndTheSpaceTheyMayStillNeed() throws Exception {
        // Review of #544: the given sizes add up to 68% of the budget, so an 80%-of-budget alert never fired
        SpillBudget spill = new SpillBudget(dir, 10_000_000_000L, true);
        spill.apply("lake", "tier-minor", 0.0001 / SpillBudget.BUDGET_SHARE, 1, List.of()); // 1,000,000 bytes
        Files.write(spill.directory("lake", "tier-minor").resolve("duckdb_temp_storage_DEFAULT-0.tmp"), new byte[900_000]);
        CompactionStats.SpillStatus status = spill.status();
        assertEquals(1_000_000L, status.allocatedBytes());
        assertEquals(900_000L, status.usedBytes());
        assertTrue(status.freeBytes() > 0);
        List<String> alerts = SpillBudget.alerts(status);
        assertEquals(1, alerts.size(), "900 KB of the 1 MB given is above 80%");
        assertTrue(alerts.get(0).contains("Spill under"), alerts.get(0));

        assertEquals(List.of(), SpillBudget.alerts(new CompactionStats.SpillStatus("/s", 10L << 30, 4L << 30, 0, 5L << 30)));
        List<String> disk = SpillBudget.alerts(new CompactionStats.SpillStatus("/s", 10L << 30, 4L << 30, 1L << 30, 2L << 30));
        assertEquals(1, disk.size(), "2 GiB free, but the instances may still spill 3 GiB");
        assertTrue(disk.get(0).contains("may still spill"), disk.get(0));
        assertNull(SpillBudget.NONE.status());
    }
}
