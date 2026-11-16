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
                "SET max_temp_directory_size = '1000000000 bytes'", "SET threads = 4"), settings,
                "20% of 10 GB over two databases");

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
    void healthReportsUsageAndAlertsWhenSpillOrDiskRunsLow() throws Exception {
        SpillBudget spill = new SpillBudget(dir, 1_000L);
        Files.write(dir.resolve("block.tmp"), new byte[900]);
        CompactionStats.SpillStatus status = spill.status();
        assertEquals(900L, status.usedBytes());
        assertTrue(status.freeBytes() > 0);
        List<String> alerts = SpillBudget.alerts(status);
        assertEquals(1, alerts.size(), "900 of 1,000 bytes is above 80%");
        assertTrue(alerts.get(0).contains("Spill under"), alerts.get(0));

        assertEquals(1, SpillBudget.alerts(new CompactionStats.SpillStatus("/s", 1_000_000L, 0, 50_000L)).size(),
                "free space below 10% of the budget");
        assertEquals(List.of(), SpillBudget.alerts(new CompactionStats.SpillStatus("/s", 1_000_000L, 0, 500_000L)));
        assertNull(SpillBudget.NONE.status());
    }
}
