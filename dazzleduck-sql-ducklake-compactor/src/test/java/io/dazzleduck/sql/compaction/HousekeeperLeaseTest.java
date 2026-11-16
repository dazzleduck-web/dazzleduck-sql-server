package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The delete-file rewrite under table leases (review on #542): a table another job holds is skipped,
 * so housekeeping is not held up, and waited for only after it was skipped three times in a row.
 */
class HousekeeperLeaseTest {

    @TempDir
    Path tmp;

    @Test
    void aHeldTableIsSkippedThenWaitedForAfterThreeSkips() throws Exception {
        Path catalog = tmp.resolve("catalog.ducklake");
        Path data = Files.createDirectories(tmp.resolve("data"));
        String startup = "INSTALL ducklake;\nLOAD ducklake;\nATTACH 'ducklake:" + catalog + "' AS lake (DATA_PATH '"
                + data + "/');\n";
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            for (String sql : startup.split(";\n")) {
                s.execute(sql);
            }
            s.execute("CALL lake.set_option('data_inlining_row_limit', 0)");
            s.execute("CREATE TABLE lake.main.t (id INTEGER)");
            s.execute("INSERT INTO lake.main.t SELECT range FROM range(100)");
            s.execute("DELETE FROM lake.main.t WHERE id < 90");
        }
        TableLeases leases = new TableLeases();
        String key = TableLeases.key("lake", "main", "t");
        assertTrue(leases.tryAcquire(key), "a tier loop is merging the table");
        CompactionState state = new CompactionState(new SimpleMeterRegistry(), List.of("lake"), List.of());
        try (DuckLakeHousekeeper housekeeper = new DuckLakeHousekeeper(startup, Duration.ofHours(1), List.of(),
                true, 0.5, state, leases)) {
            for (int run = 1; run < DuckLakeHousekeeper.SKIPS_BEFORE_WAIT; run++) {
                housekeeper.housekeep("lake"); // returns: the held table is skipped
                assertEquals(0, state.getFilesRewritten("lake"), "run " + run + " skipped the held table");
            }
            Thread third = new Thread(() -> {
                try {
                    housekeeper.housekeep("lake");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            third.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!leases.hasWaiter(key)) {
                assertTrue(System.nanoTime() < deadline, "the third run waits for the table");
                Thread.sleep(5);
            }
            leases.release(key);
            third.join(30_000);
            assertFalse(third.isAlive());
            assertEquals(1, state.getFilesRewritten("lake"), "rewritten once the merge let go");
            assertFalse(leases.isBusy(key));
        }
    }
}
