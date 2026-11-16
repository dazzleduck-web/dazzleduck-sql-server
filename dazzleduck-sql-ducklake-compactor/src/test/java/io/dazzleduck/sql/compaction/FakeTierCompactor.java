package io.dazzleduck.sql.compaction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A {@link TierCompactor} driven by the test: it lists whatever {@link #work} holds, records every
 * call, and fails a table's call while {@link #failing} names it.
 */
final class FakeTierCompactor implements TierCompactor {

    /** What discover returns. */
    volatile List<TableWork> work = List.of();
    /** Table names (schema.table) whose calls throw, with the message to throw. */
    final Map<String, String> failing = new HashMap<>();
    /** Thrown by discover when set. */
    volatile RuntimeException discoverFailure;
    /** Returned by prepare. */
    volatile Readiness readiness = Readiness.ready(0, 2);
    /** Runs inside each call, e.g. to advance a test clock. */
    volatile Consumer<TableWork> duringCall = work -> { };

    record Call(String database, String tier, String table, int cap, long target) {
    }

    final List<Call> calls = new ArrayList<>();
    int prepares;
    int resets;

    static TableWork table(String name, long files, long bytes, Instant oldest, int busy, boolean sorted) {
        String[] parts = name.split("\\.");
        return new TableWork(parts[0], parts[1], files, bytes, oldest, busy, sorted);
    }

    @Override
    public Readiness prepare(String database, CompactionTier tier) {
        prepares++;
        return readiness;
    }

    @Override
    public List<TableWork> discover(String database, CompactionTier tier) {
        if (discoverFailure != null) {
            throw discoverFailure;
        }
        return work;
    }

    @Override
    public MergeOutcome compact(String database, CompactionTier tier, TableWork table, int cap, long target) {
        calls.add(new Call(database, tier.name(), table.name(), cap, target));
        duringCall.accept(table);
        String failure = failing.get(table.name());
        if (failure != null) {
            throw new IllegalStateException(failure);
        }
        return new MergeOutcome(3, 4L, 1L);
    }

    @Override
    public void reset(String database, CompactionTier tier) {
        resets++;
    }

    List<String> calledTables() {
        return calls.stream().map(Call::table).toList();
    }
}
