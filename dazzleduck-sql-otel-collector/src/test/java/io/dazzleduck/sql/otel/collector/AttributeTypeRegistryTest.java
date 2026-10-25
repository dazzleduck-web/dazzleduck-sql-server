package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.IngestionResult;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTask;
import io.dazzleduck.sql.commons.ingestion.SidecarRows;
import io.dazzleduck.sql.otel.collector.config.AttributeTypeSettings;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link AttributeTypeRegistry}'s claim/settle bookkeeping: which mappings a batch carries, what
 * happens to them when the batch commits or does not, and the {@code max_entries} cap. The rows
 * themselves are written by the ingestion transaction, which
 * {@link AttributeTypeSidecarTest} and {@link OtelCollectorAttributeTypesTest} cover.
 */
public class AttributeTypeRegistryTest {

    private static final String TABLE = "otel_attribute_types";

    /** Stands in for the registry table's data directory, which startup creates. */
    @org.junit.jupiter.api.io.TempDir
    java.nio.file.Path dataPath;

    private static AttributeTypeRegistry registry(int maxEntries) {
        return new AttributeTypeRegistry(new AttributeTypeSettings(true, maxEntries, maxEntries),
                new OtelCollectorMetrics(new SimpleMeterRegistry()));
    }

    /** Distinct caps, so which one bites can be told apart. */
    private static AttributeTypeRegistry registry(int maxEntries, int perStream) {
        return new AttributeTypeRegistry(new AttributeTypeSettings(true, maxEntries, perStream),
                new OtelCollectorMetrics(new SimpleMeterRegistry()));
    }

    /** Claims and commits {@code count} distinct mappings for a queue, returning how many stuck. */
    private static int record(AttributeTypeRegistry registry, String queue, String prefix, int count) {
        int recorded = 0;
        for (int i = 0; i < count; i++) {
            var claimed = registry.claim(queue, Signal.LOGS, scanOf(prefix + i, str("v")));
            registry.settle(claimed, true);
            recorded += claimed.size();
        }
        return recorded;
    }

    /**
     * The reason this cap exists. One stream whose attribute NAMES embed ids exhausts its own
     * budget, and every other stream must keep recording — under a single global cap it would not,
     * which is the failure mode that punishes the wrong streams.
     */
    @Test
    void oneStreamAtItsCapDoesNotStopTheOthers() {
        var registry = registry(100, 3);

        assertEquals(3, record(registry, "noisy", "user.id-", 10),
                "the noisy stream stops at its own cap of 3");
        assertEquals(3, record(registry, "quiet", "service.name-", 3),
                "a well-behaved stream is unaffected by its neighbour");
        assertEquals(3, record(registry, "third", "host.name-", 3));

        // And the global budget was never near: 9 held against a cap of 100.
        assertEquals(9, registry.knownCount());
    }

    /**
     * A cap has to hold within one batch, not only across batches. The case it exists for — ids
     * embedded in attribute names — arrives as one scan carrying hundreds of distinct names, so a
     * check that counts only what is already committed lets a whole batch through.
     */
    @Test
    void aSingleBatchCannotExceedTheCaps() {
        var registry = registry(100, 3);
        var scan = new AttributeTypeScan();
        var cursor = scan.cursor(AttributeScope.ATTRIBUTE);
        List<KeyValue> attributes = new java.util.ArrayList<>();
        for (int i = 0; i < 50; i++) {
            attributes.add(KeyValue.newBuilder().setKey("user." + i + ".action").setValue(str("v")).build());
        }
        cursor.observe(attributes);

        var claimed = registry.claim("noisy", Signal.LOGS, scan);
        assertEquals(3, claimed.size(), "one batch of 50 names must still stop at the cap of 3");
        registry.settle(claimed, true);
        assertEquals(3, registry.knownCount());
    }

    /**
     * A claim that is never handed to a batch has to be settled anyway, or the in-flight gauge
     * drifts up for the life of the process. The services do this when {@code addBatch} throws
     * before returning a future; what is pinned here is that settling as failed releases the
     * mappings and leaves nothing in flight, so a later batch carries them again.
     */
    @Test
    void aClaimSettledAsFailedIsReleasedAndLeavesNothingInFlight() {
        var registry = registry(100);
        var claimed = registry.claim("q", Signal.LOGS, scanOf("x", str("v")));
        assertEquals(1, claimed.size());
        assertEquals(1, registry.inFlightCount());

        registry.settle(claimed, false);
        assertEquals(0, registry.inFlightCount(), "released, not left in flight");
        assertEquals(0, registry.knownCount(), "and not recorded, since the batch did not commit");

        // So the next batch carries it again.
        assertEquals(1, registry.claim("q", Signal.LOGS, scanOf("x", str("v"))).size());
    }

    /**
     * The cap is a count of mappings HELD, so it must be reached exactly rather than approached:
     * a cap of 3 holds three, and the fourth is the first one dropped. Pinned because the check
     * now sums two counters, which is where an off-by-one would hide.
     */
    @Test
    void theCapIsReachedExactlyNotApproximately() {
        var registry = registry(100, 3);
        assertEquals(2, record(registry, "q", "a", 2), "two of three");
        assertEquals(1, record(registry, "q", "b", 1), "the third is still accepted");
        assertEquals(3, registry.knownCount());
        assertEquals(0, record(registry, "q", "c", 1), "the fourth is dropped");

        // And across the boundary inside one batch, where the sum is what decides.
        var fresh = registry(100, 3);
        assertEquals(2, record(fresh, "q", "x", 2));
        var scan = new AttributeTypeScan();
        var cursor = scan.cursor(AttributeScope.ATTRIBUTE);
        cursor.observe(List.of(KeyValue.newBuilder().setKey("y0").setValue(str("v")).build(),
                KeyValue.newBuilder().setKey("y1").setValue(str("v")).build()));
        assertEquals(1, fresh.claim("q", Signal.LOGS, scan).size(),
                "two held plus a two-name batch must take exactly one more");
    }

    /** The same, for the process-wide bound. */
    @Test
    void aSingleBatchCannotExceedTheGlobalCap() {
        var registry = registry(3, 100);
        var scan = new AttributeTypeScan();
        var cursor = scan.cursor(AttributeScope.ATTRIBUTE);
        List<KeyValue> attributes = new java.util.ArrayList<>();
        for (int i = 0; i < 50; i++) {
            attributes.add(KeyValue.newBuilder().setKey("a" + i).setValue(str("v")).build());
        }
        cursor.observe(attributes);
        assertEquals(3, registry.claim("q", Signal.LOGS, scan).size());
    }

    /** A queue may raise its own cap, but not past the heap bound. */
    @Test
    void aQueueCapIsClampedToTheGlobalOne() {
        var settings = new AttributeTypeSettings(true, 100, 10);
        assertEquals(10, settings.capFor(null), "no override: the per-stream default");
        assertEquals(50, settings.capFor(50), "its own, when it set one");
        assertEquals(100, settings.capFor(1_000), "never above max_entries, which bounds the heap");
    }

    /** The global cap still bites when the per-stream caps would together exceed it. */
    @Test
    void theGlobalCapStillBoundsTheProcess() {
        var registry = registry(5, 4);
        assertEquals(4, record(registry, "a", "x", 4), "its own cap first");
        assertEquals(1, record(registry, "b", "y", 4), "then the process budget, with one left");
        assertEquals(5, registry.knownCount());
    }

    /** A one-row scan of the record's own attributes. */
    private static AttributeTypeScan scanOf(String name, AnyValue value) {
        var scan = new AttributeTypeScan();
        scan.cursor(AttributeScope.ATTRIBUTE).observe(
                List.of(KeyValue.newBuilder().setKey(name).setValue(value).build()));
        return scan;
    }

    private static AnyValue str(String v) {
        return AnyValue.newBuilder().setStringValue(v).build();
    }

    private static AnyValue num(long v) {
        return AnyValue.newBuilder().setIntValue(v).build();
    }

    /** {@code claimed} as sorted {@code queue/signal/scope/name/type} text. */
    private static String keys(AttributeTypeRegistry.Claim claimed) {
        return String.join(" ", claimed.keys().stream()
                .map(k -> String.join("/", k.queue(), k.signal().wireName(), k.scope().wireName(),
                        k.name(), k.type().wireName()))
                .sorted().toList());
    }

    @Test
    void aMappingIsCarriedUntilItIsCommittedAndNotAfterwards() {
        var registry = registry(100);
        var claimed = registry.claim("logs", Signal.LOGS, scanOf("x", str("v")));
        assertEquals("logs/logs/attribute/x/string", keys(claimed));
        assertEquals(1, registry.inFlightCount());

        registry.settle(claimed, true);
        assertEquals(1, registry.knownCount());
        assertEquals(0, registry.inFlightCount());
        assertTrue((registry.claim("logs", Signal.LOGS, scanOf("x", str("v")))).isEmpty(), "committed mappings are never carried again");
    }

    /**
     * Every batch carries every uncommitted mapping, including one another batch already has in
     * flight. The alternative — skipping it — loses the mapping in exactly this interleaving: B
     * commits while A, the only carrier, fails, and B's registered files then have no row to
     * explain them.
     */
    @Test
    void aBatchCarriesAMappingAnotherBatchAlreadyHasInFlight() {
        var registry = registry(100);
        var batchA = registry.claim("logs", Signal.LOGS, scanOf("x", num(1)));
        var batchB = registry.claim("logs", Signal.LOGS, scanOf("x", num(1)));
        assertEquals("logs/logs/attribute/x/int", keys(batchA));
        assertEquals("logs/logs/attribute/x/int", keys(batchB), "B carries it too: A has not committed");
        assertEquals(2, registry.inFlightCount(), "counted once per carrier");

        registry.settle(batchB, true);   // B commits...
        registry.settle(batchA, false);  // ...and its carrier fails afterwards

        assertEquals(1, registry.knownCount(), "B's commit recorded it, so the data is explained");
        assertEquals(0, registry.inFlightCount());
        assertTrue((registry.claim("logs", Signal.LOGS, scanOf("x", num(1)))).isEmpty(), "and it is not carried a third time");
    }

    /** The same interleaving the other way round: the failure first, the commit after. */
    @Test
    void aFailedBatchDoesNotUnrecordWhatAnotherBatchCommitted() {
        var registry = registry(100);
        var batchA = registry.claim("logs", Signal.LOGS, scanOf("x", num(1)));
        var batchB = registry.claim("logs", Signal.LOGS, scanOf("x", num(1)));
        registry.settle(batchA, false);
        registry.settle(batchB, true);
        assertEquals(1, registry.knownCount());
        assertTrue((registry.claim("logs", Signal.LOGS, scanOf("x", num(1)))).isEmpty());
    }

    /**
     * The point of settling on the batch's own future: a batch that did not commit wrote no rows,
     * so its mappings must stay unrecorded rather than be remembered as written.
     */
    @Test
    void aMappingWhoseBatchDidNotCommitIsCarriedAgain() {
        var registry = registry(100);
        var claimed = registry.claim("logs", Signal.LOGS, scanOf("x", str("v")));
        registry.settle(claimed, false);
        assertEquals(0, registry.knownCount(), "not known: nothing was committed");
        assertEquals(0, registry.inFlightCount(), "no longer counted as in flight");

        var again = registry.claim("logs", Signal.LOGS, scanOf("x", str("v")));
        assertEquals("logs/logs/attribute/x/string", keys(again));
    }

    @Test
    void aTypeChangeIsItsOwnMapping() {
        var registry = registry(100);
        registry.settle(registry.claim("logs", Signal.LOGS, scanOf("x", str("v"))), true);
        var claimed = registry.claim("logs", Signal.LOGS, scanOf("x", num(1)));
        assertEquals("logs/logs/attribute/x/int", keys(claimed));
    }

    /** Queue and signal are part of the key, so queues sharing a table still record separately. */
    @Test
    void theSameNameInAnotherQueueOrSignalIsItsOwnMapping() {
        var registry = registry(100);
        registry.settle(registry.claim("logs", Signal.LOGS, scanOf("x", str("v"))), true);
        assertEquals("other/logs/attribute/x/string",
                keys(registry.claim("other", Signal.LOGS, scanOf("x", str("v")))));
        assertEquals("logs/traces/attribute/x/string",
                keys(registry.claim("logs", Signal.TRACES, scanOf("x", str("v")))));
    }

    @Test
    void pastTheMaxEntriesCapNothingFurtherIsClaimed() {
        var registry = registry(2);
        registry.settle(registry.claim("logs", Signal.LOGS, scanOf("a", str("v"))), true);
        registry.settle(registry.claim("logs", Signal.LOGS, scanOf("b", str("v"))), true);
        assertTrue((registry.claim("logs", Signal.LOGS, scanOf("c", str("v")))).isEmpty());
        // A mapping already known is still recognised at the cap, rather than counted as dropped.
        assertTrue((registry.claim("logs", Signal.LOGS, scanOf("a", str("v")))).isEmpty());
        assertEquals(2, registry.knownCount());
    }

    /**
     * The cap is on what is held, not on what is in flight — so concurrent batches can carry the
     * same new mapping and push the held count past it by at most the batches in flight, and a
     * batch that fails costs nothing against it.
     */
    @Test
    void theCapIsOnWhatIsHeldRatherThanWhatIsInFlight() {
        var registry = registry(1);
        var claimed = registry.claim("logs", Signal.LOGS, scanOf("a", str("v")));
        assertEquals(1, claimed.size());
        assertEquals(1, registry.claim("logs", Signal.LOGS, scanOf("b", str("v"))).size(),
                "nothing is held yet, so room remains");
        registry.settle(claimed, true);
        assertTrue((registry.claim("logs", Signal.LOGS, scanOf("c", str("v")))).isEmpty(), "now one is held, and the cap is one");
    }

    @Test
    void theRowsNameTheConfiguredTableAndCarryEveryColumn() {
        var registry = registry(100);
        var claimed = registry.claim("logs", Signal.LOGS, scanOf("x", str("v")));
        SidecarRows rows = registry.sidecar(claimed, TABLE);
        assertEquals(TABLE, rows.table(), "unqualified: the post-ingestion task resolves the schema");
        assertEquals(List.of("ingestion_queue", "signal", "attribute_scope", "attribute_name",
                "value_type", "first_seen", "collector_id"), rows.columns());
        assertEquals(1, rows.rows().size());
        List<String> row = rows.rows().get(0);
        assertEquals(List.of("'logs'", "'logs'", "'attribute'", "'x'", "'string'"), row.subList(0, 5));
        assertTrue(row.get(5).startsWith("TIMESTAMPTZ '"), row.get(5));
        assertTrue(row.get(6).startsWith("'"), row.get(6));
    }

    /** Nothing from the wire reaches the statement unescaped — the batch depends on it. */
    @Test
    void anAttributeNameWithAQuoteIsEscaped() {
        var registry = registry(100);
        var claimed = registry.claim("logs", Signal.LOGS, scanOf("it's ok'", str("v")));
        assertEquals("'it''s ok'''", registry.sidecar(claimed, TABLE).rows().get(0).get(3));
    }

    /** A name that cannot be written as a SQL string at all is dropped, not sent. */
    @Test
    void anAttributeNameWithANulByteIsDroppedRatherThanRisked() {
        var registry = registry(100);
        assertTrue((registry.claim("logs", Signal.LOGS, scanOf("bad\u0000name", str("v")))).isEmpty());
        assertEquals(0, registry.inFlightCount());
    }

    @Test
    void thereAreNoRowsWhenNothingWasClaimed() {
        var registry = registry(100);
        // An empty claim — a batch whose mappings were all already committed — and no claim at all.
        assertNull(registry.sidecar(registry.claim("q", Signal.LOGS, new AttributeTypeScan()), TABLE));
        assertNull(registry.sidecar(null, TABLE));
    }

    /**
     * Settling twice must not decrement the gauge twice or double-count the counters. The services
     * attach a completion handler that settles, so a later synchronous failure on that path can
     * reach settle again; absorbing it here is what makes that safe rather than asking every
     * caller to remember.
     */
    @Test
    void settlingTheSameClaimTwiceIsIgnored() {
        var registry = registry(100);
        var claimed = registry.claim("q", Signal.LOGS, scanOf("x", str("v")));
        assertEquals(1, claimed.size());
        assertEquals(1, registry.inFlightCount());

        registry.settle(claimed, true);
        assertEquals(0, registry.inFlightCount());
        assertEquals(1, registry.knownCount());

        registry.settle(claimed, true);
        assertEquals(0, registry.inFlightCount(), "not -1: the second settle is ignored");
        assertEquals(1, registry.knownCount());

        // And a contradictory second outcome cannot un-record what the first one recorded.
        registry.settle(claimed, false);
        assertEquals(0, registry.inFlightCount());
        assertEquals(1, registry.knownCount(), "still known, and not counted as a rollback");
    }

    // --- the two optional switches -------------------------------------------------------

    /** A queue that names no table is not even scanned. */
    @Test
    void aQueueWithNoTableIsNotScanned() {
        assertNull(AttributeTypeRegistry.scanFor(registry(100), "logs", null));
        assertNotNull(AttributeTypeRegistry.scanFor(registry(100), "logs", TABLE));
    }

    /** {@code enabled = false} turns it off for every queue, whatever they name. */
    @Test
    void aDisabledRegistryScansNothingAndClaimsNothing() {
        var registry = new AttributeTypeRegistry(new AttributeTypeSettings(false, 100, 100),
                new OtelCollectorMetrics(new SimpleMeterRegistry()));
        assertFalse(registry.isEnabled());
        assertNull(AttributeTypeRegistry.scanFor(registry, "logs", TABLE));
        assertTrue((registry.claim("logs", Signal.LOGS, scanOf("x", str("v")))).isEmpty());
    }

    /**
     * A queue at its cap can only drop or skip, so scanning it collects nothing — and it is the
     * expensive case: every row of an id-in-name stream has its own layout, so the previous-row
     * filter never fires and every attribute is hashed. Its neighbours must keep being scanned.
     */
    @Test
    void aQueueAtItsCapIsNotScannedWhileOthersStillAre() {
        var registry = registry(100, 2);
        assertNotNull(AttributeTypeRegistry.scanFor(registry, "noisy", TABLE), "before the cap");

        assertEquals(2, record(registry, "noisy", "user.id-", 5));
        assertNull(AttributeTypeRegistry.scanFor(registry, "noisy", TABLE),
                "at its cap: nothing a scan collected could be recorded");
        assertNotNull(AttributeTypeRegistry.scanFor(registry, "quiet", TABLE),
                "a different queue is unaffected — this is per stream");
    }

    /**
     * Skipping the scan means claim() never runs, and claim() is where drops are counted — so
     * without a signal at the skip, a capped stream's metrics go flat right after the cap, which
     * reads as recovery. Every skipped batch must be counted, per queue.
     */
    @Test
    void aSkippedBatchIsCountedSoACappedStreamDoesNotLookRecovered() {
        var meters = new SimpleMeterRegistry();
        var registry = new AttributeTypeRegistry(new AttributeTypeSettings(true, 100, 2),
                new OtelCollectorMetrics(meters));
        assertEquals(2, record(registry, "noisy", "user.id-", 2));
        for (int i = 0; i < 3; i++) {
            assertNull(AttributeTypeRegistry.scanFor(registry, "noisy", TABLE));
        }
        assertEquals(3.0, meters.get("otel.attribute_types.capped_batches").tag("queue", "noisy")
                .functionCounter().count(), "every skipped batch is counted, not just the first");
    }

    /** A queue stopped by the GLOBAL cap is named too, though it never dropped anything itself. */
    @Test
    void aQueueStoppedByTheGlobalCapIsNamed() {
        var meters = new SimpleMeterRegistry();
        var registry = new AttributeTypeRegistry(new AttributeTypeSettings(true, 2, 100),
                new OtelCollectorMetrics(meters));
        assertEquals(2, record(registry, "noisy", "a", 2));
        assertNull(AttributeTypeRegistry.scanFor(registry, "quiet", TABLE));
        assertEquals(1.0, meters.get("otel.attribute_types.capped_batches").tag("queue", "quiet")
                .functionCounter().count());
    }

    /** The process-wide cap stops every queue being scanned, not just the one that filled it. */
    @Test
    void theGlobalCapStopsScanningEveryQueue() {
        var registry = registry(2, 100);
        assertEquals(2, record(registry, "noisy", "a", 5));
        assertNull(AttributeTypeRegistry.scanFor(registry, "noisy", TABLE));
        assertNull(AttributeTypeRegistry.scanFor(registry, "quiet", TABLE),
                "the heap bound is the process's, so no queue can record more");
    }

    @Test
    void noRegistryAtAllMeansNoScan() {
        assertNull(AttributeTypeRegistry.scanFor(null, "logs", TABLE));
    }

    // --- the startup cache ---------------------------------------------------------------

    /**
     * At startup the process reads what its queues' tables already hold, so a restart does not
     * re-append everything. Only the queues this process serves are read: another process's
     * streams may share the table, and caching their mappings would cost memory for nothing.
     */
    @Test
    void startupLoadsThisProcessesOwnMappingsAndNoOthers() throws Exception {
        String table = "memory.main.startup_cache_test";
        try {
            ConnectionPool.execute(("CREATE TABLE %s (ingestion_queue VARCHAR, signal VARCHAR,"
                    + " attribute_scope VARCHAR, attribute_name VARCHAR, value_type VARCHAR,"
                    + " first_seen TIMESTAMP WITH TIME ZONE, collector_id VARCHAR)").formatted(table));
            ConnectionPool.execute(("INSERT INTO %s VALUES"
                    + " ('logs', 'logs', 'attribute', 'mine', 'string', now(), 'c1'),"
                    + " ('logs', 'logs', 'resource', 'mine', 'int', now(), 'c1'),"
                    // a duplicate, as two processes appending the same mapping produce
                    + " ('logs', 'logs', 'attribute', 'mine', 'string', now(), 'c2'),"
                    // another process's stream, sharing the table
                    + " ('elsewhere', 'logs', 'attribute', 'theirs', 'string', now(), 'c2'),"
                    // something a later build might write
                    + " ('logs', 'profiles', 'attribute', 'future', 'whatever', now(), 'c2')")
                    .formatted(table));

            var registry = registry(100);
            registry.start(handlerFor(table, dataPath.toString(), "logs"));
            assertEquals(2, registry.knownCount(), "the duplicate counts once; the other stream and the"
                    + " unrecognised row are not cached");
            assertEquals(1, registry.knownNameCount());
            assertTrue((registry.claim("logs", Signal.LOGS, scanOf("mine", str("v")))).isEmpty(), "a mapping already in the table is not appended again after a restart");
            assertEquals(1, registry.claim("logs", Signal.LOGS, scanOf("fresh", str("v"))).size(),
                    "a mapping the table does not hold still is");
        } finally {
            ConnectionPool.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * The per-stream cap must hold at STARTUP too. Loading only against the global cap lets one
     * noisy queue's history fill the whole budget on every restart, so a quiet queue sharing the
     * table loads nothing and is globally capped from its first batch — the cross-stream
     * starvation the per-stream cap exists to prevent, re-created at each restart.
     */
    @Test
    void startupLoadRespectsEachQueuesOwnCap() throws Exception {
        String table = "memory.main.startup_cap_test";
        try {
            ConnectionPool.execute(("CREATE TABLE %s (ingestion_queue VARCHAR, signal VARCHAR,"
                    + " attribute_scope VARCHAR, attribute_name VARCHAR, value_type VARCHAR,"
                    + " first_seen TIMESTAMP WITH TIME ZONE, collector_id VARCHAR)").formatted(table));
            // The noisy queue's history is written first, so a plain LIMIT reads it first.
            StringBuilder values = new StringBuilder();
            for (int i = 0; i < 20; i++) {
                values.append(i == 0 ? "" : ", ")
                        .append("('noisy', 'logs', 'attribute', 'user.%d', 'string', now(), 'c1')".formatted(i));
            }
            values.append(", ('quiet', 'logs', 'attribute', 'service.name', 'string', now(), 'c1')");
            ConnectionPool.execute("INSERT INTO %s VALUES %s".formatted(table, values));

            // A global cap of 5, of which the noisy queue may hold 3.
            var registry = registry(5, 3);
            registry.start(handlerFor(table, dataPath.toString(), "noisy", "quiet"));

            assertEquals(4, registry.knownCount(), "three for the noisy queue, one for the quiet one");
            assertTrue(registry.claim("quiet", Signal.LOGS, scanOf("service.name", str("v"))).isEmpty(),
                    "the quiet queue's mapping was loaded, so it is not appended again");
            assertEquals(1, registry.claim("quiet", Signal.LOGS, scanOf("host.name", str("v"))).size(),
                    "and the quiet queue can still record — the noisy one did not take the budget");
        } finally {
            ConnectionPool.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * A table that does not exist yet is where every deployment starts: startup creates it, so
     * nothing has to be provisioned to turn the feature on, and no batch carries DDL.
     */
    @Test
    void startupCreatesATableThatDoesNotExistYet() throws Exception {
        String table = "memory.main.created_at_startup_test";
        try {
            var registry = registry(100);
            registry.start(handlerFor(table, dataPath.toString(), "logs"));
            assertEquals(0, registry.knownCount(), "created, and empty");
            assertTrue(java.nio.file.Files.isDirectory(dataPath),
                    "and its data directory, since each batch writes a Parquet file into it");
            assertEquals(7L, (long) ConnectionPool.collectFirst(
                    ("SELECT count(*) FROM duckdb_columns() WHERE table_name ="
                            + " 'created_at_startup_test'"), Long.class),
                    "with all seven columns of the documented schema");
            assertEquals(1, registry.claim("logs", Signal.LOGS, scanOf("x", str("v"))).size());
        } finally {
            ConnectionPool.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /** Starting again over a table it already created is a no-op, not a failure. */
    @Test
    void startupOverAnExistingTableIsANoOp() throws Exception {
        String table = "memory.main.existing_at_startup_test";
        try {
            registry(100).start(handlerFor(table, dataPath.toString(), "logs"));
            var second = registry(100);
            second.start(handlerFor(table, dataPath.toString(), "logs"));
            assertEquals(0, second.knownCount());
        } finally {
            ConnectionPool.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * The reason creation moved out of the ingestion transaction: a table that cannot exist is a
     * startup error naming it, rather than every batch of that stream failing at commit once
     * traffic arrives.
     */
    @Test
    void aTableThatCannotBeCreatedFailsStartup() {
        var registry = registry(100);
        var failure = assertThrows(IllegalStateException.class,
                () -> registry.start(handlerFor("no_such_catalog.main.types", dataPath.toString(), "logs")));
        assertTrue(failure.getMessage().contains("no_such_catalog.main.types"), failure.getMessage());
    }

    /** Equally when the catalog is there but the schema is not. */
    @Test
    void aMissingSchemaFailsStartup() {
        var registry = registry(100);
        var failure = assertThrows(IllegalStateException.class,
                () -> registry.start(handlerFor("memory.no_such_schema.types", dataPath.toString(), "logs")));
        assertTrue(failure.getMessage().contains("memory.no_such_schema.types"), failure.getMessage());
    }

    /** No configured queue records anything: nothing is read at all. */
    @Test
    void startupWithNoQueueRecordingReadsNothing() {
        var registry = registry(100);
        registry.start(handlerFor(null, null, "logs"));
        assertEquals(0, registry.knownCount());
    }

    /**
     * A table with no data path is not a DuckLake table, and its mappings have nowhere to be
     * written — a startup error, not something to discover per batch.
     */
    @Test
    void aTableWithNoDataPathFailsStartup() throws Exception {
        String table = "memory.main.no_data_path_test";
        try {
            var registry = registry(100);
            var failure = assertThrows(IllegalStateException.class,
                    () -> registry.start(handlerWithNoDataPath(table, "logs")));
            assertTrue(failure.getMessage().contains("DuckLake"), failure.getMessage());
        } finally {
            ConnectionPool.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    void aDisabledRegistryCreatesNothingAndReadsNothingAtStartup() throws Exception {
        var registry = new AttributeTypeRegistry(new AttributeTypeSettings(false, 100, 100),
                new OtelCollectorMetrics(new SimpleMeterRegistry()));
        registry.start(handlerFor("memory.main.never_created", dataPath.toString(), "logs"));
        assertEquals(0, registry.knownCount());
        assertEquals(0L, (long) ConnectionPool.collectFirst(
                "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'never_created'", Long.class));
    }

    /** A handler whose queues all record into {@code table} (null for none), at {@code path}. */
    private static IngestionHandler handlerFor(String table, String path, String... queues) {
        return new IngestionHandler() {
            @Override public PostIngestionTask createPostIngestionTask(IngestionResult r) {
                return PostIngestionTask.NOOP;
            }
            @Override public String getTargetPath(String queueId) { return "./" + queueId; }
            @Override public String[] getPartitionBy(String queueId) { return new String[0]; }
            @Override public java.util.Set<String> getKnownQueues() { return java.util.Set.of(queues); }
            @Override public String getAttributeTypesTableRef(String queueId) { return table; }
            @Override public String getAttributeTypesTablePath(String queueId) { return path; }
        };
    }

    /**
     * A handler whose table is a plain DuckDB one with no data path — which the registry refuses,
     * since a batch's mappings are written as a Parquet file into that path.
     */
    private static IngestionHandler handlerWithNoDataPath(String table, String... queues) {
        return handlerFor(table, null, queues);
    }
}
