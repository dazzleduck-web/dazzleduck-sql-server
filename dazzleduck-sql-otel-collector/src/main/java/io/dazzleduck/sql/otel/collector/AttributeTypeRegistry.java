package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.SidecarRows;
import io.dazzleduck.sql.otel.collector.config.AttributeTypeSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Records which OTLP value types each attribute name has arrived with, and when each was first
 * seen, in an append-only table <b>beside the data it describes</b>.
 *
 * <p>WHY. Every attribute is written to the map column its value type selects —
 * {@code attributes}, {@code attributes_int}, and so on (see {@link AttributeType}). A service
 * that sends {@code x} as a string in one release and as an int in the next moves its values from
 * one map to another, and without a record of that a reader has to search all six maps to find
 * {@code x}. The table answers "which types has {@code x} had, and when did each first appear?"
 * without touching the data, and it sits beside the tables it describes so a query can join it to
 * them.
 *
 * <p>It records types; it never enforces or coerces them. Ingestion stays schema-on-read, so a
 * type change is written down, not rejected.
 *
 * <h2>Where the table is, and whether there is one</h2>
 * Per queue, and optional: a queue records mappings only if its mapping sets
 * {@value io.dazzleduck.sql.commons.ingestion.QueueIdToTableMapping#ATTRIBUTE_TYPES_TABLE_KEY},
 * the same shape as {@code watermark_table}. The name is unqualified and resolved in that queue's
 * own catalog and schema, so queues sharing a schema may name one table and share it — their rows
 * stay apart by the {@code ingestion_queue} column — or each name its own. The collector never
 * names a catalog, and could not: with a dynamic queue registry it does not know a queue's schema
 * until a batch arrives.
 * {@link io.dazzleduck.sql.commons.ingestion.DuckLakePostIngestionTask} resolves it in the queue's
 * own catalog and schema; {@link #start} creates it, at startup. A queue that names none is not
 * scanned at all.
 *
 * <h2>How a mapping gets there</h2>
 * <ol>
 *   <li>{@link AttributeTypeScan} collects a batch's distinct mappings as the batch is written,
 *       off the per-attribute hot path.</li>
 *   <li>{@link #claim} keeps every one this process has not yet COMMITTED — what another batch
 *       holds in flight is deliberately not consulted, since that batch may fail — and
 *       {@link #sidecar} renders them as rows carried on the batch.</li>
 *   <li>They are written as a Parquet file and registered by the SAME transaction that registers
 *       the batch's own files, so a registered file always has its mappings committed.</li>
 *   <li>{@link #settle} then marks them known, or — if the batch failed — releases them so a later
 *       batch carries them again.</li>
 * </ol>
 *
 * <p>The trade-off of that atomicity, stated plainly: a row the database rejects fails the batch,
 * where a background writer would have retried it without touching ingestion. So nothing reaches
 * the statement that this class has not rendered itself, from a fixed column list, with literals
 * escaped and anything unrenderable dropped rather than sent.
 *
 * <p>The table is <b>append-only and tolerates duplicates</b>. Nothing is ever read, updated or
 * deleted by a write, so an append cannot conflict with another process doing the same, and it
 * adds no scan to the ingestion transaction. Two collectors seeing a mapping for the first time
 * therefore both record it, and a consumer deduplicates when it reads:
 * {@code GROUP BY ingestion_queue, signal, attribute_scope, attribute_name, value_type}, taking
 * {@code min(first_seen)}. What keeps the repeats down within one process is the cache below.
 *
 * <p>{@link #start} builds that cache from the table at startup — one query per distinct registry
 * table, restricted to the queues this process serves — so a restart does not re-append everything
 * it already knows. It also CREATES each table and its data directory there, and <b>fails startup
 * if it cannot</b>: a stream whose table cannot be created would otherwise fail every batch at
 * commit, forever, which is a worse way to learn about a misconfiguration than a startup error.
 * Nothing is created during processing, so the ingestion transaction carries no DDL.
 *
 * <p>{@code first_seen} is when the recording collector noticed the mapping — not when the data
 * was produced. {@code min(first_seen)} across the duplicates is as close to the fleet's first
 * sighting as this gets.
 *
 * <p>DuckLake only: a plain-Parquet queue has no transaction to join, and its post-ingestion task
 * discards the rows. Mappings of such a queue are counted as dropped.
 */
public final class AttributeTypeRegistry {

    private static final Logger log = LoggerFactory.getLogger(AttributeTypeRegistry.class);

    /** The table's columns as DDL, for {@link #createTable}. */
    private static final List<String> COLUMN_DEFINITIONS = List.of(
            "ingestion_queue VARCHAR NOT NULL",
            "signal VARCHAR NOT NULL",
            "attribute_scope VARCHAR NOT NULL",
            "attribute_name VARCHAR NOT NULL",
            "value_type VARCHAR NOT NULL",
            "first_seen TIMESTAMP WITH TIME ZONE NOT NULL",
            "collector_id VARCHAR");

    /** The same columns by name, in the order {@link #sidecar} renders a row. */
    private static final List<String> COLUMNS = List.of(
            "ingestion_queue", "signal", "attribute_scope", "attribute_name", "value_type",
            "first_seen", "collector_id");

    /**
     * What identifies a mapping — everything but when and by whom it was noticed. Not used in any
     * statement: the table is append-only, so this is what a READER groups by, and what the cache
     * is keyed on.
     */
    private static final List<String> KEY_COLUMNS = List.of(
            "ingestion_queue", "signal", "attribute_scope", "attribute_name", "value_type");

    private static final DateTimeFormatter TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    /** One mapping, as the table's key columns record it. */
    record Key(String queue, Signal signal, AttributeScope scope, String name, AttributeValueType type) {}

    /**
     * One batch's claim: the mappings it carries, and whether its outcome has been recorded.
     *
     * <p>A claim exists so that {@link #settle} can be <b>idempotent</b>. Settling twice would
     * otherwise decrement the in-flight gauge twice and double-count the persisted or rollback
     * counters, and the second call is easy to reach: the services attach a completion handler
     * that settles, so any later synchronous failure on that path could settle again. Absorbing it
     * here is better than asking every caller to remember, which is what a bare list required.
     *
     * <p>The flag is per claim rather than per mapping on purpose: the same mapping may legitimately
     * be claimed by two concurrent batches, which is what makes the design's invariant hold.
     */
    public static final class Claim {
        private final List<Key> keys;
        private final java.util.concurrent.atomic.AtomicBoolean settled =
                new java.util.concurrent.atomic.AtomicBoolean();

        private Claim(List<Key> keys) {
            this.keys = keys;
        }

        public boolean isEmpty() {
            return keys.isEmpty();
        }

        public int size() {
            return keys.size();
        }

        /** The mappings, for rendering and for tests; settling is the only mutation. */
        public List<Key> keys() {
            return keys;
        }

        /** True for the first settle only. */
        private boolean claimOutcome() {
            return settled.compareAndSet(false, true);
        }
    }

    private final AttributeTypeSettings settings;
    private final OtelCollectorMetrics metrics;
    private final String collectorId;

    /**
     * Mappings this process knows are in the table — loaded at startup and added as batches
     * commit. Bitmask-backed, because a thousand streams of a few hundred attributes each would
     * otherwise be hundreds of thousands of set entries; see {@link AttributeTypeCache}.
     */
    private final AttributeTypeCache known = new AttributeTypeCache();
    /**
     * How many mappings are carried on batches that have not committed yet — a count, not a set,
     * because nothing is decided by it. A batch's contents are decided against {@link #known}
     * alone; see {@link #claim}.
     */
    private final AtomicInteger inFlight = new AtomicInteger();

    /**
     * Each recording queue's cap on held mappings, resolved at startup: its own
     * {@code attribute_types_max_entries} if it set one, else {@code max_entries_per_stream},
     * never above the process-wide {@code max_entries}.
     */
    private final Map<String, Integer> capByQueue = new ConcurrentHashMap<>();

    public AttributeTypeRegistry(AttributeTypeSettings settings, OtelCollectorMetrics metrics) {
        this.settings = settings;
        this.metrics = metrics;
        this.collectorId = collectorId();
        if (settings.enabled()) {
            metrics.registerAttributeTypeGauges(inFlight::get, known::size);
            log.info("Attribute type registry available: a queue records mappings when its mapping sets '{}',"
                    + " written in the transaction that registers each batch's files (DuckLake queues only)",
                    io.dazzleduck.sql.commons.ingestion.QueueIdToTableMapping.ATTRIBUTE_TYPES_TABLE_KEY);
        } else {
            log.info("Attribute type registry disabled by attribute_types.enabled = false");
        }
    }

    /** True when per-queue tables are honoured at all; false leaves {@link #claim} a no-op. */
    public boolean isEnabled() {
        return settings.enabled();
    }

    /**
     * Creates or checks every registry table the configured streams name, then reads each one into
     * the cache so a restart does not re-append what it already recorded. One pass per distinct
     * table; the read is restricted to the streams this process serves, since another process's
     * streams may share the table and caching their mappings would cost memory for nothing.
     *
     * <p>Runs before the gRPC server accepts traffic, on the DuckDB instance whose catalogs the
     * startup script has attached.
     *
     * <p><b>Fails startup</b> if a table cannot be created or read. {@code CREATE TABLE IF NOT
     * EXISTS} is both the creation and the check: it is silent when the table is already there and
     * fails on an unattached catalog, a missing schema, a name taken by something else or missing
     * permission. Doing this here rather than in the ingestion transaction is the whole point —
     * otherwise such a stream looks healthy until traffic arrives and then fails at commit, batch
     * after batch, which is how this was first written and is the bug it fixes.
     *
     * @throws IllegalStateException naming the table that could not be prepared
     */
    public void start(IngestionHandler handler) {
        if (!settings.enabled()) {
            return;
        }
        // table -> the queues of ours that record into it, so one query serves all of them
        Map<String, List<String>> queuesByTable = new LinkedHashMap<>();
        // ...and one of those queues per table, to resolve that table's data directory with
        Map<String, String> aQueueOf = new LinkedHashMap<>();
        for (String queue : handler.getKnownQueues()) {
            String table = handler.getAttributeTypesTableRef(queue);
            if (table != null) {
                queuesByTable.computeIfAbsent(table, t -> new ArrayList<>()).add(queue);
                aQueueOf.putIfAbsent(table, queue);
                capByQueue.put(queue, settings.capFor(handler.getAttributeTypesMaxEntries(queue)));
            }
        }
        if (queuesByTable.isEmpty()) {
            log.info("Attribute type registry: no configured queue records mappings");
            return;
        }
        for (var entry : queuesByTable.entrySet()) {
            String table = entry.getKey();
            createTable(table);
            // After the table: its data directory can only be resolved once it is in the catalog.
            createDataDirectory(table, handler.getAttributeTypesTablePath(aQueueOf.get(table)));
            load(table, entry.getValue());
        }
        log.info("Attribute type registry: {} mapping(s) over {} distinct attribute name(s) already recorded"
                + " in {} table(s)", known.size(), known.nameCount(), queuesByTable.size());
    }

    /**
     * {@code CREATE TABLE IF NOT EXISTS} for one registry table — a no-op when it exists, and the
     * check that it can exist when it does not.
     */
    private void createTable(String table) {
        String ddl = "CREATE TABLE IF NOT EXISTS %s (%s)"
                .formatted(table, String.join(", ", COLUMN_DEFINITIONS));
        try (Connection connection = ConnectionPool.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        } catch (SQLException e) {
            throw new IllegalStateException(("Attribute type registry table '%s' cannot be created or used: %s."
                    + " Check that its catalog is attached by the startup script and its schema exists")
                    .formatted(table, e.getMessage()), e);
        }
    }

    /**
     * Creates the directory the table's data files go in, because a batch's mappings are written
     * there as Parquet and registered in place — a {@code COPY} into a directory that does not
     * exist fails, and that failure would land on the batch.
     *
     * <p>Only for a local path: an object store has no directories to create, and a {@code COPY}
     * to one needs none.
     */
    private void createDataDirectory(String table, String path) {
        if (path == null) {
            throw new IllegalStateException(("Attribute type registry table '%s' has no data path in the"
                    + " catalog; it must be a DuckLake table").formatted(table));
        }
        if (path.contains("://")) {
            log.debug("Attribute type registry: {} writes to object storage ({}), no directory to create",
                    table, path);
            return;
        }
        try {
            Files.createDirectories(Path.of(path));
        } catch (Exception e) {
            throw new IllegalStateException(("Attribute type registry table '%s' has data path '%s', which"
                    + " cannot be created: %s").formatted(table, path, e.getMessage()), e);
        }
    }

    private void load(String table, List<String> queues) {
        String inList = queues.stream().map(AttributeTypeRegistry::literal).collect(Collectors.joining(", "));
        // No LIMIT: a global one would let whichever queue's rows come first fill the budget, so a
        // noisy queue's history would starve every other queue on each restart. Each queue's own
        // cap is applied row by row below instead, and the global one still stops the read.
        String sql = ("SELECT DISTINCT ingestion_queue, signal, attribute_scope, attribute_name, value_type"
                + " FROM %s WHERE ingestion_queue IN (%s)")
                .formatted(table, inList);
        int loaded = 0;
        int skipped = 0;
        try (Connection connection = ConnectionPool.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                Signal signal = parse(Signal.values(), rs.getString(2), Signal::wireName);
                AttributeScope scope = parse(AttributeScope.values(), rs.getString(3), AttributeScope::wireName);
                AttributeValueType type = parse(AttributeValueType.values(), rs.getString(5),
                        AttributeValueType::wireName);
                // A row written by a newer collector, naming something this build does not know, is
                // left alone rather than cached: the table is shared and append-only.
                if (signal == null || scope == null || type == null) {
                    skipped++;
                    continue;
                }
                if (known.size() >= settings.maxEntries()) {
                    break;
                }
                String queue = rs.getString(1);
                // The same containment as at runtime: one queue's history fills only its own share.
                if (known.sizeOf(queue) >= capFor(queue)) {
                    continue;
                }
                if (known.add(new Key(queue, signal, scope, rs.getString(4), type))) {
                    loaded++;
                }
            }
        } catch (SQLException e) {
            // The table was just created or verified, so a failure here is a real problem — not
            // the empty-table case, which reads fine and simply yields nothing.
            throw new IllegalStateException("Attribute type registry table '%s' cannot be read: %s"
                    .formatted(table, e.getMessage()), e);
        }
        log.debug("Attribute type registry: loaded {} mapping(s) from {} for {} queue(s){}",
                loaded, table, queues.size(),
                skipped > 0 ? ", skipping " + skipped + " row(s) this build does not recognise" : "");
    }

    private static <E> E parse(E[] values, String wireName, java.util.function.Function<E, String> name) {
        for (E value : values) {
            if (name.apply(value).equals(wireName)) {
                return value;
            }
        }
        return null;
    }

    /**
     * A scan for one batch of {@code queue}, whose registry table is {@code table}, or null when
     * there is no registry, it is off, that queue names no table, or <b>that queue is at a cap</b>
     * — which is what the batch writers take to mean "collect nothing", at the cost of one null
     * check per row.
     *
     * <p>Skipping the scan at the cap changes no outcome, because the caps count mappings rather
     * than names: {@link #claim} can only skip what is already known or drop what is new, so
     * nothing it returns would be recorded. It matters because the stream the cap exists for is
     * also the one the scan costs most on — every row of an id-in-name stream has a different
     * layout, so the previous-row filter never fires and every attribute goes through the batch's
     * hash set, which is exactly the per-attribute hashing {@link AttributeTypeScan} exists to
     * avoid. A queue's held count never falls, so a queue at its cap stays off the scan.
     */
    static AttributeTypeScan scanFor(AttributeTypeRegistry registry, String queue, String table) {
        if (registry == null || !registry.settings.enabled() || table == null) {
            return null;
        }
        if (registry.atCap(queue)) {
            // Reported HERE, because skipping the scan means claim() never runs and all drop
            // accounting lives there: without this, dropped goes flat and the warning goes quiet
            // right after a stream is capped, which reads as the stream having recovered. It also
            // names queues stopped by the GLOBAL cap that never dropped anything themselves.
            registry.reportCappedBatch(queue);
            return null;
        }
        return new AttributeTypeScan();
    }

    /** Whether {@code queue} can still record anything: its own cap, or the process's. */
    private boolean atCap(String queue) {
        return known.size() >= settings.maxEntries() || known.sizeOf(queue) >= capFor(queue);
    }

    private void reportCappedBatch(String queue) {
        metrics.recordCappedAttributeTypeBatch(queue);
        String cause = known.size() >= settings.maxEntries()
                ? "the process is at its max_entries cap (%d)".formatted(settings.maxEntries())
                : "queue '%s' is at its own cap of %d held mapping(s)".formatted(queue, capFor(queue));
        if (shouldWarnAboutDrops(queue)) {
            log.warn("Attribute types of queue '{}' are no longer scanned or recorded: {}. Further batches"
                    + " are counted in otel.attribute_types.capped_batches and logged at debug", queue, cause);
        } else {
            log.debug("Attribute types of queue '{}' not scanned: {}", queue, cause);
        }
    }

    /**
     * Takes the mappings one batch of {@code queue} contained and returns those this batch must
     * carry: every one not yet <b>committed</b>. Never throws.
     *
     * <p>WHAT THIS DELIBERATELY DOES NOT DO is skip a mapping another in-flight batch is already
     * carrying. If it did, a batch that commits while the carrier fails would register files whose
     * mapping is in no table — and if that attribute never arrives with that type again, the
     * registry would deny data that exists, which is the one thing writing in the transaction is
     * meant to prevent. Carrying it on every batch gives the invariant instead: a batch that does
     * not carry a mapping its data contains is one whose mapping was already committed, because
     * {@link #known} only ever gains a mapping from a commit that included it.
     *
     * <p>The price is a duplicate row per overlapping batch, on a mapping's first appearance only
     * and bounded by the batches in flight — which an append-only table whose readers group
     * already absorbs.
     *
     * <p>The returned {@link Claim} must be passed to {@link #settle} once the batch's outcome is
     * known, or the in-flight gauge drifts. Settling it twice is harmless — see {@link Claim}.
     */
    public Claim claim(String queue, Signal signal, AttributeTypeScan scan) {
        if (!settings.enabled() || scan == null || scan.isEmpty()) {
            return new Claim(List.of());
        }
        List<Key> claimed = new ArrayList<>();
        int dropped = 0;
        boolean atStreamCap = false;
        boolean atGlobalCap = false;
        for (AttributeTypeScan.Mapping mapping : scan.mappings()) {
            if (!renderable(mapping.name())) {
                dropped++;
                continue;
            }
            Key key = new Key(queue, signal, mapping.scope(), mapping.name(), mapping.type());
            if (known.contains(key)) {
                continue;
            }
            // Against what is held PLUS what this batch has already claimed. Counting only what
            // is held would let a single batch through whole: the case these caps exist for — ids
            // embedded in attribute names — arrives as one scan of hundreds of distinct names.
            // What is in flight on OTHER batches is deliberately not counted, since they may yet
            // fail; that is the overshoot the caps tolerate, bounded by the batches in flight.
            if (known.size() + claimed.size() >= settings.maxEntries()) {
                dropped++;
                atGlobalCap = true;
                continue;
            }
            // This stream's own cap, so one producer's name cardinality cannot consume the budget
            // of every other stream in the process.
            if (known.sizeOf(queue) + claimed.size() >= capFor(queue)) {
                dropped++;
                atStreamCap = true;
                continue;
            }
            claimed.add(key);
        }
        inFlight.addAndGet(claimed.size());
        if (dropped > 0) {
            metrics.recordDroppedAttributeTypes(queue, dropped);
            // Which cap it is decides what an operator does about it: a stream at its own cap is a
            // producer to look at, the process at its cap is a budget to raise.
            String cause = atStreamCap
                    ? "queue '%s' is at its own cap of %d held mapping(s)".formatted(queue, capFor(queue))
                    : atGlobalCap
                    ? "the process is at its max_entries cap (%d)".formatted(settings.maxEntries())
                    : "the attribute name cannot be written as a SQL literal";
            // Once per queue, then at most once per interval. A capped stream drops on EVERY batch
            // — the id-in-name case brings new names in every export — so warning per batch would
            // fire thousands of times a minute and bury everything else in the log. The first one
            // is the operator's signal; the volume lives in otel.attribute_types.dropped.
            if (shouldWarnAboutDrops(queue)) {
                log.warn("{} attribute type mapping(s) of queue '{}' were not recorded: {}. Once a queue"
                        + " is at a cap it is no longer scanned; see otel.attribute_types.capped_batches",
                        dropped, queue, cause);
            } else {
                log.debug("{} attribute type mapping(s) of queue '{}' were not recorded: {}",
                        dropped, queue, cause);
            }
        }
        if (!claimed.isEmpty()) {
            metrics.recordNewAttributeTypes(queue, claimed.size());
        }
        return new Claim(claimed);
    }

    /**
     * The rows for {@code claimed}, to carry on the batch, or null when there are none.
     * {@code table} is the queue's configured table name, unqualified: the post-ingestion task
     * resolves it in that queue's own catalog and schema.
     */
    public SidecarRows sidecar(Claim claim, String table) {
        if (claim == null || claim.isEmpty()) {
            return null;
        }
        List<Key> claimed = claim.keys();
        String seen = "TIMESTAMPTZ " + literal(TIMESTAMP.format(Instant.now()) + "+00");
        String collector = literal(collectorId);
        List<List<String>> rows = new ArrayList<>(claimed.size());
        for (Key key : claimed) {
            rows.add(List.of(
                    literal(key.queue()),
                    literal(key.signal().wireName()),
                    literal(key.scope().wireName()),
                    literal(key.name()),
                    literal(key.type().wireName()),
                    seen,
                    collector));
        }
        return new SidecarRows(table, COLUMNS, rows);
    }

    /**
     * Records the outcome of the batch that carried {@code claimed}: committed means the rows are
     * in the table, so the mappings are known and never sent again; otherwise they are released
     * and a later batch carries them.
     */
    public void settle(Claim claim, boolean committed) {
        if (claim == null || claim.isEmpty()) {
            return;
        }
        if (!claim.claimOutcome()) {
            // Idempotent, but not silent: a second settle is a caller bug worth seeing, and
            // absorbing it without a word would hide the thing that made this necessary.
            log.debug("Attribute type claim of {} mapping(s) was already settled; ignoring the second settle",
                    claim.size());
            return;
        }
        List<Key> claimed = claim.keys();
        if (committed) {
            for (Key key : claimed) {
                known.add(key);
            }
        }
        inFlight.addAndGet(-claimed.size());
        String queue = claimed.iterator().next().queue();
        if (committed) {
            metrics.recordPersistedAttributeTypes(queue, claimed.size());
        } else {
            metrics.recordAttributeTypeRollbacks(queue, claimed.size());
            log.debug("{} attribute type mapping(s) of queue '{}' were not committed; a later batch carries them",
                    claimed.size(), queue);
        }
    }

    /**
     * An attribute name arrives from the wire and goes into a statement that, failing, fails the
     * batch. Quotes are escaped by {@link #literal}; a NUL cannot be written as part of a SQL
     * string at all, so such a name is dropped instead of risking the batch.
     */
    private static boolean renderable(String name) {
        return name.indexOf('\0') < 0;
    }

    /** A single-quoted SQL string literal, with embedded quotes doubled. */
    private static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    // -----------------------------------------------------------------------
    // Accessors, for tests and /stats
    // -----------------------------------------------------------------------

    /** How often one queue may warn about dropped mappings, after the first. */
    private static final long WARN_INTERVAL_NANOS = java.time.Duration.ofMinutes(5).toNanos();

    /** queue -> when it last warned, so a capped stream does not warn on every batch. */
    private final Map<String, Long> lastDropWarning = new ConcurrentHashMap<>();

    /**
     * True the first time a queue drops anything, and at most once per {@link #WARN_INTERVAL_NANOS}
     * after that. Raced deliberately loosely: two threads may both warn once, which costs a
     * duplicate line and avoids a lock on the write path.
     */
    private boolean shouldWarnAboutDrops(String queue) {
        long now = System.nanoTime();
        Long last = lastDropWarning.get(queue);
        if (last != null && now - last < WARN_INTERVAL_NANOS) {
            return false;
        }
        lastDropWarning.put(queue, now);
        return true;
    }

    /** This queue's cap, falling back to the default for a queue resolved after startup. */
    private int capFor(String queue) {
        Integer cap = capByQueue.get(queue);
        return cap == null ? settings.capFor(null) : cap;
    }

    /** Mappings this collector knows are recorded. */
    public int knownCount() {
        return known.size();
    }

    /** Distinct attribute names the cache holds, across every stream. */
    public int knownNameCount() {
        return known.nameCount();
    }

    /** Mappings carried on batches whose outcome is not known yet. */
    public int inFlightCount() {
        return inFlight.get();
    }

    /**
     * Identifies the collector that wrote a row, from the same environment the metrics' common
     * tags use ({@code HOSTNAME}, else the local host name, else {@code CONTAINER_ID}).
     */
    private static String collectorId() {
        String hostname = System.getenv("HOSTNAME");
        if (hostname == null || hostname.isBlank()) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                hostname = System.getenv("CONTAINER_ID");
            }
        }
        return hostname == null || hostname.isBlank() ? "unknown" : hostname;
    }
}
