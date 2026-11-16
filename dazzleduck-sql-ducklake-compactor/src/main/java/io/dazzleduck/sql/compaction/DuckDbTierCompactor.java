package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runs one tier's per-table calls on its own DuckDB instance per database (DYNAMIC_COMPACTION_SPEC.md,
 * Phase 1).
 *
 * <p>One real DuckDB instance per {@code (database, tier)} — see {@link RawConnections} for why this
 * can't be a duplicated connection: {@code memory_limit} and {@code threads} are GLOBAL and would leak
 * across tiers. Each pair's methods are called from that pair's loop thread only.
 */
public class DuckDbTierCompactor implements TierCompactor {

    private static final Logger logger = LoggerFactory.getLogger(DuckDbTierCompactor.class);

    private final String startupScript;
    private final CompactionState metrics;
    private final int availableProcessors;
    private final int databases;
    private final long totalMemoryBytes;

    /**
     * Share of the process's memory (the container's limit, else physical memory) that the tiers'
     * DuckDB instances may use together, split by {@code cpu_share} and database, for an instance
     * whose connection_settings set no {@code memory_limit}. See {@link MemoryBudget} for the rest.
     */
    static final double MEMORY_SHARE = 0.5;

    private record Open(Connection connection, Readiness readiness) {
    }

    private final ConcurrentHashMap<String, Open> open = new ConcurrentHashMap<>();

    public DuckDbTierCompactor(String startupScript, CompactionState metrics) {
        this(startupScript, metrics, 1);
    }

    /** @param databases the configured databases: each runs its own instance of every tier */
    public DuckDbTierCompactor(String startupScript, CompactionState metrics, int databases) {
        this(startupScript, metrics, Runtime.getRuntime().availableProcessors(), databases,
                ResourceSampler.totalMemoryBytes());
    }

    /**
     * @param availableProcessors the CPUs the tiers' {@code cpu_share} values divide
     * @param databases           the configured databases, which divide them again: each database runs
     *                            its own instance of every tier, so the shares are a process budget
     * @param totalMemoryBytes    the process's memory, or -1 when unknown (no default memory_limit)
     */
    public DuckDbTierCompactor(String startupScript, CompactionState metrics, int availableProcessors, int databases,
                               long totalMemoryBytes) {
        this.startupScript = startupScript;
        this.metrics = metrics;
        this.availableProcessors = availableProcessors;
        this.databases = Math.max(1, databases);
        this.totalMemoryBytes = totalMemoryBytes;
    }

    /** The instance's {@code memory_limit} when connection_settings set none, or -1 for DuckDB's default. */
    long defaultMemoryLimitBytes(CompactionTier tier) {
        if (totalMemoryBytes <= 0 || ResourceSampler.memoryLimitBytes(tier.connectionSettings()) >= 0) {
            return -1;
        }
        return MemoryBudget.limit(MEMORY_SHARE * tier.cpuShare(), databases, totalMemoryBytes);
    }

    @Override
    public Readiness prepare(String database, CompactionTier tier) throws Exception {
        String key = key(database, tier.name());
        Open existing = open.get(key);
        if (existing != null) {
            return existing.readiness();
        }
        int threads = tier.threads(availableProcessors, databases);
        long memoryLimit = defaultMemoryLimitBytes(tier);
        // threads first, so a connection_settings entry that sets it explicitly still wins
        List<String> settings = new ArrayList<>();
        settings.add("SET threads = " + threads);
        if (memoryLimit > 0) {
            settings.add("SET memory_limit = '" + memoryLimit + " bytes'");
        }
        settings.addAll(tier.connectionSettings());
        logger.info("Tier '{}' opens its DuckDB instance for {}: {} thread(s), memory_limit {}", tier.name(), database,
                threads, memoryLimit > 0 ? memoryLimit + " bytes" : "from connection_settings or DuckDB's default");
        Connection connection = RawConnections.open(startupScript, settings);
        try {
            long effectiveLimit = memoryLimit > 0 ? memoryLimit : ResourceSampler.memoryLimitBytes(tier.connectionSettings());
            Readiness readiness = check(connection, database, tier, threads, effectiveLimit);
            if (readiness.isRefused()) {
                connection.close();
            } else {
                open.put(key, new Open(connection, readiness));
            }
            return readiness;
        } catch (Exception e) {
            closeQuietly(connection);
            throw e;
        }
    }

    private static Readiness check(Connection connection, String database, CompactionTier tier, int threads,
                                   long memoryLimitBytes) throws SQLException {
        try {
            setTarget(connection, tier.targetFileSize());
        } catch (SQLException e) {
            String message = String.valueOf(e.getMessage());
            if (message.contains("unrecognized configuration parameter")) {
                return new Readiness(("the DuckLake extension has no ducklake_target_file_size session setting"
                        + " (it needs DuckDB 1.5.4 or later): %s").formatted(message.lines().findFirst().orElse("")),
                        0, threads);
            }
            throw e;
        } catch (TargetMismatchException e) {
            return new Readiness(e.getMessage(), 0, threads);
        }
        long timeoutMs = idleTxnTimeoutMs(connection, database, tier);
        String refusal = CallSizing.startupRefusal(tier, timeoutMs, memoryLimitBytes);
        if (refusal != null) {
            return new Readiness(refusal, timeoutMs, threads, memoryLimitBytes);
        }
        if (!CallSizing.fullGroupFits(tier, timeoutMs)) {
            logger.warn(("Tier '{}' for {}: one full group of its smallest files ({} s at {} s per file) does not fit in"
                            + " {}% of the catalog's idle-in-transaction timeout ({} ms); its calls run with a lowered"
                            + " target"), tier.name(), database, Math.round(CallSizing.fullGroupSeconds(tier)),
                    CallSizing.SEC_PER_FILE_SORTED, Math.round(CallSizing.TIMEOUT_SHARE * 100), timeoutMs);
        }
        return Readiness.ready(timeoutMs, threads, memoryLimitBytes);
    }

    /**
     * Sets the tier's target on the session and reads it back: a call never runs without it, since a
     * group would otherwise grow to the catalog's target (512 MB by default). The setting needs a unit.
     */
    static void setTarget(Connection connection, long targetFileSize) throws SQLException {
        String value = targetFileSize + " bytes";
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET SESSION ducklake_target_file_size = '" + value + "'");
        }
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT current_setting('ducklake_target_file_size')")) {
            String actual = rs.next() ? rs.getString(1) : null;
            long bytes = actual == null ? -1 : parseDuckDbSize(actual);
            // A display may round (e.g. "16.0 MB"); anything further off is not the target we set.
            if (bytes < 0 || Math.abs(bytes - targetFileSize) > targetFileSize / 100) {
                throw new TargetMismatchException("ducklake_target_file_size reads back as %s after setting it to %s"
                        .formatted(actual, value));
            }
        }
    }

    /** The session target did not take: the tier must not run, since its groups would be unbounded. */
    static final class TargetMismatchException extends IllegalStateException {
        TargetMismatchException(String message) {
            super(message);
        }
    }

    private static final java.util.regex.Pattern DUCKDB_SIZE =
            java.util.regex.Pattern.compile("([0-9]+(?:\\.[0-9]+)?)\\s*([a-zA-Z]*)");

    /**
     * A size as DuckDB writes one: {@code 16000000 bytes}, {@code 16.0 MB} (1000-based) or
     * {@code 15.2 MiB} (1024-based). -1 when it does not parse.
     */
    static long parseDuckDbSize(String value) {
        java.util.regex.Matcher m = DUCKDB_SIZE.matcher(value.trim());
        if (!m.matches()) {
            return -1;
        }
        double n = Double.parseDouble(m.group(1));
        double unit = switch (m.group(2).toLowerCase(java.util.Locale.ROOT)) {
            case "", "b", "byte", "bytes" -> 1;
            case "kb" -> 1e3;
            case "mb" -> 1e6;
            case "gb" -> 1e9;
            case "tb" -> 1e12;
            case "kib" -> 1024.0;
            case "mib" -> 1024.0 * 1024;
            case "gib" -> 1024.0 * 1024 * 1024;
            case "tib" -> 1024.0 * 1024 * 1024 * 1024;
            default -> -1;
        };
        return unit < 0 ? -1 : Math.round(n * unit);
    }

    /**
     * The catalog database's {@code idle_in_transaction_session_timeout} in ms, or 0 for none: a value
     * of 0 (Postgres's default, disabled), or a catalog that has no such setting (SQLite, a DuckDB
     * file). Read from Postgres's {@code pg_settings} through {@code postgres_query} on the metadata
     * catalog, whose connection carries the ATTACH string's options, so an
     * {@code options=-cidle_in_transaction_session_timeout=...} there counts. ({@code SHOW} cannot be
     * used: {@code postgres_query} wraps its query in a {@code COPY (SELECT ...)}.) A catalog attached
     * through a secret shows its resolved {@code postgres:} path too.
     *
     * <p>When a Postgres catalog's value cannot be read, a value set in the tier's connection_settings
     * is used; without one this throws, so the tier retries opening its connection rather than size
     * calls as if there were no timeout.
     */
    static long idleTxnTimeoutMs(Connection connection, String database, CompactionTier tier) throws SQLException {
        long configured = ResourceSampler.idleInTransactionTimeoutMs(tier.connectionSettings());
        // The metadata catalog itself is hidden from duckdb_databases(); the DuckLake catalog's path
        // names its backend, e.g. "postgres:host=... dbname=...".
        String path = null;
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT path FROM duckdb_databases() WHERE database_name = ? AND type = 'ducklake'")) {
            statement.setString(1, database);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    path = rs.getString(1);
                }
            }
        } catch (SQLException e) {
            // The backend is unknown, so this is no reason to hold the tier back
            logger.warn("Could not tell which backend {}'s catalog uses, for tier '{}'; {}", database, tier.name(),
                    configured >= 0 ? "using the timeout in its connection_settings" : "no timeout limit", e);
            return Math.max(configured, 0);
        }
        if (path == null || !path.regionMatches(true, 0, "postgres:", 0, "postgres:".length())) {
            return Math.max(configured, 0);
        }
        try {
            String query = "SELECT setting, unit FROM pg_settings WHERE name = 'idle_in_transaction_session_timeout'";
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT * FROM postgres_query(%s, %s)"
                         .formatted(Sql.literal(Sql.metadataCatalogName(database)), Sql.literal(query)))) {
                if (rs.next()) {
                    String unit = rs.getString(2);
                    long ms = ResourceSampler.parseDurationMs(rs.getString(1).trim() + (unit == null ? "" : unit.trim()));
                    if (ms >= 0) {
                        return ms;
                    }
                }
            }
            throw new SQLException("pg_settings returned no usable idle_in_transaction_session_timeout");
        } catch (SQLException e) {
            if (configured >= 0) {
                logger.warn("Could not read the idle-in-transaction timeout of {}'s catalog for tier '{}'; using {} ms"
                        + " from its connection_settings", database, tier.name(), configured, e);
                return configured;
            }
            throw new SQLException(("Could not read the idle-in-transaction timeout of %s's Postgres catalog for tier"
                    + " '%s'; the tier retries rather than size its calls without it").formatted(database, tier.name()), e);
        }
    }

    @Override
    public List<TableWork> discover(String database, CompactionTier tier) throws Exception {
        Connection connection = connection(database, tier);
        List<TableWork> work = new ArrayList<>();
        Timer.Sample sample = metrics.startTimer();
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(discoverySql(database, tier))) {
            while (rs.next()) {
                work.add(new TableWork(
                        rs.getString("schema_name"),
                        rs.getString("table_name"),
                        rs.getLong("files_waiting"),
                        rs.getLong("bytes_waiting"),
                        Instant.ofEpochMilli(rs.getLong("oldest_waiting_ms")),
                        rs.getInt("busy_partitions"),
                        rs.getBoolean("sorted")));
            }
        } finally {
            metrics.stopTimer(sample, tier.name(), "discover", database);
        }
        return work;
    }

    /**
     * The tables with work for a tier, from the catalog metadata, grouped the way
     * {@code ducklake_merge_adjacent_files} groups files: by table, schema version (from
     * {@code ducklake_schema_versions}, as DuckLake reads it), partition id and partition values. A
     * file is eligible when it is live, in the band (and so below the target, which is at least the
     * band's maximum) and has no delete file: DuckLake skips a file with deletes. Files whose deletes
     * are still inlined in the catalog are counted, though DuckLake skips them too; the worst that
     * costs is a call that merges nothing.
     *
     * <p>A table is listed when {@code auto_compact} is not off at its narrowest scope (table, then
     * schema, then catalog; only {@code 'true'} enables it, as in DuckLake) and one of its groups has
     * {@code min_files} files or, with a {@code max_wait}, a second-oldest file at least that old.
     * File ages are the commit times of their first snapshot. A file whose first snapshot has been
     * expired is at least as old as the oldest snapshot left, so that is its age.
     */
    static String discoverySql(String database, CompactionTier tier) {
        String md = Sql.metadataCatalog(database);
        String admitted = "g.files >= " + tier.minFiles();
        if (!tier.maxWait().isZero()) {
            admitted += " OR g.second_oldest <= now() - INTERVAL %d SECOND".formatted(tier.maxWait().toSeconds());
        }
        return """
                WITH live_tables AS (
                  SELECT t.table_id, t.table_name, s.schema_id, s.schema_name
                  FROM %1$s.ducklake_table t
                  JOIN %1$s.ducklake_schema s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL
                  WHERE t.end_snapshot IS NULL
                ),
                options AS (
                  SELECT scope, scope_id, any_value(value) AS value
                  FROM %1$s.ducklake_metadata WHERE key = 'auto_compact'
                  GROUP BY scope, scope_id
                ),
                compactable AS (
                  SELECT lt.* FROM live_tables lt
                  LEFT JOIN options ot ON ot.scope = 'table' AND ot.scope_id = lt.table_id
                  LEFT JOIN options os ON os.scope = 'schema' AND os.scope_id = lt.schema_id
                  LEFT JOIN options oc ON oc.scope IS NULL
                  WHERE coalesce(ot.value, os.value, oc.value, 'true') = 'true'
                ),
                files AS (
                  SELECT d.data_file_id, d.table_id, d.partition_id, d.file_size_bytes, d.begin_snapshot
                  FROM %1$s.ducklake_data_file d
                  WHERE d.end_snapshot IS NULL AND d.file_size_bytes >= %2$d AND d.file_size_bytes < %3$d
                    AND d.table_id IN (SELECT table_id FROM compactable)
                    AND d.data_file_id NOT IN (
                      SELECT data_file_id FROM %1$s.ducklake_delete_file WHERE data_file_id IS NOT NULL)
                ),
                versions AS (
                  SELECT table_id, begin_snapshot, schema_version,
                         lead(begin_snapshot, 1, 9223372036854775807)
                           OVER (PARTITION BY table_id ORDER BY begin_snapshot) AS next_begin
                  FROM %1$s.ducklake_schema_versions
                ),
                partition_keys AS (
                  SELECT data_file_id,
                         string_agg(coalesce(partition_value, chr(0)), chr(31) ORDER BY partition_key_index) AS pkey
                  FROM %1$s.ducklake_file_partition_value
                  WHERE data_file_id IN (SELECT data_file_id FROM files)
                  GROUP BY data_file_id
                ),
                eligible AS (
                  SELECT f.table_id, f.partition_id, f.file_size_bytes, v.schema_version,
                         coalesce(k.pkey, '') AS pkey,
                         coalesce(sn.snapshot_time, oldest.snapshot_time) AS committed
                  FROM files f
                  LEFT JOIN %1$s.ducklake_snapshot sn ON sn.snapshot_id = f.begin_snapshot
                  CROSS JOIN (SELECT min(snapshot_time) AS snapshot_time FROM %1$s.ducklake_snapshot) oldest
                  LEFT JOIN versions v ON v.table_id = f.table_id
                       AND f.begin_snapshot >= v.begin_snapshot AND f.begin_snapshot < v.next_begin
                  LEFT JOIN partition_keys k ON k.data_file_id = f.data_file_id
                ),
                groups AS (
                  SELECT table_id, count(*) AS files, sum(file_size_bytes) AS bytes,
                         list_sort(list(committed))[2] AS second_oldest,
                         count(*) FILTER (WHERE committed >= now() - INTERVAL %4$d SECOND) AS recent
                  FROM eligible
                  GROUP BY table_id, schema_version, partition_id, pkey
                  HAVING count(*) >= 2
                ),
                sorted_tables AS (
                  SELECT DISTINCT table_id FROM %1$s.ducklake_sort_info WHERE end_snapshot IS NULL
                )
                SELECT c.schema_name, c.table_name,
                       sum(g.files)::BIGINT AS files_waiting,
                       sum(g.bytes)::BIGINT AS bytes_waiting,
                       epoch_ms(min(g.second_oldest)) AS oldest_waiting_ms,
                       count(*) FILTER (WHERE g.recent > 0)::INTEGER AS busy_partitions,
                       bool_or(st.table_id IS NOT NULL) AS sorted
                FROM groups g
                JOIN compactable c USING (table_id)
                LEFT JOIN sorted_tables st USING (table_id)
                GROUP BY c.schema_name, c.table_name, c.table_id
                HAVING bool_or(%5$s)
                """.formatted(md, tier.minFileSize(), tier.maxFileSize(), CallSizing.BUSY_WINDOW.toSeconds(), admitted);
    }

    @Override
    public MergeOutcome compact(String database, CompactionTier tier, TableWork table, int cap, long targetFileSize)
            throws Exception {
        Connection connection = connection(database, tier);
        setTarget(connection, targetFileSize);
        String sql = mergeSql(database, table, tier, cap);
        Timer.Sample sample = metrics.startTimer();
        long start = System.nanoTime();
        // One row per table merged: (schema_name, table_name, files_processed, files_created); none
        // when nothing was merged.
        Long filesProcessed = null;
        Long filesCreated = null;
        try (Statement statement = connection.createStatement()) {
            if (statement.execute(sql)) {
                try (ResultSet rs = statement.getResultSet()) {
                    long processed = 0, created = 0;
                    boolean anyRows = false;
                    while (rs.next()) {
                        processed += rs.getLong("files_processed");
                        created += rs.getLong("files_created");
                        anyRows = true;
                    }
                    if (anyRows) {
                        filesProcessed = processed;
                        filesCreated = created;
                    }
                }
            }
        } finally {
            metrics.stopTimer(sample, tier.name(), "merge", database);
        }
        long durationMergeMs = (System.nanoTime() - start) / 1_000_000;
        logger.debug("Tier '{}' merged {}.{} with cap {} in {}ms (filesProcessed={}, filesCreated={})",
                tier.name(), database, table.name(), cap, durationMergeMs, filesProcessed, filesCreated);
        return new MergeOutcome(durationMergeMs, filesProcessed, filesCreated);
    }

    /**
     * One table's merge, fenced to the tier's band so tiers never touch each other's files.
     * {@code max_compacted_files} is the cap of Call sizing, never above the tier's static cap.
     */
    static String mergeSql(String database, TableWork table, CompactionTier tier, int cap) {
        return ("CALL ducklake_merge_adjacent_files(%s, %s, schema := %s, min_file_size := %d, max_file_size := %d,"
                + " max_compacted_files := %d)").formatted(Sql.literal(database), Sql.literal(table.table()),
                Sql.literal(table.schema()), tier.minFileSize(), tier.maxFileSize(), cap);
    }

    @Override
    public void reset(String database, CompactionTier tier) {
        Open removed = open.remove(key(database, tier.name()));
        if (removed != null) {
            closeQuietly(removed.connection());
        }
    }

    @Override
    public long memoryUsageBytes(String database, CompactionTier tier) {
        Open pair = open.get(key(database, tier.name()));
        if (pair == null) {
            return -1;
        }
        try (Statement statement = pair.connection().createStatement();
             ResultSet rs = statement.executeQuery("SELECT coalesce(sum(memory_usage_bytes), 0) FROM duckdb_memory()")) {
            return rs.next() ? rs.getLong(1) : -1;
        } catch (SQLException e) {
            logger.debug("Could not read duckdb_memory() for {} tier '{}'", database, tier.name(), e);
            return -1;
        }
    }

    private Connection connection(String database, CompactionTier tier) throws Exception {
        Open pair = open.get(key(database, tier.name()));
        if (pair == null) {
            Readiness readiness = prepare(database, tier);
            if (readiness.isRefused()) {
                throw new IllegalStateException("tier '%s' is refused for %s: %s"
                        .formatted(tier.name(), database, readiness.refused()));
            }
            pair = open.get(key(database, tier.name()));
        }
        return pair.connection();
    }


    private static String key(String database, String tierName) {
        return database.length() + ":" + database + "|" + tierName;
    }

    private static void closeQuietly(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.warn("Failed to close a tier connection", e);
        }
    }

    @Override
    public void close() {
        open.values().forEach(pair -> closeQuietly(pair.connection()));
        open.clear();
    }
}
