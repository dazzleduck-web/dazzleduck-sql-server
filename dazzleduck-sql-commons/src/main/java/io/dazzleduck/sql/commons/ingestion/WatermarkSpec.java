package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.util.HeaderUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Per-queue watermark configuration, parsed from a queue mapping's {@code additional_parameters}:
 * <ul>
 *   <li>{@code watermark_table} — unqualified table (same catalog/schema as the target) receiving
 *       one row per group with the MIN and MAX of the timestamp column and the row count
 *       across each ingested batch.</li>
 *   <li>{@code watermark_timestamp_column} — the SOURCE column in the ingested data; both the MIN
 *       and the MAX are computed over it. Required when the table is set.</li>
 *   <li>{@code watermark_min_timestamp_column} — required when the table is set; the watermark-table
 *       column the MIN lands in.</li>
 *   <li>{@code watermark_group_columns} — optional grouping columns as a comma-separated string
 *       (a HOCON list is tolerated: its flattened {@code [a, b]} form is unwrapped). Empty means
 *       one global MIN row per batch.</li>
 *   <li>{@code watermark_max_timestamp_column} — required when the table is set; the watermark-table
 *       column the MAX lands in.</li>
 *   <li>{@code watermark_row_count_column} — required when the table is set; receives
 *       {@code COUNT(*)} for the group.</li>
 *   <li>{@code watermark_snapshot_id_column} — OPTIONAL; receives the DuckLake snapshot the batch
 *       committed at. Absent means the column is not written at all, which is the behaviour
 *       every existing configuration keeps.</li>
 * </ul>

 * <p><b>The snapshot id is stamped after the batch commits, not inside it.</b> A transaction
 * cannot learn the snapshot it is about to create: {@code ducklake_current_snapshot()} returns the
 * snapshot being READ, and the new id only exists once the commit completes. So when
 * {@code watermark_snapshot_id_column} is configured, {@link DuckLakePostIngestionTask} follows the
 * ingest transaction with a second statement on the SAME connection:
 *
 * <pre>{@code UPDATE <watermark table>
 *      SET <snapshot column> = (SELECT * FROM ducklake_last_committed_snapshot('<catalog>'))
 *    WHERE <snapshot column> IS NULL}</pre>
 *
 * {@code ducklake_last_committed_snapshot} is per-connection and the subquery is evaluated before
 * the UPDATE's own commit, so the value recorded is the ingest batch's snapshot — the one that made
 * both the data files and the watermark row visible — and never another writer's.
 *
 * <p>Two consequences worth knowing. The stamp is its own transaction, so it creates one extra
 * snapshot per batch. And a crash between the two commits leaves the row with a NULL snapshot id;
 * the {@code IS NULL} predicate makes the next batch sweep it up, and readers must tolerate NULL
 * in the meantime. The predicate assumes a single writer per watermark table, which is the model
 * already: one queue, one watermark table.
 *
 * <p>A group whose timestamps are all NULL still produces a row: NULL min and max, with the real
 * row count. Only a genuinely empty batch is skipped.
 *
 * <p>Watermark rows are computed at WRITE time by {@link ParquetIngestionQueue} — an aggregation
 * over the same pre-COPY relation the output Parquet is written from (local data, transformation
 * already applied) — and carried through {@link IngestionResult#watermarkRows()} to
 * {@link DuckLakePostIngestionTask}, which appends them via a plain {@code INSERT ... VALUES} in
 * the SAME transaction as the {@code ducklake_add_data_files} registration. The post-ingestion
 * step therefore never re-reads the written files: no second download, no dependence on the
 * written files' schema, and partition columns (present in the relation, hive-only in the files)
 * group correctly. Only empty batches are excluded.
 *
 * <p>Validation runs at config-load time via {@link QueueIdToTableMapping}, so a malformed spec
 * fails startup rather than orphaning batches at flush time. Values are rejected when blank, and
 * unknown {@code watermark_}-prefixed keys are rejected to surface typos.
 */
public record WatermarkSpec(String table, String timestampColumn, List<String> groupColumns,
                            String minTimestampColumn, String maxTimestampColumn, String rowCountColumn,
                            String snapshotIdColumn) {

    /** Without a snapshot-id column — the shape every configuration had before that key existed. */
    public WatermarkSpec(String table, String timestampColumn, List<String> groupColumns,
                         String minTimestampColumn, String maxTimestampColumn, String rowCountColumn) {
        this(table, timestampColumn, groupColumns, minTimestampColumn, maxTimestampColumn, rowCountColumn, null);
    }

    public static final String TABLE_KEY = "watermark_table";
    public static final String TIMESTAMP_COLUMN_KEY = "watermark_timestamp_column";
    public static final String GROUP_COLUMNS_KEY = "watermark_group_columns";
    public static final String MIN_TIMESTAMP_COLUMN_KEY = "watermark_min_timestamp_column";
    public static final String MAX_TIMESTAMP_COLUMN_KEY = "watermark_max_timestamp_column";
    public static final String ROW_COUNT_COLUMN_KEY = "watermark_row_count_column";
    public static final String SNAPSHOT_ID_COLUMN_KEY = "watermark_snapshot_id_column";

    private static final List<String> KNOWN_KEYS = List.of(TABLE_KEY, TIMESTAMP_COLUMN_KEY, GROUP_COLUMNS_KEY,
            MIN_TIMESTAMP_COLUMN_KEY, MAX_TIMESTAMP_COLUMN_KEY, ROW_COUNT_COLUMN_KEY, SNAPSHOT_ID_COLUMN_KEY);

    public WatermarkSpec {
        requireNonBlank(table, TABLE_KEY);
        requireNonBlank(timestampColumn, TIMESTAMP_COLUMN_KEY);
        groupColumns = groupColumns == null ? List.of() : List.copyOf(groupColumns);
        groupColumns.forEach(c -> requireNonBlank(c, GROUP_COLUMNS_KEY));
        requireNonBlank(minTimestampColumn, MIN_TIMESTAMP_COLUMN_KEY);
        requireNonBlank(maxTimestampColumn, MAX_TIMESTAMP_COLUMN_KEY);
        requireNonBlank(rowCountColumn, ROW_COUNT_COLUMN_KEY);
        // Optional: null means "do not record it". Present-but-blank is a typo, not a choice.
        if (snapshotIdColumn != null) {
            requireNonBlank(snapshotIdColumn, SNAPSHOT_ID_COLUMN_KEY);
        }
    }

    /**
     * Parses the watermark spec out of a queue mapping's {@code additional_parameters}.
     * Returns {@code null} when no {@code watermark_} key is present.
     *
     * @throws IllegalArgumentException on a partial spec (table without timestamp column), blank
     *         values, or an unknown {@code watermark_}-prefixed key (typo guard)
     */
    public static WatermarkSpec fromParameters(String queueName, Map<String, String> parameters) {
        if (parameters == null || parameters.keySet().stream().noneMatch(k -> k.startsWith("watermark_"))) {
            return null;
        }
        for (String key : parameters.keySet()) {
            if (key.startsWith("watermark_") && !KNOWN_KEYS.contains(key)) {
                throw new IllegalArgumentException(
                        "Queue '%s': unknown watermark parameter '%s' (known: %s)".formatted(queueName, key, KNOWN_KEYS));
            }
        }
        String table = parameters.get(TABLE_KEY);
        String timestampColumn = parameters.get(TIMESTAMP_COLUMN_KEY);
        String minTimestampColumn = parameters.get(MIN_TIMESTAMP_COLUMN_KEY);
        String maxTimestampColumn = parameters.get(MAX_TIMESTAMP_COLUMN_KEY);
        String rowCountColumn = parameters.get(ROW_COUNT_COLUMN_KEY);
        String snapshotIdColumn = parameters.get(SNAPSHOT_ID_COLUMN_KEY);
        if (isBlank(table) || isBlank(timestampColumn) || isBlank(minTimestampColumn)
                || isBlank(maxTimestampColumn) || isBlank(rowCountColumn)) {
            throw new IllegalArgumentException(
                    "Queue '%s': watermark configuration requires non-blank '%s', '%s', '%s', '%s' and '%s'"
                            .formatted(queueName, TABLE_KEY, TIMESTAMP_COLUMN_KEY, MIN_TIMESTAMP_COLUMN_KEY,
                                    MAX_TIMESTAMP_COLUMN_KEY, ROW_COUNT_COLUMN_KEY));
        }
        try {
            return new WatermarkSpec(table.trim(), timestampColumn.trim(),
                    parseGroupColumns(parameters.get(GROUP_COLUMNS_KEY)),
                    minTimestampColumn.trim(), maxTimestampColumn.trim(), rowCountColumn.trim(),
                    // Only absence means "unconfigured"; a blank value is rejected by the constructor.
                    snapshotIdColumn == null ? null : snapshotIdColumn.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Queue '%s': %s".formatted(queueName, e.getMessage()), e);
        }
    }

    /**
     * Splits the comma-separated group-column value. A HOCON list flattened by
     * {@code unwrapped().toString()} arrives as {@code "[a, b]"} — the surrounding brackets are
     * stripped so both spellings configure the same columns.
     */
    private static List<String> parseGroupColumns(String value) {
        if (value == null) return List.of();
        String trimmed = value.trim();
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        if (trimmed.isBlank()) return List.of();
        List<String> columns = Arrays.stream(trimmed.split(",")).map(String::trim).toList();
        columns.forEach(c -> {
            if (c.isBlank() || c.contains("[") || c.contains("]")) {
                throw new IllegalArgumentException("malformed '%s' entry: '%s'".formatted(GROUP_COLUMNS_KEY, c));
            }
        });
        return columns;
    }

    /**
     * Aggregation over the write-time source relation: one row per group with the MIN and MAX
     * timestamp and the row count.
     * {@code HAVING COUNT(*) > 0} drops only the single all-NULL row a zero-row relation produces
     * in global (ungrouped) mode. A group whose timestamps are all NULL is kept, with NULL
     * min/max and its true row count.
     */
    public String aggregationSql(String relationSql) {
        String tsColumn = HeaderUtils.quoteIdentifier(timestampColumn);
        String groups = groupColumns.stream().map(HeaderUtils::quoteIdentifier).collect(Collectors.joining(", "));
        String selectPrefix = groups.isEmpty() ? "" : groups + ", ";
        String groupBy = groups.isEmpty() ? "" : " GROUP BY " + groups;
        // Aggregate order must match the column order in insertSql and the read order in
        // computeRows: groups, MIN, MAX, COUNT.
        String aggregates = "MIN(%s) AS %s, MAX(%s) AS %s, COUNT(*) AS %s".formatted(
                tsColumn, HeaderUtils.quoteIdentifier(minTimestampColumn),
                tsColumn, HeaderUtils.quoteIdentifier(maxTimestampColumn),
                HeaderUtils.quoteIdentifier(rowCountColumn));
        // COUNT(*) > 0, not MIN(..) IS NOT NULL: a group whose timestamps are all NULL still has
        // rows, and its row count must be recorded — it emits NULL min/max with a real count.
        // The predicate still suppresses the one all-NULL row a zero-row relation would produce
        // in global (ungrouped) mode, where COUNT(*) is 0.
        return "SELECT %s%s FROM (%s)%s HAVING COUNT(*) > 0"
                .formatted(selectPrefix, aggregates, relationSql, groupBy);
    }

    /**
     * Executes {@link #aggregationSql} and returns the rows as DuckDB-rendered strings
     * (group values in declared order, MIN timestamp last; NULL stays null). String values
     * round-trip through DuckDB's implicit VARCHAR cast on INSERT.
     */
    public List<List<String>> computeRows(Connection connection, String relationSql) throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        int columns = valueColumnCount();
        try (var statement = connection.createStatement();
             var resultSet = statement.executeQuery(aggregationSql(relationSql))) {
            while (resultSet.next()) {
                List<String> row = new ArrayList<>(columns);
                for (int i = 1; i <= columns; i++) {
                    row.add(resultSet.getString(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /**
     * Renders the INSERT appending precomputed watermark rows — explicit quoted column list
     * (group columns then timestamp column), values as escaped string literals relying on
     * DuckDB's implicit cast to the table's column types.
     */
    public String insertSql(String catalog, String schema, List<List<String>> rows) {
        String columnList = groupColumns.stream().map(HeaderUtils::quoteIdentifier).collect(Collectors.joining(", "));
        columnList = (columnList.isEmpty() ? "" : columnList + ", ") + HeaderUtils.quoteIdentifier(minTimestampColumn);
        columnList += ", " + HeaderUtils.quoteIdentifier(maxTimestampColumn)
                + ", " + HeaderUtils.quoteIdentifier(rowCountColumn);
        String values = rows.stream()
                .map(row -> row.stream().map(WatermarkSpec::literal).collect(Collectors.joining(", ", "(", ")")))
                .collect(Collectors.joining(", "));
        return "INSERT INTO %s.%s.%s (%s) VALUES %s".formatted(
                HeaderUtils.quoteIdentifier(catalog), HeaderUtils.quoteIdentifier(schema),
                HeaderUtils.quoteIdentifier(table), columnList, values);
    }

    /**
     * The statement that records the snapshot the batch committed at, or {@code null} when
     * {@code watermark_snapshot_id_column} is unconfigured.
     *
     * <p>Runs AFTER the ingest transaction, on the same connection — see this class's header for
     * why the id cannot be known inside it. {@code IS NULL} both scopes the update to rows this
     * connection has just written and sweeps up any row a previous crash left unstamped.
     */
    public String snapshotStampSql(String catalog, String schema) {
        if (snapshotIdColumn == null) {
            return null;
        }
        String column = HeaderUtils.quoteIdentifier(snapshotIdColumn);
        return "UPDATE %s.%s.%s SET %s = (SELECT * FROM ducklake_last_committed_snapshot('%s')) WHERE %s IS NULL"
                .formatted(HeaderUtils.quoteIdentifier(catalog), HeaderUtils.quoteIdentifier(schema),
                        HeaderUtils.quoteIdentifier(table), column, catalog.replace("'", "''"), column);
    }

    /** Group columns plus the three aggregates: MIN timestamp, MAX timestamp, row count. */
    private int valueColumnCount() {
        return groupColumns.size() + 3;
    }

    private static String literal(String value) {
        return value == null ? "NULL" : "'" + value.replace("'", "''") + "'";
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static void requireNonBlank(String value, String key) {
        if (isBlank(value)) {
            throw new IllegalArgumentException("watermark configuration: '%s' must not be blank".formatted(key));
        }
    }
}
