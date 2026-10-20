package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.util.HeaderUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
 *   <li>{@code watermark_snapshot_id_column} — required when the table is set; receives a LOWER
 *       BOUND on the DuckLake snapshot the batch committed in, written by the same INSERT as the
 *       rest of the row. The true snapshot is this value or higher, never lower — see
 *       {@link DuckLakePostIngestionTask} for why. Consumers must join with {@code >=}, and must
 *       read the GREATEST value recorded for the batch: the row as first written keeps its
 *       predicted id, and a correction appends the exact one rather than replacing it, so reading
 *       the raw column answers "visible as of N" too early for a corrected batch.</li>
 *   <li>{@code watermark_batch_id_column} — required when the table is set; receives an id unique
 *       to the batch, shared by every group row of it. The column should be {@code UUID}: the
 *       value is one, and a native UUID is 16 bytes where its text is 36, which halves the stored
 *       size of a column that is distinct on every row. A {@code VARCHAR} column still works. It is what the snapshot correction attaches
 *       to when the predicted id is taken by a competing writer: the correction is a second row for
 *       the same batch id, never an update of the first (see {@link #snapshotCorrectionSql}).</li>
 * </ul>
 *
 * <p>The batch id is <strong>required rather than optional</strong> so that there is one behaviour
 * to reason about: the watermark table is always append-only and the correction is always an
 * append. An optional key would have meant two shapes of table and two post-commit behaviours
 * differing only on the rare conflict path — the hardest kind of difference to test or to
 * remember. A watermark table that predates this must add the column (a nullable VARCHAR) before
 * the queue will start.
 *
 * <p><strong>Reading the snapshot column:</strong> the table is append-only and a batch may hold
 * more than one row for its snapshot, so the exact snapshot of a batch is
 * {@code max(snapshot_id)} over its batch id — restricted to rows that HAVE one. Rows written
 * before the batch id column existed carry NULL, and SQL groups those into a single NULL batch,
 * which would admit or exclude the whole history together; they need no maximum anyway, since the
 * code that wrote them corrected in place. A correction row carries ONLY the batch id and the
 * snapshot — group columns and aggregates are NULL — so aggregate queries are unaffected by it:
 * {@code sum(row_count)} and {@code max(max_ts)} skip NULLs. Aggregates must NOT be folded into
 * that {@code GROUP BY batch_id}: a batch holds one row per group, so doing so keeps one group and
 * drops the rest.
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
                            String snapshotIdColumn, String batchIdColumn) {

    public static final String TABLE_KEY = "watermark_table";
    public static final String TIMESTAMP_COLUMN_KEY = "watermark_timestamp_column";
    public static final String GROUP_COLUMNS_KEY = "watermark_group_columns";
    public static final String MIN_TIMESTAMP_COLUMN_KEY = "watermark_min_timestamp_column";
    public static final String MAX_TIMESTAMP_COLUMN_KEY = "watermark_max_timestamp_column";
    public static final String ROW_COUNT_COLUMN_KEY = "watermark_row_count_column";
    public static final String SNAPSHOT_ID_COLUMN_KEY = "watermark_snapshot_id_column";
    public static final String BATCH_ID_COLUMN_KEY = "watermark_batch_id_column";

    private static final List<String> KNOWN_KEYS = List.of(TABLE_KEY, TIMESTAMP_COLUMN_KEY, GROUP_COLUMNS_KEY,
            MIN_TIMESTAMP_COLUMN_KEY, MAX_TIMESTAMP_COLUMN_KEY, ROW_COUNT_COLUMN_KEY, SNAPSHOT_ID_COLUMN_KEY,
            BATCH_ID_COLUMN_KEY);

    public WatermarkSpec {
        requireNonBlank(table, TABLE_KEY);
        requireNonBlank(timestampColumn, TIMESTAMP_COLUMN_KEY);
        groupColumns = groupColumns == null ? List.of() : List.copyOf(groupColumns);
        groupColumns.forEach(c -> requireNonBlank(c, GROUP_COLUMNS_KEY));
        requireNonBlank(minTimestampColumn, MIN_TIMESTAMP_COLUMN_KEY);
        requireNonBlank(maxTimestampColumn, MAX_TIMESTAMP_COLUMN_KEY);
        requireNonBlank(rowCountColumn, ROW_COUNT_COLUMN_KEY);
        requireNonBlank(snapshotIdColumn, SNAPSHOT_ID_COLUMN_KEY);
        requireNonBlank(batchIdColumn, BATCH_ID_COLUMN_KEY);
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
        String batchIdColumn = parameters.get(BATCH_ID_COLUMN_KEY);
        if (isBlank(table) || isBlank(timestampColumn) || isBlank(minTimestampColumn)
                || isBlank(maxTimestampColumn) || isBlank(rowCountColumn) || isBlank(snapshotIdColumn)
                || isBlank(batchIdColumn)) {
            throw new IllegalArgumentException(
                    "Queue '%s': watermark configuration requires non-blank '%s', '%s', '%s', '%s', '%s', '%s' and '%s'"
                            .formatted(queueName, TABLE_KEY, TIMESTAMP_COLUMN_KEY, MIN_TIMESTAMP_COLUMN_KEY,
                                    MAX_TIMESTAMP_COLUMN_KEY, ROW_COUNT_COLUMN_KEY, SNAPSHOT_ID_COLUMN_KEY,
                                    BATCH_ID_COLUMN_KEY));
        }
        try {
            return new WatermarkSpec(table.trim(), timestampColumn.trim(),
                    parseGroupColumns(parameters.get(GROUP_COLUMNS_KEY)),
                    minTimestampColumn.trim(), maxTimestampColumn.trim(), rowCountColumn.trim(),
                    snapshotIdColumn.trim(), batchIdColumn.trim());
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

    /** DuckDB's JDBC type name for {@code TIMESTAMPTZ}. */
    private static final String TIMESTAMP_TZ_TYPE = "TIMESTAMP WITH TIME ZONE";

    /** UTC text DuckDB parses back as the same instant (always with seconds; microsecond precision). */
    private static final DateTimeFormatter UTC_TEXT = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS'+00'");

    /**
     * Executes {@link #aggregationSql} and returns the rows as strings (group values in declared
     * order, then MIN, MAX and the row count; NULL stays null). String values round-trip through
     * DuckDB's implicit VARCHAR cast on INSERT.
     *
     * <p>A {@code TIMESTAMP WITH TIME ZONE} value is rendered in UTC with a {@code +00} offset, never
     * through {@code getString}: the driver renders it via {@code java.time} in the JVM's zone and drops
     * zero seconds ({@code 12:00+09:00}), a form DuckDB cannot parse. In UTC the text means the same
     * thing to either column type: a {@code TIMESTAMPTZ} watermark column gets the exact instant, and a
     * {@code TIMESTAMP} column, which drops the offset, gets the UTC clock time. So neither depends on the
     * JVM's or DuckDB's time zone.
     */
    public List<List<String>> computeRows(Connection connection, String relationSql) throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        int columns = valueColumnCount();
        try (var statement = connection.createStatement();
             var resultSet = statement.executeQuery(aggregationSql(relationSql))) {
            var metaData = resultSet.getMetaData();
            boolean[] timestampTz = new boolean[columns + 1];
            for (int i = 1; i <= columns; i++) {
                timestampTz[i] = TIMESTAMP_TZ_TYPE.equalsIgnoreCase(metaData.getColumnTypeName(i));
            }
            while (resultSet.next()) {
                List<String> row = new ArrayList<>(columns);
                for (int i = 1; i <= columns; i++) {
                    row.add(timestampTz[i] ? utcText(resultSet.getObject(i, OffsetDateTime.class)) : resultSet.getString(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private static String utcText(OffsetDateTime value) {
        return value == null ? null : value.withOffsetSameInstant(ZoneOffset.UTC).format(UTC_TEXT);
    }

    /**
     * Renders the INSERT appending precomputed watermark rows — explicit quoted column list
     * (group columns, the timestamp columns, the row count, then {@link #snapshotIdColumn}), values
     * as escaped string literals relying on DuckDB's implicit cast to the table's column types.
     *
     * <p>The snapshot id is written here, in the SAME transaction as the rows, rather than patched
     * in afterwards.
     *
     * <p>{@code snapshotId} is the id the transaction is predicted to commit as
     * ({@code max(snapshot_id) + 1}). DuckLake does not expose the pending id, but it derives the
     * same value and enforces it with a primary key on {@code ducklake_snapshot} — so a commit that
     * succeeds unopposed carries exactly this id. It is a prediction only in the sense that a
     * competing writer can take the id first, in which case DuckLake's internal retry commits us
     * one higher; {@code DuckLakePostIngestionTask} appends a correction afterwards on the rare
     * occasion they disagree, which is why {@code batchId} is written here — it is what the
     * correction attaches to.
     *
     * <p>{@code batchId} is required, and checked rather than quietly omitted: rows written without
     * one would leave nothing for a correction to attach to, so the exact snapshot would be lost
     * rarely and invisibly. It is a {@link UUID} rather than a string so the column can be a
     * native {@code UUID} — 16 bytes against 36 characters of text, which is half the stored size
     * for a column that is distinct on every row and so compresses no better than its raw bytes.
     *
     * @throws IllegalArgumentException if {@code batchId} is null
     */
    public String insertSql(String catalog, String schema, List<List<String>> rows, long snapshotId, UUID batchId) {
        if (batchId == null) {
            throw new IllegalArgumentException(
                    "watermark configuration: every batch must carry an id for '%s'".formatted(BATCH_ID_COLUMN_KEY));
        }
        String columnList = groupColumns.stream().map(HeaderUtils::quoteIdentifier).collect(Collectors.joining(", "));
        columnList = (columnList.isEmpty() ? "" : columnList + ", ") + HeaderUtils.quoteIdentifier(minTimestampColumn);
        columnList += ", " + HeaderUtils.quoteIdentifier(maxTimestampColumn)
                + ", " + HeaderUtils.quoteIdentifier(rowCountColumn);
        columnList += ", " + HeaderUtils.quoteIdentifier(snapshotIdColumn);
        columnList += ", " + HeaderUtils.quoteIdentifier(batchIdColumn);
        String valueSuffix = ", " + snapshotId + ", " + uuidLiteral(batchId);
        String values = rows.stream()
                .map(row -> row.stream().map(WatermarkSpec::literal).collect(Collectors.joining(", ", "(", valueSuffix + ")")))
                .collect(Collectors.joining(", "));
        return "INSERT INTO %s.%s.%s (%s) VALUES %s".formatted(
                HeaderUtils.quoteIdentifier(catalog), HeaderUtils.quoteIdentifier(schema),
                HeaderUtils.quoteIdentifier(table), columnList, values);
    }

    /**
     * Renders the post-commit APPEND recording the exact snapshot for a batch whose predicted id
     * was taken by a competing writer. One row, carrying only {@link #batchIdColumn} and
     * {@link #snapshotIdColumn} — every other column is left NULL.
     *
     * <p>This replaces an earlier {@code UPDATE} that stamped the id onto the batch's rows in
     * place, identifying them by DuckLake's {@code rowid} from {@code ducklake_table_insertions}.
     * Appending is better on three counts: the table stays append-only, so nothing on the ingestion
     * path issues a row-level write that DuckLake could keep in the catalog as inlined data; one
     * row is written instead of rewriting the batch's rows; and it does not depend on {@code rowid}
     * stability or on reading the insertions back.
     *
     * <p>The id cannot be written by {@link #insertSql} instead: DuckLake only assigns a snapshot id
     * at COMMIT, so inside the ingest transaction it does not yet exist.
     *
     * <p>A reader therefore takes {@code max(snapshot_id)} over a batch id. That is exact rather
     * than a heuristic: the committed id is always {@code >=} the predicted one, so a correction can
     * only ever raise the value, and at most one correction is written per batch.
     */
    public String snapshotCorrectionSql(String catalog, String schema, UUID batchId, long snapshotId) {
        if (batchId == null) {
            throw new IllegalArgumentException(
                    "watermark configuration: a correction needs the batch id it corrects, for '%s'"
                            .formatted(BATCH_ID_COLUMN_KEY));
        }
        return "INSERT INTO %s.%s.%s (%s, %s) VALUES (%s, %d)".formatted(
                HeaderUtils.quoteIdentifier(catalog), HeaderUtils.quoteIdentifier(schema),
                HeaderUtils.quoteIdentifier(table), HeaderUtils.quoteIdentifier(batchIdColumn),
                HeaderUtils.quoteIdentifier(snapshotIdColumn), uuidLiteral(batchId), snapshotId);
    }

    /** Group columns plus the three aggregates: MIN timestamp, MAX timestamp, row count. */
    private int valueColumnCount() {
        return groupColumns.size() + 3;
    }

    /**
     * A typed {@code UUID} literal. DuckDB casts it to text for a legacy {@code VARCHAR} column, so
     * the same statement serves both, and {@link UUID#toString()} is canonical hex so there is
     * nothing to escape.
     */
    private static String uuidLiteral(UUID value) {
        return "UUID '" + value + "'";
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
