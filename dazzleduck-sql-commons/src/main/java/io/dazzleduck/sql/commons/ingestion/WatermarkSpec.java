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
 *       one row per group with the MIN of the timestamp column across each ingested batch.</li>
 *   <li>{@code watermark_timestamp_column} — column to MIN over; required when the table is set.
 *       The value lands in a watermark-table column of the same name.</li>
 *   <li>{@code watermark_group_columns} — optional grouping columns as a comma-separated string
 *       (a HOCON list is tolerated: its flattened {@code [a, b]} form is unwrapped). Empty means
 *       one global MIN row per batch.</li>
 * </ul>
 *
 * <p>Watermark rows are computed at WRITE time by {@link ParquetIngestionQueue} — an aggregation
 * over the same pre-COPY relation the output Parquet is written from (local data, transformation
 * already applied) — and carried through {@link IngestionResult#watermarkRows()} to
 * {@link DuckLakePostIngestionTask}, which appends them via a plain {@code INSERT ... VALUES} in
 * the SAME transaction as the {@code ducklake_add_data_files} registration. The post-ingestion
 * step therefore never re-reads the written files: no second download, no dependence on the
 * written files' schema, and partition columns (present in the relation, hive-only in the files)
 * group correctly. Rows whose MIN would be NULL (empty batch / all-NULL timestamps) are excluded.
 *
 * <p>Validation runs at config-load time via {@link QueueIdToTableMapping}, so a malformed spec
 * fails startup rather than orphaning batches at flush time. Values are rejected when blank, and
 * unknown {@code watermark_}-prefixed keys are rejected to surface typos.
 */
public record WatermarkSpec(String table, String timestampColumn, List<String> groupColumns) {

    public static final String TABLE_KEY = "watermark_table";
    public static final String TIMESTAMP_COLUMN_KEY = "watermark_timestamp_column";
    public static final String GROUP_COLUMNS_KEY = "watermark_group_columns";

    private static final List<String> KNOWN_KEYS = List.of(TABLE_KEY, TIMESTAMP_COLUMN_KEY, GROUP_COLUMNS_KEY);

    public WatermarkSpec {
        requireNonBlank(table, TABLE_KEY);
        requireNonBlank(timestampColumn, TIMESTAMP_COLUMN_KEY);
        groupColumns = groupColumns == null ? List.of() : List.copyOf(groupColumns);
        groupColumns.forEach(c -> requireNonBlank(c, GROUP_COLUMNS_KEY));
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
        if (isBlank(table) || isBlank(timestampColumn)) {
            throw new IllegalArgumentException(
                    "Queue '%s': watermark configuration requires non-blank '%s' and '%s'"
                            .formatted(queueName, TABLE_KEY, TIMESTAMP_COLUMN_KEY));
        }
        try {
            return new WatermarkSpec(table.trim(), timestampColumn.trim(), parseGroupColumns(parameters.get(GROUP_COLUMNS_KEY)));
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
     * Aggregation over the write-time source relation: one row per group with the MIN timestamp.
     * {@code HAVING MIN(..) IS NOT NULL} drops rows a NULL MIN would produce — the global row of
     * an empty batch, or a group whose timestamps are all NULL.
     */
    public String aggregationSql(String relationSql) {
        String tsColumn = HeaderUtils.quoteIdentifier(timestampColumn);
        String groups = groupColumns.stream().map(HeaderUtils::quoteIdentifier).collect(Collectors.joining(", "));
        String selectPrefix = groups.isEmpty() ? "" : groups + ", ";
        String groupBy = groups.isEmpty() ? "" : " GROUP BY " + groups;
        return "SELECT %sMIN(%s) AS %s FROM (%s)%s HAVING MIN(%s) IS NOT NULL"
                .formatted(selectPrefix, tsColumn, tsColumn, relationSql, groupBy, tsColumn);
    }

    /**
     * Executes {@link #aggregationSql} and returns the rows as DuckDB-rendered strings
     * (group values in declared order, MIN timestamp last; NULL stays null). String values
     * round-trip through DuckDB's implicit VARCHAR cast on INSERT.
     */
    public List<List<String>> computeRows(Connection connection, String relationSql) throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        int columns = groupColumns.size() + 1;
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
        columnList = (columnList.isEmpty() ? "" : columnList + ", ") + HeaderUtils.quoteIdentifier(timestampColumn);
        String values = rows.stream()
                .map(row -> row.stream().map(WatermarkSpec::literal).collect(Collectors.joining(", ", "(", ")")))
                .collect(Collectors.joining(", "));
        return "INSERT INTO %s.%s.%s (%s) VALUES %s".formatted(
                HeaderUtils.quoteIdentifier(catalog), HeaderUtils.quoteIdentifier(schema),
                HeaderUtils.quoteIdentifier(table), columnList, values);
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
