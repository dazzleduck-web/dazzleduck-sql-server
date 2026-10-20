package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.HeaderUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Startup check that DuckLake data inlining is disabled for every watermark table.
 *
 * <p>With inlining on, DuckLake keeps a small write (ten rows or fewer by default) in the catalog database instead of
 * writing a Parquet file, until {@code ducklake_flush_inlined_data} runs. The watermark table is the one table the
 * ingestion path writes with {@code INSERT}, one row per batch, so its rows would accumulate in the catalog database.
 * Registered data files are not affected: inlining only applies to inserts.
 *
 * <p>Two things are checked:
 * <ul>
 *   <li>The catalog must be attached with {@code DATA_INLINING_ROW_LIMIT 0}. Leaving the parameter out is not enough,
 *       because the default limit is 10.</li>
 *   <li>An option stored in the catalog with {@code set_option} takes precedence over the {@code ATTACH} parameter,
 *       the most specific scope winning: the table's own, else its schema's, else the catalog's. The one that applies
 *       to the watermark table must not be above zero.</li>
 * </ul>
 */
public final class DuckLakeInlining {

    /** The {@code ATTACH} parameter and the DuckLake option name. */
    public static final String OPTION = "data_inlining_row_limit";

    private DuckLakeInlining() {
    }

    /** A watermark table to check: {@code catalog.schema.table}. */
    public record WatermarkTable(String catalog, String schema, String table) {
    }

    /** One stored option row, as {@code ducklake_options()} reports it. */
    private record StoredOption(String scope, String value) {
    }

    /**
     * Fails unless inlining is disabled for each of {@code tables}: their catalog attached with
     * {@code DATA_INLINING_ROW_LIMIT 0}, and no stored option above zero applying to the table.
     *
     * @throws IllegalStateException listing every problem, with its fix
     */
    public static void requireDisabled(Collection<WatermarkTable> tables) {
        Set<String> problems = new LinkedHashSet<>();
        for (WatermarkTable table : tables) {
            String attach = attachProblem(table.catalog());
            if (attach != null) {
                problems.add(attach);
            }
            String stored = storedOptionProblem(table);
            if (stored != null) {
                problems.add(stored);
            }
        }
        if (!problems.isEmpty()) {
            throw new IllegalStateException("DuckLake data inlining must be off for watermark tables: with it on,"
                    + " watermark rows are kept in the catalog database instead of written as Parquet, until"
                    + " ducklake_flush_inlined_data runs. Found:\n  - " + String.join("\n  - ", problems));
        }
    }

    /** Why {@code catalog} was not attached with inlining disabled, with the fix, or null when it was. */
    static String attachProblem(String catalog) {
        String attached;
        try {
            // Case-insensitive, as DuckDB resolves catalog names: a config naming "Lake" writes to a catalog
            // attached as lake, and an exact match would skip its ATTACH parameter.
            attached = ConnectionPool.collectFirst(
                    "SELECT max(options['%s']) FROM duckdb_databases() WHERE lower(database_name) = lower('%s')"
                            .formatted(OPTION, literal(catalog)), String.class);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot read the ATTACH options of catalog '%s': %s"
                    .formatted(catalog, e.getMessage()), e);
        }
        if (attached == null) {
            return "catalog '%s': attached without DATA_INLINING_ROW_LIMIT, so the default of 10 applies."
                    .formatted(catalog) + " Add DATA_INLINING_ROW_LIMIT 0 to its ATTACH";
        }
        if (!isZero(attached)) {
            return "catalog '%s': ATTACH parameter DATA_INLINING_ROW_LIMIT %s. Set it to 0"
                    .formatted(catalog, attached.trim());
        }
        return null;
    }

    /**
     * Why the stored option that applies to {@code table} enables inlining, with the fix, or null when none does.
     *
     * <p>Scope entries are matched case-insensitively: DuckLake stores the object's own spelling, which need not be
     * the config's, and an exact match would miss a table option and fall through to a wider scope.
     */
    static String storedOptionProblem(WatermarkTable table) {
        String tableEntry = table.schema() + "." + table.table();
        String sql = ("SELECT upper(scope), value FROM ducklake_options('%s') WHERE option_name = '%s' AND ("
                + "(upper(scope) = 'TABLE' AND lower(scope_entry) = lower('%s'))"
                + " OR (upper(scope) = 'SCHEMA' AND lower(scope_entry) = lower('%s'))"
                + " OR upper(scope) = 'GLOBAL')")
                .formatted(literal(table.catalog()), OPTION, literal(tableEntry), literal(table.schema()));
        List<StoredOption> options = new ArrayList<>();
        try (Connection connection = ConnectionPool.getConnection()) {
            for (StoredOption option : ConnectionPool.collectAll(connection, sql,
                    rs -> new StoredOption(rs.getString(1), rs.getString(2)))) {
                options.add(option);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot read the DuckLake options of catalog '%s': %s"
                    .formatted(table.catalog(), e.getMessage()), e);
        }
        for (String scope : List.of("TABLE", "SCHEMA", "GLOBAL")) {
            StoredOption applies = options.stream().filter(o -> scope.equals(o.scope())).findFirst().orElse(null);
            if (applies == null) {
                continue;
            }
            if (isZero(applies.value())) {
                return null;
            }
            String where = switch (scope) {
                case "TABLE" -> "its own";
                case "SCHEMA" -> "schema '%s'".formatted(table.schema());
                default -> "the catalog";
            };
            // The table's own option is the fix in every case: it is the most specific scope, so it overrides
            // the others without changing any other table.
            return ("catalog '%s', watermark table '%s': %s option %s = %s takes precedence over the ATTACH"
                    + " parameter. CALL %s.set_option('%s', 0, schema => '%s', table_name => '%s')")
                    .formatted(table.catalog(), tableEntry, where, OPTION, applies.value(),
                            HeaderUtils.quoteIdentifier(table.catalog()), OPTION,
                            literal(table.schema()), literal(table.table()));
        }
        return null;
    }

    private static boolean isZero(String value) {
        try {
            return Long.parseLong(value.trim()) == 0;
        } catch (NumberFormatException e) {
            return false;   // not a number: the real limit is unknown, so report it
        }
    }

    private static String literal(String value) {
        return value.replace("'", "''");
    }
}
