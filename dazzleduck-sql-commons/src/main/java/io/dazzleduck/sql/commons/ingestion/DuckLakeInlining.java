package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Startup check that DuckLake data inlining is off for every catalog a watermark table is written to.
 *
 * <p>With inlining on, DuckLake keeps a small write (ten rows or fewer by default) in the catalog database instead of
 * writing a Parquet file, until {@code ducklake_flush_inlined_data} runs. The watermark table is the one table the
 * ingestion path writes with {@code INSERT}, one row per batch, so its rows would accumulate in the catalog database.
 * Registered data files are not affected: inlining only applies to inserts.
 *
 * <p>The check is deliberately strict rather than precise. Inlining can be enabled by the {@code ATTACH} parameter
 * {@code DATA_INLINING_ROW_LIMIT}, or by an option stored in the catalog at catalog, schema or table scope; the stored
 * options override the {@code ATTACH} parameter, so checking only the connection is not enough. Instead of resolving
 * the effective limit for each table, startup fails if any of these is above zero anywhere in the catalog, and the
 * message lists each one with the statement that turns it off. An explicit zero is fine.
 *
 * <p>Checking once at startup is sufficient: DuckLake reads a catalog's options when it is attached, so an option
 * changed while the process runs only takes effect at the next start, which runs this check again.
 */
public final class DuckLakeInlining {

    /** The DuckDB setting holding the default limit (10) when nothing else sets one. */
    public static final String SETTING = "ducklake_default_data_inlining_row_limit";
    /** The {@code ATTACH} parameter and the DuckLake option name. */
    public static final String OPTION = "data_inlining_row_limit";

    private DuckLakeInlining() {
    }

    /** One stored option row, as {@code ducklake_options()} reports it. */
    record StoredOption(String scope, String scopeEntry, String value) {
    }

    /**
     * Fails if inlining is enabled anywhere in any of {@code catalogs}, then sets {@link #SETTING} to zero for this
     * DuckDB instance, which covers the default when nothing else sets a limit.
     *
     * @throws IllegalStateException listing every source that enables inlining, with its fix
     */
    public static void requireDisabled(Collection<String> catalogs) {
        List<String> problems = new ArrayList<>();
        for (String catalog : catalogs) {
            problems.addAll(enabledSources(catalog));
        }
        if (!problems.isEmpty()) {
            throw new IllegalStateException("DuckLake data inlining must be off for the catalogs that watermark tables"
                    + " are written to: with it on, watermark rows are kept in the catalog database instead of written"
                    + " as Parquet, until ducklake_flush_inlined_data runs. Found:\n  - "
                    + String.join("\n  - ", problems));
        }
        ConnectionPool.executeOnSingleton("SET GLOBAL %s = 0".formatted(SETTING));
    }

    /** Every source that enables inlining for {@code catalog}, each with the statement that turns it off. */
    static List<String> enabledSources(String catalog) {
        List<String> found = new ArrayList<>();
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
        if (attached != null && !isZero(attached)) {
            found.add("catalog '%s': ATTACH parameter DATA_INLINING_ROW_LIMIT %s. Remove it from the ATTACH, or set it to 0"
                    .formatted(catalog, attached.trim()));
        }
        String sql = ("SELECT scope, scope_entry, value FROM ducklake_options('%s')"
                + " WHERE option_name = '%s' AND coalesce(TRY_CAST(value AS BIGINT), 1) <> 0"
                + " ORDER BY scope, scope_entry").formatted(literal(catalog), OPTION);
        try (Connection connection = ConnectionPool.getConnection()) {
            for (StoredOption option : ConnectionPool.collectAll(connection, sql,
                    rs -> new StoredOption(rs.getString(1), rs.getString(2), rs.getString(3)))) {
                found.add(describe(catalog, option));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot read the DuckLake options of catalog '%s': %s"
                    .formatted(catalog, e.getMessage()), e);
        }
        return found;
    }

    private static String describe(String catalog, StoredOption option) {
        String fix = "CALL %s.set_option('%s', 0".formatted(catalog, OPTION);
        String where;
        switch (option.scope() == null ? "" : option.scope().toUpperCase()) {
            case "SCHEMA" -> {
                where = "schema '%s'".formatted(option.scopeEntry());
                fix += ", schema => '%s')".formatted(option.scopeEntry());
            }
            case "TABLE" -> {
                where = "table '%s'".formatted(option.scopeEntry());
                String entry = option.scopeEntry();
                int dot = entry.indexOf('.');
                fix += dot < 0 ? ", table_name => '%s')".formatted(entry)
                        : ", schema => '%s', table_name => '%s')".formatted(entry.substring(0, dot), entry.substring(dot + 1));
            }
            default -> {
                where = "the catalog";
                fix += ")";
            }
        }
        return "catalog '%s': %s option %s = %s. %s".formatted(catalog, where, OPTION, option.value(), fix);
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
