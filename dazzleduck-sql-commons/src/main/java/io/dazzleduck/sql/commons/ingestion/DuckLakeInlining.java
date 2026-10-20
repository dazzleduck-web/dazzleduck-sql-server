package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Startup check that every catalog a watermark table is written to is attached with DuckLake data inlining disabled.
 *
 * <p>With inlining on, DuckLake keeps a small write (ten rows or fewer by default) in the catalog database instead of
 * writing a Parquet file, until {@code ducklake_flush_inlined_data} runs. The watermark table is the one table the
 * ingestion path writes with {@code INSERT}, one row per batch, so its rows would accumulate in the catalog database.
 * Registered data files are not affected: inlining only applies to inserts.
 *
 * <p>The catalog must be attached with {@code DATA_INLINING_ROW_LIMIT 0}. Leaving the parameter out is not enough,
 * because the default limit is 10.
 */
public final class DuckLakeInlining {

    /** The {@code ATTACH} parameter, as {@code duckdb_databases().options} names it. */
    public static final String OPTION = "data_inlining_row_limit";

    private DuckLakeInlining() {
    }

    /**
     * Fails unless each of {@code catalogs} was attached with {@code DATA_INLINING_ROW_LIMIT 0}.
     *
     * @throws IllegalStateException listing every catalog that was not, with its fix
     */
    public static void requireDisabled(Collection<String> catalogs) {
        List<String> problems = new ArrayList<>();
        for (String catalog : catalogs) {
            String problem = problem(catalog);
            if (problem != null) {
                problems.add(problem);
            }
        }
        if (!problems.isEmpty()) {
            throw new IllegalStateException("DuckLake data inlining must be off for the catalogs that watermark tables"
                    + " are written to: with it on, watermark rows are kept in the catalog database instead of written"
                    + " as Parquet, until ducklake_flush_inlined_data runs. Found:\n  - "
                    + String.join("\n  - ", problems));
        }
    }

    /** Why {@code catalog} does not have inlining disabled, with the fix, or null when it does. */
    static String problem(String catalog) {
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
