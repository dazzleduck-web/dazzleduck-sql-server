package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;

import java.sql.SQLException;

/**
 * DuckLake's data inlining, which keeps a small write in the catalog database instead of a Parquet
 * file — and which this project needs off, because its whole model is that data reaches storage as
 * Parquet.
 *
 * <p>An inlined row only reaches storage when {@code ducklake_flush_inlined_data} runs, so on a
 * deployment without compaction enabled it stays in the catalog database — which may be SQLite —
 * indefinitely. And it is <b>on by default</b>: {@value #SETTING} is 10, so a write of ten rows or
 * fewer is inlined unless something says otherwise.
 *
 * <p>FIVE THINGS CAN TURN IT ON, and they are not all visible in one place. All of this was
 * measured, because the visibility is the trap:
 * <table border="1">
 *   <caption>Sources, most specific first — DuckLake's own resolution order</caption>
 *   <tr><th>Source</th><th>Readable in</th></tr>
 *   <tr><td>Table option, {@code set_option(..., table_name => 's.t')}</td>
 *       <td>{@code ducklake_options}, scope {@code TABLE}</td></tr>
 *   <tr><td>Schema option, {@code set_option(..., schema => 's')}</td>
 *       <td>{@code ducklake_options}, scope {@code SCHEMA}</td></tr>
 *   <tr><td>Catalog option, {@code set_option(...)}</td>
 *       <td>{@code ducklake_options}, scope {@code GLOBAL}</td></tr>
 *   <tr><td>{@code ATTACH ... (DATA_INLINING_ROW_LIMIT n)}</td>
 *       <td>{@code duckdb_databases().options} — <b>not</b> {@code ducklake_options}</td></tr>
 *   <tr><td>{@value #SETTING}</td><td>{@code current_setting}</td></tr>
 * </table>
 *
 * <p>Most specific wins, verified at the three boundaries that matter: an attach parameter of 10
 * with the setting at 0 still inlined, so {@link #disable()} alone guarantees nothing; a table
 * option of 0 against an attach parameter of 10 wrote a real Parquet file; and a schema option of 0
 * under that same attach parameter wrote a file for that schema while another schema inlined. The
 * last two are why {@link #effectiveRowLimit} resolves per table rather than condemning a catalog.
 *
 * <p>The attach parameter is the one a check is most likely to miss and an operator most likely to
 * set, since the {@code ATTACH} lives in the startup script; the schema scope is the one most
 * likely to be left out of a resolver.
 *
 * <p>It applies to {@code INSERT}, {@code UPDATE} and {@code DELETE} — a delete at the default limit
 * wrote no delete file, it went to {@code ducklake_inlined_delete_<n>} — but <b>not</b> to
 * registering a file: {@code ducklake_add_data_files} at the default limit registered a one-row file
 * as a data file. So only tables written with SQL need checking.
 */
public final class DuckLakeInlining {

    /** The DuckDB setting, default 10. */
    public static final String SETTING = "ducklake_default_data_inlining_row_limit";
    /** The DuckLake option that overrides it, per catalog or per table. */
    public static final String OPTION = "data_inlining_row_limit";

    private DuckLakeInlining() {
    }

    /**
     * Turns inlining off for this DuckDB instance.
     *
     * <p>A narrower, per-connection {@code SET} was tried and does not exist in practice:
     * {@link ConnectionPool} hands out duplicates of one instance, and setting it on a duplicate
     * moved it for every other one — measured, after it changed an unrelated suite's file counts.
     * So the scope of this setting is the instance whether one asks for that or not, and pretending
     * otherwise would only hide it. Callers log it.
     *
     * <p>Necessary but not sufficient — a catalog, schema or table option still wins, which is
     * what {@link #effectiveRowLimit} is for.
     */
    public static void disable() throws SQLException {
        ConnectionPool.executeOnSingleton("SET GLOBAL %s = 0".formatted(SETTING));
    }

    /**
     * The limit that applies to a table, and where it came from — the source matters because the
     * remedy differs between a table option and an attach parameter buried in a startup script.
     *
     * @param rowLimit zero means no write to the table can be inlined
     */
    public record Limit(long rowLimit, String source) {
        public boolean inliningEnabled() {
            return rowLimit > 0;
        }
    }

    /**
     * The inlining row limit that actually applies to {@code catalog.schema.table}, resolved
     * most-specific-first over all five sources: its own option, else its schema's, else the
     * catalog's, else the {@code ATTACH} parameter, else this instance's setting.
     *
     * @throws SQLException if the catalog's options cannot be read
     */
    public static Limit effectiveRowLimit(String catalog, String schema, String table) throws SQLException {
        Long forTable = option(catalog, "TABLE", schema + "." + table);
        if (forTable != null) {
            return new Limit(forTable, "the table's own " + OPTION);
        }
        Long forSchema = option(catalog, "SCHEMA", schema);
        if (forSchema != null) {
            return new Limit(forSchema, "schema '" + schema + "' " + OPTION);
        }
        Long forCatalog = option(catalog, "GLOBAL", null);
        if (forCatalog != null) {
            return new Limit(forCatalog, "the catalog's " + OPTION);
        }
        // The ATTACH parameter, which beats the setting below and appears in neither
        // ducklake_options nor duckdb_settings.
        String attached = ConnectionPool.collectFirst(
                ("SELECT max(options['%s']) FROM duckdb_databases() WHERE database_name = '%s'")
                        .formatted(OPTION, escape(catalog)), String.class);
        if (attached != null) {
            // Reported, not cast away: a value that is not a number means the real limit is
            // unknown, and falling through to the setting would report one the writes will not use.
            try {
                return new Limit(Long.parseLong(attached.trim()),
                        "the ATTACH parameter DATA_INLINING_ROW_LIMIT");
            } catch (NumberFormatException e) {
                throw new SQLException(("The ATTACH parameter %s of catalog '%s' is '%s', which is not a"
                        + " number, so the inlining limit cannot be determined")
                        .formatted(OPTION, catalog, attached));
            }
        }
        Long setting = ConnectionPool.collectFirst(
                "SELECT current_setting('%s')::BIGINT".formatted(SETTING), Long.class);
        return new Limit(setting == null ? 0L : setting, SETTING);
    }

    /**
     * The statement an operator runs to turn inlining off for one table, quoted into an error
     * message so the fix does not have to be looked up.
     */
    public static String disableForTableSql(String catalog, String schema, String table) {
        return "CALL %s.set_option('%s', 0, table_name => '%s.%s')"
                .formatted(catalog, OPTION, schema, table);
    }

    /**
     * What to tell the operator, given where the limit came from.
     *
     * <p>The source's own fix comes first, because it is the narrower one. A table option is stored
     * in the catalog and so applies to every process touching that table; an {@code ATTACH}
     * parameter is one line in this process's startup script. Advising the stored option for a
     * local misconfiguration would be a global change to fix a local one — so the table option is
     * offered second, for when the catalog is not the operator's to change.
     */
    public static String remedy(String catalog, String schema, String table, String source) {
        String exempt = "If that is not yours to change, exempt this table instead: "
                + disableForTableSql(catalog, schema, table);
        if (source.startsWith("the ATTACH")) {
            return ("Fix the source: remove %s from the ATTACH of '%s'. %s")
                    .formatted("DATA_INLINING_ROW_LIMIT", catalog, exempt);
        }
        if (source.equals(SETTING)) {
            return "Fix the source: this process sets %s = 0 at startup, so a non-zero value here means"
                    .formatted(SETTING)
                    + " something set it afterwards. " + exempt;
        }
        // When the source already IS this table's own option, "fix it, or else exempt the table"
        // would be the same statement twice. Say it once.
        if (source.startsWith("the table's own")) {
            return "Fix the source: " + disableForTableSql(catalog, schema, table);
        }
        return "Fix the source: set it to 0 where it is set (%s). %s".formatted(source, exempt);
    }

    /**
     * One scope's option value, or null when it is not set at that scope. Aggregated so the query
     * always returns a row: an unset option is a NULL, not an empty result, which
     * {@code collectFirst} would raise.
     *
     * <p>The scope entry is matched case-insensitively, because the two sides come from different
     * places. DuckLake stores the object's own spelling — {@code set_option(..., table_name =>
     * 'mixed')} on a table created as {@code "Mixed"} records {@code scope_entry = 'main.Mixed'} —
     * while this side is the identifier a config file happened to use. An exact match would miss
     * the option and fall through to a wider scope, which is a false pass precisely when a table
     * option re-enables inlining under a catalog option of zero.
     */
    private static Long option(String catalog, String scope, String scopeEntry) throws SQLException {
        String query = ("SELECT max(value::BIGINT) FROM ducklake_options('%s')"
                + " WHERE option_name = '%s' AND scope = '%s' AND %s")
                .formatted(escape(catalog), OPTION, scope,
                        scopeEntry == null ? "scope_entry IS NULL"
                                : "lower(scope_entry) = lower('" + escape(scopeEntry) + "')");
        return ConnectionPool.collectFirst(query, Long.class);
    }

    private static String escape(String value) {
        return value.replace("'", "''");
    }
}
