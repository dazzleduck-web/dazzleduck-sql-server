package io.dazzleduck.sql.compaction;

import org.duckdb.DuckDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Opens fully independent DuckDB connections for the compactor — deliberately NOT
 * {@code io.dazzleduck.sql.commons.ConnectionPool}, whose {@code getConnection()} returns duplicates
 * of one shared, process-wide DuckDB instance.
 *
 * <p>DuckDB's {@code memory_limit}, {@code threads}, and {@code temp_directory} are all
 * <b>GLOBAL</b>-scoped (confirmed via {@code duckdb_settings()}) — a {@code SET} on one duplicate
 * silently changes it for every other duplicate of the same instance. Since different compaction
 * tiers are meant to run with genuinely different, isolated {@code connection_settings} (e.g. a
 * different {@code memory_limit} per tier), sharing one instance would make concurrently-running
 * tiers race and clobber each other's global config instead of each getting its own value. Each
 * tier (and housekeeping) therefore gets its own real DuckDB instance here.
 *
 * <p>Each connection independently re-runs the startup script, so it has whatever catalogs/
 * extensions that script sets up before layering its own {@code connection_settings} on top. The
 * script must be safe to run more than once (plain {@code ATTACH}/{@code INSTALL}/{@code LOAD} are;
 * one-time DDL like {@code CREATE TABLE} is not) — this only matters here because it's replayed per
 * raw connection instead of running once against a shared instance.
 */
final class RawConnections {

    private RawConnections() {
    }

    /**
     * Run on every instance before its own settings, which can override them. DuckDB's bundled
     * allocator keeps freed memory outside {@code memory_limit}; with its defaults (no background
     * thread, a 128 MiB flush threshold) memory a merge freed can stay held across calls, and has been
     * seen to grow by about 0.3 MiB per merged input file until the container was OOM-killed. A
     * background thread and a lower threshold return it after each task.
     */
    static final List<String> ALLOCATOR_SETTINGS = List.of(
            "SET allocator_background_threads = true",
            "SET allocator_flush_threshold = '64MB'");

    static Connection open(String startupScript, List<String> connectionSettings) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, "true");
        // The images this module ships bake in a patched DuckLake extension (see
        // DUCKLAKE_PATCH.md) that isn't signed with DuckDB Labs' key, so a startup script's
        // `LOAD ducklake` fails without this. Must be a connection property, not a `SET` statement:
        // DuckDB rejects changing it once the database instance is already running.
        properties.setProperty("allow_unsigned_extensions", "true");
        Connection connection = DriverManager.getConnection("jdbc:duckdb:", properties);
        try (Statement statement = connection.createStatement()) {
            for (String sql : splitStatements(startupScript)) {
                statement.execute(sql);
            }
            for (String sql : ALLOCATOR_SETTINGS) {
                statement.execute(sql);
            }
            for (String sql : connectionSettings) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    /** Mirrors ConnectionPool.splitStatements: semicolon followed by newline, or at end of string. */
    private static String[] splitStatements(String script) {
        if (script == null || script.isBlank()) {
            return new String[0];
        }
        return Arrays.stream(script.split("; *\n|;$"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);
    }
}
