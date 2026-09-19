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

    /**
     * Applied before the startup script and before a tier's {@code connection_settings}, so an
     * operator can still override any of them.
     *
     * <p>{@code enable_external_file_cache} is an in-memory LRU over external Parquet <b>data</b>
     * (not just footers - {@code parquet_metadata_cache} and {@code enable_http_metadata_cache} are
     * already false by default). Compaction reads each file exactly once and then retires it, so the
     * hit rate is ~0% and the cache only accumulates memory. It is GLOBAL-scoped, so it is set per
     * instance here rather than in the shared ConnectionPool, which query paths use and where the
     * cache does earn its keep.
     *
     * <p>Measured on a benchmark compactor: the minor tier grew to 14.8 GiB RSS against a 12 GB
     * {@code memory_limit} in ~44 minutes, and the major tier sat at its 8 GB limit. With this
     * setting the same tiers hold ~0.7 GiB and ~1.1 GiB, flat, with cycle duration unchanged
     * (major 17.9s vs 17-22s before) - confirming the cache was buying nothing on this workload.
     */
    private static final List<String> WORKLOAD_DEFAULTS =
            List.of("SET enable_external_file_cache = false");

    private RawConnections() {
    }

    static Connection open(String startupScript, List<String> connectionSettings) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, "true");
        Connection connection = DriverManager.getConnection("jdbc:duckdb:", properties);
        try (Statement statement = connection.createStatement()) {
            for (String sql : WORKLOAD_DEFAULTS) {
                statement.execute(sql);
            }
            for (String sql : splitStatements(startupScript)) {
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
