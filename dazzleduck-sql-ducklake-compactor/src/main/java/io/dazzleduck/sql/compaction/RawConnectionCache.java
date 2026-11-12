package io.dazzleduck.sql.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One raw DuckDB instance per key (see {@link RawConnections}), opened on first use and kept. Keyed
 * (by database, or database and tier) rather than shared: those jobs run concurrently, and a JDBC
 * Connection is not safe for use by more than one thread at a time.
 */
final class RawConnectionCache implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RawConnectionCache.class);

    private final String startupScript;
    private final List<String> connectionSettings;
    private final String purpose;
    private final ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();
    private volatile boolean closed;

    /** @param purpose names the job in log messages, e.g. "housekeeping" */
    RawConnectionCache(String startupScript, List<String> connectionSettings, String purpose) {
        this.startupScript = startupScript;
        this.connectionSettings = List.copyOf(connectionSettings);
        this.purpose = purpose;
    }

    /** The key's connection, opened with this cache's connection settings. */
    Connection get(String key) throws SQLException {
        return get(key, connectionSettings);
    }

    /** The key's connection, opened with {@code settings} if it is not open yet. */
    Connection get(String key, List<String> settings) throws SQLException {
        Connection existing = connections.get(key);
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = connections.get(key);
            if (existing != null) {
                return existing;
            }
            if (closed) {
                // a job still running at shutdown must not open an instance nothing will close
                throw new SQLException("the %s connections are closed".formatted(purpose));
            }
            Connection opened = RawConnections.open(startupScript, settings);
            connections.put(key, opened);
            return opened;
        }
    }

    /**
     * Closes the key's connection, if it is still {@code connection}, so the next {@link #get}
     * opens a new one: for a connection that may be broken, e.g. after the catalog backend restarted.
     */
    void evict(String key, Connection connection) {
        if (connections.remove(key, connection)) {
            closeQuietly(connection);
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
        }
        connections.values().forEach(this::closeQuietly);
        connections.clear();
    }

    private void closeQuietly(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.warn("Failed to close a {} connection", purpose, e);
        }
    }
}
