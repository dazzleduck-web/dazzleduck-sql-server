package io.dazzleduck.sql.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One raw DuckDB instance per database (see {@link RawConnections}), opened on first use and kept.
 * Keyed by database rather than shared: different databases' jobs run concurrently, and a JDBC
 * Connection is not safe for use by more than one thread at a time.
 */
final class RawConnectionCache implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RawConnectionCache.class);

    private final String startupScript;
    private final List<String> connectionSettings;
    private final String purpose;
    private final ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();

    /** @param purpose names the job in log messages, e.g. "housekeeping" */
    RawConnectionCache(String startupScript, List<String> connectionSettings, String purpose) {
        this.startupScript = startupScript;
        this.connectionSettings = List.copyOf(connectionSettings);
        this.purpose = purpose;
    }

    Connection get(String database) throws SQLException {
        Connection existing = connections.get(database);
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = connections.get(database);
            if (existing != null) {
                return existing;
            }
            Connection opened = RawConnections.open(startupScript, connectionSettings);
            connections.put(database, opened);
            return opened;
        }
    }

    /**
     * Closes the database's connection, if it is still {@code connection}, so the next
     * {@link #get} opens a new one: for a connection that may be broken, e.g. after the catalog
     * backend restarted.
     */
    void evict(String database, Connection connection) {
        if (connections.remove(database, connection)) {
            closeQuietly(connection);
        }
    }

    @Override
    public void close() {
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
