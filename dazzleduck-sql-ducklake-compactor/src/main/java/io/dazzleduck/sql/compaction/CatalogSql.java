package io.dazzleduck.sql.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Running the compactor's own catalog queries inside a PostgreSQL catalog.
 *
 * <p>DuckDB reads a PostgreSQL catalog's tables through the postgres scanner, which copies them in
 * {@code ctid} ranges and pushes down only simple column-versus-constant filters. A query that
 * filters with {@code IN (SELECT ...)} or joins on names therefore copies the rows of every table in
 * the lake into DuckDB, every time: at about 1.5 million live files, tier discovery ran out of the
 * minor instance's 512 MB. Wrapped in {@code postgres_query}, the query runs in PostgreSQL, which
 * does the joins, filters and grouping and returns only the result.
 */
final class CatalogSql {

    private static final Logger logger = LoggerFactory.getLogger(CatalogSql.class);

    private CatalogSql() {
    }

    /**
     * Where a catalog's own queries run.
     *
     * @param schema     the PostgreSQL schema that holds the catalog's tables, or null to run the DuckDB
     *                   queries
     * @param definitive false when the lookup failed (a catalog briefly unreachable): look again later
     *                   rather than settle for the DuckDB queries, which copy the whole catalog
     */
    record Lookup(String schema, boolean definitive) {
        static final Lookup IN_DUCKDB = new Lookup(null, true);
        static final Lookup UNKNOWN = new Lookup(null, false);
    }

    /**
     * The PostgreSQL schema that holds the DuckLake catalog attached as {@code database} (DuckLake's
     * {@code METADATA_SCHEMA}, {@code public} by default). When several schemas of the database hold a
     * DuckLake catalog, the one whose {@code data_path} is this catalog's is taken: the metadata schema
     * itself is not exposed in SQL, and taking another lake's schema would list another lake's work.
     * {@link Lookup#IN_DUCKDB} when the catalog is not PostgreSQL or no schema matches;
     * {@link Lookup#UNKNOWN} when the lookup fails.
     */
    static Lookup postgresSchema(Connection connection, String database) {
        try {
            String path = null;
            // The DuckLake catalog's path names its backend, e.g. "postgres:host=... dbname=..."
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT path FROM duckdb_databases() WHERE database_name = ? AND type = 'ducklake'")) {
                statement.setString(1, database);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        path = rs.getString(1);
                    }
                }
            }
            if (!isPostgresPath(path)) {
                return Lookup.IN_DUCKDB;
            }
            // The metadata catalog is hidden from duckdb_tables(), so ask PostgreSQL
            List<String> schemas = new ArrayList<>();
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(viaPostgres(database, "SELECT table_schema::text FROM"
                         + " information_schema.tables WHERE table_name = 'ducklake_snapshot'"))) {
                while (rs.next()) {
                    schemas.add(rs.getString(1));
                }
            }
            if (schemas.size() == 1) {
                return new Lookup(schemas.get(0), true);
            }
            String dataPath = dataPath(connection, database);
            List<String> matching = new ArrayList<>();
            for (String schema : schemas) {
                String query = ("SELECT value FROM %s.ducklake_metadata WHERE key = 'data_path' AND scope IS NULL")
                        .formatted(Sql.identifier(schema));
                try (Statement statement = connection.createStatement();
                     ResultSet rs = statement.executeQuery(viaPostgres(database, query))) {
                    if (rs.next() && dataPath != null && samePath(rs.getString(1), dataPath)) {
                        matching.add(schema);
                    }
                }
            }
            if (matching.size() == 1) {
                return new Lookup(matching.get(0), true);
            }
            logger.warn("Cannot tell which schema of {}'s PostgreSQL catalog holds its tables (found {}, {} with its"
                    + " data_path {}); its catalog queries copy the catalog into DuckDB", database, schemas,
                    matching.size(), dataPath);
            return Lookup.IN_DUCKDB;
        } catch (SQLException e) {
            logger.warn("Cannot tell where {}'s catalog queries can run; looking again later", database, e);
            return Lookup.UNKNOWN;
        }
    }

    private static String dataPath(Connection connection, String database) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT data_path FROM ducklake_settings(?)")) {
            statement.setString(1, database);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    /** Whether two data paths name the same place, ignoring a trailing separator. */
    static boolean samePath(String a, String b) {
        return a != null && b != null && stripSeparator(a).equals(stripSeparator(b));
    }

    private static String stripSeparator(String path) {
        String p = path.trim();
        while (p.length() > 1 && (p.endsWith("/") || p.endsWith("\\"))) {
            p = p.substring(0, p.length() - 1);
        }
        return p;
    }

    static boolean isPostgresPath(String path) {
        return path != null && path.regionMatches(true, 0, "postgres:", 0, "postgres:".length());
    }

    /** A DuckDB query that runs {@code postgresSql} in the PostgreSQL catalog of {@code database}. */
    static String viaPostgres(String database, String postgresSql) {
        return "SELECT * FROM postgres_query(%s, %s)".formatted(Sql.literal(Sql.metadataCatalogName(database)),
                Sql.literal(postgresSql));
    }
}
