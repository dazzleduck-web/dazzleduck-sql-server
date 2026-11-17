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
     * The PostgreSQL schema that holds the DuckLake catalog attached as {@code database} (DuckLake's
     * {@code METADATA_SCHEMA}, {@code public} by default), or null when the catalog is not
     * PostgreSQL or the schema cannot be found; callers then use their DuckDB SQL.
     */
    static String postgresSchema(Connection connection, String database) {
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
                return null;
            }
            // The metadata catalog is hidden from duckdb_tables() too, so ask PostgreSQL. Several schemas
            // hold one when several lakes share a database: the connection's own schema is the one this
            // catalog's unqualified names resolve to, as the DuckDB queries' do.
            String query = "SELECT table_schema::text, table_schema = current_schema() FROM information_schema.tables"
                    + " WHERE table_name = 'ducklake_snapshot'";
            List<String> schemas = new ArrayList<>();
            String current = null;
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(viaPostgres(database, query))) {
                while (rs.next()) {
                    schemas.add(rs.getString(1));
                    if (rs.getBoolean(2)) {
                        current = rs.getString(1);
                    }
                }
            }
            if (schemas.size() == 1) {
                return schemas.get(0);
            }
            if (current != null) {
                return current;
            }
            logger.warn("Cannot tell which schema of {}'s PostgreSQL catalog holds its tables (found {}); its catalog"
                    + " queries copy the catalog into DuckDB", database, schemas);
            return null;
        } catch (SQLException e) {
            logger.warn("Cannot tell whether {}'s catalog is PostgreSQL; its catalog queries run in DuckDB", database, e);
            return null;
        }
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
