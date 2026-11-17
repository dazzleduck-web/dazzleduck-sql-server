package io.dazzleduck.sql.compaction;

/** Quoting for the SQL the compactor builds, and DuckLake's catalog names. */
final class Sql {

    private Sql() {
    }

    /** A string literal. */
    static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /** A quoted identifier. */
    static String identifier(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    /**
     * The name DuckLake gives the metadata catalog of a catalog attached as {@code database}
     * ({@code __ducklake_metadata_<database>}), the default this compactor relies on: an ATTACH with
     * its own METADATA_CATALOG name is not supported.
     */
    static String metadataCatalogName(String database) {
        return "__ducklake_metadata_" + database;
    }

    /** {@link #metadataCatalogName}, quoted. */
    static String metadataCatalog(String database) {
        return identifier(metadataCatalogName(database));
    }
}
