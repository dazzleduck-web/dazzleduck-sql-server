package io.dazzleduck.sql.compaction;

/** Names DuckLake derives from a catalog's own. */
final class DuckLakeCatalogs {

    private DuckLakeCatalogs() {
    }

    /**
     * The quoted name of the catalog DuckLake attaches the metadata database of {@code catalog} as,
     * by default: {@code "__ducklake_metadata_<catalog>"}.
     */
    static String metadataCatalog(String catalog) {
        return "\"__ducklake_metadata_" + catalog + "\"";
    }
}
