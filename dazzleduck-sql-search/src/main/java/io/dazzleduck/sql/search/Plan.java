package io.dazzleduck.sql.search;

public interface Plan {
    /**
     *
     * @param sql input sql
     * @return array of splits
     * Sql example select * from read_parquet('/a/b/c') where time &gt; start_time and time =&lt; end_time and search( index_location, column_name, [tokens])
     * Parse the sql.
     * Get the list of the files using partition filter
     * Get the list of the index files using index partition filter.
     * Use the index files to further filer files which matches with search( index_location, column_name, [tokens])
     *
     * Get the tokens searched
     *
     */
    Split[] plan(String sql);

}
