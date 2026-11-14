package io.dazzleduck.sql.commons.ingestion;

import java.util.List;
import java.util.Set;

/**
 * A sidecar resolved against its DuckLake table, as a write needs it: its query, where its files go,
 * the table's columns, which every sidecar file is written with, cast by name, and its partitioning.
 *
 * @param table          the sidecar table's name, in the queue's catalog and schema
 * @param transformation the sidecar's query over {@code __base}
 * @param path           the table's data path; files are written directly under it
 * @param columns        the table's columns, in table order
 * @param produced       the names, in lower case, of the columns the query produces; a table column
 *                       not among them is written as NULL
 * @param partitionBy    the table's partition keys, as a COPY's {@code PARTITION_BY} names them;
 *                       empty when the table is not partitioned
 * @param projections    the expressions that compute the keys that are not columns (a {@code day} or
 *                       {@code bucket} transform), as {@code expr AS key}
 */
public record SidecarTarget(String table, String transformation, String path, List<Column> columns,
                            Set<String> produced, List<String> partitionBy, List<String> projections) {

    /** A table column: its name and DuckDB type, e.g. {@code BIGINT} or {@code STRUCT(a INTEGER)}. */
    public record Column(String name, String type) {}

    public SidecarTarget {
        columns = List.copyOf(columns);
        produced = Set.copyOf(produced);
        partitionBy = List.copyOf(partitionBy);
        projections = List.copyOf(projections);
    }
}
