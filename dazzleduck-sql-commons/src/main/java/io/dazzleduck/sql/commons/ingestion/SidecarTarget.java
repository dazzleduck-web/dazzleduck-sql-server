package io.dazzleduck.sql.commons.ingestion;

import java.util.List;

/**
 * A sidecar resolved against its DuckLake table, as a write needs it: where its files go and the
 * table's columns, which every sidecar file is written with, cast by name.
 *
 * @param table          the sidecar table's name, in the queue's catalog and schema
 * @param transformation the sidecar's query over {@code __base}
 * @param path           the table's data path; files are written directly under it
 * @param columns        the table's columns, in table order
 */
public record SidecarTarget(String table, String transformation, String path, List<Column> columns) {

    /** A table column: its name and DuckDB type, e.g. {@code BIGINT} or {@code STRUCT(a INTEGER)}. */
    public record Column(String name, String type) {}

    public SidecarTarget {
        columns = List.copyOf(columns);
    }
}
