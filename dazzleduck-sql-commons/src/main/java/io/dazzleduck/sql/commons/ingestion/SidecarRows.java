package io.dazzleduck.sql.commons.ingestion;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Rows a producer derived from a batch and wants appended to a table <b>beside the batch's target
 * table</b> — named by the producer, resolved in the target's own catalog and schema — in the SAME
 * transaction that registers the batch's data files. Whether two producers name one table and share
 * it is theirs to decide; nothing here imposes a layout.
 *
 * <p>This is the producer-side counterpart to {@link WatermarkSpec}: a watermark row is computed
 * inside {@link ParquetIngestionQueue} from the source relation, whereas these rows are derived
 * upstream, by whoever built the batch, from information the write path no longer has. The OTel
 * collector's attribute type registry is the case it exists for — which OTLP value types an
 * attribute name has arrived with is visible when the OTLP message is read and nowhere later.
 *
 * <p>Carried on {@link Batch#sidecarRows()}, unioned per bucket by
 * {@link #union(java.util.Collection)} (several batches become one COPY), handed on
 * {@link IngestionResult#sidecarRows()} and appended by {@link DuckLakePostIngestionTask}, which
 * is the only place that knows the target's catalog and schema and therefore where the table is.
 *
 * <p><b>They are written the way data is written.</b> The ingest path never inserts: it
 * {@code COPY}s to Parquet and calls {@code ducklake_add_data_files}, and these rows do the same.
 * {@link #copySql} writes the file before the transaction, as the batch's own data file is written,
 * and the registration goes inside it, so the two land or roll back together.
 *
 * <p>{@code ducklake_add_data_files} references a file in place rather than copying it, so the file
 * must be written under the sidecar table's own data directory — which only the writer knows how
 * to resolve, from the catalog's metadata. See
 * {@link DuckLakeIngestionHandler#tableDataPath(String, String, String)}.
 *
 * <p>It follows that the rows commit exactly when the files do — which is the point — and equally
 * that a statement the database rejects fails the batch. So the statements are built from this
 * record alone, with every identifier checked here against {@code [A-Za-z_][A-Za-z0-9_]*} and every
 * value already rendered as a SQL literal by the producer. A plain-Parquet queue has no transaction
 * to join, and its {@link PostIngestionTask} discards these rows.
 *
 * <p><b>Append-only, and duplicates are tolerated.</b> Nothing is read, updated or deleted — a
 * write adds a file — so it cannot conflict with another writer. Several processes recording the
 * same value therefore produce several identical rows in separate files, which is the accepted
 * trade: a consumer deduplicates when it reads (a {@code GROUP BY} over the key columns), and each
 * producer's own cache keeps its own repeats down. Small files accumulate at the rate new values
 * appear, and are merged by the catalog's ordinary compaction.
 *
 * <p>The table and its data directory must already exist: the producer creates them, once, before
 * it sends anything. This carries no DDL, so the transaction it joins only ever registers files,
 * and a table that cannot be created is the producer's startup error rather than a batch failing at
 * commit.
 *
 * <p><b>Column types matter, because registration does not cast.</b>
 * {@code ducklake_add_data_files} maps a file's columns onto the table's by name and refuses a type
 * it would have to convert — a VARCHAR column will not register into a {@code TIMESTAMP} or a
 * {@code UUID}. Values here are rendered literals, so a {@code VALUES} list of quoted strings
 * produces an all-VARCHAR file, which is right for a table of strings and wrong for anything else.
 * {@code columnTypes} therefore declares the target's types and {@link #copySql} casts to them, so
 * the file's schema is stated rather than inferred from how the producer happened to spell its
 * literals. Empty means every column is VARCHAR, which is what a table of strings wants.
 *
 * @param table       unqualified table name, resolved in the target table's catalog and schema
 * @param columns     the columns to insert into, in the order {@code rows} gives their values
 * @param columnTypes the target's column types, in {@code columns} order; empty for all-VARCHAR
 * @param rows        one list of SQL literals per row, in {@code columns} order
 */
public record SidecarRows(String table, List<String> columns, List<String> columnTypes,
                          List<List<String>> rows) {

    private static final String IDENTIFIER = "[A-Za-z_][A-Za-z0-9_]*";
    /** A plain type name, optionally with a precision/scale — nothing that could carry SQL. */
    private static final String TYPE = "[A-Za-z][A-Za-z0-9_ ]*(\\(\\d+(, ?\\d+)?\\))?";

    /** All columns VARCHAR, which is what a table of strings wants. */
    public SidecarRows(String table, List<String> columns, List<List<String>> rows) {
        this(table, columns, List.of(), rows);
    }

    public SidecarRows {
        requireIdentifier("table", table);
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("sidecar rows for '" + table + "' declare no columns");
        }
        columns = List.copyOf(columns);
        columnTypes = columnTypes == null ? List.of() : List.copyOf(columnTypes);
        rows = rows.stream().map(List::copyOf).toList();
        columns.forEach(column -> requireIdentifier("column", column));
        if (!columnTypes.isEmpty() && columnTypes.size() != columns.size()) {
            throw new IllegalArgumentException(("sidecar rows for '%s' declare %d column type(s) for %d"
                    + " column(s)").formatted(table, columnTypes.size(), columns.size()));
        }
        for (String type : columnTypes) {
            if (!type.trim().matches(TYPE)) {
                throw new IllegalArgumentException("sidecar rows for '%s' declare an unusable column type: %s"
                        .formatted(table, type));
            }
        }
        for (List<String> row : rows) {
            if (row.size() != columns.size()) {
                throw new IllegalArgumentException(("sidecar row for '%s' has %d value(s) for %d column(s): %s")
                        .formatted(table, row.size(), columns.size(), row));
            }
        }
    }

    private static void requireIdentifier(String what, String value) {
        if (value == null || !value.matches(IDENTIFIER)) {
            throw new IllegalArgumentException("sidecar " + what + " is not a plain identifier: '" + value + "'");
        }
    }

    public boolean isEmpty() {
        return rows.isEmpty();
    }

    /**
     * One bucket's rows: the batches of a bucket come from one producer with one configuration, so
     * they agree on table, columns and declared types, and only their rows differ. Returns null
     * when there is nothing to append.
     *
     * <p>Rows that are equal <em>in full</em> are collapsed, which in practice is fewer than it
     * sounds: a producer whose rows carry a timestamp or a producer id — as the attribute type
     * registry's do — makes two batches' versions of the same new mapping distinct, so both
     * survive into the file. That is harmless, because the table is append-only and its readers
     * group anyway; it is said here only so the collapsing is not mistaken for deduplication.
     *
     * @throws IllegalArgumentException if two batches disagree about the table, or about its
     *                                  columns or their declared types, which would mean one
     *                                  bucket holding two producers' schemas
     */
    static SidecarRows union(java.util.Collection<SidecarRows> all) {
        SidecarRows first = null;
        Set<List<String>> rows = new LinkedHashSet<>();
        for (SidecarRows sidecar : all) {
            if (sidecar == null || sidecar.isEmpty()) {
                continue;
            }
            if (first == null) {
                first = sidecar;
            } else if (!first.table.equals(sidecar.table)) {
                throw new IllegalArgumentException(("one bucket carries sidecar rows for two different tables:"
                        + " '%s' and '%s'").formatted(first.table, sidecar.table));
            } else if (!first.columns.equals(sidecar.columns) || !first.columnTypes.equals(sidecar.columnTypes)) {
                // Types are part of what makes two sets of rows the same shape: unioning rows that
                // declare different ones would produce a file that matches neither. Reported apart
                // from the table mismatch above, because naming the table twice would say nothing
                // when it is the shape that differs.
                throw new IllegalArgumentException(("one bucket carries sidecar rows for table '%s' with two"
                        + " different shapes: %s typed %s, and %s typed %s")
                        .formatted(first.table, first.columns, first.columnTypes,
                                sidecar.columns, sidecar.columnTypes));
            }
            rows.addAll(sidecar.rows);
        }
        // Types carried through: dropping them here would silently undo the declaration, and every
        // batch goes through this on its way to the COPY.
        return first == null ? null
                : new SidecarRows(first.table, first.columns, first.columnTypes, List.copyOf(rows));
    }

    /**
     * Writes these rows to {@code file} as Parquet — run BEFORE the transaction, like the batch's
     * own data {@code COPY}. The column names come from this record and the types from the
     * producer's literals, so the file's schema is the table's.
     */
    String copySql(String file) {
        String values = rows.stream()
                .map(row -> "(" + String.join(", ", row) + ")")
                .reduce((a, b) -> a + ", " + b)
                .orElseThrow();
        // Projected with casts when types are declared, so the file's schema is the target's and
        // registration has nothing to convert.
        // Quoted, so a column whose name is a reserved word — `at`, `end`, `order` — works. The
        // name is already checked against IDENTIFIER, so quoting cannot smuggle anything in.
        String quoted = columns.stream().map(c -> "\"" + c + "\"").collect(java.util.stream.Collectors.joining(", "));
        String projection = "*";
        if (!columnTypes.isEmpty()) {
            StringBuilder cast = new StringBuilder();
            for (int i = 0; i < columns.size(); i++) {
                cast.append(i == 0 ? "" : ", ")
                        .append("CAST(\"").append(columns.get(i)).append("\" AS ").append(columnTypes.get(i).trim())
                        .append(") AS \"").append(columns.get(i)).append('"');
            }
            projection = cast.toString();
        }
        return "COPY (SELECT %s FROM (VALUES %s) AS t (%s)) TO '%s' (FORMAT parquet)"
                .formatted(projection, values, quoted, file.replace("'", "''"));
    }

    /** A file name for one batch's rows, to create inside the table's data directory. */
    String fileName() {
        return "dd_" + java.util.UUID.randomUUID() + ".parquet";
    }
}
