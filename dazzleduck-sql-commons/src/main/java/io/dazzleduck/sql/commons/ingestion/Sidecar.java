package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.Transformations;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A table written beside a queue's target table from every batch: {@code transformation} is a query
 * over the batch's base rows, exposed as {@code __base}, and its rows are registered in the same
 * DuckLake transaction as the base files. The table lives in the queue's catalog and schema.
 *
 * <p>{@code __base} is the base table's rows for the batch as written: after the queue's
 * transformation and its partition projections. See {@code doc/SIDECAR_VIEWS_SPEC.md}.
 *
 * <p>The query is either given as {@code transformation}, or read from a view
 * ({@code transformation_view}): a query over the queue's table (or {@code input_table}), whose one
 * reference to that table is rewritten to {@code __base}. A view is read again on every refresh, so
 * the query can change without a restart.
 *
 * @param table          the sidecar table's name, in the queue's catalog and schema
 * @param transformation a single query that reads {@code __base}; {@code null} for a view
 * @param view           the view the query is read from, as {@code catalog.schema.view}; {@code null}
 *                       for a transformation
 * @param inputTable     the table the view reads, as {@code catalog.schema.table}, rewritten to
 *                       {@code __base}; {@code null} for the queue's own table
 */
public record Sidecar(String table, String transformation, String view, String inputTable) {

    /** Name the batch's base rows are exposed under in a sidecar's transformation. */
    public static final String BASE = "__base";

    public Sidecar {
        if (table == null || table.isBlank()) {
            throw new IllegalArgumentException("a sidecar needs a 'table'");
        }
        boolean hasTransformation = transformation != null && !transformation.isBlank();
        boolean hasView = view != null && !view.isBlank();
        if (hasTransformation == hasView) {
            throw new IllegalArgumentException("sidecar '%s' needs exactly one of 'transformation' and 'transformation_view'"
                    .formatted(table));
        }
        if (hasView) {
            requireQualified(table, "transformation_view", view);
        }
        if (inputTable != null && !inputTable.isBlank()) {
            if (!hasView) {
                throw new IllegalArgumentException("sidecar '%s': 'input_table' goes with 'transformation_view'".formatted(table));
            }
            requireQualified(table, "input_table", inputTable);
        } else {
            inputTable = null;
        }
        if (!hasTransformation) {
            transformation = null;
        }
        if (!hasView) {
            view = null;
        }
    }

    /** A sidecar whose query is given as {@code transformation}. */
    public Sidecar(String table, String transformation) {
        this(table, transformation, null, null);
    }

    /** Whether the query is read from a view. */
    public boolean hasView() {
        return view != null;
    }

    private static void requireQualified(String table, String key, String name) {
        if (name.split("\\.", -1).length != 3 || name.contains("..") || name.startsWith(".") || name.endsWith(".")) {
            throw new IllegalArgumentException("sidecar '%s': '%s' must be fully qualified as 'catalog.schema.name', got '%s'"
                    .formatted(table, key, name));
        }
    }

    /**
     * Checks a queue's sidecars against each other and the queue's own table: each table is named
     * once, none is the queue's table, and each transformation is one query that reads
     * {@code __base}.
     */
    static void validate(String ingestionQueue, String baseTable, List<Sidecar> sidecars) {
        Set<String> seen = new HashSet<>();
        for (Sidecar sidecar : sidecars) {
            String name = sidecar.table().toLowerCase(Locale.ROOT);
            if (baseTable != null && name.equals(baseTable.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("Queue '%s': sidecar table '%s' is the queue's own table"
                        .formatted(ingestionQueue, sidecar.table()));
            }
            if (!seen.add(name)) {
                throw new IllegalArgumentException("Queue '%s': sidecar table '%s' is declared more than once"
                        .formatted(ingestionQueue, sidecar.table()));
            }
            if (!sidecar.hasView()) {
                validateTransformation(ingestionQueue, sidecar); // a view's query is checked when it is read
            }
        }
    }

    private static void validateTransformation(String ingestionQueue, Sidecar sidecar) {
        if (TransformationText.hasMultipleStatements(sidecar.transformation())) {
            throw new IllegalArgumentException("Queue '%s': sidecar '%s' transformation must be a single statement"
                    .formatted(ingestionQueue, sidecar.table()));
        }
        List<Transformations.CatalogSchemaTable> tables;
        try {
            var statement = Transformations.getFirstStatementNode(Transformations.parseToTree(sidecar.transformation()));
            tables = Transformations.collectAllTableReferences(statement, "", "");
        } catch (Exception e) {
            throw new IllegalArgumentException("Queue '%s': invalid SQL in sidecar '%s' transformation: %s"
                    .formatted(ingestionQueue, sidecar.table(), e.getMessage()), e);
        }
        boolean readsBase = tables.stream().anyMatch(t ->
                Transformations.TableType.BASE_TABLE == t.type() && BASE.equalsIgnoreCase(t.tableOrPath()));
        if (!readsBase) {
            throw new IllegalArgumentException("Queue '%s': sidecar '%s' transformation must read '%s', but reads %s"
                    .formatted(ingestionQueue, sidecar.table(), BASE,
                            tables.stream().map(Transformations.CatalogSchemaTable::tableOrPath).toList()));
        }
    }
}
