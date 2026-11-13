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
 * @param table          the sidecar table's name, in the queue's catalog and schema
 * @param transformation a single query that reads {@code __base}
 */
public record Sidecar(String table, String transformation) {

    /** Name the batch's base rows are exposed under in a sidecar's transformation. */
    public static final String BASE = "__base";

    public Sidecar {
        if (table == null || table.isBlank()) {
            throw new IllegalArgumentException("a sidecar needs a 'table'");
        }
        if (transformation == null || transformation.isBlank()) {
            throw new IllegalArgumentException("sidecar '%s' needs a 'transformation'".formatted(table));
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
            validateTransformation(ingestionQueue, sidecar);
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
