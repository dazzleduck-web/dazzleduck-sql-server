package io.dazzleduck.sql.commons.ingestion;

import java.util.Map;

/**
 * Maps an ingestion queue to a target DuckLake table and its optional transformation.
 *
 * <p>Exactly one of the following must hold:
 * <ul>
 *   <li>{@code view} and {@code inputTable} are both non-null — transformation is derived
 *       at runtime by reading the view definition and replacing {@code inputTable} with
 *       the {@code __this} CTE placeholder.</li>
 *   <li>{@code transformation} is non-null — explicit SQL transformation referencing
 *       {@code __this}.</li>
 *   <li>All three are null — data is written as-is with no transformation.</li>
 * </ul>
 *
 * @param view        fully-qualified view name ({@code catalog.schema.view}), nullable
 * @param inputTable  fully-qualified table name inside the view to replace with {@code __this}
 *                    ({@code catalog.schema.table}), nullable; required when {@code view} is set
 * @param inputSchema DuckDB column-definition fragment for the raw input columns (e.g.
 *                    {@code "severity_number INTEGER, body VARCHAR"}), nullable; used only by
 *                    {@code manageTables} as the {@code __this} input over which the transformation
 *                    is described to derive the target table's columns
 * @param extractClaims when true, ingested rows carry a {@code claims} MAP column from the
 *                      caller's JWT (see {@code IngestionHandler#extractClaims})
 */
public record QueueIdToTableMapping(
        String ingestionQueue,
        String outputPath,
        String catalog,
        String schema,
        String table,
        Map<String, String> additionalParameters,
        String transformation,
        String view,
        String inputTable,
        String inputSchema,
        boolean extractClaims) {

    /** Validates invariants on every construction path. */
    public QueueIdToTableMapping {
        if ((view == null) != (inputTable == null)) {
            throw new IllegalArgumentException(
                    "Queue '%s': 'view' and 'input_table' must both be provided or both omitted"
                            .formatted(ingestionQueue));
        }
        if (transformation != null && view != null) {
            throw new IllegalArgumentException(
                    "Queue '%s': 'transformation' and 'view'/'input_table' are mutually exclusive"
                            .formatted(ingestionQueue));
        }
        // Fail at config load, not per batch: a malformed watermark spec (partial/blank/typo'd
        // keys) would otherwise write each batch's output and then orphan it at post-ingestion.
        WatermarkSpec.fromParameters(ingestionQueue, additionalParameters);
    }

    /** Backward-compatible constructor without outputPath/inputSchema (DuckLake-managed entries). */
    public QueueIdToTableMapping(String ingestionQueue, String catalog, String schema, String table,
                                 Map<String, String> additionalParameters, String transformation,
                                 String view, String inputTable) {
        this(ingestionQueue, null, catalog, schema, table, additionalParameters, transformation, view, inputTable, null, false);
    }

    /** Convenience constructor for mappings that use an explicit transformation or none at all. */
    public QueueIdToTableMapping(String ingestionQueue, String catalog, String schema, String table,
                                 Map<String, String> additionalParameters, String transformation) {
        this(ingestionQueue, null, catalog, schema, table, additionalParameters, transformation, null, null, null, false);
    }

    public boolean hasViewTransformation() {
        return view != null; // inputTable non-null is guaranteed by the constructor invariant
    }

    /** Returns a copy of this mapping with {@code inputSchema} set (registry-sourced field). */
    public QueueIdToTableMapping withInputSchema(String inputSchema) {
        return new QueueIdToTableMapping(ingestionQueue, outputPath, catalog, schema, table,
                additionalParameters, transformation, view, inputTable, inputSchema, extractClaims);
    }

    /** Returns a copy of this mapping with {@code extractClaims} set. */
    public QueueIdToTableMapping withExtractClaims(boolean extractClaims) {
        return new QueueIdToTableMapping(ingestionQueue, outputPath, catalog, schema, table,
                additionalParameters, transformation, view, inputTable, inputSchema, extractClaims);
    }
}
