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
 * @param numPartitions number of hash-routed sub-queues the queue is split into ({@code >= 1};
 *                      {@code 1} disables partitioning). When {@code > 1} the queue owns that many
 *                      child queues — independent writers with their own backpressure, all writing
 *                      into the target path; a batch is routed to the single partition its rows map
 *                      to and rejected if its rows span more than one partition.
 * @param variables     DuckDB session variables set on the connection that writes this queue's
 *                      batches, so the transformation can read them with {@code getvariable('name')};
 *                      {@link IngestionVariables#NONE} when none are configured
 * @param partitionExpression SQL expression over the raw input row that the partition index is
 *                      derived from as {@code hash(partitionExpression) % numPartitions} — e.g. a
 *                      column {@code "user_id"}, a map/struct field access
 *                      {@code "resource['user_id']"}, or {@code "claims['user_id']"} when the batch
 *                      carries a claims column; required when {@code numPartitions > 1}, ignored otherwise
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
        boolean extractClaims,
        int numPartitions,
        String partitionExpression,
        IngestionVariables variables) {

    /**
     * {@code additional_parameters} key naming this queue's attribute type registry table —
     * unqualified, resolved in the queue's own catalog and schema. Several queues may name the
     * same table to share it.
     */
    public static final String ATTRIBUTE_TYPES_TABLE_KEY = "attribute_types_table";
    public static final String ATTRIBUTE_TYPES_MAX_ENTRIES_KEY = "attribute_types_max_entries";


    /** Validates invariants on every construction path. */
    public QueueIdToTableMapping {
        if (variables == null) {
            variables = IngestionVariables.NONE;
        }
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
        if (numPartitions < 1) {
            throw new IllegalArgumentException(
                    "Queue '%s': 'num_partitions' must be >= 1, got %d"
                            .formatted(ingestionQueue, numPartitions));
        }
        if (numPartitions > 1 && (partitionExpression == null || partitionExpression.isBlank())) {
            throw new IllegalArgumentException(
                    "Queue '%s': 'num_partitions' > 1 requires a non-blank 'partition_expression'"
                            .formatted(ingestionQueue));
        }
        // Fail at config load, not per batch: a malformed watermark spec (partial/blank/typo'd
        // keys) would otherwise write each batch's output and then orphan it at post-ingestion.
        WatermarkSpec.fromParameters(ingestionQueue, additionalParameters);
        // Same reason: the attribute type table goes into DDL and INSERT text, and a bad name
        // would now fail every batch rather than just itself.
        attributeTypesTable(ingestionQueue, additionalParameters);
        attributeTypesMaxEntries(ingestionQueue, additionalParameters);
        // The table is resolved in this queue's own catalog and schema, so without them there is
        // nowhere to put it. Rejected here rather than at startup, where it surfaced as a
        // reference to 'null.null.<table>' and a message about attaching the catalog.
        if (attributeTypesTable(ingestionQueue, additionalParameters) != null
                && (catalog == null || schema == null)) {
            throw new IllegalArgumentException(("Queue '%s': '%s' needs a catalog and schema to resolve the"
                    + " table in, but this queue has catalog=%s, schema=%s")
                    .formatted(ingestionQueue, ATTRIBUTE_TYPES_TABLE_KEY, catalog, schema));
        }
    }

    /**
     * The queue's attribute type registry table ({@value #ATTRIBUTE_TYPES_TABLE_KEY}), or null when
     * the queue records none. Unqualified, like {@link WatermarkSpec#table()}: it is resolved in
     * this queue's own catalog and schema, so queues sharing a schema can name one table and share
     * it — their rows stay apart by the {@code ingestion_queue} column.
     */
    public String attributeTypesTable() {
        return attributeTypesTable(ingestionQueue, additionalParameters);
    }

    /**
     * This queue's own cap on mappings held for it ({@value #ATTRIBUTE_TYPES_MAX_ENTRIES_KEY}), or
     * null to use the process-wide default. It contains one stream's attribute-name cardinality to
     * that stream; the process-wide {@code max_entries} still bounds the heap, and a queue cannot
     * raise itself past it.
     */
    public Integer attributeTypesMaxEntries() {
        return attributeTypesMaxEntries(ingestionQueue, additionalParameters);
    }

    private static Integer attributeTypesMaxEntries(String queueName, Map<String, String> parameters) {
        if (parameters == null) {
            return null;
        }
        String value = parameters.get(ATTRIBUTE_TYPES_MAX_ENTRIES_KEY);
        if (value == null) {
            return null;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Queue '%s': '%s' must be a positive integer, got '%s'"
                    .formatted(queueName, ATTRIBUTE_TYPES_MAX_ENTRIES_KEY, value), e);
        }
        if (parsed <= 0) {
            throw new IllegalArgumentException("Queue '%s': '%s' must be positive, got %d"
                    .formatted(queueName, ATTRIBUTE_TYPES_MAX_ENTRIES_KEY, parsed));
        }
        return parsed;
    }

    private static String attributeTypesTable(String queueName, Map<String, String> parameters) {
        if (parameters == null) {
            return null;
        }
        String table = parameters.get(ATTRIBUTE_TYPES_TABLE_KEY);
        if (table == null) {
            return null;
        }
        String trimmed = table.trim();
        if (!trimmed.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(("Queue '%s': '%s' must be a single plain identifier — it is"
                    + " resolved in the queue's own catalog and schema — got '%s'")
                    .formatted(queueName, ATTRIBUTE_TYPES_TABLE_KEY, table));
        }
        return trimmed;
    }

    /** Backward-compatible constructor without outputPath/inputSchema (DuckLake-managed entries). */
    public QueueIdToTableMapping(String ingestionQueue, String catalog, String schema, String table,
                                 Map<String, String> additionalParameters, String transformation,
                                 String view, String inputTable) {
        this(ingestionQueue, null, catalog, schema, table, additionalParameters, transformation, view,
                inputTable, null, false, 1, null, IngestionVariables.NONE);
    }

    /** Convenience constructor for mappings that use an explicit transformation or none at all. */
    public QueueIdToTableMapping(String ingestionQueue, String catalog, String schema, String table,
                                 Map<String, String> additionalParameters, String transformation) {
        this(ingestionQueue, null, catalog, schema, table, additionalParameters, transformation, null,
                null, null, false, 1, null, IngestionVariables.NONE);
    }

    public boolean hasViewTransformation() {
        return view != null; // inputTable non-null is guaranteed by the constructor invariant
    }

    /** Whether this queue is split into more than one hash-routed sub-queue. */
    public boolean isPartitioned() {
        return numPartitions > 1;
    }

    /** Returns a copy of this mapping with {@code inputSchema} set (registry-sourced field). */
    public QueueIdToTableMapping withInputSchema(String inputSchema) {
        return new QueueIdToTableMapping(ingestionQueue, outputPath, catalog, schema, table,
                additionalParameters, transformation, view, inputTable, inputSchema, extractClaims,
                numPartitions, partitionExpression, variables);
    }

    /** Returns a copy of this mapping with {@code extractClaims} set. */
    public QueueIdToTableMapping withExtractClaims(boolean extractClaims) {
        return new QueueIdToTableMapping(ingestionQueue, outputPath, catalog, schema, table,
                additionalParameters, transformation, view, inputTable, inputSchema, extractClaims,
                numPartitions, partitionExpression, variables);
    }

    /** Returns a copy of this mapping with the partitioning fields set. */
    public QueueIdToTableMapping withPartitioning(int numPartitions, String partitionExpression) {
        return new QueueIdToTableMapping(ingestionQueue, outputPath, catalog, schema, table,
                additionalParameters, transformation, view, inputTable, inputSchema, extractClaims,
                numPartitions, partitionExpression, variables);
    }

    /** Returns a copy of this mapping with its session variables set. */
    public QueueIdToTableMapping withVariables(IngestionVariables variables) {
        return new QueueIdToTableMapping(ingestionQueue, outputPath, catalog, schema, table,
                additionalParameters, transformation, view, inputTable, inputSchema, extractClaims,
                numPartitions, partitionExpression, variables);
    }
}
