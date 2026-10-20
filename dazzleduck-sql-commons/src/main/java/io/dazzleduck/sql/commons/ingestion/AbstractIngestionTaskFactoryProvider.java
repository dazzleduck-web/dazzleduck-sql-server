package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.Transformations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Base class for {@link IngestionTaskFactoryProvider} implementations.
 *
 * <p>Subclasses implement {@link #loadMappings()} to supply the queue-to-table mapping from
 * any source (HOCON config, SQLite, etc.) and {@link #getIngestionHandler()} to create the
 * appropriate handler. Validation of transformation SQL is provided here and is source-agnostic.
 */
public abstract class AbstractIngestionTaskFactoryProvider implements IngestionTaskFactoryProvider {

    protected Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    /**
     * Loads the current queue-to-table mappings from the underlying source.
     * Called by {@link #validate()} and by {@link #getIngestionHandler()} implementations.
     */
    protected abstract Map<String, QueueIdToTableMapping> loadMappings();

    /**
     * Validates transformation SQL, and the session variables, for every mapping returned by
     * {@link #loadMappings()}. Only entries with a non-null {@code transformation} or {@code view}
     * are checked for SQL.
     *
     * <p>Also fails startup unless DuckLake data inlining is disabled for every watermark table, by the
     * catalog's ATTACH and by any option stored with set_option; see {@link DuckLakeInlining}.
     */
    @Override
    public void validate() {
        Map<String, QueueIdToTableMapping> mappings = loadMappings();
        // Only watermark tables: the watermark INSERT is the only SQL write on the ingest path, and target
        // tables are registered files, which inlining cannot affect. With no watermark configured nothing is
        // checked. The watermark table lives in the target table's catalog and schema.
        List<DuckLakeInlining.WatermarkTable> watermarkTables = new ArrayList<>();
        for (QueueIdToTableMapping m : mappings.values()) {
            WatermarkSpec spec = m.catalog() == null ? null
                    : WatermarkSpec.fromParameters(m.ingestionQueue(), m.additionalParameters());
            if (spec != null) {
                watermarkTables.add(new DuckLakeInlining.WatermarkTable(m.catalog(), m.schema(), spec.table()));
            }
        }
        if (!watermarkTables.isEmpty()) {
            DuckLakeInlining.requireDisabled(watermarkTables);
        }
        mappings.values().forEach(mapping -> {
            if (mapping.transformation() != null) {
                validateTransformation(mapping.ingestionQueue(), mapping.transformation());
            }
            if (mapping.view() != null) {
                validateViewTransformation(mapping.ingestionQueue(), mapping.view(), mapping.inputTable());
            }
            validateVariables(mapping);
        });
    }

    /**
     * Fails startup on a variable the write path could not apply. The static pairs and the
     * relation's identifiers are already checked when the mapping is built; reading the relation
     * here is what turns a missing view or an unusable row into a startup error rather than a
     * failed batch hours later.
     */
    protected static void validateVariables(QueueIdToTableMapping mapping) {
        if (mapping.variables().isEmpty()) {
            return;
        }
        try {
            mapping.variables().resolve(mapping.ingestionQueue());
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid variables for queue '%s': %s".formatted(mapping.ingestionQueue(), e.getMessage()), e);
        }
    }

    protected static void validateViewTransformation(String queueId, String fqView, String fqInputTable) {
        try {
            String derived = DuckLakeIngestionHandler.resolveViewTransformationStatic(fqView, fqInputTable);
            validateTransformation(queueId, derived);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to validate view transformation for queue '%s' (view='%s', input_table='%s'): %s"
                            .formatted(queueId, fqView, fqInputTable, e.getMessage()), e);
        }
    }

    protected static void validateTransformation(String ingestionQueue, String transformation) {
        // The transformation is embedded in the write's statement, so a second statement would make
        // every write for the queue fail; only the first would be checked below.
        if (TransformationText.hasMultipleStatements(transformation)) {
            throw new IllegalArgumentException(
                    "transformation for queue '%s' must be a single statement".formatted(ingestionQueue));
        }
        try {
            var ast = Transformations.parseToTree(transformation);
            var statementNode = Transformations.getFirstStatementNode(ast);
            // CTE-aware walk: __this may be referenced only inside a CTE body of the transformation.
            var tables = Transformations.collectAllTableReferences(statementNode, "", "");
            boolean hasThis = tables.stream()
                    .anyMatch(t -> Transformations.TableType.BASE_TABLE == t.type()
                            && "__this".equals(t.tableOrPath()));
            if (!hasThis) {
                throw new IllegalArgumentException(
                        "transformation for queue '%s' must reference '__this' as a table, but found: %s"
                                .formatted(ingestionQueue,
                                        tables.stream().map(Transformations.CatalogSchemaTable::tableOrPath).toList()));
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid transformation SQL for queue '%s': %s".formatted(ingestionQueue, e.getMessage()), e);
        }
    }
}
