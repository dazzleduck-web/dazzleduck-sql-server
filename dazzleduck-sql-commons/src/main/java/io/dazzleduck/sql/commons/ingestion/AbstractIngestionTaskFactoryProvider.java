package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.Transformations;

import java.util.Map;

/**
 * Base class for {@link IngestionTaskFactoryProvider} implementations.
 *
 * <p>Subclasses implement {@link #loadMappings()} to supply the queue-to-table mapping from
 * any source (HOCON config, SQLite, etc.) and {@link #getIngestionHandler()} to create the
 * appropriate handler. Validation of transformation SQL is provided here and is source-agnostic.
 */
public abstract class AbstractIngestionTaskFactoryProvider implements IngestionTaskFactoryProvider {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(AbstractIngestionTaskFactoryProvider.class);


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
     */
    @Override
    public void validate() {
        Map<String, QueueIdToTableMapping> mappings = loadMappings();
        // Only when something here actually writes with SQL — that is, when a queue configures a
        // watermark table. Target tables are registered files, which inlining cannot touch, so with
        // no watermark configured there is nothing on this path to protect and the setting would
        // only change other people's writes: it is instance-wide, so it would make every small
        // INSERT on every attached DuckLake its own file for no benefit here.
        if (mappings.values().stream().anyMatch(m -> m.catalog() != null && writesWatermarkRows(m))) {
            // Once, before the per-table checks below read the effective limits: this removes the
            // default (10) so only an explicitly set option can still enable inlining.
            try {
                DuckLakeInlining.disable();
                // The ATTACH parameter is named first and explicitly: it also beats this setting
                // (measured — setting 0 with DATA_INLINING_ROW_LIMIT 10 still inlined), it is
                // invisible to ducklake_options(), and it is the source most likely to be set,
                // since it sits in this deployment's own startup script. An operator reading a
                // line that mentioned only stored options would think the disable was sufficient.
                LOG.info("DuckLake data inlining disabled ({} = 0 for this DuckDB instance). This is a"
                        + " default only: DATA_INLINING_ROW_LIMIT on a catalog's ATTACH, or a table-,"
                        + " schema- or catalog-scoped option, still overrides it — which is what the"
                        + " per-table check below is for", DuckLakeInlining.SETTING);
            } catch (Exception e) {
                throw new IllegalStateException("Could not disable DuckLake data inlining: " + e.getMessage(), e);
            }
        }
        mappings.values().forEach(mapping -> {
            if (mapping.transformation() != null) {
                validateTransformation(mapping.ingestionQueue(), mapping.transformation());
            }
            if (mapping.view() != null) {
                validateViewTransformation(mapping.ingestionQueue(), mapping.view(), mapping.inputTable());
            }
            validateVariables(mapping);
            validateNoDataInlining(mapping);
        });
    }

    /** Whether this queue inserts watermark rows, which is the only SQL write on the ingest path. */
    private static boolean writesWatermarkRows(QueueIdToTableMapping mapping) {
        try {
            return WatermarkSpec.fromParameters(mapping.ingestionQueue(), mapping.additionalParameters()) != null;
        } catch (RuntimeException e) {
            // A malformed spec is reported by validateNoDataInlining/WatermarkSpec itself, with a
            // better message than anything this could add; treat it as configured.
            return true;
        }
    }

    /**
     * Fails startup when DuckLake would keep this queue's watermark rows in the catalog database
     * instead of writing them as Parquet.
     *
     * <p>Here, rather than in any one server: the watermark insert is in
     * {@link DuckLakePostIngestionTask}, which the OTel collector and the Flight/HTTP ingestion
     * path both reach through this provider. A check living in one of them would leave the other
     * exposed to exactly the fault it is meant to prevent.
     *
     * <p>Only the watermark table is checked, because it is the only one written with SQL:
     * registering a file is not an insert, so target tables and the attribute type registry cannot
     * be inlined. A queue with no watermark has nothing to check.
     *
     * <p>{@link #validate()} has already removed the default; this catches a catalog, schema or
     * table option — or an {@code ATTACH} parameter — that overrides it, which is the only way
     * inlining can still apply.
     */
    protected static void validateNoDataInlining(QueueIdToTableMapping mapping) {
        WatermarkSpec spec = WatermarkSpec.fromParameters(mapping.ingestionQueue(), mapping.additionalParameters());
        if (spec == null || mapping.catalog() == null || mapping.schema() == null) {
            return;
        }
        String table = "%s.%s.%s".formatted(mapping.catalog(), mapping.schema(), spec.table());
        DuckLakeInlining.Limit limit;
        try {
            limit = DuckLakeInlining.effectiveRowLimit(mapping.catalog(), mapping.schema(), spec.table());
        } catch (Exception e) {
            throw new IllegalStateException(("Queue '%s': cannot determine the data inlining limit of"
                    + " watermark table '%s': %s").formatted(mapping.ingestionQueue(), table, e.getMessage()), e);
        }
        if (limit.inliningEnabled()) {
            throw new IllegalStateException(("Queue '%s': data inlining is enabled for watermark table '%s'"
                    + " (%d rows, from %s), so its rows would be written into the catalog database instead"
                    + " of storage, on the insertion path of every batch. %s")
                    .formatted(mapping.ingestionQueue(), table, limit.rowLimit(), limit.source(),
                            DuckLakeInlining.remedy(mapping.catalog(), mapping.schema(), spec.table(),
                                    limit.source())));
        }
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
