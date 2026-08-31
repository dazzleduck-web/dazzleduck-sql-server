package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Creates and evolves the DuckLake table backing an ingestion queue so that its columns match the
 * queue's transformation output. Used only when {@code manageTables} is enabled on
 * {@link DynamicIngestionHandler}.
 *
 * <p>The desired column set is derived from the queue's {@code input_schema} and {@code transformation}:
 * a zero-row temp table is created from the {@code input_schema} column-definition fragment (e.g.
 * {@code "severity_number INTEGER, body VARCHAR"}), the transformation is expanded over it exactly as
 * at write time ({@code WITH __this AS (...) <transformation>}, see {@code ParquetIngestionQueue}), and
 * the result is described. This reuses DuckDB's own type resolution, so the derived schema matches what
 * real ingestion produces.
 *
 * <p>Reconciliation is non-destructive by design:
 * <ul>
 *   <li>missing table → {@code CREATE TABLE} with the derived columns, in order;</li>
 *   <li>existing table → {@code ALTER TABLE ADD COLUMN} for each derived column not present (appended
 *       in derived order) and {@code DROP COLUMN} for each existing column no longer derived.</li>
 * </ul>
 * Only columns are added/removed: existing columns are never re-typed or re-ordered (a DuckDB
 * limitation), and tables are never dropped (queue deletion only stops ingestion).
 */
public final class DuckLakeTableManager {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeTableManager.class);

    /** Connection-local name the input relation is created under while deriving the schema. */
    private static final String INPUT_RELATION = "__dd_this_input";
    private static final String DERIVE_VIEW = "__dd_mgmt_derive";

    private DuckLakeTableManager() {}

    /** A target column: its name and DuckDB type (e.g. {@code VARCHAR}, {@code MAP(VARCHAR, VARCHAR)}). */
    record Column(String name, String type) {}

    /**
     * Ensures the DuckLake table for {@code mapping} exists with the columns produced by its
     * transformation over its {@code input_schema}. Failure-isolated: any error is logged and
     * swallowed so a single bad mapping never aborts a registry reload.
     */
    public static void ensureTable(QueueIdToTableMapping mapping) {
        String inputSchema = mapping.inputSchema();
        if (inputSchema == null || inputSchema.isBlank()) {
            logger.warn("manageTables: no input_schema for queue '{}'; skipping table management",
                    mapping.ingestionQueue());
            return;
        }
        try (DuckDBConnection conn = ConnectionPool.getConnection()) {
            List<Column> desired = deriveSchema(conn, mapping, inputSchema);
            if (desired.isEmpty()) {
                logger.warn("manageTables: derived empty schema for queue '{}'; skipping", mapping.ingestionQueue());
                return;
            }
            List<String> current = currentColumnNames(conn, mapping);
            if (current.isEmpty()) {
                createTable(conn, mapping, desired);
            } else {
                alterTable(conn, mapping, desired, current);
            }
        } catch (Exception e) {
            logger.atWarn().setCause(e).log("manageTables: failed to ensure table {}.{}.{} for queue '{}'",
                    mapping.catalog(), mapping.schema(), mapping.table(), mapping.ingestionQueue());
        }
    }

    /**
     * Derives the transformation's output columns by materialising {@code inputSchema} as a zero-row
     * temp table and describing the expanded transformation over it.
     */
    static List<Column> deriveSchema(DuckDBConnection conn, QueueIdToTableMapping mapping, String inputSchema)
            throws SQLException {
        String transformation = effectiveTransformation(mapping);
        String inner = "SELECT * FROM " + INPUT_RELATION;
        String query = (transformation != null && !transformation.isBlank())
                ? "WITH __this AS (%s) %s".formatted(inner, transformation)
                : inner;

        List<Column> columns = new ArrayList<>();
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE OR REPLACE TEMP TABLE " + INPUT_RELATION + " (" + inputSchema + ")");
            try {
                st.execute("CREATE OR REPLACE TEMP VIEW " + DERIVE_VIEW + " AS " + query);
                try (ResultSet rs = st.executeQuery("DESCRIBE " + DERIVE_VIEW)) {
                    while (rs.next()) {
                        columns.add(new Column(rs.getString("column_name"), rs.getString("column_type")));
                    }
                }
            } finally {
                st.execute("DROP VIEW IF EXISTS " + DERIVE_VIEW);
                st.execute("DROP TABLE IF EXISTS " + INPUT_RELATION);
            }
        }
        return columns;
    }

    /** Resolves a view-based transformation to concrete SQL; otherwise returns the explicit transformation. */
    private static String effectiveTransformation(QueueIdToTableMapping mapping) {
        if (mapping.hasViewTransformation()) {
            return DuckLakeIngestionHandler.resolveViewTransformationStatic(mapping.view(), mapping.inputTable());
        }
        return mapping.transformation();
    }

    static List<String> currentColumnNames(DuckDBConnection conn, QueueIdToTableMapping mapping)
            throws SQLException {
        List<String> names = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT column_name FROM information_schema.columns " +
                "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
            ps.setString(1, mapping.catalog());
            ps.setString(2, mapping.schema());
            ps.setString(3, mapping.table());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) names.add(rs.getString(1));
            }
        }
        return names;
    }

    private static void createTable(DuckDBConnection conn, QueueIdToTableMapping mapping,
                                    List<Column> desired) throws SQLException {
        String cols = desired.stream()
                .map(c -> quote(c.name()) + " " + c.type())
                .collect(Collectors.joining(", "));
        String sql = "CREATE TABLE %s (%s)".formatted(qualified(mapping), cols);
        ConnectionPool.executeBatchInTxn(conn, new String[]{sql});
        logger.info("manageTables: created table {} with {} column(s) for queue '{}'",
                qualified(mapping), desired.size(), mapping.ingestionQueue());
    }

    private static void alterTable(DuckDBConnection conn, QueueIdToTableMapping mapping,
                                   List<Column> desired, List<String> current) throws SQLException {
        Set<String> currentLower = current.stream().map(DuckLakeTableManager::lower)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> desiredLower = desired.stream().map(c -> lower(c.name()))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        List<String> statements = new ArrayList<>();
        // Add derived columns missing from the table, in derived order (DuckDB appends).
        for (Column c : desired) {
            if (!currentLower.contains(lower(c.name()))) {
                statements.add("ALTER TABLE %s ADD COLUMN %s %s".formatted(qualified(mapping), quote(c.name()), c.type()));
            }
        }
        // Drop columns no longer produced by the transformation.
        for (String name : current) {
            if (!desiredLower.contains(lower(name))) {
                statements.add("ALTER TABLE %s DROP COLUMN %s".formatted(qualified(mapping), quote(name)));
            }
        }
        if (statements.isEmpty()) {
            logger.debug("manageTables: table {} already matches transformation for queue '{}'",
                    qualified(mapping), mapping.ingestionQueue());
            return;
        }
        ConnectionPool.executeBatchInTxn(conn, statements.toArray(new String[0]));
        logger.info("manageTables: reconciled table {} for queue '{}' ({} DDL change(s))",
                qualified(mapping), mapping.ingestionQueue(), statements.size());
    }

    private static String qualified(QueueIdToTableMapping m) {
        return quote(m.catalog()) + "." + quote(m.schema()) + "." + quote(m.table());
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String lower(String s) {
        return s.toLowerCase(Locale.ROOT);
    }
}
