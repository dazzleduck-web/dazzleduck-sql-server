package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.SqlVariables;
import io.dazzleduck.sql.commons.util.HeaderUtils;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Creates and checks a queue's sidecar tables, and resolves them for writing.
 *
 * <p>At startup each sidecar's query is run over an empty {@code __base} built the way a batch builds
 * it, and described. A missing table is created with those columns; an existing table whose columns
 * differ, in name or type, fails startup naming the table and the column. No DDL runs while a batch
 * is ingested.
 *
 * <p>The empty {@code __base} is the queue's transformation over its {@code input_schema} when one is
 * configured, otherwise the queue's table with no rows, the table being what the transformation's
 * output is registered into; either way with the table's partition projections added.
 */
final class SidecarTables {

    private static final Logger logger = LoggerFactory.getLogger(SidecarTables.class);

    private static final String INPUT_RELATION = "__dd_sidecar_input";
    private static final String DERIVE_VIEW = "__dd_sidecar_derive";

    /**
     * Types, at any depth, that a Parquet file written by DuckDB cannot carry back into a DuckLake
     * column of the same type: Parquet stores HUGEINT and UHUGEINT as DOUBLE, and TIMETZ is not
     * mapped, so {@code ducklake_add_data_files} would reject every batch's file.
     */
    private static final java.util.regex.Pattern UNREGISTRABLE =
            java.util.regex.Pattern.compile("(^|[ (])(U?HUGEINT|TIME WITH TIME ZONE)($|[),\\[])",
                    java.util.regex.Pattern.CASE_INSENSITIVE);

    private SidecarTables() {}

    /**
     * Creates every missing sidecar table of {@code mapping} and checks the existing ones, throwing
     * {@link IllegalArgumentException} for one that cannot be used.
     *
     * @param transformation       the queue's resolved transformation, or {@code null}
     * @param partitionProjections the base table's partition projections, as a batch adds them
     * @return how many sidecar tables this call created, and the columns each sidecar's query
     *         produces (lower case), by table
     */
    static Ensured ensure(QueueIdToTableMapping mapping, Map<String, String> sidecarSql, String transformation,
                          String[] partitionProjections, Map<String, String> variables) {
        if (mapping.sidecars().isEmpty()) {
            return new Ensured(0, Map.of(), Map.of());
        }
        // A statement per step: the driver closes a statement whose execution failed.
        try (DuckDBConnection conn = ConnectionPool.getConnection()) {
            for (String set : SqlVariables.toSetStatements(variables, IngestionVariables.NOUN)) {
                execute(conn, set);
            }
            String relation = emptyBase(conn, mapping, transformation, partitionProjections);
            // A batch's sidecars read its base back from staged Parquet, which changes some types
            // (ENUM to VARCHAR, TIMESTAMP_S to TIMESTAMP, ...): check them over the same round trip.
            Path roundTrip = Files.createTempFile("dd_sidecar_check_", ".parquet");
            int created = 0;
            Map<String, java.util.Set<String>> produced = new LinkedHashMap<>();
            Map<String, List<DuckLakeIngestionHandler.ResolvedPartition>> partitions = new LinkedHashMap<>();
            try {
                execute(conn, "COPY (SELECT * FROM (%s) LIMIT 0) TO '%s' (FORMAT parquet)"
                        .formatted(relation, roundTrip.toString().replace("'", "''")));
                String base = "SELECT * FROM read_parquet('%s', hive_partitioning = false)"
                        .formatted(roundTrip.toString().replace("'", "''"));
                for (Sidecar sidecar : mapping.sidecars()) {
                    List<SidecarTarget.Column> derived = new ArrayList<>();
                    List<DuckLakeIngestionHandler.ResolvedPartition> tablePartitions = new ArrayList<>();
                    if (ensure(conn, mapping, sidecar, sidecarSql.get(sidecar.table()), base, derived, tablePartitions)) {
                        created++;
                    }
                    produced.put(sidecar.table(), derived.stream()
                            .map(c -> c.name().toLowerCase(Locale.ROOT)).collect(Collectors.toSet()));
                    partitions.put(sidecar.table(), List.copyOf(tablePartitions));
                }
                return new Ensured(created, produced, partitions);
            } finally {
                execute(conn, "DROP TABLE IF EXISTS " + INPUT_RELATION);
                Files.deleteIfExists(roundTrip);
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        } catch (SQLException e) {
            throw new IllegalArgumentException("Queue '%s': could not check its sidecar tables: %s"
                    .formatted(mapping.ingestionQueue(), e.getMessage()), e);
        }
    }

    /**
     * A sidecar query that does not fit its table: a column it lacks or types differently, a type
     * DuckLake cannot register, a partitioning it cannot be written with, or a query that does not run
     * over the queue's rows. Unlike a failure to check at all (a metadata read error), a changed view
     * that throws it is rejected: its sidecar keeps its previous query.
     */
    static final class SidecarMismatchException extends IllegalArgumentException {
        SidecarMismatchException(String message) {
            super(message);
        }

        SidecarMismatchException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * What {@link #ensure} found: tables it created, and for each sidecar table the columns its query
     * produces and the table's partitioning.
     */
    record Ensured(int created, Map<String, java.util.Set<String>> produced,
                   Map<String, List<DuckLakeIngestionHandler.ResolvedPartition>> partitions) {}

    /**
     * Creates or checks one sidecar's table; returns whether this call created it, and adds the
     * query's columns to {@code derivedOut}.
     */
    private static boolean ensure(DuckDBConnection conn, QueueIdToTableMapping mapping, Sidecar sidecar, String sql,
                                  String base, List<SidecarTarget.Column> derivedOut,
                                  List<DuckLakeIngestionHandler.ResolvedPartition> partitionsOut) throws SQLException {
        boolean created = false;
        List<SidecarTarget.Column> derived;
        try {
            derived = describe(conn, writeQuery(base, sql));
            derivedOut.addAll(derived);
        } catch (SQLException e) {
            throw new SidecarMismatchException("Queue '%s': sidecar '%s' transformation does not run over the queue's rows: %s"
                    .formatted(mapping.ingestionQueue(), sidecar.table(), e.getMessage()), e);
        }
        for (SidecarTarget.Column column : derived) {
            if (UNREGISTRABLE.matcher(column.type()).find()) {
                throw new SidecarMismatchException(("Queue '%s': sidecar '%s' column '%s' is %s, which DuckLake cannot "
                        + "register from a Parquet file; cast it in the transformation, for example a sum to BIGINT "
                        + "or DECIMAL(38,0)").formatted(mapping.ingestionQueue(), sidecar.table(), column.name(), column.type()));
            }
        }
        List<SidecarTarget.Column> existing = tableColumns(conn, mapping.catalog(), mapping.schema(), sidecar.table());
        if (existing.isEmpty()) {
            // IF NOT EXISTS: another process starting with the same configuration may create it
            // first, in which case its columns are checked below like any existing table's.
            String columns = derived.stream()
                    .map(c -> HeaderUtils.quoteIdentifier(c.name()) + " " + c.type())
                    .collect(Collectors.joining(", "));
            String sameNameInOtherCase = tableNamedInAnyCase(conn, mapping, sidecar.table());
            if (sameNameInOtherCase != null) {
                throw new IllegalArgumentException("Queue '%s': sidecar table '%s' exists as '%s'; configure the name as it is in the catalog"
                        .formatted(mapping.ingestionQueue(), sidecar.table(), sameNameInOtherCase));
            }
            try {
                ConnectionPool.executeBatchInTxn(conn, new String[]{
                        "CREATE TABLE IF NOT EXISTS %s (%s)".formatted(qualified(mapping, sidecar.table()), columns)});
                logger.info("Queue '{}': created sidecar table {} with {} column(s)",
                        mapping.ingestionQueue(), qualified(mapping, sidecar.table()), derived.size());
                created = true;
            } catch (SQLException e) {
                // DuckLake checks IF NOT EXISTS against this transaction's snapshot, so a process
                // that created the table meanwhile makes this commit conflict. Then it exists.
                if (tableColumns(conn, mapping.catalog(), mapping.schema(), sidecar.table()).isEmpty()) {
                    throw e;
                }
                logger.info("Queue '{}': sidecar table {} was created by another process",
                        mapping.ingestionQueue(), qualified(mapping, sidecar.table()));
            }
            existing = tableColumns(conn, mapping.catalog(), mapping.schema(), sidecar.table());
        }
        String mismatch = mismatch(derived, existing);
        if (mismatch != null) {
            throw new SidecarMismatchException("Queue '%s': sidecar table %s does not match its transformation: %s"
                    .formatted(mapping.ingestionQueue(), qualified(mapping, sidecar.table()), mismatch));
        }
        checkPartitioning(conn, mapping, sidecar, sql, base, derived, partitionsOut);
        return created;
    }

    /**
     * Reads the sidecar table's partitioning into {@code partitionsOut} and checks that each sidecar
     * file can be written with it: an identity partition column must be one the query produces (a
     * NULL for every row would be one partition holding everything), and a transform must bind over
     * the query's columns. An unsupported transform fails like the main table's.
     */
    private static void checkPartitioning(DuckDBConnection conn, QueueIdToTableMapping mapping, Sidecar sidecar,
                                          String sql, String base, List<SidecarTarget.Column> derived,
                                          List<DuckLakeIngestionHandler.ResolvedPartition> partitionsOut) throws SQLException {
        List<DuckLakeIngestionHandler.ResolvedPartition> partitions;
        try {
            partitions = DuckLakeIngestionHandler.fetchPartitionsStrict(mapping.catalog(), mapping.schema(), sidecar.table());
        } catch (UnsupportedPartitionTransformException e) {
            throw new SidecarMismatchException("Queue '%s': sidecar table %s: %s"
                    .formatted(mapping.ingestionQueue(), qualified(mapping, sidecar.table()), e.getMessage()), e);
        }
        java.util.Set<String> produced = derived.stream().map(c -> c.name().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        List<String> projections = new ArrayList<>();
        for (var partition : partitions) {
            if (partition.projection() == null) {
                if (!produced.contains(partition.token().toLowerCase(Locale.ROOT))) {
                    throw new SidecarMismatchException(("Queue '%s': sidecar table %s is partitioned by '%s', which "
                            + "its transformation does not produce").formatted(mapping.ingestionQueue(),
                            qualified(mapping, sidecar.table()), partition.token()));
                }
            } else {
                projections.add(partition.projection());
            }
        }
        if (!projections.isEmpty()) {
            try {
                describe(conn, ParquetIngestionQueue.withProjections(writeQuery(base, sql), projections));
            } catch (SQLException e) {
                throw new SidecarMismatchException(("Queue '%s': sidecar table %s is partitioned by a column its "
                        + "transformation does not produce: %s").formatted(mapping.ingestionQueue(),
                        qualified(mapping, sidecar.table()), e.getMessage()), e);
            }
        }
        partitionsOut.addAll(partitions);
    }

    /** Resolves {@code sidecar} for writing: its query, its table's data path, columns and partitioning. */
    static SidecarTarget resolve(QueueIdToTableMapping mapping, Sidecar sidecar, String sql, String path,
                                 java.util.Set<String> produced, List<DuckLakeIngestionHandler.ResolvedPartition> partitions) {
        try (DuckDBConnection conn = ConnectionPool.getConnection()) {
            List<SidecarTarget.Column> columns = tableColumns(conn, mapping.catalog(), mapping.schema(), sidecar.table());
            if (columns.isEmpty() || path == null) {
                throw new IllegalStateException("Queue '%s': sidecar table %s does not exist"
                        .formatted(mapping.ingestionQueue(), qualified(mapping, sidecar.table())));
            }
            createLocalDirectory(path);
            return new SidecarTarget(sidecar.table(), sql, path, columns, produced,
                    partitions.stream().map(DuckLakeIngestionHandler.ResolvedPartition::token).toList(),
                    partitions.stream().map(DuckLakeIngestionHandler.ResolvedPartition::projection)
                            .filter(java.util.Objects::nonNull).toList());
        } catch (SQLException e) {
            throw new IllegalStateException("Queue '%s': could not read sidecar table %s: %s"
                    .formatted(mapping.ingestionQueue(), qualified(mapping, sidecar.table()), e.getMessage()), e);
        }
    }

    /**
     * DuckLake creates a table's directory when it first writes there; a sidecar file is written
     * there by a plain COPY, which does not, so a local one is created up front. Object stores have
     * no directories.
     */
    private static void createLocalDirectory(String path) {
        String local = ParquetIngestionQueue.localPath(path);
        if (local == null) {
            return;
        }
        try {
            java.nio.file.Files.createDirectories(java.nio.file.Path.of(local));
        } catch (java.io.IOException e) {
            throw new IllegalStateException("could not create the sidecar directory " + path + ": " + e.getMessage(), e);
        }
    }

    /**
     * The first difference between a sidecar query's columns and its table's, by name (in any case)
     * and type; {@code null} when they match. Column order does not matter: files are written with
     * the table's columns, by name. A table column the query does not produce is not a difference:
     * it is written as NULL, so a column can be added to the table before the query produces it.
     */
    static String mismatch(List<SidecarTarget.Column> derived, List<SidecarTarget.Column> table) {
        Map<String, SidecarTarget.Column> byName = new LinkedHashMap<>();
        table.forEach(c -> byName.put(c.name().toLowerCase(Locale.ROOT), c));
        for (SidecarTarget.Column column : derived) {
            SidecarTarget.Column existing = byName.remove(column.name().toLowerCase(Locale.ROOT));
            if (existing == null) {
                return "column '%s' %s is not in the table".formatted(column.name(), column.type());
            }
            if (!existing.type().equalsIgnoreCase(column.type())) {
                return "column '%s' is %s in the table but %s in the transformation"
                        .formatted(existing.name(), existing.type(), column.type());
            }
        }
        return null;
    }

    /**
     * The statement every sidecar file is written from: the sidecar's query over {@code base}, its
     * columns selected by name in table order and cast to the table's types. With {@code columns}
     * null, the query's own columns.
     */
    static String writeQuery(String base, String transformation, List<SidecarTarget.Column> columns,
                             java.util.Set<String> produced) {
        String select = columns == null ? "*" : columns.stream()
                .map(c -> produced.contains(c.name().toLowerCase(Locale.ROOT))
                        ? "CAST(%1$s AS %2$s) AS %1$s".formatted(HeaderUtils.quoteIdentifier(c.name()), c.type())
                        : "CAST(NULL AS %2$s) AS %1$s".formatted(HeaderUtils.quoteIdentifier(c.name()), c.type()))
                .collect(Collectors.joining(", "));
        return "WITH %s AS (%s) SELECT %s FROM (%s\n)".formatted(Sidecar.BASE, base, select,
                ParquetIngestionQueue.embeddable(transformation));
    }

    private static String writeQuery(String base, String transformation) {
        return writeQuery(base, transformation, null, null);
    }

    private static String emptyBase(DuckDBConnection conn, QueueIdToTableMapping mapping, String transformation,
                                    String[] partitionProjections) throws SQLException {
        String base;
        if (mapping.inputSchema() != null && !mapping.inputSchema().isBlank()) {
            execute(conn, "CREATE OR REPLACE TEMP TABLE %s (%s)".formatted(INPUT_RELATION, mapping.inputSchema()));
            base = ParquetIngestionQueue.applyTransformation("SELECT * FROM " + INPUT_RELATION, transformation);
        } else {
            base = "SELECT * FROM %s LIMIT 0".formatted(qualified(mapping, mapping.table()));
        }
        return ParquetIngestionQueue.withProjections(base,
                partitionProjections == null ? List.of() : List.of(partitionProjections));
    }

    private static void execute(DuckDBConnection conn, String sql) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.execute(sql);
        }
    }

    private static List<SidecarTarget.Column> describe(DuckDBConnection conn, String query) throws SQLException {
        execute(conn, "CREATE OR REPLACE TEMP VIEW %s AS %s".formatted(DERIVE_VIEW, query));
        try (PreparedStatement ps = conn.prepareStatement("SELECT column_name, data_type FROM duckdb_columns() "
                + "WHERE database_name = 'temp' AND table_name = ? ORDER BY column_index")) {
            ps.setString(1, DERIVE_VIEW);
            return columns(ps);
        } finally {
            execute(conn, "DROP VIEW IF EXISTS " + DERIVE_VIEW);
        }
    }

    static List<SidecarTarget.Column> tableColumns(DuckDBConnection conn, String catalog, String schema, String table)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT column_name, data_type FROM duckdb_columns() "
                + "WHERE database_name = ? AND schema_name = ? AND table_name = ? ORDER BY column_index")) {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
            return columns(ps);
        }
    }

    private static List<SidecarTarget.Column> columns(PreparedStatement ps) throws SQLException {
        List<SidecarTarget.Column> columns = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                columns.add(new SidecarTarget.Column(rs.getString(1), Objects.requireNonNull(rs.getString(2))));
            }
        }
        return columns;
    }

    /** The name of a table in the queue's schema named {@code table} in another case, or {@code null}. */
    private static String tableNamedInAnyCase(DuckDBConnection conn, QueueIdToTableMapping mapping, String table)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT table_name FROM duckdb_tables() WHERE database_name = ? "
                + "AND schema_name = ? AND lower(table_name) = lower(?) AND table_name <> ? LIMIT 1")) {
            ps.setString(1, mapping.catalog());
            ps.setString(2, mapping.schema());
            ps.setString(3, table);
            ps.setString(4, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private static String qualified(QueueIdToTableMapping mapping, String table) {
        return HeaderUtils.quoteIdentifier(mapping.catalog()) + "." + HeaderUtils.quoteIdentifier(mapping.schema())
                + "." + HeaderUtils.quoteIdentifier(table);
    }
}
