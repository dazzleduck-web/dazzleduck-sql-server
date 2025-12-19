package io.dazzleduck.sql.commons.delta;

import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;


/**
 * Utility class for efficient file pruning in Delta tables using partition filters.
 * Provides mechanisms to filter Delta table files based on their partition values.
 */
public class PartitionPruning {
    private static final Logger logger = LoggerFactory.getLogger(PartitionPruning.class);

    /**
     * Prunes files in a Delta table based on the provided filter and partition data types.
     *
     * @param basePath          the base path of the Delta table
     * @param filter            the filter to apply to the files
     * @param partitionDataTypes the data types of the partition columns
     * @return a list of FileStatus objects representing the pruned files
     * @throws SQLException if an error occurs while parsing the filter
     * @throws IOException  if an error occurs while reading the Delta table
     */
    public static List<io.dazzleduck.sql.commons.FileStatus> pruneFiles(String basePath,
                                                                        String filter,
                                                                        String[][] partitionDataTypes) throws SQLException, IOException {
        // If no partition data types are provided or the filter is empty, prune files without partition filtering
        if (filter == null || filter.isBlank()) {
            return getAllFilesFromDeltaTable(basePath);
        }
        // Parse the filter into a Delta predicate
        JsonNode tree = io.dazzleduck.sql.commons.Transformations.parseToTree(String.format("SELECT * FROM T WHERE %s", filter));
        var statement = io.dazzleduck.sql.commons.Transformations.getFirstStatementNode(tree);
        var where = io.dazzleduck.sql.commons.Transformations.getWhereClauseForTableFunction(statement);
        return pruneFiles(basePath, where);
    }

    public static List<io.dazzleduck.sql.commons.FileStatus> pruneFiles(String basePath,
                                                                        JsonNode whereClause) throws SQLException, IOException {

        // If the where clause is empty, prune files without partition filtering
        if (whereClause.isEmpty()) {
            return getAllFilesFromDeltaTable(basePath);
        }

        // Create a new Engine instance with the provided Configuration
        Engine engine = DefaultEngine.create(new Configuration());

        // Get the Delta table and its latest snapshot
        Table deltaTable = Table.forPath(engine, basePath);
        Snapshot snapshot = deltaTable.getLatestSnapshot(engine);
        // Convert the where clause to a Delta predicate
        Predicate deltaLakePredicate = (Predicate) Transformations.toDeltaPredicate(whereClause);
        Scan filterScan = snapshot.getScanBuilder(engine)
                .withFilter(engine, deltaLakePredicate)
                .build();

        // Process the scan to collect matching files
        List<io.dazzleduck.sql.commons.FileStatus> result = new ArrayList<>();
        try (CloseableIterator<FilteredColumnarBatch> fileIter = filterScan.getScanFiles(engine)) {
            while (fileIter.hasNext()) {
                FilteredColumnarBatch batch = fileIter.next();
                try (CloseableIterator<Row> rowIter = batch.getRows()) {
                    while (rowIter.hasNext()) {
                        Row row = rowIter.next();
                        FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(row);
                        // Create a new FileStatus object with the file path, size, and modification time
                        result.add(new io.dazzleduck.sql.commons.FileStatus(
                                fileStatus.getPath().replaceFirst("^file:", ""),
                                fileStatus.getSize(),
                                fileStatus.getModificationTime())
                        );
                    }
                }
            }
            result.sort(Comparator.comparing(io.dazzleduck.sql.commons.FileStatus::lastModified));
            return result;
        }
    }

    /**
     * List all files in a Delta table
     *
     * @param basePath the base path of the Delta table
     * @return a list of FileStatus objects representing the pruned files
     */
    private static List<io.dazzleduck.sql.commons.FileStatus> getAllFilesFromDeltaTable(String basePath) {
        // Create a new Engine instance with the provided Configuration
        Engine engine = DefaultEngine.create(new Configuration());

        // Get the Delta table and its latest snapshot
        Table deltaTable = Table.forPath(engine, basePath);
        Snapshot snapshot = deltaTable.getLatestSnapshot(engine);

        // Create a new Scan instance without any filters
        Scan scan = snapshot.getScanBuilder(engine).build();

        // Initialize an empty list to store the pruned files
        List<io.dazzleduck.sql.commons.FileStatus> result = new ArrayList<>();

        try (CloseableIterator<FilteredColumnarBatch> fileIter = scan.getScanFiles(engine)) {
            // Iterate over the files in the scan
            while (fileIter.hasNext()) {
                FilteredColumnarBatch batch = fileIter.next();
                try (CloseableIterator<Row> rowIter = batch.getRows()) {
                    // Iterate over the rows in the batch
                    while (rowIter.hasNext()) {
                        Row row = rowIter.next();
                        // Get the FileStatus object from the row
                        FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(row);
                        // Create a new FileStatus object with the file path, size, and modification time
                        result.add(new io.dazzleduck.sql.commons.FileStatus(
                                fileStatus.getPath().replaceFirst("^file:", ""),
                                fileStatus.getSize(),
                                fileStatus.getModificationTime())
                        );
                    }
                }
            }
        } catch (IOException e) {
            // Log any errors that occur during file pruning
            logger.error("Error processing scan files: {}", e.getMessage());
        }
        return result;
    }
}