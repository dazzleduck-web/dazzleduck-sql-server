package io.dazzleduck.sql.commons.ingestion;

import java.util.List;

/**
 * Thrown by a {@link PostIngestionTask} when it rejected a write's output files <em>before</em>
 * committing anything — for example {@code ducklake_add_data_files} refusing a file whose layout no
 * longer matches the table's partitioning. The files are then referenced by nothing, so
 * {@link ParquetIngestionQueue} may delete them.
 *
 * <p>A failure whose outcome is unknown (such as an error from the commit itself) must not be
 * reported with this type: deleting files that did get registered would break the table.
 */
public class FilesNotRegisteredException extends RuntimeException {

    private final List<String> files;

    public FilesNotRegisteredException(String message, List<String> files, Throwable cause) {
        super(message, cause);
        this.files = List.copyOf(files);
    }

    /** The output files that were not registered and are safe to delete. */
    public List<String> files() {
        return files;
    }
}
