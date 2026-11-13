package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.SqlVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class ParquetIngestionQueue extends BulkIngestQueue<String, IngestionResult> {

    private static final Logger logger = LoggerFactory.getLogger(ParquetIngestionQueue.class);

    /**
     * Virtual thread executor for async file cleanup.
     * Virtual threads are ideal for I/O-bound tasks like file deletion.
     * This is a shared executor - virtual threads are lightweight so no pooling needed.
     */
    private static final ExecutorService CLEANUP_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * Runs sidecar COPYs beside the base COPY, which runs on the queue's writer thread. Platform
     * threads, not virtual: a COPY is one long native call into DuckDB, which would pin a virtual
     * thread's carrier for its whole length and starve the server's other virtual threads.
     */
    /**
     * Uploads staged files. Its own pool, so a batch with many partition files does not hold up other
     * queues' sidecar COPYs; platform threads for the same reason as the COPYs.
     */
    private static final ExecutorService UPLOAD_EXECUTOR = Executors.newFixedThreadPool(16, runnable -> {
        Thread thread = new Thread(runnable, "staged-upload");
        thread.setDaemon(true);
        return thread;
    });

    private static final ExecutorService SIDECAR_COPY_EXECUTOR = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors()), runnable -> {
                Thread thread = new Thread(runnable, "sidecar-copy");
                thread.setDaemon(true);
                return thread;
            });

    private final String outputPath;
    private final String queueId;
    private final IngestionHandler postIngestionHandler;
    private final String applicationId;
    private final String inputFormat;
    private final String parquetCompression;

    /**
     * Per-phase commit timings. The write is two phases with different parallelism potential:
     * the data phase (DuckDB COPY to Parquet — internally multi-threaded, and overlappable
     * across commit lanes) and the post-ingestion phase (e.g. the DuckLake catalog commit —
     * single-writer, serialized no matter how many lanes exist). Their ratio bounds what
     * parallelizing the commit path can gain (Amdahl: max speedup = (data + post) / post),
     * so both are tracked separately and exposed as metrics.
     */
    private final java.util.concurrent.atomic.LongAccumulator dataPhaseNanos =
            new java.util.concurrent.atomic.LongAccumulator(Long::sum, 0L);
    private final java.util.concurrent.atomic.LongAccumulator postIngestPhaseNanos =
            new java.util.concurrent.atomic.LongAccumulator(Long::sum, 0L);

    /** Cumulative rows written to output (from each {@link IngestionResult#rowCount()}). */
    private final java.util.concurrent.atomic.LongAccumulator rowsWritten =
            new java.util.concurrent.atomic.LongAccumulator(Long::sum, 0L);

    /** Catalog-change retries and their failures; output files of failed attempts deleted / left behind. */
    private final java.util.concurrent.atomic.LongAdder catalogRetries = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder catalogRetryFailures = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder orphanFilesDeleted = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder orphanFilesLeft = new java.util.concurrent.atomic.LongAdder();

    /**
     * What a write is built from that can change under a running queue: the transformation and the
     * partitioning (from the batch header, or the handler's partition columns and projections).
     * Captured once per attempt, so the attempt is built from one consistent view and a retry can
     * tell whether the catalog changed since.
     */
    private record WriteInputs(String transformation, List<String> partitionBy, List<String> partitionProjections,
                               List<SidecarTarget> sidecars, List<String> sidecarProjections) {}

    /** What one COPY returned: its row count and the files it wrote. */
    private record CopyOutput(long rows, List<String> files) {}

    /** A write whose files were written but whose post-ingestion step (registration) failed. */
    private static final class RegistrationFailedException extends RuntimeException {
        private final List<String> files;

        RegistrationFailedException(List<String> files, RuntimeException cause) {
            super(cause.getMessage(), cause);
            this.files = files;
        }
    }

    /**
     * @param applicationId    producer identifier
     * @param inputFormat      source file format (e.g. {@code "parquet"}, {@code "arrow"})
     * @param outputPath       destination path for written Parquet files
     * @param ingestionQueue   queue identifier — passed to {@link IngestionHandler#getTransformation}
     *                         on every write so the transformation is always current
     * @param minBucketSize    flush when accumulated size reaches this threshold (bytes)
     * @param maxBucketSize    hard upper limit before forced flush (bytes)
     * @param maxBatches       max number of batches before forced flush
     * @param maxPendingWrite  backpressure limit (bytes)
     * @param maxDelay         time-based flush interval
     * @param postIngestionHandler handler that provides transformation SQL and post-write tasks
     * @param executorService  scheduler for time-based flush
     * @param clock            clock for scheduling
     */
    public ParquetIngestionQueue(String applicationId,
                                 String inputFormat,
                                 String outputPath,
                                 String ingestionQueue,
                                 long minBucketSize,
                                 long maxBucketSize,
                                 int maxBatches,
                                 long maxPendingWrite,
                                 Duration maxDelay,
                                 IngestionHandler postIngestionHandler,
                                 ScheduledExecutorService executorService,
                                 Clock clock) {
        this(applicationId, inputFormat, outputPath, ingestionQueue, minBucketSize, maxBucketSize,
                maxBatches, maxPendingWrite, maxDelay, null, postIngestionHandler, executorService, clock);
    }

    /**
     * @param parquetCompression codec for written Parquet files, or {@code null} for DuckDB's
     *                           default; see {@link IngestionConfig#parquetCompression()}
     */
    public ParquetIngestionQueue(String applicationId,
                                 String inputFormat,
                                 String outputPath,
                                 String ingestionQueue,
                                 long minBucketSize,
                                 long maxBucketSize,
                                 int maxBatches,
                                 long maxPendingWrite,
                                 Duration maxDelay,
                                 String parquetCompression,
                                 IngestionHandler postIngestionHandler,
                                 ScheduledExecutorService executorService,
                                 Clock clock) {
        super(ingestionQueue, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, executorService, clock);
        this.outputPath = outputPath;
        this.queueId = ingestionQueue;
        this.postIngestionHandler = postIngestionHandler;
        this.applicationId = applicationId;
        this.inputFormat = inputFormat;
        this.parquetCompression = parquetCompression;
        // The output path (local or object store) is expected to already exist — provisioning it is
        // the operator's responsibility, outside the scope of this project. We never create it here.
    }

    @Override
    public void write(WriteTask<String, IngestionResult> writeTask) {
        logger.debug("Ingestion queue '{}' received batch with {} files, outputPath={}",
                queueId, writeTask.bucket().batches().size(), outputPath);
        // The COPY of the most recent attempt, for the failure log: rebuilding it there would read the
        // handler's state again, which after a catalog refresh is not what the failed attempt ran.
        var lastAttemptSql = new java.util.concurrent.atomic.AtomicReference<String>();
        try {
            IngestionResult ingestionResult = writeWithCatalogRetry(writeTask, lastAttemptSql);
            writeTask.bucket().futures().forEach(action -> action.complete(ingestionResult));
        } catch (Exception e) {
            logger.atError().setCause(e).log("Failed to write to queue {} sql {}", queueId,
                    lastAttemptSql.get() == null ? "(not built)" : lastAttemptSql.get());
            // Propagate instead of completing the futures here: BulkIngestQueue.processWriteQueue
            // must account this bucket as failed (pendingWrite stays truthful, failed-write
            // metrics accumulate) and roll back producer sequences BEFORE the futures complete,
            // so a client observing the failure can immediately retry the same batch. Swallowing
            // the exception would make the failed bytes count as written and leave retries
            // rejected as OutOfSequenceBatch.
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(e);
        } finally {
            cleanupInputFiles(writeTask);
        }
    }

    /**
     * Writes and registers the batch, retrying once if the failure was caused by a catalog change.
     *
     * <p>The handler caches catalog-derived state (partitioning, transformation) and normally
     * re-reads it only every refresh interval. A batch written under a stale partition spec is
     * rejected at registration, and without this the client's resend would be written with the same
     * stale spec and rejected again until the cache expired. So after any failure the state is
     * re-read immediately ({@link IngestionHandler#refreshNow}); if the inputs this batch was built
     * from changed, the batch is written again from its input files (still present: they are only
     * deleted after this method returns) and registered. The client sees no failure. If nothing
     * changed, or the retry fails too, the original failure propagates as before.
     *
     * <p>A queue whose cached state is unusable (unsupported partitioning, a view that does not
     * resolve) is re-read once before the batch is rejected, so fixing the cause takes effect on the
     * next batch rather than after the refresh interval.
     *
     * <p>Output files of a failed attempt are deleted when the post-ingestion step reports that it
     * rejected them before committing ({@link FilesNotRegisteredException}); otherwise they are
     * counted as orphans, for {@code ducklake_delete_orphaned_files} to reclaim.
     */
    private IngestionResult writeWithCatalogRetry(WriteTask<String, IngestionResult> writeTask,
                                                  java.util.concurrent.atomic.AtomicReference<String> lastAttemptSql)
            throws Exception {
        WriteInputs used;
        try {
            used = currentWriteInputs(writeTask);
        } catch (UnsupportedPartitionTransformException | TransformationUnavailableException unusable) {
            // The cached state says this queue cannot be written. Its cause may have been fixed since
            // it was cached (partitioning changed, view recreated), so re-read the catalog once rather
            // than reject every batch until the refresh interval expires.
            try {
                postIngestionHandler.refreshNow(queueId);
            } catch (RuntimeException refreshFailure) {
                unusable.addSuppressed(refreshFailure);
                throw unusable;
            }
            used = currentWriteInputs(writeTask); // still unusable: throws the current reason
        }
        try {
            return writeAndRegister(writeTask, used, lastAttemptSql);
        } catch (Exception first) {
            discardOutput(first);
            if (writeTask.isCancel()) {
                throw unwrap(first);
            }
            WriteInputs current;
            try {
                postIngestionHandler.refreshNow(queueId);
                current = currentWriteInputs(writeTask);
            } catch (Exception refreshFailure) {
                Exception original = unwrap(first);
                original.addSuppressed(refreshFailure);
                throw original;
            }
            if (current.equals(used)) {
                throw unwrap(first);
            }
            if (!nothingRegistered(first)) {
                // The failed attempt may be in the table (e.g. its commit failed after landing), so
                // writing the batch again could register every row twice. Fail it as before; the
                // refresh above still applies to the next batch.
                logger.atWarn().setCause(unwrap(first)).log(
                        "Queue '{}': write failed and the catalog has changed, but the failed attempt's "
                                + "registration outcome is unknown; not retrying, to avoid registering the batch twice",
                        queueId);
                throw unwrap(first);
            }
            catalogRetries.increment();
            logger.atInfo().setCause(unwrap(first)).log(
                    "Queue '{}': write failed and the catalog has changed since it was built "
                            + "(partitioning {} -> {}, transformation changed: {}); retrying once",
                    queueId, used.partitionBy(), current.partitionBy(),
                    !Objects.equals(used.transformation(), current.transformation()));
            try {
                return writeAndRegister(writeTask, current, lastAttemptSql);
            } catch (Exception second) {
                discardOutput(second);
                catalogRetryFailures.increment();
                Exception failure = unwrap(second);
                failure.addSuppressed(unwrap(first));
                throw failure;
            }
        }
    }

    /** One attempt: COPY the batch to output files, then run the post-ingestion step (registration). */
    private IngestionResult writeAndRegister(WriteTask<String, IngestionResult> writeTask, WriteInputs inputs,
                                             java.util.concurrent.atomic.AtomicReference<String> lastAttemptSql)
            throws Exception {
        long start = System.nanoTime();
        IngestionResult ingestionResult = tryWrite(writeTask, inputs, lastAttemptSql);
        long copyDone = System.nanoTime();
        try {
            postIngestionHandler.createPostIngestionTask(ingestionResult).execute();
        } catch (RuntimeException e) {
            throw new RegistrationFailedException(ingestionResult.allFiles(), e);
        }
        // The batch is registered. Nothing below may throw: a failure here would not be a
        // RegistrationFailedException, so nothingRegistered() would read it as "failed before
        // registration" and the catalog-change retry could register the batch a second time.
        long postIngestDone = System.nanoTime();
        dataPhaseNanos.accumulate(copyDone - start);
        postIngestPhaseNanos.accumulate(postIngestDone - copyDone);
        rowsWritten.accumulate(ingestionResult.rowCount());
        recordRowsWritten(ingestionResult.rowCount());
        logger.debug("Queue '{}' commit phases: data(COPY)={}ms, postIngest(catalog)={}ms",
                queueId, (copyDone - start) / 1_000_000, (postIngestDone - copyDone) / 1_000_000);
        return ingestionResult;
    }

    /**
     * Deals with the output files of a failed attempt: deletes them when the post-ingestion step
     * says it rejected them before committing, otherwise counts them as orphans. A failure before
     * any file was reported (e.g. the COPY itself) has nothing to clean up here.
     */
    private void discardOutput(Exception failure) {
        if (!(failure instanceof RegistrationFailedException registration)) {
            return;
        }
        List<String> files = registration.files == null ? List.of() : registration.files;
        if (files.isEmpty()) {
            return;
        }
        if (!(registration.getCause() instanceof FilesNotRegisteredException)) {
            // The registration outcome is unknown (e.g. the commit failed): the files may be live.
            orphanFilesLeft.add(files.size());
            logger.warn("Queue '{}': left {} output file(s) of a failed write in place because their "
                    + "registration outcome is unknown: {}", queueId, files.size(), files);
            return;
        }
        discardWritten(files, "registration rejected the batch's files");
    }

    /**
     * Deletes {@code file} if it is on the local filesystem (a plain path or a {@code file:} URI).
     * Returns {@code false} for object-store paths ({@code s3://} and the like), which this class
     * has no client for, and for local files that could not be deleted.
     */
    static boolean deleteLocalFile(String file) {
        String path = localPath(file);
        if (path == null) {
            return false;
        }
        try {
            Files.deleteIfExists(Path.of(path));
            return true;
        } catch (Exception e) {
            logger.warn("Failed to delete output file of a failed write: {}", file, e);
            return false;
        }
    }

    /**
     * {@code file} as a local filesystem path (from a plain path or a {@code file:} URI), or
     * {@code null} for an object-store path ({@code s3://} and the like).
     */
    static String localPath(String file) {
        if (file.startsWith("file://")) {
            return file.substring("file://".length());
        }
        if (file.startsWith("file:")) {
            return file.substring("file:".length());
        }
        return file.contains("://") ? null : file;
    }

    /**
     * Whether a failed attempt is certainly not in the table, so the batch may be written again: it
     * failed before registration (e.g. the COPY), or registration rejected its files before
     * committing ({@link FilesNotRegisteredException}). Any other registration failure — a failed
     * commit above all — may have landed.
     *
     * <p>"Not a {@link RegistrationFailedException}" is read as "failed before registration", which
     * holds only because nothing in {@link #writeAndRegister} after the post-ingestion step can
     * throw.
     */
    static boolean nothingRegistered(Exception failure) {
        return !(failure instanceof RegistrationFailedException)
                || failure.getCause() instanceof FilesNotRegisteredException;
    }

    /** The underlying failure of an attempt, without this class's registration wrapper. */
    private static Exception unwrap(Exception e) {
        return e instanceof RegistrationFailedException && e.getCause() instanceof Exception cause ? cause : e;
    }

    /** Writes retried once after a failure revealed a catalog change. */
    public long getCatalogRetries() {
        return catalogRetries.sum();
    }

    /** Catalog-change retries that failed again. */
    public long getCatalogRetryFailures() {
        return catalogRetryFailures.sum();
    }

    /** Output files of failed attempts that were deleted. */
    public long getOrphanFilesDeleted() {
        return orphanFilesDeleted.sum();
    }

    /** Output files of failed attempts left in place (object store, or registration outcome unknown). */
    public long getOrphanFilesLeft() {
        return orphanFilesLeft.sum();
    }

    @Override
    protected long catalogRetries() {
        return getCatalogRetries();
    }

    @Override
    protected long catalogRetryFailures() {
        return getCatalogRetryFailures();
    }

    @Override
    protected long orphanFilesDeleted() {
        return getOrphanFilesDeleted();
    }

    @Override
    protected long orphanFilesLeft() {
        return getOrphanFilesLeft();
    }

    /** Cumulative nanoseconds spent in the data phase (DuckDB COPY to Parquet). */
    public long getDataPhaseNanos() {
        return dataPhaseNanos.get();
    }

    /** Cumulative nanoseconds spent in the post-ingestion phase (e.g. DuckLake catalog commit). */
    public long getPostIngestPhaseNanos() {
        return postIngestPhaseNanos.get();
    }

    /** Cumulative rows written to output. */
    public long getRowsWritten() {
        return rowsWritten.get();
    }

    @Override
    protected long rowsWritten() {
        return rowsWritten.get();
    }

    @Override
    protected long dataPhaseMillis() {
        return getDataPhaseNanos() / 1_000_000;
    }

    @Override
    protected long postIngestMillis() {
        return getPostIngestPhaseNanos() / 1_000_000;
    }

    /**
     * Asynchronously cleans up input files using virtual threads.
     * This is fire-and-forget - we don't wait for deletion to complete
     * since it doesn't affect the write result.
     */
    private void cleanupInputFiles(WriteTask<String, IngestionResult> writeTask) {
        writeTask.bucket().batches().forEach(this::onBatchAbandoned);
    }

    @Override
    protected void onBatchAbandoned(Batch<String> batch) {
        final String filePath = batch.record();
        CLEANUP_EXECUTOR.execute(() -> {
            try {
                Files.deleteIfExists(Path.of(filePath));
            } catch (Exception e) {
                logger.warn("Failed to delete temporary input file: {}", filePath, e);
            }
        });
    }

    private String getClause(String[] values, String clause){
        if(values == null || values.length == 0){
            return "";
        } else {
            var nested = Arrays.stream(values).filter(Objects::nonNull).map(String::trim).collect(Collectors.joining(","));
            return clause.formatted(nested);
        }
    }

    /**
     * Exposes {@code inputSql} as {@code __this} and applies {@code transformation} over it.
     * The transformation is nested as a subquery rather than appended after the {@code __this}
     * CTE, so a transformation that declares its own {@code WITH} clause stays valid SQL; its
     * CTEs can still reference the outer {@code __this}. Returns {@code inputSql} unchanged when
     * there is no transformation.
     *
     * <p>The closing parenthesis goes on its own line so a trailing {@code --} comment cannot
     * swallow it; see {@link #embeddable} for semicolons and comments.
     */
    static String applyTransformation(String inputSql, String transformation) {
        if (transformation == null || transformation.isBlank()) {
            return inputSql;
        }
        return "WITH __this AS (%s) SELECT * FROM (%s\n)".formatted(inputSql, embeddable(transformation));
    }

    /**
     * {@code transformation} as text that can be nested inside parentheses: the user's own SQL with
     * only its trailing statement terminators and comments removed (see
     * {@link TransformationText#stripTrailing}). A {@code ;} is valid at the end of a statement but
     * not inside a subquery, and a trailing comment would otherwise run into the closing
     * parenthesis.
     */
    static String embeddable(String transformation) {
        return TransformationText.stripTrailing(transformation);
    }

    /**
     * The relation the output files are written from: the temp input files with the
     * transformation and any partition projections applied.
     */
    private String constructSourceRelation(WriteTask<String, IngestionResult> writeTask, WriteInputs inputs) {
        var batches = writeTask.bucket().batches();
        // All Arrow files
        var arrowFiles = batches.stream().map(Batch::record).map("'%s'"::formatted).collect(Collectors.joining(","));
        String sortOrderClause = getClause(batches.get(0).sortOrder(), "ORDER BY %s ");

        // Inner SQL reads from the temp Arrow files
        // hive_partitioning off: DuckDB would otherwise read a key=value directory in the input
        // files' path (the temp write location) as a column, replacing a real one of that name.
        var innerSql = "SELECT * FROM read_%s([%s], hive_partitioning = false) %s".formatted(this.inputFormat, arrowFiles, sortOrderClause);

        // The transformation is fetched fresh from the handler on every write (currentWriteInputs)
        // so view-based and handler-refreshed transformations are always current without caching.
        var querySql = applyTransformation(innerSql, inputs.transformation());

        // Add handler-supplied derived-column projections (e.g. day(timestamp) AS day) so the
        // PARTITION_BY tokens resolve. Not needed for header-supplied partitionBy, which names
        // columns that already exist in the relation.
        // (Empty when the batch header supplies partitionBy — see currentWriteInputs.)
        return withProjections(querySql, inputs.partitionProjections());
    }

    /**
     * {@code relation} with the table's partition projections added as columns; the write path and the
     * startup check of sidecar tables both build {@code __base} with it.
     */
    static String withProjections(String relation, List<String> projections) {
        return projections.isEmpty() ? relation
                : "SELECT *, %s FROM (%s)".formatted(String.join(", ", projections), relation);
    }

    /** Reads the current {@link WriteInputs} for this batch from the batch header and the handler. */
    private WriteInputs currentWriteInputs(WriteTask<String, IngestionResult> writeTask) {
        String[] batchPartitionBy = writeTask.bucket().batches().get(0).partitionBy();
        boolean hasBatchPartitionBy = batchPartitionBy != null && batchPartitionBy.length > 0;
        String transformation = postIngestionHandler.getTransformation(queueId);
        if (hasBatchPartitionBy) {
            List<SidecarTarget> sidecars = postIngestionHandler.getSidecars(queueId);
            // The header's columns already exist, so the base gets no projections; a sidecar's __base
            // still gets the table's, which its table was checked against. A table whose partitioning
            // is unsupported has none, and does not stop a batch that names its own.
            List<String> sidecarProjections = List.of();
            if (!sidecars.isEmpty()) {
                try {
                    sidecarProjections = nonNullList(postIngestionHandler.getPartitionProjections(queueId));
                } catch (UnsupportedPartitionTransformException unsupported) {
                    // no projections to add
                }
            }
            return new WriteInputs(transformation, List.of(batchPartitionBy), List.of(), sidecars, sidecarProjections);
        }
        // The table's partitioning first, so an unsupported one is the reason a batch is rejected.
        List<String> partitionBy = nonNullList(postIngestionHandler.getPartitionBy(queueId));
        List<String> partitionProjections = nonNullList(postIngestionHandler.getPartitionProjections(queueId));
        return new WriteInputs(transformation, partitionBy, partitionProjections,
                postIngestionHandler.getSidecars(queueId), List.of());
    }

    private static List<String> nonNullList(String[] values) {
        return values == null ? List.of() : Arrays.asList(values);
    }

    /** The base COPY, and the one file it writes when the batch is not partitioned ({@code null} otherwise). */
    private record BaseCopy(String sql, String file, String format) {}

    private BaseCopy constructWriteQuery(WriteTask<String, IngestionResult> writeTask, WriteInputs inputs, String querySql,
                                         String targetDirectory) {
        var batches = writeTask.bucket().batches();
        String partitionByClause = getClause(inputs.partitionBy().toArray(String[]::new), ", PARTITION_BY(%s)");
        // Last format
        var outputFormat = batches.isEmpty() ? "" : batches.get(batches.size() - 1).format();
        String fullFilePath;
        String file = null;
        if (partitionByClause.isEmpty()) {
            String uniqueFileName = "dd_" + UUID.randomUUID() + "." + outputFormat;
            fullFilePath = targetDirectory + "/" + uniqueFileName;
            file = fullFilePath;
        } else {
            fullFilePath = targetDirectory;
        }

        String compressionClause = compressionClause(outputFormat);

        // Build SQL
        // https://duckdb.org/docs/stable/sql/statements/copy
        var sql = """
                COPY
                    (%s)
                    TO '%s'
                    (FORMAT %s%s %s, RETURN_FILES, APPEND);
                """.formatted(querySql, fullFilePath.replace("'", "''"), outputFormat, compressionClause, partitionByClause);
        return new BaseCopy(sql, file, outputFormat);
    }

    private String compressionClause(String outputFormat) {
        return parquetCompression != null && "parquet".equalsIgnoreCase(outputFormat)
                ? ", COMPRESSION %s".formatted(parquetCompression) : "";
    }

    /**
     * The COPY that writes {@code sidecar}'s rows for this batch: its query over {@code baseRelation}
     * (the batch's staged base files) as {@code __base}, its columns cast by name to its table's
     * types, to {@code file} in the staging directory; no file when the query returns no rows.
     */
    private String constructSidecarWriteQuery(WriteInputs inputs, SidecarTarget sidecar, String baseRelation, String file) {
        String query = SidecarTables.writeQuery(withProjections(baseRelation, inputs.sidecarProjections()), sidecar.transformation(),
                sidecar.columns(), sidecar.produced());
        return """
                COPY
                    (%s)
                    TO '%s'
                    (FORMAT parquet%s, RETURN_FILES, WRITE_EMPTY_FILE false);
                """.formatted(query, file.replace("'", "''"), compressionClause("parquet"));
    }

    /**
     * Sets the queue's configured session variables on this write's connection, so a transformation
     * can read a deployment- or operator-supplied value with {@code getvariable('name')} instead of
     * carrying it as a literal. Read from the handler on every write, like the transformation
     * itself, so a reloaded value takes effect on the next batch.
     */
    protected void applyVariables(java.sql.Statement stmt) throws java.sql.SQLException {
        applyVariables(stmt, postIngestionHandler.getVariables(queueId));
    }

    /** Sets {@code variables}, read once for the whole write so all its statements see the same values. */
    private void applyVariables(java.sql.Statement stmt, Map<String, String> variables) throws java.sql.SQLException {
        if (variables.isEmpty()) {
            return;
        }
        for (String setSql : SqlVariables.toSetStatements(variables, IngestionVariables.NOUN)) {
            stmt.execute(setSql);
        }
        logger.debug("Queue '{}': applied {} session variable(s): {}",
                queueId, variables.size(), variables.keySet());
    }

    /**
     * Writes the batch. A queue without sidecars writes its files straight to the output path with
     * one COPY, on this thread, the queue's writer. A queue with sidecars stages the batch; see
     * {@link #writeStaged}.
     */
    private IngestionResult tryWrite(WriteTask<String, IngestionResult> writeTask, WriteInputs inputs,
                                     java.util.concurrent.atomic.AtomicReference<String> lastAttemptSql) throws Exception {
        Map<String, String> variables = postIngestionHandler.getVariables(queueId);
        var write = new Attempt(writeTask, variables, lastAttemptSql);
        // If cancel was already called, don't execute the query
        if (!writeTask.setCancelHook(write::abort)) {
            throw new IllegalStateException("Write task was cancelled");
        }
        String source = constructSourceRelation(writeTask, inputs);
        if (inputs.sidecars().isEmpty()) {
            BaseCopy base = constructWriteQuery(writeTask, inputs, source, outputPath);
            lastAttemptSql.set(base.sql());
            logger.debug("Executing COPY SQL: {}", base.sql());
            CopyOutput output = write.copyOrThrow(base.sql());
            logger.debug("COPY completed for queue '{}': {} rows written, {} files: {}",
                    queueId, output.rows(), output.files().size(), output.files());
            return new IngestionResult(this.queueId, writeTask.taskId(), this.applicationId,
                    writeTask.bucket().getProducerMaxBatchId(), output.rows(), output.files(), base.sql());
        }
        return writeStaged(writeTask, inputs, source, write);
    }

    /**
     * Writes a batch of a queue with sidecars, staged in a local directory next to its input files:
     * <ol>
     *   <li>the base files are written there by one COPY, from the input files with the
     *       transformation and partition projections applied: the transformation runs once;</li>
     *   <li>each sidecar is a COPY from those staged base files as {@code __base}, in parallel, so the
     *       base and every sidecar have the same rows even when the transformation is not
     *       deterministic ({@code now()}, {@code random()});</li>
     *   <li>every staged file is uploaded unchanged, in parallel, by DuckDB with
     *       {@code COPY (SELECT content FROM read_blob(file)) TO dest (FORMAT blob)}, through its own
     *       file systems and secrets: the same path for object storage and a local output path.</li>
     * </ol>
     * Nothing reaches the output path until every output is derived. If a step fails, the others are
     * cancelled, uploaded files are deleted where they are local (otherwise counted as orphans) and
     * the first failure is thrown; nothing has been registered. The staging directory is deleted when
     * the write ends.
     */
    private IngestionResult writeStaged(WriteTask<String, IngestionResult> writeTask, WriteInputs inputs, String source,
                                        Attempt write) throws Exception {
        Path stage = stagingRoot(Path.of(writeTask.bucket().batches().get(0).record()).toAbsolutePath().getParent())
                .resolve("dd_stage_" + UUID.randomUUID());
        Path baseDir = Files.createDirectories(stage.resolve("base"));
        List<String> uploaded = java.util.Collections.synchronizedList(new ArrayList<>());
        try {
            BaseCopy base = constructWriteQuery(writeTask, inputs, source, baseDir.toString());
            if (!"parquet".equalsIgnoreCase(base.format())) {
                throw new IllegalStateException("Queue '%s': sidecars need Parquet output, got '%s'"
                        .formatted(queueId, base.format()));
            }
            write.lastAttemptSql.set(base.sql());
            logger.debug("Executing COPY SQL: {}", base.sql());
            CopyOutput baseOutput = write.copyOrThrow(base.sql());

            String stagedBase = stagedBaseRelation(write, inputs, source, baseOutput.files());
            Map<String, String> sidecarSql = new LinkedHashMap<>();
            for (SidecarTarget sidecar : inputs.sidecars()) {
                Path file = Files.createDirectories(stage.resolve("sidecar_" + sidecarSql.size()))
                        .resolve("dd_" + UUID.randomUUID() + ".parquet");
                sidecarSql.put(sidecar.table(), constructSidecarWriteQuery(inputs, sidecar, stagedBase, file.toString()));
            }
            Map<String, CopyOutput> sidecarOutputs = write.parallel(sidecarSql, sidecarSql::get,
                    (table, copySql) -> write.copy(copySql), SIDECAR_COPY_EXECUTOR);

            // Every staged file and where it goes: a base file keeps its path under the staging
            // directory (its hive partition directories), a sidecar file goes under its table's path.
            Map<String, String> destinations = new LinkedHashMap<>();
            List<String> baseFiles = new ArrayList<>();
            for (String file : baseOutput.files()) {
                String relative = baseDir.relativize(Path.of(file)).toString().replace('\\', '/');
                String destination = outputPath + "/" + relative;
                destinations.put(destination, file);
                baseFiles.add(destination);
            }
            Map<String, List<String>> sidecarFiles = new LinkedHashMap<>();
            for (SidecarTarget sidecar : inputs.sidecars()) {
                CopyOutput output = sidecarOutputs.get(sidecar.table());
                if (output == null || output.rows() == 0) {
                    continue; // no rows for this batch: no file was written
                }
                List<String> files = new ArrayList<>();
                for (String file : output.files()) {
                    String destination = sidecar.path() + "/" + Path.of(file).getFileName();
                    destinations.put(destination, file);
                    files.add(destination);
                }
                sidecarFiles.put(sidecar.table(), files);
            }
            write.parallel(destinations, destination -> "upload " + destinations.get(destination) + " to " + destination,
                    (destination, local) -> {
                        upload(write, local, destination, uploaded);
                        return Boolean.TRUE;
                    }, UPLOAD_EXECUTOR);

            logger.debug("Queue '{}': staged write of {} rows, base files {}, sidecar files {}",
                    queueId, baseOutput.rows(), baseFiles, sidecarFiles);
            return new IngestionResult(this.queueId, writeTask.taskId(), this.applicationId,
                    writeTask.bucket().getProducerMaxBatchId(), baseOutput.rows(), baseFiles, base.sql(), sidecarFiles);
        } catch (Exception | Error e) {
            discardWritten(new ArrayList<>(uploaded), "the batch's write failed");
            throw e;
        } finally {
            deleteRecursively(stage);
        }
    }

    /**
     * Where a batch is staged: next to its input files, unless that path has a character DuckDB would
     * misread in a staged file's path. An {@code =} in any directory makes a hive partition column of
     * it when the base is read back, and {@code * ? [ {} make a glob of the path in read_parquet and
     * read_blob. Then the system temp directory, which must be free of them.
     */
    static Path stagingRoot(Path inputDirectory) {
        if (isSafeStagingPath(inputDirectory)) {
            return inputDirectory;
        }
        Path temp = Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath();
        if (isSafeStagingPath(temp)) {
            return temp;
        }
        throw new IllegalStateException(("Cannot stage a batch: neither %s nor %s is usable, since a path with "
                + "'=', '*', '?', '[' or '{' is misread by DuckDB").formatted(inputDirectory, temp));
    }

    static boolean isSafeStagingPath(Path path) {
        return path.toString().chars().noneMatch(c -> c == '=' || c == '*' || c == '?' || c == '[' || c == '{');
    }

    /**
     * {@code __base} for a batch's sidecars: its staged base files, read back with the partition
     * columns a partitioned COPY moved into the directory names, typed as the batch's relation types
     * them (not guessed from the names: a VARCHAR {@code 007} stays text). A partitioned COPY of no
     * rows writes no file; the relation is then the batch's, empty.
     */
    private String stagedBaseRelation(Attempt write, WriteInputs inputs, String source, List<String> files)
            throws java.sql.SQLException {
        if (files.isEmpty()) {
            return "SELECT * FROM (%s) LIMIT 0".formatted(source);
        }
        String list = files.stream().map(f -> "'" + f.replace("'", "''") + "'").collect(Collectors.joining(", "));
        if (inputs.partitionBy().isEmpty()) {
            // Explicitly off: DuckDB would otherwise detect it from a key=value directory in the path.
            return "SELECT * FROM read_parquet([%s], hive_partitioning = false)".formatted(list);
        }
        Map<String, String[]> types = write.columnTypes(source);
        String hiveTypes = inputs.partitionBy().stream().map(token -> {
            String name = token.trim();
            if (name.length() > 1 && name.startsWith("\"") && name.endsWith("\"")) {
                name = name.substring(1, name.length() - 1).replace("\"\"", "\"");
            }
            String[] column = types.get(name.toLowerCase(Locale.ROOT));
            if (column == null) {
                throw new IllegalStateException("Queue '%s': partition column '%s' is not in the batch's rows".formatted(queueId, name));
            }
            // Keyed by the column's own name: DuckDB names the partition directories after it.
            return "'%s': '%s'".formatted(column[0].replace("'", "''"), column[1].replace("'", "''"));
        }).collect(Collectors.joining(", "));
        return "SELECT * FROM read_parquet([%s], hive_partitioning = true, hive_types = {%s})".formatted(list, hiveTypes);
    }

    /**
     * Uploads one staged file to {@code destination} unchanged, by DuckDB through its own file system
     * and secrets. A local destination's directory is created first: DuckDB's local writer does not.
     */
    private void upload(Attempt write, String local, String destination, List<String> uploaded) {
        String localDestination = localPath(destination);
        if (localDestination != null) {
            try {
                Files.createDirectories(Path.of(localDestination).getParent());
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
        // Recorded once it is sure to start, as it may be partly written if it then fails.
        write.execute("COPY (SELECT content FROM read_blob('%s')) TO '%s' (FORMAT blob)"
                .formatted(local.replace("'", "''"), destination.replace("'", "''")), () -> uploaded.add(destination));
    }

    private static void deleteRecursively(Path dir) {
        if (!Files.exists(dir)) {
            return;
        }
        try (var paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (java.io.IOException e) {
                    logger.warn("Failed to delete staged file {}", path, e);
                }
            });
        } catch (java.io.IOException e) {
            logger.warn("Failed to delete staging directory {}", dir, e);
        }
    }

    private static final int QUEUED = 0, STARTED = 1, SKIPPED = 2;

    /**
     * One write attempt: its statements, so cancelling the write, or one step failing, cancels them
     * all; its variables, read once so every statement sees the same values; and the statement the
     * failure log shows.
     */
    private final class Attempt {
        final WriteTask<String, IngestionResult> writeTask;
        final Map<String, String> variables;
        final java.util.concurrent.atomic.AtomicReference<String> lastAttemptSql;
        final List<java.sql.Statement> statements = new java.util.concurrent.CopyOnWriteArrayList<>();
        final java.util.concurrent.atomic.AtomicBoolean aborted = new java.util.concurrent.atomic.AtomicBoolean();
        /** Pool tasks of this attempt; those still queued are skipped on abort, so no one waits for a thread. */
        final List<java.util.concurrent.atomic.AtomicInteger> taskStates = new java.util.concurrent.CopyOnWriteArrayList<>();

        Attempt(WriteTask<String, IngestionResult> writeTask, Map<String, String> variables,
                java.util.concurrent.atomic.AtomicReference<String> lastAttemptSql) {
            this.writeTask = writeTask;
            this.variables = variables;
            this.lastAttemptSql = lastAttemptSql;
        }

        void abort() {
            aborted.set(true);
            taskStates.forEach(state -> state.compareAndSet(QUEUED, SKIPPED));
            statements.forEach(stmt -> {
                try {
                    stmt.cancel();
                } catch (Exception e) {
                    // Ignore cancellation errors
                }
            });
        }

        CopyOutput copy(String sql) {
            return ParquetIngestionQueue.this.copy(writeTask, sql, statements, aborted, variables);
        }

        /** {@link #copy}, with a COPY's own SQL error thrown as itself. */
        CopyOutput copyOrThrow(String sql) throws java.sql.SQLException {
            try {
                return copy(sql);
            } catch (UncheckedSql e) {
                throw e.getCause();
            }
        }

        /** Runs {@code sql}; {@code starting} runs once it is past the cancel check, just before it. */
        void execute(String sql, Runnable starting) {
            try (var conn = ConnectionPool.getConnection(); var stmt = conn.createStatement()) {
                statements.add(stmt);
                if (writeTask.isCancel() || aborted.get()) {
                    throw new CopyNotStarted();
                }
                starting.run();
                stmt.execute(sql);
            } catch (java.sql.SQLException e) {
                throw new UncheckedSql(e);
            }
        }

        /** Column name (lower case) to its name and type in {@code relation}, bound with this attempt's variables. */
        Map<String, String[]> columnTypes(String relation) throws java.sql.SQLException {
            Map<String, String[]> types = new HashMap<>();
            try (var conn = ConnectionPool.getConnection(); var stmt = conn.createStatement()) {
                applyVariables(stmt, variables);
                try (var rs = stmt.executeQuery("DESCRIBE SELECT * FROM (%s)".formatted(relation))) {
                    while (rs.next()) {
                        String name = rs.getString("column_name");
                        types.put(name.toLowerCase(Locale.ROOT), new String[]{name, rs.getString("column_type")});
                    }
                }
            }
            return types;
        }

        /**
         * Runs {@code run} for every entry on the pool, in parallel. The first failure aborts the
         * attempt (cancelling running statements, skipping queued tasks) and is thrown, with the
         * others suppressed, once every started task has ended; its {@code describe} becomes the
         * statement the failure log shows.
         */
        <K, V, T> Map<K, T> parallel(Map<K, V> tasks, java.util.function.Function<K, String> describe,
                                     java.util.function.BiFunction<K, V, T> run, ExecutorService executor) throws Exception {
            var firstFailure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            Map<K, java.util.concurrent.CompletableFuture<T>> futures = new LinkedHashMap<>();
            Map<K, java.util.concurrent.atomic.AtomicInteger> states = new LinkedHashMap<>();
            for (var task : tasks.entrySet()) {
                var state = new java.util.concurrent.atomic.AtomicInteger(QUEUED);
                states.put(task.getKey(), state);
                taskStates.add(state);
                if (aborted.get()) {
                    state.compareAndSet(QUEUED, SKIPPED);
                }
                futures.put(task.getKey(), java.util.concurrent.CompletableFuture
                        .supplyAsync(() -> state.compareAndSet(QUEUED, STARTED) ? run.apply(task.getKey(), task.getValue()) : null,
                                executor)
                        .whenComplete((result, t) -> {
                            if (t == null) return;
                            Throwable cause = t instanceof java.util.concurrent.CompletionException && t.getCause() != null
                                    ? t.getCause() : t;
                            if (!(cause instanceof CopyNotStarted) && firstFailure.compareAndSet(null, cause)) {
                                lastAttemptSql.set(describe.apply(task.getKey()));
                                abort();
                            }
                        }));
            }
            Map<K, T> results = new LinkedHashMap<>();
            List<Throwable> others = new ArrayList<>();
            for (var entry : futures.entrySet()) {
                if (states.get(entry.getKey()).get() == SKIPPED) {
                    continue; // never started, and never will
                }
                try {
                    T result = entry.getValue().join();
                    if (result != null) results.put(entry.getKey(), result);
                } catch (java.util.concurrent.CompletionException e) {
                    Throwable cause = e.getCause() == null ? e : e.getCause();
                    if (!(cause instanceof CopyNotStarted)) others.add(cause);
                }
            }
            Throwable failure = firstFailure.get();
            if (failure == null && aborted.get()) {
                failure = new CopyNotStarted(); // the write was cancelled
            }
            if (failure != null) {
                Throwable primary = failure instanceof UncheckedSql sql ? sql.getCause() : failure;
                for (Throwable other : others) {
                    if (other != failure) {
                        primary.addSuppressed(other instanceof UncheckedSql sql ? sql.getCause() : other);
                    }
                }
                if (primary instanceof Exception exception) throw exception;
                throw (Error) primary;
            }
            return results;
        }
    }

    /** A COPY that did not run because the write was cancelled or another of its COPYs failed first. */
    private static final class CopyNotStarted extends IllegalStateException {
        CopyNotStarted() {
            super("Write task was cancelled");
        }
    }

    /** A COPY's {@link java.sql.SQLException}, carried out of the task that ran it. */
    private static final class UncheckedSql extends RuntimeException {
        UncheckedSql(java.sql.SQLException cause) {
            super(cause);
        }

        @Override
        public synchronized java.sql.SQLException getCause() {
            return (java.sql.SQLException) super.getCause();
        }
    }

    /**
     * Runs one COPY of the write on a connection of its own, with the queue's variables set, and
     * reads a COPY's RETURN_FILES result. {@code aborted} is set once the write is cancelled or
     * another of its statements failed.
     */
    private CopyOutput copy(WriteTask<String, IngestionResult> writeTask, String sql,
                            List<java.sql.Statement> statements, java.util.concurrent.atomic.AtomicBoolean aborted,
                            Map<String, String> variables) {
        List<String> files = new ArrayList<>();
        long count = 0;
        try (var conn = ConnectionPool.getConnection();
             var stmt = conn.createStatement()) {
            statements.add(stmt);
            if (writeTask.isCancel() || aborted.get()) {
                throw new CopyNotStarted();
            }

            // The queue's session variables, before anything reads the relation: the transformation
            // inside it, and so the COPY, may reference them with getvariable(). ConnectionPool hands
            // out a duplicate per use, so these are scoped to this write and cannot leak into another
            // queue's connection.
            applyVariables(stmt, variables);
            // Again: a cancel that came after the check above, before execute, would be lost.
            if (writeTask.isCancel() || aborted.get()) {
                throw new CopyNotStarted();
            }

            // Execute the query using our statement so the cancel hook works
            stmt.execute(sql);
            try (var rs = stmt.getResultSet()) {
                while (rs.next()) {
                    var rowCount = rs.getLong("count");
                    var rowFilesArray = rs.getArray("files");
                    count += rowCount;
                    if (rowFilesArray != null) {
                        var rowFiles = (Object[]) rowFilesArray.getArray();
                        files.addAll(Arrays.stream(rowFiles).map(Object::toString).toList());
                    }
                }
            }
        } catch (java.sql.SQLException e) {
            throw new UncheckedSql(e);
        }
        return new CopyOutput(count, files);
    }

    /**
     * Deletes files written but not to be registered, where they are local; counts the rest as
     * orphans for {@code ducklake_delete_orphaned_files} to reclaim.
     */
    private void discardWritten(List<String> files, String reason) {
        List<String> left = new ArrayList<>();
        for (String file : files) {
            if (deleteLocalFile(file)) {
                orphanFilesDeleted.increment();
            } else {
                left.add(file);
            }
        }
        if (!left.isEmpty()) {
            orphanFilesLeft.add(left.size());
            logger.warn("Queue '{}': {}; left {} unregistered output file(s), reclaim them with "
                    + "ducklake_delete_orphaned_files: {}", queueId, reason, left.size(), left);
        }
    }
}
