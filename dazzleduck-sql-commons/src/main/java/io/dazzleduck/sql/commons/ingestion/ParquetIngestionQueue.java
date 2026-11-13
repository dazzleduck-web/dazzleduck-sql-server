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
        var innerSql = "SELECT * FROM read_%s([%s]) %s".formatted(this.inputFormat, arrowFiles, sortOrderClause);

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
    private record BaseCopy(String sql, String file) {}

    private BaseCopy constructWriteQuery(WriteTask<String, IngestionResult> writeTask, WriteInputs inputs, String querySql) {
        var batches = writeTask.bucket().batches();
        String partitionByClause = getClause(inputs.partitionBy().toArray(String[]::new), ", PARTITION_BY(%s)");
        // Last format
        var outputFormat = batches.isEmpty() ? "" : batches.get(batches.size() - 1).format();
        String fullFilePath;
        String file = null;
        if (partitionByClause.isEmpty()) {
            String uniqueFileName = "dd_" + UUID.randomUUID() + "." + outputFormat;
            fullFilePath = this.outputPath + "/" + uniqueFileName;
            file = fullFilePath;
        } else {
            fullFilePath = this.outputPath;
        }

        String compressionClause = compressionClause(outputFormat);

        // Build SQL
        // https://duckdb.org/docs/stable/sql/statements/copy
        var sql = """
                COPY
                    (%s)
                    TO '%s'
                    (FORMAT %s%s %s, RETURN_FILES, APPEND);
                """.formatted(querySql, fullFilePath, outputFormat, compressionClause, partitionByClause);
        return new BaseCopy(sql, file);
    }

    private String compressionClause(String outputFormat) {
        return parquetCompression != null && "parquet".equalsIgnoreCase(outputFormat)
                ? ", COMPRESSION %s".formatted(parquetCompression) : "";
    }

    /**
     * The COPY that writes {@code sidecar}'s rows for this batch: its query over {@code baseRelation}
     * (the batch's rows as the base files are written from them) as {@code __base}, its columns cast
     * by name to its table's types, to one new Parquet file directly under the table's data path; no
     * file when the query returns no rows.
     */
    private String constructSidecarWriteQuery(WriteInputs inputs, SidecarTarget sidecar, String baseRelation, String file) {
        String query = SidecarTables.writeQuery(withProjections(baseRelation, inputs.sidecarProjections()), sidecar.transformation(), sidecar.columns());
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
     * Writes the batch: the base files, and each sidecar's file, each by its own COPY on its own
     * connection, all in parallel.
     *
     * <p>With sidecars, the batch's rows (the input files with the transformation and partition
     * projections applied) are first stored once in an in-memory table, which every COPY then reads:
     * so the base and each sidecar see the same rows even when the transformation is not
     * deterministic ({@code now()}, {@code random()}), and the input is read and transformed once.
     * The table is dropped when the write ends. Without sidecars the base is written straight from the
     * input files, as before.
     *
     * <p>If any COPY fails, the others are cancelled, the files they wrote are deleted where they are
     * local (otherwise counted as orphans), and the first failure is thrown; nothing has been
     * registered, so the batch may be written again.
     */
    private IngestionResult tryWrite(WriteTask<String, IngestionResult> writeTask, WriteInputs inputs,
                                     java.util.concurrent.atomic.AtomicReference<String> lastAttemptSql) throws Exception {
        Map<String, String> variables = postIngestionHandler.getVariables(queueId);
        String source = constructSourceRelation(writeTask, inputs);
        String staged = inputs.sidecars().isEmpty() ? null
                : "memory.main.\"__dd_batch_" + UUID.randomUUID().toString().replace('-', '_') + "\"";
        String stage = staged == null ? null : "CREATE TABLE %s AS %s".formatted(staged, source);
        String baseRelation = staged == null ? source : "SELECT * FROM " + staged;
        BaseCopy baseCopy = constructWriteQuery(writeTask, inputs, baseRelation);
        var sql = baseCopy.sql();
        lastAttemptSql.set(stage == null ? sql : stage + ";\n" + sql);
        logger.debug("Executing COPY SQL: {}", lastAttemptSql.get());
        Map<String, SidecarCopy> sidecarCopies = new LinkedHashMap<>();
        for (SidecarTarget sidecar : inputs.sidecars()) {
            String file = sidecar.path() + "/dd_" + UUID.randomUUID() + ".parquet";
            sidecarCopies.put(sidecar.table(),
                    new SidecarCopy(constructSidecarWriteQuery(inputs, sidecar, baseRelation, file), file));
        }

        // Every COPY's statement, so cancelling the write, or one COPY failing, cancels them all.
        var statements = new java.util.concurrent.CopyOnWriteArrayList<java.sql.Statement>();
        var aborted = new java.util.concurrent.atomic.AtomicBoolean();
        Runnable cancelAll = () -> statements.forEach(stmt -> {
            try {
                stmt.cancel();
            } catch (Exception e) {
                // Ignore cancellation errors
            }
        });
        // Sidecar COPYs still queued for a pool thread have no statement to cancel: cancelling the
        // write, or a COPY failing, marks them skipped so the writer does not wait for a thread.
        Map<String, java.util.concurrent.atomic.AtomicInteger> taskStates = new LinkedHashMap<>();
        sidecarCopies.keySet().forEach(table -> taskStates.put(table, new java.util.concurrent.atomic.AtomicInteger(QUEUED)));
        Runnable skipQueued = () -> taskStates.values().forEach(state -> state.compareAndSet(QUEUED, SKIPPED));
        boolean cancelHookSet = writeTask.setCancelHook(() -> {
            aborted.set(true);
            skipQueued.run();
            cancelAll.run();
        });
        // If cancel was already called, don't execute the query
        if (!cancelHookSet) {
            throw new IllegalStateException("Write task was cancelled");
        }

        try {
            if (stage != null) {
                stage(writeTask, stage, statements, variables);
            }
            return copyAll(writeTask, baseCopy, sidecarCopies, taskStates, statements, aborted,
                    () -> { skipQueued.run(); cancelAll.run(); }, lastAttemptSql, variables);
        } finally {
            if (staged != null) {
                try (var conn = ConnectionPool.getConnection(); var stmt = conn.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS " + staged);
                } catch (Exception e) {
                    logger.warn("Queue '{}': could not drop the batch's staging table {}", queueId, staged, e);
                }
            }
        }
    }

    private static final int QUEUED = 0, STARTED = 1, SKIPPED = 2;

    /** A sidecar's COPY and the one file it writes, if its query returns rows. */
    private record SidecarCopy(String sql, String file) {}

    /**
     * Runs the base COPY on this thread, the queue's writer, and every sidecar COPY in parallel with
     * it; see {@link #tryWrite}.
     */
    private IngestionResult copyAll(WriteTask<String, IngestionResult> writeTask, BaseCopy baseCopy,
                                    Map<String, SidecarCopy> sidecarCopies,
                                    Map<String, java.util.concurrent.atomic.AtomicInteger> taskStates,
                                    List<java.sql.Statement> statements, java.util.concurrent.atomic.AtomicBoolean aborted,
                                    Runnable cancelAll, java.util.concurrent.atomic.AtomicReference<String> lastAttemptSql,
                                    Map<String, String> variables)
            throws Exception {
        String sql = baseCopy.sql();
        // The failure that came first, not the cancellations it caused in the other COPYs; its
        // statement is the one the failure log shows.
        var firstFailure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        Map<String, java.util.concurrent.CompletableFuture<CopyOutput>> sidecars = new LinkedHashMap<>();
        java.util.function.BiConsumer<Throwable, String> onFailure = (t, failedSql) -> {
            if (firstFailure.compareAndSet(null, t instanceof java.util.concurrent.CompletionException
                    && t.getCause() != null ? t.getCause() : t)) {
                aborted.set(true);
                cancelAll.run(); // also marks queued sidecar COPYs skipped
                if (!failedSql.equals(sql)) {
                    lastAttemptSql.set(failedSql);
                }
            }
        };
        sidecarCopies.forEach((table, copy) -> sidecars.put(table, java.util.concurrent.CompletableFuture
                .supplyAsync(() -> {
                    if (!taskStates.get(table).compareAndSet(QUEUED, STARTED)) {
                        return new CopyOutput(0, List.of()); // skipped: the write already failed
                    }
                    return copy(writeTask, copy.sql(), statements, aborted, variables);
                }, SIDECAR_COPY_EXECUTOR)
                .whenComplete((output, t) -> {
                    if (t != null) onFailure.accept(t, copy.sql());
                })));

        List<Throwable> others = new ArrayList<>();
        List<String> written = new ArrayList<>();
        CopyOutput baseOutput = null;
        try {
            baseOutput = copy(writeTask, sql, statements, aborted, variables);
            written.addAll(baseOutput.files());
        } catch (CopyNotStarted e) {
            onFailure.accept(e, sql); // the first failure only if the write itself was cancelled
        } catch (RuntimeException | Error e) {
            onFailure.accept(e, sql);
            others.add(e);
            // Partitioned output goes to files DuckDB names; only the unpartitioned file is known.
            if (baseCopy.file() != null) {
                partialFile(baseCopy.file(), written);
            } else {
                logger.warn("Queue '{}': a failed partitioned COPY may have left partial files under {}; "
                        + "reclaim them with ducklake_delete_orphaned_files", queueId, outputPath);
            }
        }
        for (var entry : sidecars.entrySet()) {
            if (taskStates.get(entry.getKey()).get() == SKIPPED) {
                continue; // never started, and never will: nothing written
            }
            try {
                written.addAll(entry.getValue().join().files());
            } catch (java.util.concurrent.CompletionException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                if (!(cause instanceof CopyNotStarted)) {
                    others.add(cause);
                    partialFile(sidecarCopies.get(entry.getKey()).file(), written);
                }
            }
        }
        Throwable failure = firstFailure.get();
        if (failure != null) {
            discardWritten(written, "the batch's write failed");
            Throwable primary = failure instanceof UncheckedSql sqlFailure ? sqlFailure.getCause() : failure;
            for (Throwable other : others) {
                if (other != failure) {
                    primary.addSuppressed(other instanceof UncheckedSql sqlOther ? sqlOther.getCause() : other);
                }
            }
            if (primary instanceof Exception exception) {
                throw exception;
            }
            throw (Error) primary;
        }

        Map<String, List<String>> sidecarFiles = new LinkedHashMap<>();
        sidecars.forEach((table, future) -> {
            CopyOutput output = future.join();
            if (output.rows() > 0) {
                sidecarFiles.put(table, output.files());
            } else {
                discardWritten(output.files(), "sidecar '" + table + "' returned no rows");
            }
        });
        logger.debug("COPY completed for queue '{}': {} rows written, {} files: {}; sidecar files: {}",
                queueId, baseOutput.rows(), baseOutput.files().size(), baseOutput.files(), sidecarFiles);
        return new IngestionResult(this.queueId, writeTask.taskId(), this.applicationId,
                writeTask.bucket().getProducerMaxBatchId(),
                baseOutput.rows(),
                baseOutput.files(), sql, sidecarFiles);
    }

    /**
     * A COPY that failed or was cancelled midway may have left {@code file} behind: a local one is
     * added to {@code written} to be deleted; on object storage its existence is not checked, so it
     * is logged for {@code ducklake_delete_orphaned_files}.
     */
    private void partialFile(String file, List<String> written) {
        String local = localPath(file);
        if (local == null) {
            logger.warn("Queue '{}': a failed COPY may have left a partial file at {}; reclaim it with "
                    + "ducklake_delete_orphaned_files", queueId, file);
        } else if (Files.exists(Path.of(local))) {
            written.add(file);
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

    /** Stores the batch's rows in the staging table, with the queue's variables set. */
    private void stage(WriteTask<String, IngestionResult> writeTask, String sql, List<java.sql.Statement> statements,
                       Map<String, String> variables) throws java.sql.SQLException {
        try (var conn = ConnectionPool.getConnection();
             var stmt = conn.createStatement()) {
            statements.add(stmt);
            if (writeTask.isCancel()) {
                throw new IllegalStateException("Write task was cancelled");
            }
            applyVariables(stmt, variables);
            stmt.execute(sql);
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
