package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.FlightRecorder;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

public class ResultSetStreamUtil {

    private static final Logger logger = LoggerFactory.getLogger(ResultSetStreamUtil.class);

    private ResultSetStreamUtil() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Submits a stream task whose {@code finalBlock} releases what the stream holds (a connection,
     * a cursor entry). If the executor rejects the task (shutting down), the task never runs, so
     * {@code finalBlock} runs here instead; the rejection is rethrown for the caller to report.
     */
    private static void submit(ExecutorService executorService, Runnable finalBlock, Runnable task) {
        try {
            executorService.submit(task);
        } catch (RejectedExecutionException e) {
            finalBlock.run();
            throw e;
        }
    }
    static void streamResultSet(ExecutorService executorService,
                                ResultSetSupplier supplier,
                                BufferAllocator allocator,
                                final int batchSize,
                                final FlightProducer.ServerStreamListener listener,
                                Runnable finalBlock,
                                FlightRecorder recorder) {
        submit(executorService, finalBlock, () -> {
            BufferAllocator childAllocator = null;
            var error = false;
            try {
                childAllocator = allocator.newChildAllocator("statement-allocator", 0, allocator.getLimit());
                recorder.startStream(false);
                try (DuckDBResultSet resultSet = supplier.get();
                     ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(childAllocator, batchSize)) {
                    listener.start(reader.getVectorSchemaRoot());
                    while (!listener.isCancelled() && reader.loadNextBatch()) {
                        var size = childAllocator.getAllocatedMemory();
                        recorder.recordGetStream(false, size);
                        listener.putNext();
                    }
                }
            } catch (Throwable throwable) {
                error = true;
                recorder.errorStream(false);
                ErrorHandling.handleThrowable(listener, throwable);
            } finally {
                if (!error && !listener.isCancelled()) {
                    listener.completed();
                }
                recorder.endStream(false);
                finalBlock.run();
                if (childAllocator != null) {
                    childAllocator.close();
                }
            }
        });
    }

    static <T extends Statement> void streamResultSet(ExecutorService executorService,
                                                      StatementContext<T> statementContext,
                                                      DuckDBFlightSqlProducer.CacheKey key,
                                                      OptionalResultSetSupplier supplier,
                                                      BufferAllocator allocator,
                                                      final int batchSize,
                                                      final FlightProducer.ServerStreamListener listener,
                                                      Runnable finalBlock, FlightRecorder recorder) {

        submit(executorService, finalBlock, () -> {
            BufferAllocator childAllocator = null;
            var error = false;
            // Whether this stream owns the context. start() throws if another stream is already
            // using it (the same prepared handle streamed twice at once); ending it then would clear
            // that other stream's in-use state and let a cancel close the statement under it.
            var started = false;
            try {
                childAllocator = allocator.newChildAllocator("statement-allocator", 0, allocator.getLimit());
                statementContext.start();
                started = true;
                // A client that disconnects or cancels the DoGet must stop the query. Without this,
                // Flight drops every later putNext() silently and the query runs to completion.
                listener.setOnCancelHandler(() -> {
                    try {
                        statementContext.cancel();
                    } catch (Exception e) {
                        logger.atDebug().setCause(e).log("Failed to cancel statement for a cancelled stream");
                    }
                });
                recorder.startStream(statementContext.isPreparedStatementContext());
                recorder.recordStatementStreamStart(key, statementContext);
                if (listener.isCancelled()) {
                    return; // cancelled before the handler was registered; finally still cleans up
                }
                if (statementContext.isCancelRequested()) {
                    // CancelFlightInfo reached it while it was queued: end without running it.
                    error = true;
                    listener.error(CallStatus.CANCELLED.withDescription("Query was cancelled").toRuntimeException());
                    return;
                }
                supplier.execute();
                if (supplier.hasResultSet()) {
                    try (DuckDBResultSet resultSet = supplier.get();
                         ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(childAllocator, batchSize)) {
                        listener.start(reader.getVectorSchemaRoot());
                        while (!listener.isCancelled() && reader.loadNextBatch()) {
                            listener.putNext();
                            var size = childAllocator.getAllocatedMemory();
                            statementContext.bytesOut(size);
                            recorder.recordGetStream(statementContext.isPreparedStatementContext(),
                                    size);
                        }
                    }
                } else {
                    listener.start(new VectorSchemaRoot(List.of()));
                }
            } catch (Throwable throwable) {
                error = true;
                if (listener.isCancelled()) {
                    // The caller went away and we interrupted the query: not a query error.
                    logger.atDebug().setCause(throwable).log("Stream ended after the caller cancelled");
                    return;
                }
                if (statementContext.isCancelRequested()) {
                    // Interrupted by CancelFlightInfo while the client is still connected: tell it the
                    // query was cancelled, and don't count it as a query error.
                    logger.atDebug().setCause(throwable).log("Stream ended after a server-side cancel");
                    listener.error(CallStatus.CANCELLED.withDescription("Query was cancelled").toRuntimeException());
                    return;
                }
                recorder.errorStream(statementContext.isPreparedStatementContext());
                recorder.recordStatementStreamError(key, statementContext, throwable);
                ErrorHandling.handleThrowable(listener, throwable);
            } finally {
                try {
                    if (!error && !listener.isCancelled()) {
                        listener.completed();
                    }
                    if (started) {
                        statementContext.end();
                        recorder.endStream(statementContext.isPreparedStatementContext());
                        recorder.recordStatementStreamEnd(key, statementContext);
                    }
                    finalBlock.run();
                    if (childAllocator != null) {
                        childAllocator.close();
                    }
                } catch (Exception e){
                    logger.atError().setCause(e).log("Error running finally block");
                }
            }
        });
    }

    static void streamResultSet(ExecutorService executorService,
                                ResultSetSupplierFromConnection supplier,
                                FlightProducer.CallContext context, AccessMode accessMode,
                                BufferAllocator allocator,
                                final FlightProducer.ServerStreamListener listener, FlightRecorder recorder) {

        streamResultSet(executorService, supplier, context, accessMode, allocator, listener, () -> {}, recorder);
    }

    static void streamResultSet(ExecutorService executorService,
                                        ResultSetSupplierFromConnection supplier,
                                        FlightProducer.CallContext context,
                                        AccessMode  accessMode,
                                        BufferAllocator allocator,
                                        final FlightProducer.ServerStreamListener listener,
                                        Runnable finalBlock,
                                        FlightRecorder recorder) {
        try {
            DuckDBConnection connection = DuckDBFlightSqlProducer.getConnection(context, accessMode );
            streamResultSet(executorService,
                    () -> supplier.get(connection),
                    allocator,
                    DuckDBFlightSqlProducer.getBatchSize(context),
                    listener,
                    () -> {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            logger.atError().setCause(e).log("Error closing connection");
                        }
                        finalBlock.run();
                    }, recorder);
        } catch (Throwable t) {
            ErrorHandling.handleThrowable(listener, t);
        }
    }
}
