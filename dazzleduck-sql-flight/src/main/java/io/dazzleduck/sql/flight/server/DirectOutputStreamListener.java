package io.dazzleduck.sql.flight.server;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A ServerStreamListener that writes Arrow IPC format directly to an OutputStream.
 *
 * <p>This listener is used by {@link HttpFlightAdaptor#getStreamStatementDirect} to convert
 * the async streaming API to a CompletableFuture-based API suitable for HTTP responses.
 *
 * <p>Key features:
 * <ul>
 *   <li>Defers obtaining the OutputStream until {@link #start} is called, allowing HTTP
 *       error responses to set proper status codes before the response is committed</li>
 *   <li>Supports configurable compression codec (default: ZSTD) for efficient data transfer</li>
 *   <li>Completes the future when streaming finishes or fails</li>
 * </ul>
 */
public class DirectOutputStreamListener implements FlightProducer.ServerStreamListener {

    private static final Logger logger = LoggerFactory.getLogger(DirectOutputStreamListener.class);

    private final Supplier<OutputStream> outputStreamSupplier;
    private final CompletableFuture<Void> future;
    private final CodecType compressionCodec;
    private OutputStream outputStream;
    private ArrowStreamWriter writer;
    private volatile boolean completed;
    private int batchCount = 0;

    /**
     * Create a new DirectOutputStreamListener with default ZSTD compression.
     *
     * @param outputStreamSupplier supplier that provides the output stream when data is ready to write
     * @param future the future to complete when streaming is done or fails
     */
    public DirectOutputStreamListener(
            Supplier<OutputStream> outputStreamSupplier,
            CompletableFuture<Void> future) {
        this(outputStreamSupplier, future, CompressionUtil.CodecType.ZSTD);
    }

    /**
     * Create a new DirectOutputStreamListener with configurable compression.
     *
     * @param outputStreamSupplier supplier that provides the output stream when data is ready to write
     * @param future the future to complete when streaming is done or fails
     * @param compressionCodec the compression codec to use (e.g., ZSTD, LZ4, NONE)
     */
    public DirectOutputStreamListener(
            Supplier<OutputStream> outputStreamSupplier,
            CompletableFuture<Void> future,
            CodecType compressionCodec) {
        this.outputStreamSupplier = outputStreamSupplier;
        this.future = future;
        this.compressionCodec = compressionCodec;
        this.completed = false;
        logger.debug("DirectOutputStreamListener created with compression codec: {}", compressionCodec);
    }

    @Override
    public synchronized boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public synchronized void setOnCancelHandler(Runnable handler) {
        // No-op for HTTP streaming
    }

    @Override
    public synchronized boolean isReady() {
        return writer != null;
    }

    @Override
    public synchronized void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
        logger.debug("start() called with schema: {} and compression codec: {}", root.getSchema(), compressionCodec);
        try {
            // Lazily get the outputStream - this commits the HTTP response (status 200)
            // Only do this when we're ready to write data
            this.outputStream = outputStreamSupplier.get();

            // Handle NO_COMPRESSION separately - use constructor without compression factory
            if (compressionCodec == CompressionUtil.CodecType.NO_COMPRESSION) {
                this.writer = new ArrowStreamWriter(root, dictionaries, outputStream);
            } else {
                this.writer = new ArrowStreamWriter(
                        root,
                        dictionaries,
                        Channels.newChannel(outputStream),
                        option != null ? option : IpcOption.DEFAULT,
                        CommonsCompressionFactory.INSTANCE,
                        compressionCodec);
            }
            writer.start();
            outputStream.flush();
            logger.debug("writer.start() and flush completed successfully with compression: {}", compressionCodec);
        } catch (IOException e) {
            logger.error("Error in start()", e);
            future.completeExceptionally(e);
        }
    }

    @Override
    public synchronized void putNext() {
        batchCount++;
        logger.debug("putNext() called, batch #{}", batchCount);
        try {
            writer.writeBatch();
            outputStream.flush();
            logger.debug("writeBatch() and flush completed for batch #{}", batchCount);
        } catch (IOException e) {
            logger.error("Error in putNext()", e);
            future.completeExceptionally(e);
        }
    }

    @Override
    public synchronized void putNext(ArrowBuf metadata) {
        logger.debug("putNext(metadata) called, delegating to putNext()");
        putNext();
    }

    @Override
    public synchronized void putMetadata(ArrowBuf metadata) {
        // Metadata not supported in HTTP streaming
    }

    @Override
    public synchronized void error(Throwable ex) {
        logger.debug("error() called", ex);
        try {
            if (outputStream != null) {
                outputStream.close();
            }
        } catch (Exception ignored) {
        } finally {
            future.completeExceptionally(ex);
        }
    }

    @Override
    public synchronized void completed() {
        logger.debug("completed() called, batchCount={}, alreadyCompleted={}", batchCount, completed);
        if (completed) {
            return; // Already completed, avoid double execution
        }
        try {
            // Close the outputStream - this triggers the HTTP response to complete
            if (outputStream != null) {
                outputStream.close();
                logger.debug("outputStream closed successfully");
            }
            future.complete(null);
            logger.debug("future completed successfully");
        } catch (Exception e) {
            logger.error("Error in completed()", e);
            future.completeExceptionally(e);
        } finally {
            this.completed = true;
        }
    }
}
