package io.dazzleduck.sql.http.server;

import io.helidon.http.Status;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.hadoop.shaded.org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.time.Clock;

public class OutputStreamServerStreamListener implements FlightProducer.ServerStreamListener {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(OutputStreamServerStreamListener.class);

    private final OutputStream outputStream;
    private volatile boolean end;
    private volatile boolean completed;
    private ServerResponse response;
    private ArrowStreamWriter writer;
    private boolean isReady;
    private final Clock clock;

    public OutputStreamServerStreamListener(ServerResponse response) {
        this(response, Clock.systemDefaultZone());
    }

    public OutputStreamServerStreamListener(ServerResponse response, Clock clock) {
        this.response = response;
        this.end = false;
        this.completed = false;
        this.isReady = false;
        this.outputStream = response.outputStream();
        this.clock = clock;
    }

    @Override
    public synchronized boolean isCancelled() {
        return false;
    }

    @Override
    public synchronized void setOnCancelHandler(Runnable handler) {

    }

    @Override
    public synchronized boolean isReady() {
        return isReady;
    }

    @Override
    public synchronized void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
        this.isReady = true;
        this.writer = new ArrowStreamWriter(root, dictionaries, Channels.newChannel(outputStream));
        try {
            writer.start();
        } catch (IOException e) {
            sendError(e);
            updateEnd(true);
        }
    }

    @Override
    public synchronized void putNext() {
        try {
            writer.writeBatch();
        } catch (IOException e) {
            end = true;
            notifyAll();
            sendError(e);
        }
    }

    @Override
    public synchronized void putNext(ArrowBuf metadata) {
        throw new NotImplementedException("putNext is not supported");
    }

    @Override
    public synchronized void putMetadata(ArrowBuf metadata) {
        throw new NotImplementedException("putMetadata is not supported");
    }

    @Override
    public synchronized void error(Throwable ex) {
        try {
            sendError(ex);
        } finally {
            updateEnd(true);
        }
    }

    @Override
    public synchronized void completed() {
        if (completed) {
            return; // Already completed, avoid double execution
        }
        try {
            this.outputStream.close();
        } catch (IOException e) {
            sendError(e);
        } finally {
            this.completed = true;
            updateEnd(true);
        }
    }

    private void sendError(Throwable ex) {
        String errorMsg = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getName();
        try {
            // Only write error if stream hasn't started
            if (!isReady) {
                this.response.status(Status.INTERNAL_SERVER_ERROR_500);
                outputStream.write(errorMsg.getBytes());
            } else {
                // Stream already started, just log error
                logger.error("Error during streaming: {}", errorMsg, ex);
            }
        } catch (IOException e) {
            logger.error("Failed to send error message", e);
        }
    }

    /**
     * Wait for the stream to end with a timeout.
     * @param timeoutMs Maximum time to wait in milliseconds
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws java.util.concurrent.TimeoutException if the timeout expires
     */
    public synchronized void waitForEnd(long timeoutMs) throws InterruptedException {
        long deadline = clock.millis() + timeoutMs;
        while (!end) {
            long remaining = deadline - clock.millis();
            if (remaining <= 0) {
                logger.error("Query execution timeout after {}ms", timeoutMs);
                // Force completion on timeout
                forceComplete();
                throw new RuntimeException("Query execution timeout after " + timeoutMs + "ms");
            }
            this.wait(remaining);
        }
    }

    /**
     * Check if the listener has completed.
     */
    public boolean isCompleted() {
        return completed;
    }

    /**
     * Force complete the listener, ensuring cleanup happens.
     * This is called when an exception occurs or timeout happens.
     */
    public synchronized void forceComplete() {
        if (!completed) {
            logger.warn("Forcing listener completion");
            try {
                if (writer != null) {
                    writer.close();
                }
                if (outputStream != null) {
                    outputStream.close();
                }
            } catch (Exception e) {
                logger.error("Error during forced completion", e);
            } finally {
                this.completed = true;
                updateEnd(true);
            }
        }
    }

    private void updateEnd(boolean value) {
        this.end = value;
        this.notifyAll();
    }
}
