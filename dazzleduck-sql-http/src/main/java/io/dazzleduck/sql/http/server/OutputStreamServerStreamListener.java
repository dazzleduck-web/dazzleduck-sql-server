package io.dazzleduck.sql.http.server;

import io.helidon.http.Status;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.hadoop.shaded.org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;

public class OutputStreamServerStreamListener implements FlightProducer.ServerStreamListener {
    private final OutputStream outputStream;
    private boolean end;
    private ServerResponse response;
    private ArrowStreamWriter writer;
    private boolean isReady;

    public OutputStreamServerStreamListener(ServerResponse response) {
        this.response = response;
        this.end = false;
        this.isReady = false;
        this.outputStream = response.outputStream();
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
        try {
            this.outputStream.close();
        } catch (IOException e) {
            sendError(e);
        } finally {
            updateEnd(true);
        }
    }

    private void sendError(Throwable ex) {
        try {
            outputStream.write(ex.getMessage().getBytes());
        } catch (IOException e) {
            throw new RuntimeException();
        }

        this.response.status(Status.INTERNAL_SERVER_ERROR_500);
    }

    public synchronized void waitForEnd() throws InterruptedException {
        while (!end) {
            this.wait();
        }
    }

    public void updateEnd(boolean value) {
        this.end = value;
        this.notifyAll();
    }
}
