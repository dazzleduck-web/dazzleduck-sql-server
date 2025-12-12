package io.dazzleduck.sql.common.ingestion;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public interface FlightSender {

    enum StoreStatus {
        IN_MEMORY, ON_DISK, FULL
    }

    void setIngestEndpoint(String uri);
    void enqueue(byte[] input);
    long getMaxInMemorySize();
    long getMaxOnDiskSize();

    abstract class AbstractFlightSender implements FlightSender {
        private final BlockingQueue<SendElement> queue = new ArrayBlockingQueue<>(1024 * 1024);
        private final Thread senderThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        var current = queue.take();
                        doSend(current);
                        updateState(current);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
        private long inMemorySize = 0;

        private long onDiskSize = 0;
        protected String ingestEndpoint;
        protected long getCurrentInMemorySize() {
            return inMemorySize;
        }

        protected long getCurrentOnDiskSize() {
            return onDiskSize;
        }
        @Override
        public void setIngestEndpoint(String uri) {
            this.ingestEndpoint = uri;
        }


        @Override
        public void enqueue(byte[] input) {
            var storeStatus = getStoreStatus(input.length);
            switch (storeStatus) {
                case FULL -> throw new IllegalStateException("queue is full");
                case IN_MEMORY -> queue.add(new MemoryElement(input));
                case ON_DISK -> queue.add(new FileMappedMemoryElement(input));
            }
        }

        public synchronized StoreStatus getStoreStatus(int size) {
            if (inMemorySize + size < getMaxInMemorySize()) {
                inMemorySize += size;
                return StoreStatus.IN_MEMORY;
            }

            if (onDiskSize + size < getMaxOnDiskSize()) {
                onDiskSize += size;
                return StoreStatus.ON_DISK;
            }
            return StoreStatus.FULL;
        }

        private synchronized void updateState(SendElement sendElement) {
            if (sendElement instanceof MemoryElement) {
                inMemorySize -= sendElement.length();
            } else {
                onDiskSize -= sendElement.length();
            }
        }

        abstract protected void doSend(SendElement element) throws InterruptedException;

        public void start() {
            senderThread.setDaemon(true);
            senderThread.start();
        }
    }

    public interface SendElement {
        InputStream read();

        long length();
        default void cleanup() {} // ADDED: default no-op cleanup
    }

    public class MemoryElement implements SendElement {

        private byte[] data;

        public MemoryElement(byte[] data) {
            this.data = data;
        }

        @Override
        public InputStream read() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public long length() {
            return data.length;
        }
    }

    public class FileMappedMemoryElement implements SendElement {
        private Path tempFile; // ADDED: store file path
        private long length;

        public FileMappedMemoryElement(byte[] data) {
            try {
                this.tempFile = Files.createTempFile("flight-", ".arrow");
                Files.write(tempFile, data);
                this.length = data.length;
            } catch (IOException e) {
                throw new RuntimeException("Failed to create temp file", e);
            }
        }

        @Override
        public InputStream read() {
            try {
                return Files.newInputStream(tempFile); // FIXED: return actual stream
            } catch (IOException e) {
                throw new RuntimeException("Failed to read temp file", e);
            }
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public void cleanup() { // ADDED: delete temp file
            try {
                if (tempFile != null) {
                    Files.deleteIfExists(tempFile);
                }
            } catch (IOException e) {
                // Ignore cleanup errors
            }
        }
    }
}