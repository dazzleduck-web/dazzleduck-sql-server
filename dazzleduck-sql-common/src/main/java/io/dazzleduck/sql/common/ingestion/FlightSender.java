package io.dazzleduck.sql.common.ingestion;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public interface FlightSender {

    void close() throws InterruptedException;

    enum StoreStatus {
        IN_MEMORY, ON_DISK, FULL
    }

    void enqueue(byte[] input);

    long getMaxInMemorySize();

    long getMaxOnDiskSize();

    abstract class AbstractFlightSender implements FlightSender {
        private final BlockingQueue<SendElement> queue = new ArrayBlockingQueue<>(1024 * 1024);
        private volatile boolean shutdown = false;

        protected final Thread senderThread = new Thread(() -> {
            while (!shutdown || !queue.isEmpty()) {
                try {
                    var current = queue.take();
                    doSend(current);
                    updateState(current);
                } catch (InterruptedException e) {
                    // If interrupted during shutdown, exit gracefully
                    if (shutdown) {
                        break;
                    }
                    // Otherwise restore interrupt status
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        private long inMemorySize = 0;
        private long onDiskSize = 0;

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
            shutdown = false;
            if (!senderThread.isAlive()) {
                senderThread.setDaemon(true);
                senderThread.start();
            }
        }

        @Override
        public void close() throws InterruptedException {
            shutdown = true;
            senderThread.join(2_000);
        }
    }

    public interface SendElement {
        InputStream read();
        long length();
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
        private final Path tempFile;
        private final long length;

        public FileMappedMemoryElement(byte[] data) {
            try {
                tempFile = Files.createTempFile("flight-", ".arrow");
                Files.write(tempFile, data);
                this.length = data.length;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public InputStream read() {
            try {
                return Files.newInputStream(tempFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long length() {
            return length;
        }
    }
}
