package io.dazzleduck.sql.common.ingestion;

import io.dazzleduck.sql.common.types.JavaRow;
import io.dazzleduck.sql.common.types.VectorSchemaRootWriter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;  // FIXED: Use Arrow Schema
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public interface FlightSender extends Closeable {

    void close();

    enum StoreStatus {
        IN_MEMORY, ON_DISK, FULL
    }

    void enqueue(byte[] input);
    void addRow(JavaRow row);
    long getMaxInMemorySize();

    long getMaxOnDiskSize();

    abstract class AbstractFlightSender implements FlightSender {

        private static final Logger logger  = LoggerFactory.getLogger(AbstractFlightSender.class);
        private final BlockingQueue<SendElement> queue = new ArrayBlockingQueue<>(1024 * 1024);
        protected final Clock clock;
        private volatile boolean shutdown = false;

        protected final Thread senderThread;

        private long inMemorySize = 0;
        private long onDiskSize = 0;

        private final long minBatchSize;

        private final Duration maxDataSendInterval;

        private Instant lastSent;
        private Bucket currentBucket;
        final Schema schema;

        final RootAllocator bufferAllocator = new RootAllocator(Long.MAX_VALUE);

        private final ScheduledExecutorService executorService;


        public AbstractFlightSender(long minBatchSize, Duration maxDataSendInterval, Schema schema, Clock clock){
            this(minBatchSize, maxDataSendInterval, schema, clock, Executors.newSingleThreadScheduledExecutor());
        }
        public AbstractFlightSender(long minBatchSize, Duration maxDataSendInterval, Schema schema, Clock clock, ScheduledExecutorService scheduledExecutorService ){
            logger.info("FlightSender started at {} with send interval {}", clock.instant(), maxDataSendInterval);
            this.minBatchSize = minBatchSize;
            this.maxDataSendInterval = maxDataSendInterval;
            this.clock = clock;
            this.schema = schema;
            this.lastSent = clock.instant();
            this.currentBucket = new Bucket();
            this.senderThread = new Thread(() -> {
                while (!shutdown || !queue.isEmpty()) {
                    try {
                        var current = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                        if (current != null) {
                            doSend(current);
                            updateState(current);
                        }
                    } catch (InterruptedException e) {
                        if (shutdown) {
                            // Drain and process remaining items
                            SendElement element;
                            while ((element = queue.poll()) != null) {
                                try {
                                    doSend(element);
                                    updateState(element);
                                } catch (InterruptedException ex) {
                                    // If interrupted again, exit
                                    break;
                                }
                            }
                        }
                        break;
                    }
                }
            });
            this.executorService = scheduledExecutorService;
            // Set daemon flag before thread is started
            this.senderThread.setDaemon(true);
            this.senderThread.start();
            executorService.submit(() -> sendOrScheduleCurrentBucket(maxDataSendInterval));
        }

        private synchronized void sendCurrentBucket(){
            if (this.currentBucket.size() > 0) {
                try (var c = bufferAllocator.newChildAllocator("child", minBatchSize, Long.MAX_VALUE)) {
                    var bytes = this.currentBucket.getArrowBytes(schema, c);
                    if (bytes != null && bytes.length > 0) {
                        enqueue(bytes);
                        lastSent = clock.instant();
                        currentBucket = new Bucket();
                    }
                }
            }
        }


        private synchronized void sendOrScheduleCurrentBucket(Duration maxDataSendInterval){

            var now = clock.instant();
            logger.debug("Checking bucket send at {}", now);
            var toBeSent = lastSent.plus(maxDataSendInterval);
            var timeRemaining = maxDataSendInterval;
            try {
                if (toBeSent.isBefore(now) || toBeSent.equals(now)) {
                    sendCurrentBucket();
                } else {
                    timeRemaining = Duration.between(now, toBeSent);
                }
            } finally {
                logger.debug("Next bucket send scheduled in {}", timeRemaining);
                if (!shutdown) {
                    executorService.schedule(() -> sendOrScheduleCurrentBucket(maxDataSendInterval),
                            timeRemaining.toMillis(), TimeUnit.MILLISECONDS);
                }
            }
        }
        @Override
        public synchronized void addRow(JavaRow row) {
            if (shutdown) {
                throw new IllegalStateException("Sender is shutdown, cannot enqueue");
            }
            var currentSize = currentBucket.add(row);
            if (currentSize > minBatchSize) {
                try(var c = bufferAllocator.newChildAllocator("child",  minBatchSize, Long.MAX_VALUE)) {
                    var arrowBytes = currentBucket.getArrowBytes(schema, c);
                    enqueue(arrowBytes);
                    this.lastSent = clock.instant();
                    currentBucket = new Bucket();
                }
            }
        }

        @Override
        public synchronized void enqueue(byte[] input) {
            if (shutdown) {
                throw new IllegalStateException("Sender is shutdown, cannot enqueue");
            }

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
                ((FileMappedMemoryElement) sendElement).cleanup();
            }
        }

        abstract protected void doSend(SendElement element) throws InterruptedException;


        @Override
        public void close()  {
            synchronized (this) {
                sendCurrentBucket();
                shutdown = true;
            }

            // Shutdown the scheduled executor service (outside synchronized block to avoid deadlock)
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    var toExecute = executorService.shutdownNow();
                    logger.warn("ExecutorService did not terminate gracefully, forced shutdown of {} tasks", toExecute.size());
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
                logger.atError().setCause(e).log("error closing executor service");
            }

            // Shutdown the sender thread
            senderThread.interrupt();
            bufferAllocator.close();
            try {
                senderThread.join();
            } catch (InterruptedException e) {
                logger.atError().setCause(e).log("error closing sender");
            }
            cleanupQueue();
        }

        private void cleanupQueue() {
            SendElement element;
            while ((element = queue.poll()) != null) {
                synchronized(this) {
                    if (element instanceof MemoryElement) {
                        inMemorySize -= element.length();
                    } else {
                        onDiskSize -= element.length();
                    }
                }

                if (element instanceof FileMappedMemoryElement) {
                    ((FileMappedMemoryElement) element).cleanup();
                }
            }
        }
    }

    interface SendElement {
        InputStream read();
        long length();
    }

    class MemoryElement implements SendElement {
        private final byte[] data;

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

    class FileMappedMemoryElement implements SendElement {
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

        public void cleanup() {
            try {
                if (tempFile != null && Files.exists(tempFile)) {
                    Files.delete(tempFile);
                }
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

    final class Bucket {

        private final List<JavaRow> buffer = new ArrayList<>();
        private long size = 0;
        private boolean serialized = false;

        public Bucket() {

        }

        public long add(JavaRow row) {
            if(serialized) {
                throw new IllegalStateException("the bucket is already serialized");
            }
            buffer.add(row);
            size += row.getActualSize();
            return size;
        }

        public long size() {
            return size;
        }

        public byte[] getArrowBytes(Schema schema, BufferAllocator allocator) {
            if (serialized || buffer.isEmpty()) {
                return null;
            }
            serialized = true;

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                 ByteArrayOutputStream out = new ByteArrayOutputStream();
                 ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                JavaRow[] rows = buffer.toArray(JavaRow[]::new);
                VectorSchemaRootWriter.of(schema).writeToVector(rows, root);
                root.setRowCount(rows.length);
                writer.start();
                writer.writeBatch();
                writer.end();
                return out.toByteArray();

            } catch (Exception e) {
                throw new RuntimeException("Arrow serialization failed", e);
            }
        }
    }
}
