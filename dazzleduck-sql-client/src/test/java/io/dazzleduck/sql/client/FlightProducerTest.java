package io.dazzleduck.sql.client;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FlightProducerTest {

    private static final long KB = 1024;
    private static final long MB = 1024 * KB;

    private OnDemandProducer sender;

    @AfterEach
    void cleanup() throws InterruptedException {
        if (sender != null) {
            sender.close();
        }
    }

    private OnDemandProducer createSender(long mem, long disk) {
        return new OnDemandProducer(mem, disk, Clock.systemDefaultZone(), new CountDownLatch(1),null);
    }

    @Test
    void testStoreStatusFull() {
        sender = createSender(MB, 4 * MB);
        assertEquals(FlightProducer.StoreStatus.ON_DISK, sender.getStoreStatus((int) MB));
        assertEquals(FlightProducer.StoreStatus.FULL, sender.getStoreStatus((int) (5 * MB)));
    }

    @Test
    void testEnqueueInMemory() throws Exception {
        sender = createSender(10 * MB, 10 * MB);
        // Use same schema as OnDemandProducer
        Schema schema = new Schema(List.of(new Field("ts", FieldType.nullable(new ArrowType.Utf8()), null)));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            FlightProducer.ProducerElement element = createSendElement(schema, allocator, new int[]{}, new String[]{"a", "b", "c"});
            assertDoesNotThrow(() -> sender.enqueue(((FlightProducer.MemoryElement)element).data));
        }
    }

    @Test
    void testEnqueueOnDisk() throws Exception {
        sender = createSender(100, 10 * MB);
        // Use same schema as OnDemandProducer
        Schema schema = new Schema(List.of(new Field("ts", FieldType.nullable(new ArrowType.Utf8()), null)));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            FlightProducer.ProducerElement element = createSendElement(schema, allocator, new int[]{}, new String[]{"a", "b", "c"});
            sender.enqueue(((FlightProducer.MemoryElement)element).data);
            assertEquals(1, sender.filesCreated.get());
        }
    }

    @Test
    void testEnqueueThrowsWhenFull() throws Exception {
        sender = createSender(MB, 5 * MB);
        // Use same schema as OnDemandProducer
        Schema schema = new Schema(List.of(new Field("ts", FieldType.nullable(new ArrowType.Utf8()), null)));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            // Create a large Arrow element (~1MB)
            String[] largeData = new String[100000]; // Large array of strings
            for (int i = 0; i < largeData.length; i++) {
                largeData[i] = "data_" + i;
            }
            FlightProducer.ProducerElement element1 = createSendElement(schema, allocator, new int[]{}, largeData);
            sender.enqueue(((FlightProducer.MemoryElement)element1).data); // goes to disk
            sender.release(); // let it process

            // Try to enqueue something larger than remaining space
            String[] veryLargeData = new String[600000]; // ~6MB
            for (int i = 0; i < veryLargeData.length; i++) {
                veryLargeData[i] = "data_" + i;
            }
            FlightProducer.ProducerElement element2 = createSendElement(schema, allocator, new int[]{}, veryLargeData);
            IllegalStateException ex = assertThrows(
                    IllegalStateException.class,
                    () -> sender.enqueue(((FlightProducer.MemoryElement)element2).data)
            );

            assertEquals("queue is full", ex.getMessage());
        }
    }

    @Test
    void testFileCleanupAfterProcessing() throws Exception {
        CountDownLatch sendDone = new CountDownLatch(1);
        sender = new OnDemandProducer(100, 10 * MB, Clock.systemUTC(), new CountDownLatch(1), sendDone);

        // Use same schema as OnDemandProducer
        Schema schema = new Schema(List.of(new Field("ts", FieldType.nullable(new ArrowType.Utf8()), null)));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            FlightProducer.ProducerElement element = createSendElement(schema, allocator, new int[]{}, new String[]{"a", "b", "c"});
            sender.enqueue(((FlightProducer.MemoryElement)element).data);
            assertEquals(1, sender.filesCreated.get());

            sender.release(); // allow doSend()

            assertTrue(sendDone.await(2, TimeUnit.SECONDS));
            assertEquals(1, sender.filesDeleted.get());
        }
    }

    @Test
    void testFileCleanupOnEnqueueFailure() throws Exception {
        sender = createSender(100, 1024);

        // Use same schema as OnDemandProducer
        Schema schema = new Schema(List.of(new Field("ts", FieldType.nullable(new ArrowType.Utf8()), null)));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            // Create first element that fits within limits
            String[] data1Strings = new String[]{"a", "b"};
            FlightProducer.ProducerElement element1 = createSendElement(schema, allocator, new int[]{}, data1Strings);
            byte[] data1 = ((FlightProducer.MemoryElement)element1).data;
            sender.enqueue(data1); // fits in ON_DISK
            assertEquals(1, sender.filesCreated.get());

            // Create second element that's large enough to exceed remaining capacity
            String[] data2Strings = new String[200]; // Large enough to exceed 1024 bytes when combined with data1
            for (int i = 0; i < data2Strings.length; i++) {
                data2Strings[i] = "large_string_value_" + i;
            }
            FlightProducer.ProducerElement element2 = createSendElement(schema, allocator, new int[]{}, data2Strings);
            byte[] data2 = ((FlightProducer.MemoryElement)element2).data;
            assertThrows(IllegalStateException.class, () -> sender.enqueue(data2));
            assertEquals(2, sender.filesCreated.get());
            assertEquals(1, sender.filesDeleted.get());
        }
    }

    @Test
    void testCloseInterruptsInFlightProcessing() throws Exception {
        CountDownLatch blockLatch = new CountDownLatch(1);
        sender = new OnDemandProducer(10 * MB, 10 * MB, Clock.systemDefaultZone(), blockLatch, null);

        // Use same schema as OnDemandProducer
        Schema schema = new Schema(List.of(new Field("ts", FieldType.nullable(new ArrowType.Utf8()), null)));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            FlightProducer.ProducerElement element = createSendElement(schema, allocator, new int[]{}, new String[]{"a", "b", "c"});
            sender.enqueue(((FlightProducer.MemoryElement)element).data);

            sender.close();
            assertEquals(0, sender.filesDeleted.get());
            blockLatch.countDown();
        }
    }

    @Test
    void testCreateCombinedReaderWithMultipleElements() throws Exception {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        ));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            // Create three SendElements with different data
            List<FlightProducer.ProducerElement> elements = new ArrayList<>();

            // Element 1: rows 1-3
            elements.add(createSendElement(schema, allocator, new int[]{1, 2, 3}, new String[]{"Alice", "Bob", "Charlie"}));

            // Element 2: rows 4-5
            elements.add(createSendElement(schema, allocator, new int[]{4, 5}, new String[]{"David", "Eve"}));

            // Element 3: rows 6-7
            elements.add(createSendElement(schema, allocator, new int[]{6, 7}, new String[]{"Frank", "Grace"}));

            // Combine all elements
            FlightProducer.ProducerElement combinedElement = FlightProducer.createCombinedReader(elements, schema, allocator);
            try (java.io.InputStream in = combinedElement.read();
                 ArrowStreamReader combinedReader = new ArrowStreamReader(in, allocator)) {

                List<Integer> allIds = new ArrayList<>();
                List<String> allNames = new ArrayList<>();

                // Read all batches from the combined reader
                while (combinedReader.loadNextBatch()) {
                    VectorSchemaRoot root = combinedReader.getVectorSchemaRoot();
                    IntVector idVector = (IntVector) root.getVector("id");
                    VarCharVector nameVector = (VarCharVector) root.getVector("name");

                    for (int i = 0; i < root.getRowCount(); i++) {
                        allIds.add(idVector.get(i));
                        allNames.add(nameVector.getObject(i).toString());
                    }
                }

                // Verify all data is present and in correct order
                assertEquals(7, allIds.size(), "Should have 7 total rows");
                assertEquals(List.of(1, 2, 3, 4, 5, 6, 7), allIds);
                assertEquals(List.of("Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace"), allNames);
            } finally {
                combinedElement.close();
                // Clean up SendElements
                for (FlightProducer.ProducerElement element : elements) {
                    element.close();
                }
            }
        }
    }

    @Test
    void testCreateCombinedReaderWithSingleElement() throws Exception {
        Schema schema = new Schema(List.of(
                new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            List<FlightProducer.ProducerElement> elements = new ArrayList<>();
            FlightProducer.ProducerElement originalElement = createSendElement(schema, allocator, new int[]{10, 20, 30}, new String[]{});
            elements.add(originalElement);

            FlightProducer.ProducerElement combinedElement = FlightProducer.createCombinedReader(elements, schema, allocator);

            // Verify it returns the same element instance
            assertSame(originalElement, combinedElement, "Should return the same element for single-element list");

            // Verify the data is correct
            try (java.io.InputStream in = combinedElement.read();
                 ArrowStreamReader combinedReader = new ArrowStreamReader(in, allocator)) {

                assertTrue(combinedReader.loadNextBatch());
                VectorSchemaRoot root = combinedReader.getVectorSchemaRoot();
                IntVector vector = (IntVector) root.getVector("value");

                assertEquals(3, root.getRowCount());
                assertEquals(10, vector.get(0));
                assertEquals(20, vector.get(1));
                assertEquals(30, vector.get(2));

                assertFalse(combinedReader.loadNextBatch(), "Should only have one batch");
            } finally {
                combinedElement.close();
            }
        }
    }

    @Test
    void testCreateCombinedReaderWithEmptyList() throws Exception {
        Schema schema = new Schema(List.of(
                new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            List<FlightProducer.ProducerElement> elements = new ArrayList<>();

            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> FlightProducer.createCombinedReader(elements, schema, allocator)
            );
            assertEquals("Cannot create combined reader from empty list", exception.getMessage());
        }
    }

    @Test
    void testCreateCombinedReaderPreservesOrder() throws Exception {
        Schema schema = new Schema(List.of(
                new Field("sequence", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            List<FlightProducer.ProducerElement> elements = new ArrayList<>();

            // Create elements in specific order
            for (int batch = 0; batch < 5; batch++) {
                int[] values = new int[]{batch * 10, batch * 10 + 1};
                elements.add(createSendElement(schema, allocator, values, new String[]{}));
            }

            FlightProducer.ProducerElement combinedElement = FlightProducer.createCombinedReader(elements, schema, allocator);
            try (java.io.InputStream in = combinedElement.read();
                 ArrowStreamReader combinedReader = new ArrowStreamReader(in, allocator)) {

                List<Integer> allValues = new ArrayList<>();
                while (combinedReader.loadNextBatch()) {
                    VectorSchemaRoot root = combinedReader.getVectorSchemaRoot();
                    IntVector vector = (IntVector) root.getVector("sequence");

                    for (int i = 0; i < root.getRowCount(); i++) {
                        allValues.add(vector.get(i));
                    }
                }

                // Verify order is preserved: 0,1, 10,11, 20,21, 30,31, 40,41
                List<Integer> expected = List.of(0, 1, 10, 11, 20, 21, 30, 31, 40, 41);
                assertEquals(expected, allValues, "Order should be preserved across batches");
            } finally {
                combinedElement.close();
                for (FlightProducer.ProducerElement element : elements) {
                    element.close();
                }
            }
        }
    }

    /**
     * Helper method to create a SendElement with Arrow data
     */
    private FlightProducer.ProducerElement createSendElement(Schema schema, BufferAllocator allocator,
                                                             int[] intValues, String[] stringValues) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {

            writer.start();

            // Set values based on schema - check what fields we actually have
            // Use getFields() and check field names instead of findField() which may throw exceptions
            List<String> fieldNames = schema.getFields().stream()
                    .map(Field::getName)
                    .collect(java.util.stream.Collectors.toList());

            boolean hasId = fieldNames.contains("id");
            boolean hasName = fieldNames.contains("name");
            boolean hasValue = fieldNames.contains("value");
            boolean hasSequence = fieldNames.contains("sequence");
            boolean hasTs = fieldNames.contains("ts");

            if (hasId && hasName) {
                // Schema with id and name fields
                IntVector idVector = (IntVector) root.getVector("id");
                VarCharVector nameVector = (VarCharVector) root.getVector("name");

                idVector.allocateNew(intValues.length);
                nameVector.allocateNew(intValues.length);

                for (int i = 0; i < intValues.length; i++) {
                    idVector.set(i, intValues[i]);
                    nameVector.setSafe(i, stringValues[i].getBytes());
                }

                idVector.setValueCount(intValues.length);
                nameVector.setValueCount(intValues.length);
                root.setRowCount(intValues.length);
            } else if (hasTs) {
                // Schema with single ts (timestamp/string) field
                VarCharVector vector = (VarCharVector) root.getVector("ts");
                vector.allocateNew(stringValues.length);

                for (int i = 0; i < stringValues.length; i++) {
                    vector.setSafe(i, stringValues[i].getBytes());
                }

                vector.setValueCount(stringValues.length);
                root.setRowCount(stringValues.length);
            } else if (hasValue) {
                // Schema with single value field
                IntVector vector = (IntVector) root.getVector("value");
                vector.allocateNew(intValues.length);

                for (int i = 0; i < intValues.length; i++) {
                    vector.set(i, intValues[i]);
                }

                vector.setValueCount(intValues.length);
                root.setRowCount(intValues.length);
            } else if (hasSequence) {
                // Schema with single sequence field
                IntVector vector = (IntVector) root.getVector("sequence");
                vector.allocateNew(intValues.length);

                for (int i = 0; i < intValues.length; i++) {
                    vector.set(i, intValues[i]);
                }

                vector.setValueCount(intValues.length);
                root.setRowCount(intValues.length);
            }

            writer.writeBatch();
            writer.end();
        }

        return new FlightProducer.MemoryElement(out.toByteArray(), 0);
    }


    static class OnDemandProducer extends FlightProducer.AbstractFlightProducer {

    private final long maxOnDiskSize;
    private final long maxInMemorySize;
    private final CountDownLatch latch;
    public final AtomicInteger filesCreated = new AtomicInteger();
    public final AtomicInteger filesDeleted = new AtomicInteger();
    private final CountDownLatch sendDone;

        OnDemandProducer(long maxInMemorySize, long maxOnDiskSize, Clock clock, CountDownLatch blockLatch, CountDownLatch sendDone) {
            super(1024 * 1024, 2048 * 1024, Duration.ofSeconds(1),  new Schema(java.util.List.of(new Field("ts", FieldType.nullable(new ArrowType.Utf8()), null))), clock, 3, 1000, java.util.List.of(), java.util.List.of());
            this.maxInMemorySize = maxInMemorySize;
            this.maxOnDiskSize = maxOnDiskSize;
            this.latch = blockLatch;
            this.sendDone = sendDone;
        }

    @Override
    public long getMaxInMemorySize() {
        return maxInMemorySize;
    }

    @Override
    public long getMaxOnDiskSize() {
        return maxOnDiskSize;
    }

        @Override
        public void enqueue(byte[] input) {
            filesCreated.incrementAndGet(); // always count
            try {
                super.enqueue(input);
            } catch (Exception e) {
                filesDeleted.incrementAndGet(); // rollback
                throw e;
            }
        }

        @Override
        protected void doSend(ProducerElement element) throws InterruptedException {
            latch.await();
            filesDeleted.incrementAndGet();
            if (sendDone != null) {
                sendDone.countDown();
            }
            // Consume the element to simulate actual usage
            try (org.apache.arrow.memory.BufferAllocator childAllocator =
                    bufferAllocator.newChildAllocator("test-send", 0, Long.MAX_VALUE);
                 java.io.InputStream in = element.read();
                 org.apache.arrow.vector.ipc.ArrowStreamReader reader =
                        new org.apache.arrow.vector.ipc.ArrowStreamReader(in, childAllocator)) {
                while (reader.loadNextBatch()) {
                    // Just iterate through batches
                }
            } catch (Exception e) {
                // Ignore errors in test
            }
        }

        void release() {
            latch.countDown();
        }
    }
}