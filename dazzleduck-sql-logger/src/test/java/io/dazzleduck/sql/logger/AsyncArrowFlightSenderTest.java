package io.dazzleduck.sql.logger;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.io.*;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AsyncArrowFlightSenderTest {

    @Mock
    private FlightClient mockClient;

    @Mock
    private FlightClient.ClientStreamListener mockStream;

    private AsyncArrowFlightSender sender;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);

        // Mock startPut to always return a valid stream
        when(mockClient.startPut(any(FlightDescriptor.class), any(), any()))
                .thenReturn(mockStream);

        doNothing().when(mockStream).putNext();
        doNothing().when(mockStream).completed();

        // Create sender using mocked client and disable background threads
        sender = new AsyncArrowFlightSender(mockClient, 10, 2, Duration.ofMillis(200), false);
    }

    @AfterEach
    void teardown() {
        sender.close();
    }

    /** Utility: Create dummy Arrow stream bytes */
    private byte[] createDummyArrowStream() throws IOException {
        Field field = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(List.of(field));

        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {

            root.allocateNew();
            root.setRowCount(0);
            writer.start();
            writer.writeBatch();
            writer.end();
            return out.toByteArray();
        }
    }

    @Test
    void testEnqueue_Success() throws Exception {
        byte[] dummy = createDummyArrowStream();
        assertTrue(sender.enqueue(dummy), "Queue should accept bytes when running");
    }

    @Test
    void testEnqueue_FailsWhenClosed() {
        sender.close();
        assertFalse(sender.enqueue(new byte[10]), "Queue should reject bytes after close");
    }

    @Test
    void testFlushSafely_EmptyQueue_NoCrash() throws Exception {
        var method = AsyncArrowFlightSender.class.getDeclaredMethod("flushSafely");
        method.setAccessible(true);
        method.invoke(sender);
        // Should not throw exception
    }


    @Test
    void testSendBatch_HandlesExceptionGracefully() throws Exception {
        when(mockClient.startPut(any(), any(), any()))
                .thenThrow(new RuntimeException("Simulated error"));

        var method = AsyncArrowFlightSender.class.getDeclaredMethod("sendBatch", List.class);
        method.setAccessible(true);
        byte[] dummy = createDummyArrowStream();

        // Should not throw
        method.invoke(sender, List.of(dummy));
    }

    @Test
    void testClose_ShutsDownCleanly() {
        sender.close();
        assertTrue(sender.scheduler.isShutdown() || sender.scheduler.isTerminated(), "Scheduler should be shutdown");
    }

    @Test
    void testDefaultInstance_NotNull() {
        assertNotNull(AsyncArrowFlightSender.getDefault(), "Default instance should exist");
    }
}
