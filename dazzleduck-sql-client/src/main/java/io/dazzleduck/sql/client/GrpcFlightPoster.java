package io.dazzleduck.sql.client;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;

public final class GrpcFlightPoster {

    private GrpcFlightPoster() {}

    /**
     * Send Arrow IPC bytes to a Flight server without any action/path.
     * Example:
     *   grpc://localhost:59307
     */
    public static int postBytes(String host, int port, byte[] arrowBytes) {
        try (var allocator = new RootAllocator();
             var client = FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build();
             var reader = new ArrowStreamReader(new ByteArrayInputStream(arrowBytes), allocator)
        ) {
            // NO ACTION -> Use empty path descriptor
            FlightDescriptor descriptor = FlightDescriptor.path("");

            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            AsyncPutListener ack = new AsyncPutListener();
            FlightClient.ClientStreamListener writer =
                    client.startPut(descriptor, root, ack);

            while (reader.loadNextBatch()) {
                writer.putNext();
            }

            writer.completed();
            ack.getResult();
            return 200;

        } catch (Exception ex) {
            System.err.println("[GrpcFlightPoster] ERROR: " + ex.getMessage());
            return 500;
        }
    }
}
