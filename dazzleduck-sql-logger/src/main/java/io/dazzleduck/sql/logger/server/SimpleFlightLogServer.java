package io.dazzleduck.sql.logger.server;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class SimpleFlightLogServer {
    static Schema emptySchema = new Schema(Collections.emptyList());
    public static void main(String[] args) throws Exception {
        var allocator = new RootAllocator(Long.MAX_VALUE);
//use http
        FlightProducer producer = new FlightProducer() {
            @Override
            public Runnable acceptPut(CallContext context, FlightStream stream, StreamListener<PutResult> ackStream) {
                return () -> {
                    try {
                        VectorSchemaRoot root = stream.getRoot();
                        while (stream.next()) {
                            for (int i = 0; i < root.getRowCount(); i++) {
                                StringBuilder sb = new StringBuilder();
                                int finalI = i;
                                root.getFieldVectors().forEach(v -> sb.append(v.getObject(finalI)).append(" | "));
                                System.out.println("[LOG RECEIVED] " + sb);
                            }
                        }

                        var buf = allocator.buffer(2);
                        buf.setBytes(0, "OK".getBytes(StandardCharsets.UTF_8));
                        ackStream.onNext(PutResult.metadata(buf));
                        ackStream.onCompleted();

                    } catch (Exception e) {
                        ackStream.onError(e);
                    }
                };
            }

            // Minimal implementations for the interface
            @Override
            public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
                // Arrow 16.x allows empty endpoint list using Collections.emptyList()
                return new FlightInfo(
                        emptySchema,
                        descriptor,
                        Collections.emptyList(),
                        -1,
                        -1
                );
            }

            @Override
            public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
                listener.onCompleted();
            }

            @Override
            public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
                listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
            }

            @Override
            public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
                listener.onCompleted();
            }

            @Override
            public void listActions(CallContext context, StreamListener<ActionType> listener) {
                listener.onCompleted();
            }
        };

        // Build and start Flight server
        var server = FlightServer.builder(allocator, Location.forGrpcInsecure("0.0.0.0", 32010), producer).build();
        server.start();
        server.awaitTermination();
    }
}
