package io.dazzleduck.sql.logger.server;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class SimpleFlightLogServer {

    private final List<String> receivedLogs = new CopyOnWriteArrayList<>();
    private FlightServer server;

    public List<String> getReceivedLogs() {
        return receivedLogs;
    }

    static final Schema emptySchema = new Schema(Collections.emptyList());

    public void start() throws Exception {
        var allocator = new RootAllocator(Long.MAX_VALUE);

        FlightProducer producer = new FlightProducer() {
            @Override
            public Runnable acceptPut(CallContext context, FlightStream stream, StreamListener<PutResult> ackStream) {
                return () -> {
                    try {
                        VectorSchemaRoot root = stream.getRoot();

                        while (stream.next()) {
                            for (int row = 0; row < root.getRowCount(); row++) {
                                StringBuilder sb = new StringBuilder();
                                int finalRow = row;
                                root.getFieldVectors().forEach(v ->
                                        sb.append(v.getObject(finalRow)).append(" | ")
                                );

                                receivedLogs.add(sb.toString());
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

            @Override
            public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
                return new FlightInfo(emptySchema, descriptor, Collections.emptyList(), -1, -1);
            }

            @Override public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
                listener.onCompleted();
            }

            @Override public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
                listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
            }

            @Override public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
                listener.onCompleted();
            }

            @Override public void listActions(CallContext context, StreamListener<ActionType> listener) {
                listener.onCompleted();
            }
        };

        server = FlightServer.builder(allocator, Location.forGrpcInsecure("0.0.0.0", 32010), producer)
                .build();

        server.start();
    }

    public void stop() throws Exception {
        if (server != null) {
            server.close();
        }
    }
}
