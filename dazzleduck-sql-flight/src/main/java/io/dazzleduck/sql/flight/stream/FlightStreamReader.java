package io.dazzleduck.sql.flight.stream;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

public class FlightStreamReader extends ArrowReader {
    public FlightStream flightStream;

    public static FlightStreamReader of(FlightStream flightStream, BufferAllocator bufferAllocator) {
        return new FlightStreamReader(flightStream, bufferAllocator);
    }

    protected FlightStreamReader(FlightStream flightStream, BufferAllocator bufferAllocator) {
        super(bufferAllocator);
        this.flightStream = flightStream;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        return flightStream.next();
    }


    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
        try {
            flightStream.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Schema readSchema() throws IOException {
        return flightStream.getSchema();
    }

    @Override
    public  VectorSchemaRoot getVectorSchemaRoot(){
        return flightStream.getRoot();
    }
}
