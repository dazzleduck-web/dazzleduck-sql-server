package io.dazzleduck.sql.flight.stream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ArrowStreamReaderWrapper extends ArrowStreamReader {
    ArrowReader arrowReader;

    public ArrowStreamReaderWrapper(ArrowReader reader, BufferAllocator allocator) {
        super((InputStream) new ByteArrayInputStream(new byte[0]), allocator);
        this.arrowReader = reader;
    }

    @Override
    protected Schema readSchema() throws IOException {
        return arrowReader.getVectorSchemaRoot().getSchema();
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
        return arrowReader.getVectorSchemaRoot();
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        return arrowReader.loadNextBatch();
    }
}
