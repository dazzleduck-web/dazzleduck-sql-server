package io.dazzleduck.sql.client;



import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.ingestion.FlightSender;
import io.dazzleduck.sql.client.auth.AuthUtils;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

public final class GrpcFlightSender extends FlightSender.AbstractFlightSender implements AutoCloseable {

    private final FlightSqlClient client;
    private final BufferAllocator allocator;
    private final FlightSqlClient.ExecuteIngestOptions ingestOptions;
    private final long maxMem;
    private final long maxDisk;

    public GrpcFlightSender(
            Schema schema,
            long minBatchSize,
            Duration maxSendInterval,
            Clock clock,
            long maxInMemorySize,
            long maxOnDiskSize,
            BufferAllocator allocator,
            Location location,
            String username,
            String password,
            String catalog,
            String schemaName,
            Map<String, String> ingestParams
    ) {
        super(minBatchSize, maxSendInterval, schema, clock);

        this.maxMem = maxInMemorySize;
        this.maxDisk = maxOnDiskSize;
        this.allocator = Objects.requireNonNull(allocator, "allocator");

        this.client = new FlightSqlClient(FlightClient.builder(allocator, location)
                        .intercept(AuthUtils.createClientMiddlewareFactory(
                                username,
                                password,
                                Map.of(
                                        Headers.HEADER_DATABASE, catalog,
                                        Headers.HEADER_SCHEMA, schemaName
                                )
                        ))
                        .build()
        );

        this.ingestOptions = new FlightSqlClient.ExecuteIngestOptions("", FlightSql.CommandStatementIngest
                                .TableDefinitionOptions
                                .newBuilder()
                                .build(),
                        false,
                        catalog,
                        schemaName,
                        ingestParams
                );
    }

    @Override
    public long getMaxInMemorySize() {
        return maxMem;
    }

    @Override
    public long getMaxOnDiskSize() {
        return maxDisk;
    }

    @Override
    protected void doSend(SendElement element) throws InterruptedException {
        try (InputStream in = element.read()) {

            try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
                client.executeIngest(reader, ingestOptions);
            } catch (Exception ignore) {
            }

        } catch (Exception e) {
            throw new RuntimeException("gRPC ingestion failed", e);
        }
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (RuntimeException e) {
            throw e;
        } finally {
            try {
                client.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close FlightSqlClient", e);
            }
        }
    }


}
