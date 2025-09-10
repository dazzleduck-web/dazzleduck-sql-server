package io.dazzleduck.sql.flight.server;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;

import java.io.Closeable;

public record ServerClient(FlightServer flightServer, FlightSqlClient flightSqlClient, RootAllocator clientAllocator, String warehousePath) implements Closeable {
    @Override
    public void close() {
        try {
            flightServer.close();
            flightSqlClient.close();
            clientAllocator.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
