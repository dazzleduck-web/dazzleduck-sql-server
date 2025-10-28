package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.ingestion.IngestionParameters;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.vector.ipc.ArrowReader;

public interface SimpleBulkIngestConsumer {
    Runnable acceptPutStatementBulkIngest(
            FlightProducer.CallContext context,
            IngestionParameters ingestionParameters,
            ArrowReader inputReader,
            FlightProducer.StreamListener<PutResult> ackStream);
}
