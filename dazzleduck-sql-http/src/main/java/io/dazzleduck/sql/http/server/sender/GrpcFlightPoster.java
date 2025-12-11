package io.dazzleduck.sql.http.server.sender;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class GrpcFlightPoster {

    private static final Logger log = LoggerFactory.getLogger(GrpcFlightPoster.class);

    private GrpcFlightPoster() {
        // utility class → no instances
    }

    /**
     * Sends Arrow bytes to a Flight server via a Flight Action.
     *
     * @param flightClient  pre-built FlightClient instance
     * @param arrowBytes    data to send
     * @param actionName    action name (ingest endpoint)
     * @return number of results returned by the Flight server
     */
    public static int postBytes(FlightClient flightClient,
                                byte[] arrowBytes,
                                String actionName) throws Exception {

        Objects.requireNonNull(flightClient, "flightClient");
        Objects.requireNonNull(arrowBytes, "arrowBytes");

        if (actionName == null || actionName.isBlank()) {
            actionName = "ingest"; // default for compatibility
        }

        Action action = new Action(actionName, ByteBuffer.wrap(arrowBytes).array());

        int resultCount = 0;

        try {
            Iterable<Result> results = (Iterable<Result>) flightClient.doAction(action);

            for (Result r : results) {
                resultCount++;
                String msg = new String(r.getBody(), StandardCharsets.UTF_8);
                log.debug("Flight ingest result: {}", msg);
            }

            log.debug("gRPC POST to action '{}' completed with {} result(s).",
                    actionName, resultCount);

        } catch (Exception ex) {
            log.warn("gRPC POST to action '{}' failed: {}", actionName, ex.toString());
            throw ex;
        }

        return resultCount;
    }
}
