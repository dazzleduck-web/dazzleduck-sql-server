package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.SqlProducerMBean;
import io.helidon.http.Status;
import io.helidon.webserver.http.*;
import io.dazzleduck.sql.flight.server.SimpleBulkIngestConsumer;
import org.apache.arrow.flight.FlightProducer;
import java.sql.Connection;
import java.time.Instant;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HealthCheckService implements HttpService {

    private static final Logger LOGGER = Logger.getLogger(HealthCheckService.class.getName());

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Instant startTime = Instant.now();
    private final SqlProducerMBean producerMBean;

    public HealthCheckService(SqlProducerMBean producerMBean) {
        this.producerMBean = producerMBean;
    }

    @Override
    public void routing(HttpRules rules) {rules.get("/", this::health);}

    private void health(ServerRequest req, ServerResponse res) {

        boolean dbUp;
        String dbError = null;

        try (Connection conn = ConnectionPool.getConnection();
             var stmt = conn.createStatement();
             var rs = stmt.executeQuery("select 1")) {

            dbUp = rs.next();
            if (!dbUp) {
                dbError = "SELECT 1 returned no rows";
            }

        } catch (Exception e) {
            dbUp = false;
            dbError = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            LOGGER.log(Level.WARNING, "Database health check failed", e);
        }

        Map<String, Object> body = Map.of(
                "status", dbUp ? "UP" : "DEGRADED",
                "uptime_seconds", uptimeSeconds(),
                "database", dbUp ? Map.of("status", "UP", "check", "SELECT 1") : Map.of("status", "DOWN", "check", "SELECT 1", "error", dbError),
                "metrics", Map.of("bytes_in", producerMBean.getBytesIn(), "bytes_out", producerMBean.getBytesOut()),
                "timestamp", Instant.now().toString()
        );

        send(res, dbUp ? Status.OK_200 : Status.SERVICE_UNAVAILABLE_503, body);
    }

    private long uptimeSeconds() {
        return Instant.now().getEpochSecond() - startTime.getEpochSecond();
    }

    private void send(ServerResponse res, Status status, Map<String, Object> body) {
        try {
            String json = MAPPER.writeValueAsString(body);
            res.status(status)
                    .header("Content-Type", "application/json")
                    .send(json);

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Health response serialization failed", e);
            res.status(Status.INTERNAL_SERVER_ERROR_500)
                    .send("{\"error\":\"internal_error\"}");
        }
    }
}
