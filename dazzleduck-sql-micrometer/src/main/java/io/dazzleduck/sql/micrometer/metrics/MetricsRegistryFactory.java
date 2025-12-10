package io.dazzleduck.sql.micrometer.metrics;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ingestion.FlightSender;
import io.dazzleduck.sql.micrometer.config.ArrowRegistryConfig;
import io.dazzleduck.sql.micrometer.service.ArrowHttpPoster;
import io.dazzleduck.sql.micrometer.service.ArrowMicroMeterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.io.InputStream;
import java.net.http.HttpClient;
import java.time.Duration;
public class MetricsRegistryFactory {

    private static FlightSender sender;

    public static MeterRegistry create() {

        Config dazzConfig = ConfigFactory.load().getConfig("dazzleduck_micrometer");

        String applicationId   = dazzConfig.getString("application_id");
        String applicationName = dazzConfig.getString("application_name");
        String host            = dazzConfig.getString("host");

        String endpoint = dazzConfig.hasPath("arrow_endpoint")
                ? dazzConfig.getString("arrow_endpoint")
                : "http://localhost:8081/ingest?path=metrics";

        ArrowRegistryConfig config = new ArrowRegistryConfig() {
            @Override
            public String get(String key) {
                return switch (key) {
                    case "arrow.enabled"  -> "true";
                    case "arrow.endpoint" -> endpoint;
                    default -> null;
                };
            }
            @Override
            public String uri() {
                return endpoint;
            }
        };


        if (sender == null) {
            synchronized (MetricsRegistryFactory.class) {
                if (sender == null) {

                    sender = new FlightSender.AbstractFlightSender() {

                        private final HttpClient client = HttpClient.newHttpClient();

                        @Override
                        protected void doSend(SendElement element) throws InterruptedException {
                            try (InputStream in = element.read()) {

                                if (ingestEndpoint == null) {
                                    System.err.println("[FlightSender] ERROR: ingest endpoint is NULL!");
                                    return;
                                }

                                byte[] arrowBytes = in.readAllBytes();

                                System.out.println("[FlightSender] Sending " + arrowBytes.length + " bytes to " + ingestEndpoint);

                                // TEMP: fallback until real Arrow Flight RPC implemented
                                int status = ArrowHttpPoster.postBytes(
                                        client, arrowBytes, ingestEndpoint, Duration.ofSeconds(10)
                                );

                                if (status / 100 != 2) {
                                    System.err.println("[FlightSender] Failed to post metrics, status: " + status);
                                } else {
                                    System.out.println("[FlightSender] Successfully posted metrics, status: " + status);
                                }

                            } catch (Exception e) {
                                System.err.println("[FlightSender] Error sending metrics: " + e.getMessage());
                                e.printStackTrace();
                            }
                        }

                        @Override
                        public long getMaxInMemorySize() {
                            return 2 * 1024 * 1024;
                        }

                        @Override
                        public long getMaxOnDiskSize() {
                            return 4 * 1024 * 1024;
                        }
                    };

                    // Start the sender thread
                    ((FlightSender.AbstractFlightSender) sender).start();
                    System.out.println("[FlightSender] Started sender thread");
                }
            }
        }

        // Set the endpoint on the sender
        sender.setIngestEndpoint(endpoint);
        System.out.println("[MetricsRegistryFactory] Set endpoint to: " + endpoint);

        ArrowMicroMeterRegistry arrow =
                new ArrowMicroMeterRegistry.Builder()
                        .config(config)
                        .flightSender(sender)
                        .endpoint(config.uri())
                        .httpTimeout(Duration.ofMinutes(2))
                        .applicationId(applicationId)
                        .applicationName(applicationName)
                        .host(host)
                        .clock(Clock.SYSTEM)
                        .build();

        CompositeMeterRegistry composite = new CompositeMeterRegistry();
        composite.add(arrow);
        return composite;
    }
}
