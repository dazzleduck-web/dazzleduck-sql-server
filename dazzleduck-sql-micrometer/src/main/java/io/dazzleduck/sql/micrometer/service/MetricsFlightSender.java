package io.dazzleduck.sql.micrometer.service;

import io.dazzleduck.sql.client.ArrowHttpPoster;
import io.dazzleduck.sql.client.GrpcFlightPoster;
import io.dazzleduck.sql.common.ingestion.FlightSender;

import java.io.InputStream;
import java.net.http.HttpClient;
import java.time.Duration;
public final class MetricsFlightSender extends FlightSender.AbstractFlightSender {

    private final HttpClient httpClient = HttpClient.newHttpClient();

    private boolean grpcEnabled = false;
    private String grpcHost;
    private int grpcPort;

    private boolean httpEnabled = false;
    private String httpUrl;

    public void enableGrpc(String host, int port) {
        this.grpcHost = host;
        this.grpcPort = port;
        this.grpcEnabled = true;
    }

    public void enableHttp(String url) {
        this.httpUrl = url;
        this.httpEnabled = true;
    }

    @Override
    protected void doSend(SendElement element) throws InterruptedException {

        byte[] bytes;
        try (InputStream in = element.read()) {
            bytes = in.readAllBytes();
        } catch (Exception e) {
            return;
        }

        // gRPC first
        if (grpcEnabled) {
            int status = GrpcFlightPoster.postBytes(grpcHost, grpcPort, bytes);
            System.out.println("[MetricsFlightSender] gRPC POST → " + status);
        }

        // HTTP fallback or dual send
        if (httpEnabled) {
            try {
                int httpStatus = ArrowHttpPoster.postBytes(httpClient, bytes, httpUrl, Duration.ofSeconds(10));
                System.out.println("[MetricsFlightSender] HTTP POST → " + httpStatus);
            } catch (Exception ex) {
                System.err.println("[MetricsFlightSender] HTTP ERROR: " + ex.getMessage());
            }
        }
    }

    @Override
    public long getMaxInMemorySize() { return 2 * 1024 * 1024; }

    @Override
    public long getMaxOnDiskSize() { return 4 * 1024 * 1024; }
}
