package io.dazzleduck.sql.compaction;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;

public class HealthServer implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(HealthServer.class);

    private final HttpServer server;

    public HealthServer(int port, Supplier<CompactionStats> statsSupplier) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", exchange -> handle(exchange, statsSupplier));
        server.setExecutor(null);
    }

    public void start() {
        server.start();
        logger.info("Health server listening on port {}", server.getAddress().getPort());
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange, Supplier<CompactionStats> statsSupplier) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        byte[] body = toJson(statsSupplier.get()).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private String toJson(CompactionStats stats) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"status\": \"UP\",\n");
        sb.append("  \"uptime\": \"").append(stats.uptime()).append("\",\n");
        sb.append("  \"serviceStartTime\": \"").append(stats.serviceStart()).append("\",\n");
        sb.append("  \"databases\": {");

        var entries = stats.databases().entrySet().stream().toList();
        for (int i = 0; i < entries.size(); i++) {
            Map.Entry<String, CompactionStats.DatabaseStats> entry = entries.get(i);
            CompactionStats.DatabaseStats ds = entry.getValue();
            sb.append("\n    \"").append(entry.getKey()).append("\": {\n");
            sb.append("      \"totalMinorCompactions\": ").append(ds.totalMinorCompactions()).append(",\n");
            sb.append("      \"totalMajorCompactions\": ").append(ds.totalMajorCompactions()).append(",\n");
            sb.append("      \"totalFailedCycles\": ").append(ds.totalFailedCycles()).append(",\n");
            sb.append("      \"totalFilesCompacted\": ").append(ds.totalFilesCompacted()).append(",\n");
            sb.append("      \"lastSuccessTime\": ").append(instant(ds.lastSuccessTime())).append(",\n");
            sb.append("      \"nextExecutionTime\": ").append(instant(ds.nextExecutionTime())).append(",\n");
            sb.append("      \"currentSmallFiles\": ").append(ds.currentSmallFiles()).append(",\n");
            sb.append("      \"currentMediumFiles\": ").append(ds.currentMediumFiles()).append(",\n");
            sb.append("      \"currentTotalFiles\": ").append(ds.currentTotalFiles()).append("\n");
            sb.append("    }").append(i < entries.size() - 1 ? "," : "");
        }

        sb.append(entries.isEmpty() ? "" : "\n  ");
        sb.append("}\n}");
        return sb.toString();
    }

    private static String instant(Instant i) {
        return i == null ? "null" : "\"" + i + "\"";
    }
}
