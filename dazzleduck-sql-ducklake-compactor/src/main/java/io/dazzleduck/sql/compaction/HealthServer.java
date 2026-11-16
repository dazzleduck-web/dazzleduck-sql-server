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

    private static final int UI_REFRESH_SECONDS = 5;

    private final HttpServer server;
    private final CompactionRunLog runLog;

    public HealthServer(int port, Supplier<CompactionStats> statsSupplier) throws IOException {
        this(port, statsSupplier, null);
    }

    /**
     * @param runLog per-cycle telemetry ring buffer for the {@code /ui} dashboard and the
     *               {@code /health} telemetry section (null disables both). The durationHeadroom
     *               denominator is carried on each run, so no timeout parameter is needed here.
     */
    public HealthServer(int port, Supplier<CompactionStats> statsSupplier,
                        CompactionRunLog runLog) throws IOException {
        this.runLog = runLog;
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", exchange -> handle(exchange, statsSupplier));
        if (runLog != null) {
            server.createContext("/ui", exchange -> handleUi(exchange, runLog));
        }
        server.setExecutor(null);
    }

    private void handleUi(HttpExchange exchange, CompactionRunLog runLog) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        byte[] body;
        try {
            body = CompactionHtml.renderPage(runLog, UI_REFRESH_SECONDS)
                    .getBytes(StandardCharsets.UTF_8);
        } catch (RuntimeException e) {
            logger.warn("Failed to render compaction telemetry UI", e);
            body = ("<!doctype html><p>telemetry unavailable: " + e.getMessage() + "</p>").getBytes(StandardCharsets.UTF_8);
        }
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    public void start() {
        server.start();
        logger.info("Health server listening on port {} (/health, /ui)", server.getAddress().getPort());
    }

    /** The actual bound port — useful when started on port 0 (ephemeral) in tests. */
    public int boundPort() {
        return server.getAddress().getPort();
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
        sb.append("  \"alerts\": ").append(stringArray(stats.alerts())).append(",\n");
        sb.append("  \"databases\": {");

        var entries = stats.databases().entrySet().stream().toList();
        for (int i = 0; i < entries.size(); i++) {
            Map.Entry<String, CompactionStats.DatabaseStats> entry = entries.get(i);
            CompactionStats.DatabaseStats ds = entry.getValue();
            sb.append("\n    \"").append(entry.getKey()).append("\": {\n");
            sb.append("      \"tiers\": {");
            var tierNames = ds.tierCompactionCounts().keySet().stream().sorted().toList();
            for (int j = 0; j < tierNames.size(); j++) {
                String tierName = tierNames.get(j);
                sb.append("\n        \"").append(tierName).append("\": {\n");
                sb.append("          \"totalCompactions\": ").append(ds.tierCompactionCounts().get(tierName)).append(",\n");
                sb.append("          \"currentFiles\": ").append(ds.currentTierFileCounts().getOrDefault(tierName, 0L)).append(",\n");
                sb.append("          \"loop\": ").append(loopJson(ds.tiers().get(tierName)))
                        .append(runLog != null ? ",\n" : "\n");
                if (runLog != null) {
                    sb.append("          \"telemetry\": ").append(telemetryJson(entry.getKey(), tierName)).append("\n");
                }
                sb.append("        }").append(j < tierNames.size() - 1 ? "," : "");
            }
            sb.append(tierNames.isEmpty() ? "" : "\n      ");
            sb.append("},\n");
            sb.append("      \"totalFailedCycles\": ").append(ds.totalFailedCycles()).append(",\n");
            sb.append("      \"totalFilesCompacted\": ").append(ds.totalFilesCompacted()).append(",\n");
            sb.append("      \"totalFilesRewritten\": ").append(ds.totalFilesRewritten()).append(",\n");
            sb.append("      \"lastSuccessTime\": ").append(instant(ds.lastSuccessTime())).append(",\n");
            sb.append("      \"currentTotalFiles\": ").append(ds.currentTotalFiles()).append(",\n");
            sb.append("      \"unexpiredSnapshots\": ").append(ds.unexpiredSnapshots()).append("\n");
            sb.append("    }").append(i < entries.size() - 1 ? "," : "");
        }

        sb.append(entries.isEmpty() ? "" : "\n  ");
        sb.append("}\n}");
        return sb.toString();
    }

    private static String instant(Instant i) {
        return i == null ? "null" : "\"" + i + "\"";
    }

    /** A tier loop's state, queue, carry-over, back-offs and memory, or null for a disabled tier. */
    private static String loopJson(CompactionStats.TierStatus t) {
        if (t == null) {
            return "null";
        }
        return "{"
                + "\"state\": " + string(t.state())
                + ", \"refused\": " + string(t.refused())
                + ", \"lastPass\": " + instant(t.lastPass())
                + ", \"passes\": " + t.passes()
                + ", \"queue\": " + t.queue()
                + ", \"carryOver\": " + t.carryOver()
                + ", \"backoffs\": " + stringArray(t.backoffs())
                + ", \"starved\": " + stringArray(t.starved())
                + ", \"stale\": " + stringArray(t.stale())
                + ", \"idleTxnTimeoutMs\": " + t.idleTxnTimeoutMs()
                + ", \"threads\": " + t.threads()
                + ", \"duckdbMemoryBytes\": " + t.duckdbMemoryBytes()
                + "}";
    }

    private static String stringArray(java.util.List<String> values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            sb.append(i > 0 ? ", " : "").append(string(values.get(i)));
        }
        return sb.append("]").toString();
    }

    /** A JSON string literal, or null. */
    static String string(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    /**
     * The tier's latest table call plus the derived aggregates over its retained calls, or
     * {@code null} when it has made none yet. Compact inline JSON on one object.
     */
    private String telemetryJson(String db, String tierName) {
        CompactionRunLog.Key key = new CompactionRunLog.Key(db, tierName);
        CompactionRun r = runLog.latest(key);
        if (r == null) {
            return "null";
        }
        CompactionRunLog.DerivedAggregates a = runLog.aggregates(key);
        return "{"
                + "\"runId\": " + r.runId()
                + ", \"table\": " + string(r.tableName())
                + ", \"outcome\": \"" + r.outcome() + "\""
                + ", \"failureClass\": \"" + r.failureClass() + "\""
                + ", \"durationTotalMs\": " + r.durationTotalMs()
                + ", \"durationMergeMs\": " + r.durationMergeMs()
                + ", \"cap\": " + r.cap()
                + ", \"staticCap\": " + r.staticCap()
                + ", \"busyPartitions\": " + r.busyPartitions()
                + ", \"filesWaiting\": " + r.filesWaiting()
                + ", \"bytesWaiting\": " + r.bytesWaiting()
                + ", \"filesProcessed\": " + r.filesProcessed()
                + ", \"filesCreated\": " + r.filesCreated()
                + ", \"filesRetired\": " + r.filesRetired()
                + ", \"actualGapMs\": " + r.actualGapMs()
                + ", \"rssPeakBytes\": " + r.rssPeakBytes()
                + ", \"spillPeakBytes\": " + r.spillPeakBytes()
                + ", \"memoryLimitBytes\": " + r.memoryLimitBytes()
                + ", \"commitTimeoutMs\": " + r.commitTimeoutMs()
                + ", \"derived\": {"
                + "\"windowSize\": " + a.windowSize()
                + ", \"throughputFilesPerSec\": " + round(a.throughputFilesPerSec())
                + ", \"p95DurationMs\": " + a.p95DurationMs()
                + ", \"durationHeadroom\": " + round(a.durationHeadroom())
                + ", \"idleRatio\": " + round(a.idleRatio())
                + "}}";
    }

    private static double round(double d) {
        return Math.round(d * 1000.0) / 1000.0;
    }
}
