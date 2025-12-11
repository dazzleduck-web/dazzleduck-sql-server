package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.flight.server.SqlProducerMBean;
import io.dazzleduck.sql.flight.model.RunningStatementInfo;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

/**
 * UI Service for DazzleDuck metrics dashboard
 * Uses SqlProducerMBean to fetch live metrics
 */
public class UIService implements HttpService {

    private final SqlProducerMBean producerMBean;
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    public UIService(SqlProducerMBean producerMBean) {
        this.producerMBean = producerMBean;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get("/", this::serveIndexPage)
                .get("/styles.css", this::serveStylesCSS)
                .get("/script.js", this::serveScriptJS)
                .get("/api/metrics", this::handleGetMetrics);
    }

    private void serveIndexPage(ServerRequest req, ServerResponse res) {
        sendResponse(res, "text/html", """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>DazzleDuck Metrics Dashboard</title>
                    <link rel="stylesheet" href="/ui/styles.css">
                </head>
                <body>
                    <div class="container">
                        <h1>DazzleDuck Metrics Dashboard</h1>
                        <div class="timestamp">Last Updated: <span id="lastUpdate"></span></div>
                        <button class="refresh-btn" onclick="refreshMetrics()">Refresh Metrics</button>
                        <div id="metricsContainer"></div>
                    </div>
                    <script src="/ui/script.js"></script>
                </body>
                </html>
                """);
    }

    private void serveStylesCSS(ServerRequest req, ServerResponse res) {
        sendResponse(res, "text/css", """
                * { margin: 0; padding: 0; box-sizing: border-box; }
                body { font-family: Arial, sans-serif; padding: 20px; background-color: #f5f5f5; }
                .container { max-width: 100vw; margin: 0 0; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                h1 { color: #333; margin-bottom: 20px; font-size: 24px; }
                h2 { color: #333; margin-top: 30px; margin-bottom: 15px; font-size: 16px; font-weight: bold; }
                table { width: 100%; border-collapse: collapse; margin-bottom: 20px; border: 1px solid #000; }
                th, td { border: 1px solid #000; padding: 8px 12px; text-align: left; font-size: 14px; }
                th { background-color: #f0f0f0; font-weight: bold; }
                .table-wrapper { max-height: 400px; overflow-y: auto; margin-bottom: 30px; }
                .refresh-btn { background-color: #007bff; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-size: 14px; margin-bottom: 20px; }
                .refresh-btn:hover { background-color: #0056b3; }
                .timestamp { color: #666; font-size: 12px; margin-bottom: 10px; }
                caption { text-align: left; margin: 10px 2px; font-weight: bold; font-size: 16px; }
                .action-btn { padding: 4px 8px; font-size: 12px; cursor: pointer; background-color: #dc3545; color: white; border: none; border-radius: 3px; }
                .action-btn:hover { background-color: #c82333; }
                """);
    }

    private void serveScriptJS(ServerRequest req, ServerResponse res) {
        sendResponse(res, "application/javascript", """
                    const API_BASE_URL = '';
                
                    async function fetchMetrics() {
                        const r = await fetch('/ui/api/metrics');
                        return await r.text();
                    }
                
                    function parse(html) {
                        return new DOMParser().parseFromString(html, 'text/html')
                            .querySelectorAll('table');
                    }
                
                    function render(tables) {
                        const c = document.getElementById('metricsContainer');
                        c.innerHTML = '';
                        tables.forEach(t => {
                            const w = document.createElement('div');
                            w.className = 'table-wrapper';
                            w.appendChild(t.cloneNode(true));
                            c.appendChild(w);
                        });
                    }
                
                    async function refreshMetrics() {
                        const tables = parse(await fetchMetrics());
                        render(tables);
                        document.getElementById('lastUpdate').innerText = new Date().toLocaleString();
                    }
                
                    async function cancelQuery(query, id) {
                        try {
                            const resp = await fetch('/cancel', {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json'
                                },
                                body: JSON.stringify({ query: query, id: id })
                            });
                        } catch (e) {
                            throw new Error("error canceling query : ", e);
                        }
                        refreshMetrics();
                    }
                    window.onload = refreshMetrics;
                """);
    }

    private void handleGetMetrics(ServerRequest req, ServerResponse res) {
        sendResponse(res, "text/html", buildMetricsTables());
    }

    private String buildMetricsTables() {
        return buildApplicationMetricsTable()
                + buildNetworkMetricsTable()
                + buildRunningStatementsTable()
                + buildOpenPreparedStatementsTable()
                + buildRunningBulkIngestTable();
    }

    private String buildApplicationMetricsTable() {
        return """
                <table>
                    <caption>Application</caption>
                    <thead>
                        <tr>
                            <th>Start Time</th>
                            <th>Running Statements</th>
                            <th>Completed Statements</th>
                            <th>Cancelled Statements</th>
                            <th>Running Prepared Statements</th>
                            <th>Completed Prepared Statements</th>
                            <th>Cancelled Prepared Statements</th>
                            <th>Running Bulk Ingest</th>
                            <th>Completed Bulk Ingest</th>
                            <th>Canceled Bulk Ingest</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>%s</td>
                            <td>%d</td>
                            <td>%d</td>
                            <td>%d</td>
                            <td>%d</td>
                            <td>%d</td>
                            <td>%d</td>
                            <td>No get method</td>
                            <td>%d</td>
                            <td>No GM</td>
                        </tr>
                    </tbody>
                </table>
                """.formatted(
                TIME_FORMATTER.format(producerMBean.getStartTime()),
                producerMBean.getRunningStatements(),
                producerMBean.getCompletedStatements(),
                producerMBean.getCancelledStatements(),
                producerMBean.getRunningPreparedStatements(),
                producerMBean.getCompletedPreparedStatements(),
                producerMBean.getCancelledPreparedStatements(),
                producerMBean.getCompletedBulkIngest());
    }

    private String buildNetworkMetricsTable() {
        return """
                <table>
                    <caption>Network</caption>
                    <thead>
                        <tr>
                            <th>Data In</th>
                            <th>Data Out</th>
                            <th>Arrow Batch In</th>
                            <th>Arrow Batch Out</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>%.2f</td>
                            <td>%.2f</td>
                            <td>No GM</td>
                            <td>NO GM</td>
                        </tr>
                    </tbody>
                </table>
                """.formatted(producerMBean.getBytesIn(), producerMBean.getBytesOut());
    }

    private String buildRunningStatementsTable() {
        List<RunningStatementInfo> statements = producerMBean.getRunningStatementDetails();
        String rows = statements.isEmpty()
                ? "<tr><td colspan=\"6\" style=\"text-align: center;\">No running statements</td></tr>"
                : statements.stream()
                .map(info -> buildStatementRow(info, true))
                .collect(Collectors.joining());

        return buildTableWithCaption("Running Statements", rows);
    }

    private String buildOpenPreparedStatementsTable() {
        List<RunningStatementInfo> statements = producerMBean.getOpenPreparedStatementDetails();
        String rows = statements.isEmpty()
                ? "<tr><td colspan=\"6\" style=\"text-align: center;\">No open prepared statements</td></tr>"
                : statements.stream()
                .map(info -> buildStatementRow(info, true))
                .collect(Collectors.joining());

        return buildTableWithCaption("Open Prepared Statements", rows);
    }

    private String buildRunningBulkIngestTable() {
        List<RunningStatementInfo> statements = producerMBean.getRunningBulkIngestDetails();
        String rows = statements.isEmpty()
                ? "<tr><td colspan=\"6\" style=\"text-align: center;\">No running bulk ingestion</td></tr>"
                : statements.stream()
                .map(info -> buildStatementRow(info, true))
                .collect(Collectors.joining());

        return buildTableWithCaption("Running Bulk Ingestion", rows);
    }

    private String buildStatementRow(RunningStatementInfo info, boolean withCancelButton) {
        String startTime = info.startInstant() != null ? TIME_FORMATTER.format(info.startInstant()) : "N/A";
        String duration = calculateDuration(info.startInstant());
        String action = withCancelButton
                ? "<button class=\"action-btn\" onclick=\"cancelQuery(`%s`, %s)\">Cancel</button>"
                .formatted(escapeHtml(info.query()), Long.parseLong(info.statementId()))
                : escapeHtml(info.action());

        return """
                <tr>
                    <td>%s</td>
                    <td>%s</td>
                    <td>%s</td>
                    <td>%s</td>
                    <td>%s</td>
                    <td>%s</td>
                </tr>
                """.formatted(
                escapeHtml(info.user()),
                escapeHtml(info.statementId()),
                escapeHtml(info.query()),
                startTime,
                duration,
                action);
    }

    private String buildTableWithCaption(String caption, String rows) {
        return """
                <table>
                    <caption>%s</caption>
                    <thead>
                        <tr>
                            <th>User</th>
                            <th>Statement ID</th>
                            <th>Query</th>
                            <th>Start Time</th>
                            <th>Duration</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        %s
                    </tbody>
                </table>
                """.formatted(caption, rows);
    }

    private String calculateDuration(Instant startInstant) {
        if (startInstant == null) return "N/A";

        Duration duration = Duration.between(startInstant, Instant.now());
        long hours = duration.toHours();
        long minutes = duration.toMinutes() % 60;
        long seconds = duration.getSeconds() % 60;

        if (hours > 0) return String.format("%dh %dm", hours, minutes);
        if (minutes > 0) return String.format("%dm %ds", minutes, seconds);
        return String.format("%ds", seconds);
    }

    private String escapeHtml(String text) {
        return text == null ? "" : text
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#x27;");
    }

    private void sendResponse(ServerResponse res, String contentType, String content) {
        res.header("Content-Type", contentType + "; charset=UTF-8");
        res.send(content);
    }
}