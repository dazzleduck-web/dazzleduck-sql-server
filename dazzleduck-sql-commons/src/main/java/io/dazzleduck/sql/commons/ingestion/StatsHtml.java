package io.dazzleduck.sql.commons.ingestion;

import java.util.List;

/**
 * Renders per-queue {@link Stats} as HTML — shared by the main server's {@code /v1/ui} dashboard and
 * the OTLP collector's {@code /stats} page so both show the same columns and formatting.
 *
 * <p>{@link #renderTable} emits a single {@code <table>} (one row per queue, with indented child rows
 * for each partition of a {@link PartitionedIngestionQueue}); {@link #renderPage} wraps it in a
 * self-contained, auto-refreshing document for a standalone endpoint.
 */
public final class StatsHtml {

    private StatsHtml() {}

    private static final String[] HEADERS = {
            "Queue", "Rows", "Bytes", "Batches", "Buckets",
            "Pending (b/bk)", "Pending bytes (% max)", "Avg write ms/bucket", "Data ms", "Post ms",
            "Failed (b/bytes)", "Rej 429", "Rej out-of-seq", "Rej multi-part", "Evictions",
            "Catalog retries (ok/failed)", "Orphan files (deleted/left)",
            "Rows/min (15m)", "Arrivals/min (15m)",
            "Last write", "Last receive", "Last error"
    };

    /** Full standalone page: inline CSS, a {@code <meta refresh>} every {@code refreshSeconds}, and the table. */
    public static String renderPage(List<Stats> stats, String title, int refreshSeconds) {
        return """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <meta http-equiv="refresh" content="%d">
                    <title>%s</title>
                    <style>
                        * { box-sizing: border-box; }
                        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; color: #222; }
                        h1 { font-size: 20px; margin: 0 0 8px; }
                        .ts { color: #666; font-size: 12px; margin-bottom: 16px; }
                        .table-wrapper { overflow-x: auto; background: #fff; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,.1); }
                        table { border-collapse: collapse; width: 100%%; font-size: 13px; }
                        th, td { border: 1px solid #ddd; padding: 6px 10px; text-align: right; white-space: nowrap; }
                        th { background: #f0f0f0; position: sticky; top: 0; }
                        td:first-child, th:first-child, td:last-child { text-align: left; }
                        tr.partition td { color: #555; background: #fafafa; }
                        tr.partition td:first-child { padding-left: 24px; font-style: italic; }
                        .warn { color: #b26a00; font-weight: bold; }
                        .bad { color: #c0392b; font-weight: bold; }
                        .empty { text-align: center; color: #888; padding: 20px; }
                    </style>
                </head>
                <body>
                    <h1>%s</h1>
                    <div class="ts">Auto-refresh every %ds — generated %s</div>
                    <div class="table-wrapper">%s</div>
                </body>
                </html>
                """.formatted(refreshSeconds, escape(title), escape(title), refreshSeconds,
                java.time.ZonedDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")),
                renderTable(stats, null));
    }

    /** One {@code <table>} of per-queue stats; {@code caption} is optional (null to omit). */
    public static String renderTable(List<Stats> stats, String caption) {
        StringBuilder sb = new StringBuilder();
        sb.append("<table>");
        if (caption != null) {
            sb.append("<caption style=\"text-align:left;font-weight:bold;margin:8px 2px;\">")
                    .append(escape(caption)).append("</caption>");
        }
        sb.append("<thead><tr>");
        for (String h : HEADERS) {
            sb.append("<th>").append(escape(h)).append("</th>");
        }
        sb.append("</tr></thead><tbody>");
        if (stats == null || stats.isEmpty()) {
            sb.append("<tr><td class=\"empty\" colspan=\"").append(HEADERS.length)
                    .append("\">No active ingestion queues</td></tr>");
        } else {
            long now = System.currentTimeMillis();
            for (Stats s : stats) {
                appendRow(sb, s, now, false);
                for (Stats p : s.partitions()) {
                    appendRow(sb, p, now, true);
                }
            }
        }
        sb.append("</tbody></table>");
        return sb.toString();
    }

    private static void appendRow(StringBuilder sb, Stats s, long now, boolean partition) {
        sb.append(partition ? "<tr class=\"partition\">" : "<tr>");
        cell(sb, partition ? "↳ " + escape(s.identifier()) : escape(s.identifier()), true);
        cell(sb, Long.toString(s.rowsWritten()), false);
        cell(sb, formatBytes(s.totalWriteBytes()), false);
        cell(sb, Long.toString(s.totalWriteBatches()), false);
        cell(sb, Long.toString(s.totalWriteBuckets()), false);
        cell(sb, s.pendingBatches() + " / " + s.pendingBuckets(), false);
        cell(sb, formatBytes(s.pendingBytes()) + pctSuffix(s.pendingBytes(), s.maxPendingWrite()), false);
        cell(sb, s.totalWriteBuckets() > 0 ? Long.toString(s.timeSpentWriting() / s.totalWriteBuckets()) : "-", false);
        cell(sb, Long.toString(s.dataPhaseMillis()), false);
        cell(sb, Long.toString(s.postIngestMillis()), false);
        cellClass(sb, s.failedWriteBatches() + " / " + formatBytes(s.failedWriteBytes()),
                s.failedWriteBatches() > 0 ? "bad" : null);
        cellClass(sb, Long.toString(s.rejected429()), s.rejected429() > 0 ? "warn" : null);
        cellClass(sb, Long.toString(s.rejectedOutOfSequence()), s.rejectedOutOfSequence() > 0 ? "warn" : null);
        cellClass(sb, Long.toString(s.rejectedMultiPartition()), s.rejectedMultiPartition() > 0 ? "bad" : null);
        cell(sb, Long.toString(s.producerIdEvictions()), false);
        cellClass(sb, (s.catalogRetries() - s.catalogRetryFailures()) + " / " + s.catalogRetryFailures(),
                s.catalogRetryFailures() > 0 ? "bad" : null);
        cellClass(sb, s.orphanFilesDeleted() + " / " + s.orphanFilesLeft(), s.orphanFilesLeft() > 0 ? "warn" : null);
        sb.append(sparklineCell(s.rowsWrittenPerMinute(), "rows/min"));
        sb.append(sparklineCell(s.batchesReceivedPerMinute(), "batches/min"));
        cell(sb, age(s.lastWriteEpochMs(), now), false);
        cell(sb, age(s.lastReceiveEpochMs(), now), false);
        String err = s.lastError() == null ? "" : escape(s.lastError());
        sb.append("<td class=\"bad\" title=\"").append(err).append("\">")
                .append(err.length() > 60 ? err.substring(0, 60) + "…" : err).append("</td>");
        sb.append("</tr>");
    }

    private static void cell(StringBuilder sb, String value, boolean leftAlign) {
        sb.append(leftAlign ? "<td style=\"text-align:left\">" : "<td>").append(value).append("</td>");
    }

    private static void cellClass(StringBuilder sb, String value, String cssClass) {
        if (cssClass == null) {
            sb.append("<td>").append(value).append("</td>");
        } else {
            sb.append("<td class=\"").append(cssClass).append("\">").append(value).append("</td>");
        }
    }

    /**
     * A table cell holding a tiny inline-SVG sparkline of a per-minute series (oldest→newest), with
     * an emphasized latest-point dot. Renders "—" when the series is empty or all zero. Kept as inline
     * SVG (no library) so it works in the collector's dependency-free HttpServer page too.
     */
    private static String sparklineCell(long[] values, String label) {
        if (values == null || values.length == 0) {
            return "<td>—</td>";
        }
        long max = 0, last = values[values.length - 1];
        for (long v : values) {
            if (v > max) max = v;
        }
        if (max == 0) {
            return "<td title=\"" + escape(label) + ": no activity in the last " + values.length + " min\">—</td>";
        }
        int w = 110, h = 22, n = values.length;
        double innerH = h - 4;
        StringBuilder pts = new StringBuilder();
        double lastX = 0, lastY = 0;
        for (int i = 0; i < n; i++) {
            double x = n == 1 ? w / 2.0 : (double) i / (n - 1) * (w - 2) + 1;
            double y = h - 2 - innerH * ((double) values[i] / max);
            pts.append(fmt(x)).append(',').append(fmt(y)).append(' ');
            lastX = x;
            lastY = y;
        }
        String title = "%s — peak %d/min (~%d/s), now %d/min".formatted(label, max, max / 60, last);
        return "<td title=\"" + escape(title) + "\">"
                + "<svg width=\"" + w + "\" height=\"" + h + "\" viewBox=\"0 0 " + w + " " + h + "\" "
                + "preserveAspectRatio=\"none\" style=\"vertical-align:middle\">"
                + "<polyline fill=\"none\" stroke=\"#2b6cb0\" stroke-width=\"1.5\" points=\"" + pts.toString().trim() + "\"/>"
                + "<circle cx=\"" + fmt(lastX) + "\" cy=\"" + fmt(lastY) + "\" r=\"2\" fill=\"#2b6cb0\"/>"
                + "</svg></td>";
    }

    private static String fmt(double d) {
        return String.format(java.util.Locale.ROOT, "%.1f", d);
    }

    private static String pctSuffix(long pending, long max) {
        if (max <= 0) return "";
        long pct = Math.round(100.0 * pending / max);
        return " (" + pct + "%)";
    }

    /** Human age like "12s", "3m", "2h", or "—" when never (epoch 0) or in the future. */
    static String age(long epochMs, long now) {
        if (epochMs <= 0 || epochMs > now) return "—";
        long secs = (now - epochMs) / 1000;
        if (secs < 60) return secs + "s";
        if (secs < 3600) return (secs / 60) + "m";
        if (secs < 86400) return (secs / 3600) + "h";
        return (secs / 86400) + "d";
    }

    static String formatBytes(double bytes) {
        if (bytes <= 0) return "0 B";
        if (bytes < 1024) return String.format("%.0f B", bytes);
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024 * 1024));
        return String.format("%.2f GB", bytes / (1024 * 1024 * 1024));
    }

    static String escape(String text) {
        return text == null ? "" : text
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#x27;");
    }
}
