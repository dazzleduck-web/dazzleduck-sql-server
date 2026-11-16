package io.dazzleduck.sql.compaction;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Renders the compaction telemetry ring buffer ({@link CompactionRunLog}) as a self-contained,
 * auto-refreshing HTML dashboard — one card per {@code (database, tier)} showing its latest table
 * call, the derived aggregates, failure-by-class, resource headroom, and sparklines of call duration
 * and files retired over the retained window. Dependency-free (inline CSS + inline SVG) so
 * it serves from the compactor's bare {@code HttpServer}.
 */
final class CompactionHtml {

    private CompactionHtml() {}

    static String renderPage(CompactionRunLog log, int refreshSeconds) {
        List<CompactionRunLog.Key> keys = log.keys();
        StringBuilder cards = new StringBuilder();
        if (keys.isEmpty()) {
            cards.append("<div class=\"empty\">No table calls recorded yet.</div>");
        } else {
            for (CompactionRunLog.Key key : keys) {
                cards.append(card(key, log.latest(key), log.recent(key), log.aggregates(key)));
            }
        }
        return """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <meta http-equiv="refresh" content="%d">
                    <title>DuckLake Compaction Telemetry</title>
                    <style>
                        * { box-sizing: border-box; }
                        body { font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0;
                               padding: 20px; background: #f4f5f7; color: #1c2330; }
                        h1 { font-size: 19px; margin: 0 0 2px; }
                        .ts { color: #6b7280; font-size: 12px; margin-bottom: 18px; }
                        .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(360px, 1fr)); gap: 16px; }
                        .card { background: #fff; border: 1px solid #e3e6ea; border-radius: 8px; padding: 14px 16px;
                                box-shadow: 0 1px 2px rgba(0,0,0,.05); }
                        .card h2 { font-size: 15px; margin: 0 0 2px; display: flex; align-items: center; gap: 8px; }
                        .db { color: #6b7280; font-weight: normal; font-size: 12px; }
                        .badge { font-size: 11px; font-weight: 700; padding: 2px 8px; border-radius: 999px; letter-spacing: .02em; }
                        .b-success { background: #e6f4ea; color: #1e7e34; }
                        .b-empty   { background: #eef0f2; color: #6b7280; }
                        .b-failed  { background: #fdecea; color: #c0392b; }
                        .metrics { display: grid; grid-template-columns: 1fr 1fr; gap: 6px 14px; margin: 12px 0; font-size: 13px; }
                        .metric { display: flex; justify-content: space-between; gap: 8px; }
                        .metric .k { color: #6b7280; }
                        .metric .v { font-variant-numeric: tabular-nums; font-weight: 600; }
                        .warn { color: #b26a00; }
                        .bad  { color: #c0392b; }
                        .good { color: #1e7e34; }
                        .spark { margin: 8px 0 2px; }
                        .spark .lbl { font-size: 11px; color: #6b7280; margin-bottom: 2px; }
                        .chips { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 6px; }
                        .chip { font-size: 11px; background: #fdecea; color: #c0392b; border-radius: 4px; padding: 1px 6px;
                                font-variant-numeric: tabular-nums; }
                        .err { margin-top: 8px; font-size: 12px; color: #c0392b; word-break: break-word; }
                        .empty { color: #6b7280; padding: 40px; text-align: center; }
                        .res { margin-top: 8px; font-size: 11px; color: #6b7280; font-variant-numeric: tabular-nums; }
                    </style>
                </head>
                <body>
                    <h1>DuckLake Compaction Telemetry</h1>
                    <div class="ts">Per-call capture · last %d calls per tier · auto-refresh %ds · generated %s</div>
                    <div class="grid">%s</div>
                </body>
                </html>
                """.formatted(refreshSeconds, CompactionRunLog.DEFAULT_CAPACITY, refreshSeconds,
                Instant.now().toString(), cards.toString());
    }

    private static String card(CompactionRunLog.Key key, CompactionRun latest, List<CompactionRun> runs,
                               CompactionRunLog.DerivedAggregates agg) {
        String badge = switch (latest.outcome()) {
            case SUCCESS -> "<span class=\"badge b-success\">SUCCESS</span>";
            case EMPTY -> "<span class=\"badge b-empty\">EMPTY</span>";
            case FAILED -> "<span class=\"badge b-failed\">FAILED</span>";
        };
        String headroomClass = agg.durationHeadroom() >= 0.8 ? "bad" : agg.durationHeadroom() >= 0.5 ? "warn" : "good";
        String capClass = latest.cap() < latest.staticCap() ? "warn" : null;

        StringBuilder chips = new StringBuilder();
        for (Map.Entry<CompactionRun.FailureClass, Long> e : agg.failuresByClass().entrySet()) {
            if (e.getValue() > 0) {
                chips.append("<span class=\"chip\">").append(escape(e.getKey().name())).append(" ").append(e.getValue()).append("</span>");
            }
        }

        long[] durations = runs.stream().mapToLong(CompactionRun::durationTotalMs).toArray();
        long[] filesRetired = runs.stream().mapToLong(r -> r.filesRetired() != null ? r.filesRetired() : 0).toArray();

        String err = latest.outcome() == CompactionRun.Outcome.FAILED && latest.errorMessage() != null
                ? "<div class=\"err\" title=\"" + escape(latest.errorMessage()) + "\">"
                    + escape(latest.failureClass().name()) + ": " + escape(latest.errorMessage()) + "</div>"
                : "";

        return """
                <div class="card">
                  <h2>%s %s <span class="db">· %s</span></h2>
                  <div class="db">call #%d on %s · %s ago · gap %s</div>
                  <div class="metrics">
                    %s%s%s%s%s%s%s%s
                  </div>
                  <div class="spark"><div class="lbl">call duration (ms), last %d</div>%s</div>
                  <div class="spark"><div class="lbl">files retired, last %d</div>%s</div>
                  <div class="chips">%s</div>
                  <div class="res">RSS peak %s · spill %s · mem limit %s</div>
                  %s
                </div>
                """.formatted(
                escape(key.tierName()), badge, escape(key.database()),
                latest.runId(), escape(latest.tableName()), age(latest.endedAt()), gap(latest.actualGapMs()),
                metric("waiting", (latest.filesWaiting() != null ? latest.filesWaiting() + " files" : "?")
                        + " · " + (latest.bytesWaiting() != null ? formatBytes(latest.bytesWaiting()) : "?"), null),
                metric("merged", (latest.filesProcessed() != null ? latest.filesProcessed() : 0) + " → "
                        + (latest.filesCreated() != null ? latest.filesCreated() : 0) + " files", null),
                metric("cap", latest.cap() + " of " + latest.staticCap(), capClass),
                metric("busy partitions", latest.busyPartitions(), null),
                metric("throughput", fmt1(agg.throughputFilesPerSec()) + " f/s", null),
                metric("idle calls", pct(agg.idleRatio()), null),
                metric("p95 duration", agg.p95DurationMs() + " ms", null),
                metric("headroom", latest.commitTimeoutMs() > 0 ? pct(agg.durationHeadroom()) : "no timeout",
                        latest.commitTimeoutMs() > 0 ? headroomClass : null),
                durations.length, sparkline(durations, "#2b6cb0"),
                filesRetired.length, sparkline(filesRetired, "#2f855a"),
                chips.toString(),
                latest.rssPeakBytes() < 0 ? "n/a" : formatBytes(latest.rssPeakBytes()),
                latest.spillPeakBytes() < 0 ? "n/a" : formatBytes(latest.spillPeakBytes()),
                latest.memoryLimitBytes() < 0 ? "n/a" : formatBytes(latest.memoryLimitBytes()),
                err);
    }

    private static String metric(String k, Object v, String valueClass) {
        String vc = valueClass == null ? "v" : "v " + valueClass;
        return "<div class=\"metric\"><span class=\"k\">" + escape(k) + "</span><span class=\"" + vc + "\">"
                + escape(String.valueOf(v)) + "</span></div>";
    }

    private static String sparkline(long[] values, String stroke) {
        if (values.length == 0) {
            return "<span class=\"db\">—</span>";
        }
        long max = 0;
        for (long v : values) {
            if (v > max) max = v;
        }
        if (max == 0) {
            return "<span class=\"db\">flat (0)</span>";
        }
        int w = 320, h = 30, n = values.length;
        double innerH = h - 4;
        StringBuilder pts = new StringBuilder();
        double lastX = 0, lastY = 0;
        for (int i = 0; i < n; i++) {
            double x = n == 1 ? w / 2.0 : (double) i / (n - 1) * (w - 2) + 1;
            double y = h - 2 - innerH * ((double) values[i] / max);
            pts.append(fmt1(x)).append(',').append(fmt1(y)).append(' ');
            lastX = x;
            lastY = y;
        }
        return "<svg width=\"100%\" height=\"" + h + "\" viewBox=\"0 0 " + w + " " + h + "\" preserveAspectRatio=\"none\">"
                + "<polyline fill=\"none\" stroke=\"" + stroke + "\" stroke-width=\"1.5\" points=\"" + pts.toString().trim() + "\"/>"
                + "<circle cx=\"" + fmt1(lastX) + "\" cy=\"" + fmt1(lastY) + "\" r=\"2.2\" fill=\"" + stroke + "\"/>"
                + "<title>max " + max + "</title></svg>";
    }

    private static String age(Instant t) {
        if (t == null) return "—";
        long s = Duration.between(t, Instant.now()).getSeconds();
        if (s < 0) return "—";
        if (s < 60) return s + "s";
        if (s < 3600) return (s / 60) + "m";
        if (s < 86400) return (s / 3600) + "h";
        return (s / 86400) + "d";
    }

    private static String gap(long ms) {
        if (ms < 0) return "—";
        if (ms < 1000) return ms + "ms";
        return fmt1(ms / 1000.0) + "s";
    }

    private static String pct(double ratio) {
        return Math.round(ratio * 100) + "%";
    }

    private static String fmt1(double d) {
        return String.format(Locale.ROOT, "%.1f", d);
    }

    static String formatBytes(long bytes) {
        if (bytes <= 0) return "0 B";
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
        if (bytes < 1024L * 1024 * 1024) return String.format(Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024));
        return String.format(Locale.ROOT, "%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    static String escape(String s) {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&#x27;");
    }
}
