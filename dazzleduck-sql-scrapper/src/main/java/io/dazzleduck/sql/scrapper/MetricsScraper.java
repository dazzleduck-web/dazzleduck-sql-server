package io.dazzleduck.sql.scrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Scrapes metrics from Prometheus endpoints and adds them to the buffer.
 * Parses Prometheus text exposition format.
 */
public class MetricsScraper {

    private static final Logger log = LoggerFactory.getLogger(MetricsScraper.class);

    // Prometheus format patterns
    private static final Pattern METRIC_LINE_PATTERN = Pattern.compile(
        "^([a-zA-Z_:][a-zA-Z0-9_:]*)(\\{(.*)\\})?\\s+([\\d.eE+-]+|NaN|\\+Inf|-Inf)(\\s+\\d+)?$"
    );
    private static final Pattern LABEL_PATTERN = Pattern.compile(
        "([a-zA-Z_][a-zA-Z0-9_]*)=\"([^\"]*)\""
    );
    private static final Pattern TYPE_COMMENT_PATTERN = Pattern.compile(
        "^#\\s*TYPE\\s+([a-zA-Z_:][a-zA-Z0-9_:]*)\\s+(counter|gauge|histogram|summary|untyped)$"
    );

    private final CollectorProperties properties;
    private final MetricsBuffer buffer;
    private final HttpClient httpClient;
    private final String collectorHost;

    public MetricsScraper(CollectorProperties properties, MetricsBuffer buffer) {
        this.properties = properties;
        this.buffer = buffer;

        if (properties.getCollectorHost() != null && !properties.getCollectorHost().isEmpty()) {
            this.collectorHost = properties.getCollectorHost();
        } else {
            this.collectorHost = getHostname();
        }

        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(properties.getConnectionTimeoutMs()))
            .build();
    }

    private String getHostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    /**
     * Scrape metrics from all configured targets and add to buffer.
     * Returns the number of metrics scraped.
     */
    public int scrapeAllTargets() {
        int totalScraped = 0;

        for (String target : properties.getResolvedTargets()) {
            try {
                List<CollectedMetric> metrics = scrapeTarget(target);
                buffer.addAll(metrics);
                totalScraped += metrics.size();
                log.debug("Scraped {} metrics from {}", metrics.size(), target);
            } catch (Exception e) {
                log.warn("Failed to scrape metrics from {}: {}", target, e.getMessage());
            }
        }

        return totalScraped;
    }

    /**
     * Scrape metrics from a single target endpoint.
     */
    public List<CollectedMetric> scrapeTarget(String targetUrl) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(targetUrl))
            .header("Accept", "text/plain")
            .timeout(Duration.ofMillis(properties.getReadTimeoutMs()))
            .GET()
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() >= 200 && response.statusCode() < 300) {
            return parsePrometheusFormat(response.body(), targetUrl);
        } else {
            throw new IOException("Scrape failed with status " + response.statusCode());
        }
    }

    /**
     * Parse Prometheus text format into CollectedMetric objects.
     * Uses two passes: first to collect all raw metric data and accumulate _sum/_count
     * for histogram/summary types, then to build final CollectedMetric objects with
     * computed mean values.
     */
    private List<CollectedMetric> parsePrometheusFormat(String body, String sourceUrl) {
        Map<String, String> metricTypes = new HashMap<>();

        record RawMetric(String name, String type, Map<String, String> labels, double value) {}
        List<RawMetric> rawMetrics = new ArrayList<>();

        // Keyed by baseName + ":" + labelsWithoutHistogramKeys (le, quantile removed)
        Map<String, Double> sumMap = new HashMap<>();
        Map<String, Double> countMap = new HashMap<>();

        // Phase 1: parse all lines
        for (String line : body.split("\n")) {
            line = line.trim();

            if (line.isEmpty() || line.startsWith("# HELP")) {
                continue;
            }

            Matcher typeMatcher = TYPE_COMMENT_PATTERN.matcher(line);
            if (typeMatcher.matches()) {
                metricTypes.put(typeMatcher.group(1), typeMatcher.group(2));
                continue;
            }

            if (line.startsWith("#")) {
                continue;
            }

            Matcher metricMatcher = METRIC_LINE_PATTERN.matcher(line);
            if (metricMatcher.matches()) {
                String name = metricMatcher.group(1);
                String labelsStr = metricMatcher.group(3);
                String valueStr = metricMatcher.group(4);

                Map<String, String> labels = new LinkedHashMap<>();
                if (labelsStr != null && !labelsStr.isEmpty()) {
                    Matcher labelMatcher = LABEL_PATTERN.matcher(labelsStr);
                    while (labelMatcher.find()) {
                        labels.put(labelMatcher.group(1), labelMatcher.group(2));
                    }
                }

                double value;
                try {
                    value = switch (valueStr) {
                        case "NaN" -> Double.NaN;
                        case "+Inf" -> Double.POSITIVE_INFINITY;
                        case "-Inf" -> Double.NEGATIVE_INFINITY;
                        case null, default -> {
                            assert valueStr != null;
                            yield Double.parseDouble(valueStr);
                        }
                    };
                } catch (NumberFormatException e) {
                    continue;
                }

                String type = getMetricType(name, metricTypes);
                rawMetrics.add(new RawMetric(name, type, labels, value));

                // Accumulate _sum and _count for histogram/summary mean computation
                if ("histogram".equals(type) || "summary".equals(type)) {
                    if (name.endsWith("_sum")) {
                        String baseName = name.substring(0, name.length() - "_sum".length());
                        sumMap.put(meanKey(baseName, labels), value);
                    } else if (name.endsWith("_count")) {
                        String baseName = name.substring(0, name.length() - "_count".length());
                        countMap.put(meanKey(baseName, labels), value);
                    }
                }
            }
        }

        // Phase 2: build CollectedMetrics with computed mean
        List<CollectedMetric> metrics = new ArrayList<>();
        for (RawMetric raw : rawMetrics) {
            double mean = 0.0;
            if ("histogram".equals(raw.type()) || "summary".equals(raw.type())) {
                String baseName = getBaseName(raw.name());
                String key = meanKey(baseName, raw.labels());
                Double sum = sumMap.get(key);
                Double count = countMap.get(key);
                if (sum != null && count != null && count > 0) {
                    mean = sum / count;
                }
            }

            Map<String, String> tags = new LinkedHashMap<>(raw.labels());
            tags.put("source_url", sourceUrl);
            tags.put("collector_id", properties.getCollectorId());
            tags.put("collector_host", collectorHost);

            metrics.add(new CollectedMetric(java.time.Instant.now(), raw.name(), raw.type(), tags, raw.value(), 0.0, 0.0, mean));
        }

        return metrics;
    }

    /**
     * Build a stable lookup key for matching _sum/_count to their histogram/summary family.
     * Strips "le" (histogram bucket label) and "quantile" (summary quantile label) so that
     * _bucket{method="GET",le="0.1"} and _sum{method="GET"} share the same key.
     */
    private String meanKey(String baseName, Map<String, String> labels) {
        TreeMap<String, String> sorted = new TreeMap<>(labels);
        sorted.remove("le");
        sorted.remove("quantile");
        return baseName + ":" + sorted;
    }

    /**
     * Strip histogram/summary suffixes to get the base metric family name.
     */
    private String getBaseName(String metricName) {
        String[] suffixes = {"_bucket", "_sum", "_count"};
        for (String suffix : suffixes) {
            if (metricName.endsWith(suffix)) {
                return metricName.substring(0, metricName.length() - suffix.length());
            }
        }
        return metricName;
    }

    /**
     * Get the buffer for external access.
     */
    public MetricsBuffer getBuffer() {
        return buffer;
    }

    /**
     * Get metric type, handling histogram/summary suffixes.
     */
    private String getMetricType(String metricName, Map<String, String> metricTypes) {
        // Direct match
        if (metricTypes.containsKey(metricName)) {
            return metricTypes.get(metricName);
        }

        // Try stripping histogram/summary suffixes
        String[] suffixes = {"_bucket", "_sum", "_count", "_total"};
        for (String suffix : suffixes) {
            if (metricName.endsWith(suffix)) {
                String baseName = metricName.substring(0, metricName.length() - suffix.length());
                if (metricTypes.containsKey(baseName)) {
                    return metricTypes.get(baseName);
                }
            }
        }

        return "gauge";
    }
}
