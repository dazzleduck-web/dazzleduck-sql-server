package io.dazzleduck.sql.collector.metricscollector;

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
     */
    private List<CollectedMetric> parsePrometheusFormat(String body, String sourceUrl) {
        List<CollectedMetric> metrics = new ArrayList<>();
        Map<String, String> metricTypes = new HashMap<>();

        String[] lines = body.split("\n");

        for (String line : lines) {
            line = line.trim();

            if (line.isEmpty() || line.startsWith("# HELP")) {
                continue;
            }

            // Parse TYPE comments
            Matcher typeMatcher = TYPE_COMMENT_PATTERN.matcher(line);
            if (typeMatcher.matches()) {
                metricTypes.put(typeMatcher.group(1), typeMatcher.group(2));
                continue;
            }

            if (line.startsWith("#")) {
                continue;
            }

            // Parse metric lines
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
                    if ("NaN".equals(valueStr)) {
                        value = Double.NaN;
                    } else if ("+Inf".equals(valueStr)) {
                        value = Double.POSITIVE_INFINITY;
                    } else if ("-Inf".equals(valueStr)) {
                        value = Double.NEGATIVE_INFINITY;
                    } else {
                        value = Double.parseDouble(valueStr);
                    }
                } catch (NumberFormatException e) {
                    continue;
                }

                // Get type from TYPE comments, default to "gauge"
                // For histogram/summary, sub-metrics have suffixes like _bucket, _sum, _count
                String type = getMetricType(name, metricTypes);

                metrics.add(new CollectedMetric(
                    name,
                    type,
                    sourceUrl,
                    properties.getCollectorId(),
                    properties.getCollectorName(),
                    collectorHost,
                    labels,
                    value
                ));
            }
        }

        return metrics;
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
