# DazzleDuck SQL Scrapper

A standalone metrics collector that scrapes Prometheus metrics from configured endpoints and forwards them to any server that accepts Apache Arrow streams via HTTP POST.

## Overview

The DazzleDuck SQL Scrapper is a portable, lightweight module that:

1. **Scrapes** Prometheus-format metrics from one or more HTTP endpoints
2. **Buffers** metrics in memory with tailing mode (threshold-based or interval-based flushing)
3. **Forwards** metrics to a remote server as Apache Arrow streams via HTTP POST

It works with any system that exposes a standard Prometheus text format endpoint — including Spring Boot Actuator, Envoy, Istio, Kafka, JVM exporters, and more.

## Features

- HOCON configuration (`.conf` files)
- Prometheus text format parsing (counter, gauge, histogram, summary)
- Apache Arrow serialization for efficient data transfer
- Multiple scrape targets per instance
- Tailing mode with configurable flush threshold and interval
- Retry logic with exponential backoff
- Graceful shutdown with final flush

## Requirements

- Java 21 or higher
- Maven 3.6+

## Building

```bash
# Build the fat JAR (standalone)
./mvnw clean package -pl dazzleduck-sql-scrapper -DskipTests

# Build the Docker image (uses Jib — no Docker daemon required)
./mvnw package jib:dockerBuild -pl dazzleduck-sql-scrapper -DskipTests
```

The fat JAR lands in `target/dazzleduck-sql-scrapper-<version>.jar`.
The Docker image is tagged `dazzleduck-sql-scrapper:latest`.

---

## Docker

### Building the Image

```bash
./mvnw package jib:dockerBuild -pl dazzleduck-sql-scrapper -DskipTests
```

Jib builds the image directly from Maven without needing a Docker daemon or a `docker build` step.
The output image is `dazzleduck-sql-scrapper:latest`.

### How Environment Variables Work

The container entrypoint reads environment variables whose names start with `collector.` or `logging.`
and passes them as Java system properties (`-D` flags) to the JVM.
This is the recommended way to configure the scrapper in Docker — no config file mounting required.

```
Docker env:  collector.enabled=true
JVM flag:   -Dcollector.enabled=true
HOCON path: collector.enabled
```

> **Note on `collector.targets`:** HOCON does not support list-index notation via system properties
> (e.g. `collector.targets.0=...` is not equivalent to a list entry).
> Use the dedicated `COLLECTOR_TARGETS` environment variable instead — see below.

### Setting Scrape Targets

Use `COLLECTOR_TARGETS` with a comma-separated list of URLs:

```yaml
environment:
  - COLLECTOR_TARGETS=http://envoy:9901/stats/prometheus?usedonly
```

Multiple targets:

```yaml
environment:
  - COLLECTOR_TARGETS=http://envoy-edge:9901/stats/prometheus?usedonly,http://envoy-internal:9901/stats/prometheus?usedonly
```

The entrypoint script converts this into a HOCON list and passes it via `--config` at startup.

### Docker Compose — Envoy Metrics

```yaml
services:

  dazzleduck-scrapper:
    image: dazzleduck-sql-scrapper:latest
    container_name: dazzleduck-scrapper
    depends_on:
      - metrics-log-backend
      - envoy
    environment:
      # ---- what to scrape ----
      - COLLECTOR_TARGETS=http://envoy:9901/stats/prometheus?usedonly

      # ---- where to send ----
      - collector.enabled=true
      - collector.target-prefix=http://envoy:9901
      - collector.base-url=http://metrics-log-backend:8081
      - collector.server-url=http://metrics-log-backend:8081/ingest
      - collector.path=envoy_metrics

      # ---- scrape settings ----
      - collector.scrape-interval-ms=15000

      # ---- auth ----
      - collector.auth.username=admin
      - collector.auth.password=admin

      # ---- identity (shows up in every metric row) ----
      - collector.identity.id=envoy-scraper
      - collector.identity.name=Envoy Scraper
    restart: unless-stopped
```

### Docker Compose — Spring Boot Actuator

```yaml
services:

  dazzleduck-scrapper:
    image: dazzleduck-sql-scrapper:latest
    container_name: dazzleduck-scrapper
    depends_on:
      - my-spring-app
      - dazzleduck-server
    environment:
      - COLLECTOR_TARGETS=http://my-spring-app:8080/actuator/prometheus

      - collector.enabled=true
      - collector.base-url=http://dazzleduck-server:8081
      - collector.server-url=http://dazzleduck-server:8081/ingest
      - collector.path=app_metrics
      - collector.scrape-interval-ms=15000

      - collector.auth.username=admin
      - collector.auth.password=admin

      - collector.identity.id=spring-scraper
      - collector.identity.name=Spring Boot Scraper
    restart: unless-stopped
```

### Mounting a Config File

For more complex configuration (multiple targets, fine-grained tailing settings, custom logging),
mount a HOCON config file into the container:

```yaml
services:

  dazzleduck-scrapper:
    image: dazzleduck-sql-scrapper:latest
    volumes:
      - ./collector.conf:/etc/collector/collector.conf:ro
    command: ["--config", "/etc/collector/collector.conf"]
```

`collector.conf`:

```hocon
collector {
    enabled = true

    targets = [
        "http://envoy:9901/stats/prometheus?usedonly",
        "http://app:8080/actuator/prometheus"
    ]

    base-url   = "http://metrics-log-backend:8081"
    server-url = "http://metrics-log-backend:8081/ingest"
    path = "metrics"

    scrape-interval-ms = 15000

    tailing {
        flush-threshold  = 200
        flush-interval-ms = 3000
        max-buffer-size  = 20000
    }

    auth {
        username = "admin"
        password = "admin"
    }

    identity {
        id   = "multi-scraper"
        name = "Multi-Target Scraper"
    }
}
```

---

## Usage

### Command Line

```bash
# Run with default configuration (application.conf from classpath)
java -jar dazzleduck-sql-scrapper.jar

# Run with custom configuration file
java -jar dazzleduck-sql-scrapper.jar --config /path/to/config.conf
java -jar dazzleduck-sql-scrapper.jar -c /path/to/config.conf

# Show help
java -jar dazzleduck-sql-scrapper.jar --help

# Show version
java -jar dazzleduck-sql-scrapper.jar --version
```

## Configuration

The scrapper uses HOCON format (`.conf` files). Configuration is loaded in the following order — later sources override earlier ones:

1. `application.conf` from classpath (built-in defaults)
2. External config file specified via `--config` argument
3. System properties (highest priority)

### Full Configuration Reference

```hocon
# Logging configuration
logging {
    level = "INFO"
    pattern = "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
    loggers {
        "io.dazzleduck.sql.scrapper" = "INFO"
        "jdk.httpclient" = "WARN"
        "org.apache.arrow" = "WARN"
    }
}

collector {
    # Enable/disable the collector
    enabled = true

    # Target endpoints to scrape metrics from (Prometheus text format)
    # Supports full URLs or relative paths when target-prefix is set
    targets = [
        "/actuator/prometheus"
    ]

    # Optional prefix for relative target paths
    # "http://host:port" + "/actuator/prometheus" → "http://host:port/actuator/prometheus"
    target-prefix = "http://localhost:8080"

    # DazzleDuck server base URL (used for login + ingest)
    base-url = "http://localhost:8081"

    # Full ingest endpoint URL
    server-url = "http://localhost:8081/ingest"

    # Table/path name used when storing metrics on the server
    path = "scraped_metrics"

    # How often to scrape targets (milliseconds)
    scrape-interval-ms = 15000

    # Tailing mode: controls when the buffer is flushed to the server
    tailing {
        # Flush when buffer holds this many metrics
        flush-threshold = 100

        # Flush after this many milliseconds even if threshold is not reached
        flush-interval-ms = 5000

        # Drop oldest metrics when buffer exceeds this size
        max-buffer-size = 10000
    }

    # HTTP client settings (used when scraping target endpoints)
    http {
        connection-timeout-ms = 5000
        read-timeout-ms = 10000
        max-retries = 3
        retry-delay-ms = 1000  # doubles on each retry (exponential backoff)
    }

    # Identity metadata attached to every forwarded metric row
    identity {
        id = "metrics-collector"   # unique ID for this scrapper instance
        name = "Metrics Collector" # human-readable label
        host = ""                  # defaults to system hostname if empty
    }

    # Credentials for authenticating with the DazzleDuck server
    auth {
        username = "admin"
        password = "admin"
    }

    # Arrow producer batching and buffering settings
    producer {
        min-batch-size = 1024         # minimum bytes before a batch is sent
        max-batch-size = 16777216     # 16 MB
        max-in-memory-size = 10485760 # 10 MB in-memory buffer
        max-on-disk-size = 1073741824 # 1 GB on-disk spill buffer
    }
}
```

### Using Full URLs vs. target-prefix

**Single host with relative paths** — use `target-prefix`:

```hocon
collector {
    target-prefix = "http://localhost:8080"
    targets = [
        "/actuator/prometheus",
        "/custom/metrics"
    ]
}
```

**Multiple distinct hosts** — use full URLs directly and leave `target-prefix` empty:

```hocon
collector {
    target-prefix = ""
    targets = [
        "http://service-a:8080/actuator/prometheus",
        "http://service-b:8080/actuator/prometheus"
    ]
}
```

### System Property Overrides

Any config value can be overridden at runtime using `-D` system properties:

```bash
java -Dcollector.enabled=true \
     -Dcollector.target-prefix=http://prod-host:8080 \
     -Dcollector.auth.username=myuser \
     -Dcollector.auth.password=mypass \
     -jar dazzleduck-sql-scrapper.jar
```

---

## Collecting Envoy Metrics

Envoy exposes a standard Prometheus text endpoint at `/stats/prometheus` on its admin port (default `9901`). No code changes are needed — you only need to point the scrapper at that endpoint.

### Single Envoy Instance

```hocon
collector {
    enabled = true

    target-prefix = "http://envoy-host:9901"
    targets = [
        "/stats/prometheus"
    ]

    base-url   = "http://localhost:8081"
    server-url = "http://localhost:8081/ingest"
    path = "envoy_metrics"

    scrape-interval-ms = 15000

    identity {
        id   = "envoy-scraper"
        name = "Envoy Scraper"
    }

    auth {
        username = "admin"
        password = "admin"
    }
}
```

### Multiple Envoy Instances

When scraping more than one Envoy proxy, use full URLs in `targets` so each instance is identified by its `source_url` column in the stored data:

```hocon
collector {
    enabled = true

    target-prefix = ""
    targets = [
        "http://envoy-edge:9901/stats/prometheus",
        "http://envoy-internal:9901/stats/prometheus",
        "http://envoy-egress:9901/stats/prometheus"
    ]

    base-url   = "http://localhost:8081"
    server-url = "http://localhost:8081/ingest"
    path = "envoy_metrics"

    scrape-interval-ms = 15000
}
```

### Reducing Envoy Metric Volume

Envoy can emit thousands of metrics per scrape, most of which are zero-value at startup. Use Envoy's built-in query parameters to filter before the data reaches the scrapper:

| Query Parameter | Effect |
|---|---|
| `?usedonly` | Only metrics updated at least once (recommended) |
| `?filter=cluster` | Only metrics whose name matches the regex |
| `?usedonly&filter=cluster\.my_service` | Combine both |

```hocon
targets = [
    "http://envoy-host:9901/stats/prometheus?usedonly"
]
```

Or to focus on a specific cluster:

```hocon
targets = [
    "http://envoy-host:9901/stats/prometheus?usedonly&filter=envoy_cluster_my_service"
]
```

### Querying Envoy Metrics in DuckDB

Once the scrapper is running, metrics land in the DazzleDuck warehouse and can be queried directly:

```sql
-- All Envoy metrics in the last hour
SELECT timestamp, name, value, labels
FROM read_parquet('warehouse/envoy_metrics/**/*.parquet')
WHERE timestamp > now() - INTERVAL 1 HOUR
ORDER BY timestamp DESC;

-- Upstream request rate by cluster
SELECT
    name,
    labels['envoy_cluster_name'] AS cluster,
    MAX(value) AS requests
FROM read_parquet('warehouse/envoy_metrics/**/*.parquet')
WHERE name = 'envoy_cluster_upstream_rq_total'
GROUP BY 1, 2
ORDER BY requests DESC;

-- P99 latency histogram buckets for a specific cluster
SELECT timestamp, name, labels, value
FROM read_parquet('warehouse/envoy_metrics/**/*.parquet')
WHERE name LIKE 'envoy_cluster_upstream_rq_time_bucket'
  AND labels['envoy_cluster_name'] = 'my_service'
ORDER BY timestamp DESC
LIMIT 100;

-- Compare metrics across multiple Envoy instances
SELECT
    source_url,
    name,
    AVG(value) AS avg_value
FROM read_parquet('warehouse/envoy_metrics/**/*.parquet')
WHERE name = 'envoy_server_uptime'
GROUP BY source_url, name;
```

---

## Data Flow

```
Prometheus Endpoints
(e.g. /stats/prometheus, /actuator/prometheus)
           |
           | HTTP GET text/plain
           v
     MetricsScraper
     - Scrapes at configured interval
     - Parses Prometheus text exposition format
     - Resolves metric type from TYPE comments
           |
           v
     MetricsBuffer  (tailing mode)
     - Thread-safe in-memory ring buffer
     - Flushes when threshold OR interval reached
     - Drops oldest when max-buffer-size exceeded
           |
           v
     MetricsForwarder
     - Converts CollectedMetric to Arrow rows
     - Authenticates with DazzleDuck server (JWT)
     - Batches and sends via HTTP POST
     - Retry with exponential backoff
           |
           v
     DazzleDuck /v1/ingest
     - Stored as Parquet in the warehouse
     - Queryable via DuckDB SQL
```

## Arrow Schema

Every metric row is serialized using this Apache Arrow schema:

| Field | Arrow Type | Description |
|---|---|---|
| `timestamp` | Int64 | Epoch milliseconds when scraped |
| `name` | Utf8 | Metric name |
| `type` | Utf8 | Metric type: gauge, counter, histogram, summary |
| `source_url` | Utf8 | URL the metric was scraped from |
| `collector_id` | Utf8 | Collector instance ID |
| `collector_name` | Utf8 | Collector display name |
| `collector_host` | Utf8 | Hostname of the collector |
| `labels` | Map(Utf8, Utf8) | Prometheus label key-value pairs |
| `value` | Float64 | Metric value (null for NaN or Inf) |

---

## Programmatic Usage

You can embed the scrapper directly in a Java application:

```java
import io.dazzleduck.sql.scrapper.CollectorProperties;
import io.dazzleduck.sql.scrapper.MetricsCollector;
import io.dazzleduck.sql.scrapper.config.CollectorConfig;

import java.util.List;

public class Example {

    public static void main(String[] args) {
        // Option 1: Load from a HOCON config file
        CollectorConfig config = new CollectorConfig("/path/to/config.conf");
        CollectorProperties properties = config.toProperties();

        // Option 2: Configure programmatically
        CollectorProperties props = new CollectorProperties();
        props.setEnabled(true);
        props.setTargets(List.of("http://envoy-host:9901/stats/prometheus?usedonly"));
        props.setBaseUrl("http://localhost:8081");
        props.setServerUrl("http://localhost:8081/ingest");
        props.setPath("envoy_metrics");
        props.setScrapeIntervalMs(15000);
        props.setFlushThreshold(100);
        props.setFlushIntervalMs(5000);
        props.setUsername("admin");
        props.setPassword("admin");

        MetricsCollector collector = new MetricsCollector(props);
        collector.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            collector.stop();
            System.out.println("Sent:    " + collector.getMetricsSentCount());
            System.out.println("Dropped: " + collector.getMetricsDroppedCount());
        }));

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            collector.stop();
        }
    }
}
```

---

## Monitoring

The `MetricsCollector` exposes runtime state for observability:

```java
MetricsCollector collector = ...;

boolean running   = collector.isRunning();
int bufferSize    = collector.getBufferSize();
long sent         = collector.getMetricsSentCount();
long dropped      = collector.getMetricsDroppedCount();
```

---

## Troubleshooting

**Collector starts but no metrics arrive on the server**

- Check that `collector.enabled = true` is set
- Verify the target URL is reachable: `curl http://envoy-host:9901/stats/prometheus`
- Check that `base-url`, `server-url`, and `path` point to the correct DazzleDuck instance
- Increase log level to DEBUG: `logging.level = "DEBUG"`

**Too many metrics / buffer fills up quickly**

- Add `?usedonly` to the target URL to filter zero-value metrics
- Add `?filter=<regex>` to focus on specific metric families
- Reduce `scrape-interval-ms` to scrape less frequently
- Increase `tailing.flush-threshold` and `tailing.flush-interval-ms`

**Authentication failures**

- Confirm `auth.username` and `auth.password` match the DazzleDuck server config
- Check that the server has `http.authentication = "jwt"` enabled if auth is required

**Metrics are scraped but not flushed**

- The buffer only flushes when `flush-threshold` is reached or `flush-interval-ms` elapses
- Lower `tailing.flush-threshold` (e.g. to `10`) for low-volume targets to see data sooner

---

## License

Apache-2.0 — part of the [DazzleDuck SQL Server](https://github.com/dazzleduck/dazzleduck-sql-server) project.
