# DazzleDuck SQL Scrapper

A standalone metrics collector that scrapes Prometheus metrics from configured endpoints and forwards them to any server that accepts Apache Arrow streams via HTTP POST.

## Overview

The DazzleDuck SQL Scrapper is a portable, lightweight module that:

1. **Scrapes** Prometheus-format metrics from one or more endpoints
2. **Buffers** metrics in memory with tailing mode (threshold-based or interval-based flushing)
3. **Forwards** metrics to a remote server as Apache Arrow streams via HTTP POST

## Features

- HOCON configuration (`.conf` files)
- Prometheus text format parsing
- Apache Arrow serialization for efficient data transfer
- Tailing mode with configurable flush threshold and interval
- Retry logic with exponential backoff
- Graceful shutdown handling

## Requirements

- Java 21 or higher
- Maven 3.6+

## Building

```bash
./mvnw clean package
```

This produces an executable fat JAR in `target/dazzleduck-sql-scrapper-<version>.jar`.

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

### Configuration

The scrapper uses HOCON format (`.conf` files) for configuration. Configuration is loaded in the following order (later sources override earlier):

1. `application.conf` from classpath (defaults)
2. External config file specified via `--config` argument
3. System properties (highest priority)

#### Sample Configuration (`application.conf`)

All configuration uses HOCON format - no XML files required.

```hocon
# Logging configuration (replaces logback.xml)
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

    # Target endpoints to scrape (Prometheus format)
    targets = [
        "/actuator/prometheus"
    ]

    # Optional prefix for target URLs (relative paths resolved against this)
    target-prefix = "http://localhost:8080"

    # Server URL to send collected metrics (any Arrow-compatible endpoint)
    server-url = "http://localhost:8081"

    # Path parameter appended to server URL query string
    path = "scraped_metrics"

    # Scrape interval in milliseconds
    scrape-interval-ms = 15000

    # Tailing mode settings
    tailing {
        # Flush when buffer reaches this number of metrics
        flush-threshold = 100

        # Maximum time in milliseconds before flushing buffer
        flush-interval-ms = 5000

        # Maximum metrics to buffer before dropping oldest
        max-buffer-size = 10000
    }

    # HTTP client settings
    http {
        connection-timeout-ms = 5000
        read-timeout-ms = 10000
        max-retries = 3
        retry-delay-ms = 1000
    }

    # Collector identity (included in forwarded metrics)
    identity {
        id = "metrics-collector"
        name = "Metrics Collector"
        host = ""  # Defaults to hostname if empty
    }

    # Server authentication (if required by target server)
    auth {
        username = "admin"
        password = "admin"
    }

    # HttpProducer settings (batching and buffering)
    producer {
        # Minimum batch size in bytes before sending
        min-batch-size = 1024

        # Maximum batch size in bytes
        max-batch-size = 16777216

        # Maximum in-memory buffer size in bytes (10 MB)
        max-in-memory-size = 10485760

        # Maximum on-disk buffer size in bytes (1 GB)
        max-on-disk-size = 1073741824
    }
}
```

### Programmatic Usage

You can also use the scrapper programmatically in your Java application:

```java
import io.dazzleduck.sql.scrapper.CollectorProperties;
import io.dazzleduck.sql.scrapper.MetricsCollector;
import io.dazzleduck.sql.scrapper.config.CollectorConfig;

import java.util.List;

public class Example {

    public static void main(String[] args) {
        // Option 1: Load from configuration file
        CollectorConfig config = new CollectorConfig("/path/to/config.conf");
        CollectorProperties properties = config.toProperties();

        // Option 2: Configure programmatically
        CollectorProperties props = new CollectorProperties();
        props.setEnabled(true);
        props.setTargets(List.of("/actuator/prometheus"));
        props.setTargetPrefix("http://localhost:8080");
        props.setServerUrl("http://target-server:8081");
        props.setPath("my_metrics");
        props.setScrapeIntervalMs(15000);
        props.setFlushThreshold(100);
        props.setFlushIntervalMs(5000);

        // Authentication settings
        props.setUsername("admin");
        props.setPassword("admin");

        // Producer settings
        props.setMinBatchSize(1024);
        props.setMaxBatchSize(16 * 1024 * 1024);
        props.setMaxInMemorySize(10 * 1024 * 1024);
        props.setMaxOnDiskSize(1024 * 1024 * 1024);

        // Create and start the collector
        MetricsCollector collector = new MetricsCollector(props);
        collector.start();

        // Register shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            collector.stop();
            System.out.println("Metrics sent: " + collector.getMetricsSentCount());
            System.out.println("Metrics dropped: " + collector.getMetricsDroppedCount());
        }));

        // Keep the application running
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            collector.stop();
        }
    }
}
```

### Environment Variable Overrides

Configuration values can be overridden using system properties:

```bash
java -Dcollector.enabled=true \
     -Dcollector.server-url=http://prod-server:8081/v1/ingest \
     -jar dazzleduck-sql-scrapper.jar
```

## Data Flow

```
┌─────────────────────────────────────────────────────────┐
│ Prometheus Endpoints                                    │
│ (e.g., /actuator/prometheus)                           │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│ MetricsScraper                                          │
│ - Scrapes at configured interval                        │
│ - Parses Prometheus text format                         │
│ - Creates CollectedMetric objects                       │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│ MetricsBuffer (Tailing Mode)                            │
│ - Thread-safe in-memory buffer                          │
│ - Flushes on threshold OR interval                      │
│ - Drops oldest when max size exceeded                   │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│ MetricsForwarder (uses HttpProducer)                    │
│ - Serializes to Apache Arrow format                     │
│ - Authenticates with target server (JWT)                │
│ - Batches and sends via HTTP POST                       │
│ - Retry with exponential backoff                        │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│ Target Server                                           │
│ (Any server accepting Arrow streams via HTTP POST)      │
└─────────────────────────────────────────────────────────┘
```

## Arrow Schema

Metrics are serialized using the following Apache Arrow schema:

| Field           | Type          | Description                      |
|-----------------|---------------|----------------------------------|
| timestamp       | Int64         | Epoch milliseconds               |
| name            | Utf8          | Metric name                      |
| type            | Utf8          | Metric type (gauge, counter, etc)|
| source_url      | Utf8          | Source endpoint URL              |
| collector_id    | Utf8          | Collector instance ID            |
| collector_name  | Utf8          | Collector display name           |
| collector_host  | Utf8          | Collector hostname               |
| labels          | Map<Utf8,Utf8>| Prometheus labels                |
| value           | Float64       | Metric value                     |

## Monitoring

The collector exposes monitoring information:

```java
MetricsCollector collector = ...;

// Check if running
boolean running = collector.isRunning();

// Get buffer status
int bufferSize = collector.getBufferSize();

// Get send statistics
long sent = collector.getMetricsSentCount();
long dropped = collector.getMetricsDroppedCount();
```

## License

This project is part of the DazzleDuck SQL Server project.
