# Arrow-Micrometer

A Micrometer metrics library that forwards application metrics to any HTTP server using Apache Arrow format.

## Overview

This library provides a custom Micrometer `MeterRegistry` implementation that:
- Collects standard Micrometer metrics (counters, gauges, timers, distribution summaries, etc.)
- Serializes metrics to Apache Arrow format for efficient transport
- Forwards metrics to any HTTP server that accepts Arrow data
- Supports batching, retries, and disk buffering for reliability

## Requirements

- Java 21+
- Maven 3.6+

## Installation

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.dazzleduck.sql</groupId>
    <artifactId>dazzleduck-sql-micrometer</artifactId>
</dependency>
```

## Quick Start

### Option 1: Configuration File (Recommended)

Create an `application.conf` file in your resources:

```hocon
dazzleduck_micrometer = {
  application_id   = "my-app"
  application_name = "My Application"
  application_host = "localhost"
  enabled          = true

  step_interval_ms = 10000

  min_batch_size       = 1048576    # 1 MB
  max_batch_size       = 16777216   # 16 MB
  max_send_interval_ms = 1000
  max_in_memory_bytes  = 10485760   # 10 MB
  max_on_disk_bytes    = 1073741824 # 1 GB
  retry_count          = 3
  retry_interval_ms    = 1000
  transformations      = []
  partition_by         = []

  http {
    base_url               = "http://localhost:8081"
    username               = "admin"
    password               = "admin"
    target_path            = "metrics"
    http_client_timeout_ms = 5000
  }
}
```

Then use the factory:

```java
import io.dazzleduck.sql.micrometer.metrics.MetricsRegistryFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

public class MetricsExample {
    public static void main(String[] args) {
        // Create registry from application.conf
        MeterRegistry registry = MetricsRegistryFactory.create();

        // Record metrics
        Counter requestCounter = registry.counter("http.requests", "method", "GET", "ingestionQueue", "/api/users");
        requestCounter.increment();

        // ... your application code ...
    }
}
```

### Option 2: Programmatic Configuration

```java
import io.dazzleduck.sql.micrometer.MicrometerForwarder;
import io.dazzleduck.sql.micrometer.config.MicrometerForwarderConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.Duration;

public class MetricsExample {
    public static void main(String[] args) {
        // Build configuration
        MicrometerForwarderConfig config = MicrometerForwarderConfig.builder()
                .baseUrl("http://localhost:8081")
                .username("admin")
                .password("admin")
                .targetPath("metrics")
                .applicationId("my-app")
                .applicationName("My Application")
                .stepInterval(Duration.ofSeconds(10))
                .build();

        // Create and start the forwarder
        MicrometerForwarder forwarder = MicrometerForwarder.createAndStart(config);

        // Get the registry and record metrics
        MeterRegistry registry = forwarder.getRegistry();

        Counter requestCounter = registry.counter("http.requests", "method", "GET", "ingestionQueue", "/api/users");
        requestCounter.increment();

        // Cleanup on shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(forwarder::close));
    }
}
```

## Usage Examples

### Counter

```java
Counter counter = registry.counter("orders.placed", "region", "us-east");
counter.increment();
counter.increment(5);
```

### Gauge

```java
AtomicInteger activeConnections = new AtomicInteger(0);
registry.gauge("db.connections.active", activeConnections);

activeConnections.set(10);
```

### Timer

```java
Timer timer = registry.timer("http.request.duration", "endpoint", "/api/orders");

// Record duration manually
timer.record(Duration.ofMillis(250));

// Or wrap a callable
String result = timer.record(() -> {
    return processRequest();
});

// Or use a sample
Timer.Sample sample = Timer.start(registry);
// ... do work ...
sample.stop(timer);
```

### Distribution Summary

```java
DistributionSummary summary = registry.summary("order.value", "currency", "USD");
summary.record(99.99);
summary.record(149.50);
```

### Tags (Labels)

```java
// Add common tags to all metrics
registry.config().commonTags("env", "production", "service", "order-service");

// Add tags per metric
Counter counter = Counter.builder("api.calls")
        .tag("method", "GET")
        .tag("ingestionQueue", "/users")
        .tag("status", "200")
        .register(registry);
```

## Configuration Options

| Property | Description | Default |
|----------|-------------|---------|
| `applicationId` | Unique identifier for your application | `default-app` |
| `applicationName` | Human-readable application name | `DefaultApplication` |
| `applicationHost` | Hostname of the application | auto-detected |
| `baseUrl` | Target server URL | `http://localhost:8081` |
| `username` | Authentication username | `admin` |
| `password` | Authentication password | `admin` |
| `targetPath` | Server endpoint path | `metrics` |
| `stepInterval` | How often metrics are published | `10 seconds` |
| `httpClientTimeout` | HTTP request timeout | `3 seconds` |
| `minBatchSize` | Minimum batch size before sending | `1 MB` |
| `maxBatchSize` | Maximum batch size per request | `10 MB` |
| `maxSendInterval` | Maximum time between sends | `2 seconds` |
| `maxInMemorySize` | Max memory buffer size | `10 MB` |
| `maxOnDiskSize` | Max disk buffer size | `1 GB` |
| `retryCount` | Number of retry attempts | `3` |
| `retryIntervalMillis` | Delay between retries | `1000 ms` |
| `transformations` | SQL transformations for metrics | `[]` |
| `partitionBy` | Partition columns for storage | `[]` |
| `enabled` | Enable/disable forwarding | `true` |

## Metrics Schema

Metrics are stored with the following Arrow schema:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | Timestamp | Timestamp (millisecond precision) |
| `name` | String | Metric name |
| `type` | String | Metric type (counter, gauge, timer, etc.) |
| `tags` | `Map<String, String>` | Metric tags/labels |
| `value` | Double | Primary metric value |
| `min` | Double | Minimum value (for distributions) |
| `max` | Double | Maximum value (for distributions) |
| `mean` | Double | Mean value (for distributions) |

## Spring Boot Integration

```java
import io.dazzleduck.sql.micrometer.MicrometerForwarder;
import io.dazzleduck.sql.micrometer.config.MicrometerForwarderConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class MetricsConfig {

    @Bean
    public MicrometerForwarder micrometerForwarder() {
        MicrometerForwarderConfig config = MicrometerForwarderConfig.builder()
                .baseUrl("http://metrics-server:8081")
                .username("admin")
                .password("admin")
                .targetPath("metrics")
                .applicationId("spring-app")
                .applicationName("Spring Boot Application")
                .stepInterval(Duration.ofSeconds(15))
                .enabled(true)
                .build();

        return MicrometerForwarder.createAndStart(config);
    }

    @Bean
    public MeterRegistry meterRegistry(MicrometerForwarder forwarder) {
        return forwarder.getRegistry();
    }
}
```

## Lifecycle Management

```java
MicrometerForwarder forwarder = MicrometerForwarder.createAndStart(config);

// Check status
boolean running = forwarder.isRunning();
boolean enabled = forwarder.isEnabled();

// Graceful shutdown - flushes pending metrics
forwarder.close();
```

## Disabling Metrics

Via code:

```java
MicrometerForwarderConfig config = MicrometerForwarderConfig.builder()
        .enabled(false)
        .build();
```

Or via `application.conf`:

```hocon
dazzleduck_micrometer {
  enabled = false
}
```

## Troubleshooting

### Metrics not appearing on server

1. Check that the server URL is correct and accessible
2. Verify authentication credentials
3. Ensure `enabled = true` in `application.conf`
4. Verify the forwarder is started before recording metrics

### High memory usage

Reduce buffer sizes:

```java
.maxInMemorySize(5 * 1024 * 1024)  // 5 MB
.maxBatchSize(5 * 1024 * 1024)     // 5 MB
```

### Slow metric publishing

Adjust timing parameters:

```java
.stepInterval(Duration.ofSeconds(30))
.maxSendInterval(Duration.ofSeconds(5))
```

## License

Apache License 2.0
