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
  enabled          = true

  step_interval_ms = 10000

  min_batch_size       = 1048576    # 1 MB
  max_batch_size       = 16777216   # 16 MB
  max_send_interval_ms = 1000
  max_in_memory_bytes  = 10485760   # 10 MB
  max_on_disk_bytes    = 1073741824 # 1 GB
  retry_count          = 3
  retry_interval_ms    = 1000
  project              = ["*"]
  partition_by         = []

  http {
    base_url               = "http://localhost:8081"
    username               = "admin"
    password               = "admin"
    ingestion_queue        = "metrics"
    http_client_timeout_ms = 5000

    # Optional: JWT claims for row-level security (RESTRICTED access mode)
    claims = {
      database = "metrics_db"
      schema   = "app_metrics"
      table    = "events"
    }
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
| `username` | Authentication username (login mode) | `admin` |
| `password` | Authentication password (login mode) | `admin` |
| `jwt` | Preconfigured `Bearer` token string (skips login; builder-only, mutually exclusive with username/password) | _(none)_ |
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
| `claims` | JWT claims passed to the server at login for row-level security | `{}` (empty) |
| `transformations` | SQL transformations for metrics | `[]` |
| `partitionBy` | Partition columns for storage | `[]` |
| `enabled` | Enable/disable forwarding | `true` |

## Authentication

The forwarder supports two mutually exclusive authentication modes.

### Username / Password (default)

The forwarder logs in to `/v1/login` on startup and caches the JWT. The token is
refreshed automatically before it expires.

```java
MicrometerForwarderConfig config = MicrometerForwarderConfig.builder()
        .baseUrl("http://localhost:8081")
        .username("admin")
        .password("admin")
        .ingestionQueue("metrics")
        .stepInterval(Duration.ofSeconds(10))
        .build();
```

### Preconfigured JWT

When your deployment already provides a token (e.g. a sidecar injects it via an
environment variable, or an external auth service issues it), pass the full
`Bearer <token>` string directly. Login is skipped entirely and the token is used
as-is for every request. If the server rejects it the call fails immediately —
there is no re-login attempt.

```java
MicrometerForwarderConfig config = MicrometerForwarderConfig.builder()
        .baseUrl("http://localhost:8081")
        .jwt("Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
        .ingestionQueue("metrics")
        .stepInterval(Duration.ofSeconds(10))
        .build();

MicrometerForwarder forwarder = MicrometerForwarder.createAndStart(config);
```

`jwt` and `username`/`password` are mutually exclusive. Setting `jwt` overrides
the login flow; `username` and `password` are ignored when `jwt` is present.

Note: the preconfigured JWT is available only through the programmatic builder —
`MetricsRegistryFactory` / `application.conf` supports the username/password mode.

---

## JWT Claims

Claims are key-value pairs sent to the server during login. The server embeds them in the JWT token, which is then used for row-level security and query filtering on subsequent requests.

Use claims when the server is configured with `access_mode = RESTRICTED` and you need to scope metric ingestion to a specific database, schema, or table. Claims apply only in username/password mode — when using a preconfigured JWT the token already encodes any required claims.

### Common Claim Keys

Claims are forwarded verbatim to `/v1/login`; the server embeds them in the JWT. The claim
names the server understands are:

| Key | Description |
|-----|-------------|
| `database` | Target database (unprefixed — Flight SQL / JDBC interop) |
| `schema` | Target schema (unprefixed) |
| `x-dd-table` | Authorized table |
| `x-dd-filter` | Row-level filter expression |
| `x-dd-path` | Authorized storage path prefix |
| `x-dd-function` | Authorized table function |
| `x-dd-access` | Full access tuple list (see the root README) |

### Programmatic Configuration

Claims are set via the builder:

```java
MicrometerForwarderConfig config = MicrometerForwarderConfig.builder()
        .baseUrl("http://localhost:8081")
        .username("admin")
        .password("admin")
        .claims(Map.of(
                "database",   "metrics_db",
                "schema",     "app_metrics",
                "x-dd-table", "events"
        ))
        .ingestionQueue("metrics")
        .stepInterval(Duration.ofSeconds(10))
        .build();

MicrometerForwarder forwarder = MicrometerForwarder.createAndStart(config);
```

> Claims are also read automatically from `application.conf` when using `MetricsRegistryFactory`.
> Set them under `dazzleduck_micrometer.http.claims` as shown in the Quick Start section above.

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
