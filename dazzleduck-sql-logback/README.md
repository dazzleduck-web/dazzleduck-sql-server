# DazzleDuck Logback

A Logback appender that forwards application logs to DazzleDuck server using Apache Arrow format.

## Overview

This library provides a custom Logback appender that:
- Captures log events via standard SLF4J/Logback logging
- Buffers logs in-memory with configurable size limits
- Serializes logs to Apache Arrow format for efficient transport
- Forwards logs to DazzleDuck HTTP server
- Supports batching, retries, and disk buffering for reliability
- Supports column projection and date-based partitioning

## Requirements

- Java 11+
- Logback

## Installation

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.dazzleduck.sql</groupId>
    <artifactId>dazzleduck-sql-logback</artifactId>
</dependency>
```

## Quick Start

Configure in `logback.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <!-- Shutdown hook ensures forwarder is flushed on JVM exit -->
    <shutdownHook/>

    <!-- Define properties with defaults -->
    <property name="DAZZLEDUCK_BASE_URL" value="${DAZZLEDUCK_BASE_URL:-http://localhost:8081}" />
    <property name="DAZZLEDUCK_USERNAME" value="${DAZZLEDUCK_USERNAME:-admin}" />
    <property name="DAZZLEDUCK_PASSWORD" value="${DAZZLEDUCK_PASSWORD:-admin}" />
    <property name="DAZZLEDUCK_INGESTION_QUEUE" value="${DAZZLEDUCK_INGESTION_QUEUE:-log}" />

    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <appender name="DAZZLEDUCK" class="io.dazzleduck.sql.logback.LogForwardingAppender">
        <baseUrl>${DAZZLEDUCK_BASE_URL}</baseUrl>
        <username>${DAZZLEDUCK_USERNAME}</username>
        <password>${DAZZLEDUCK_PASSWORD}</password>
        <ingestionQueue>${DAZZLEDUCK_INGESTION_QUEUE}</ingestionQueue>
        <maxBufferSize>10000</maxBufferSize>
        <project>*, CAST(timestamp AS DATE) AS date</project>
        <partitionBy>date</partitionBy>
    </appender>

    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
        <appender-ref ref="DAZZLEDUCK"/>
    </root>
</configuration>
```

That's it. Logs are automatically forwarded to DazzleDuck, partitioned by date.

## Environment Variables

The recommended approach is to define Logback `property` entries that resolve environment variables with fallback defaults, then reference those properties in the appender:

```xml
<property name="DAZZLEDUCK_BASE_URL" value="${DAZZLEDUCK_BASE_URL:-http://localhost:8081}" />
<property name="DAZZLEDUCK_USERNAME" value="${DAZZLEDUCK_USERNAME:-admin}" />
<property name="DAZZLEDUCK_PASSWORD" value="${DAZZLEDUCK_PASSWORD:-admin}" />
<property name="DAZZLEDUCK_INGESTION_QUEUE" value="${DAZZLEDUCK_INGESTION_QUEUE:-log}" />

<appender name="DAZZLEDUCK" class="io.dazzleduck.sql.logback.LogForwardingAppender">
    <baseUrl>${DAZZLEDUCK_BASE_URL}</baseUrl>
    <username>${DAZZLEDUCK_USERNAME}</username>
    <password>${DAZZLEDUCK_PASSWORD}</password>
    <ingestionQueue>${DAZZLEDUCK_INGESTION_QUEUE}</ingestionQueue>
</appender>
```

> **Note:** Do not use the `${VAR:-default}` syntax directly in `<baseUrl>` — define a `<property>` first.
> The appender detects unresolved `${...}` variables and logs a clear error at startup.

## Programmatic Configuration

```java
import io.dazzleduck.sql.logback.LogForwarder;
import io.dazzleduck.sql.logback.LogForwarderConfig;
import java.util.List;

LogForwarderConfig config = LogForwarderConfig.builder()
        .baseUrl("http://localhost:8081")
        .username("admin")
        .password("admin")
        .ingestionQueue("log")
        .project(List.of("*", "CAST(timestamp AS DATE) AS date"))
        .partitionBy(List.of("date"))
        .build();

// LogForwarder auto-starts on construction
LogForwarder forwarder = new LogForwarder(config);

// Cleanup on shutdown
Runtime.getRuntime().addShutdownHook(new Thread(forwarder::close));
```

## Configuration Options

| Property | Description | Default |
|----------|-------------|---------|
| `baseUrl` | DazzleDuck server URL | `http://localhost:8081` |
| `username` | Authentication username | `admin` |
| `password` | Authentication password | `admin` |
| `ingestionQueue` | Target ingestion queue | `log` |
| `maxBufferSize` | Max log entries to buffer | `10000` |
| `pollInterval` | How often to send logs | `5 seconds` |
| `minBatchSize` | Min batch size before sending | `1 KB` |
| `maxBatchSize` | Max batch size per request | `10 MB` |
| `maxSendInterval` | Max time between sends | `2 seconds` |
| `httpClientTimeout` | HTTP client timeout | `30 seconds` |
| `project` | Comma-separated projection expressions | _(none)_ |
| `partitionBy` | Comma-separated partition columns | _(none)_ |
| `enabled` | Enable/disable forwarding | `true` |

## Projection and Partitioning

Use `project` to add derived columns and `partitionBy` to partition output files:

```xml
<appender name="DAZZLEDUCK" class="io.dazzleduck.sql.logback.LogForwardingAppender">
    <baseUrl>http://localhost:8081</baseUrl>
    <ingestionQueue>log</ingestionQueue>
    <!-- Keep all columns and add a derived date column -->
    <project>*, CAST(timestamp AS DATE) AS date</project>
    <!-- Partition Parquet files by date -->
    <partitionBy>date</partitionBy>
</appender>
```

You can also add static host labels:

```xml
<project>*, 'myhost' AS application_host, CAST(timestamp AS DATE) AS date</project>
<partitionBy>date</partitionBy>
```

## Log Schema

Logs are stored with the following Arrow schema:

| Column | Type | Description |
|--------|------|-------------|
| `s_no` | Long | Sequence number |
| `timestamp` | Timestamp | Timestamp (millisecond precision) |
| `level` | String | Log level (TRACE, DEBUG, INFO, WARN, ERROR) |
| `logger` | String | Logger name |
| `thread` | String | Thread name |
| `message` | String | Formatted log message |
| `mdc` | Map | MDC context values |
| `marker` | String | Log marker (if any) |

## Structured Logging with MDC

```java
import org.slf4j.MDC;

MDC.put("requestId", "REQ-123");
MDC.put("userId", "user-456");
try {
    logger.info("Processing request");
} finally {
    MDC.clear();
}
```

MDC values are captured in the `mdc` column as a map.

## Multiple Appenders

Each appender instance maintains its own independent `LogForwarder`, so multiple appenders can coexist in the same JVM with different configurations:

```xml
<appender name="LOG_FORWARDER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
    <baseUrl>http://localhost:8081</baseUrl>
    <ingestionQueue>log</ingestionQueue>
</appender>

<appender name="AUDIT_FORWARDER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
    <baseUrl>http://audit-server:8081</baseUrl>
    <ingestionQueue>audit</ingestionQueue>
</appender>

<root level="INFO">
    <appender-ref ref="LOG_FORWARDER"/>
</root>

<logger name="com.example.audit" level="INFO" additivity="false">
    <appender-ref ref="AUDIT_FORWARDER"/>
</logger>
```

## Excluding Packages

To prevent infinite loops, logs from these packages are excluded:

- `io.dazzleduck.sql.logback.Log` (LogForwarder, LogEntry, LogForwardingAppender)
- `io.dazzleduck.sql.client`
- `org.apache.arrow`

## Error Handling

The appender provides clear startup diagnostics:

- If `baseUrl` is not set, an error is logged and forwarding is skipped.
- If `baseUrl` contains unresolved `${...}` variables, an error is logged with the raw value.
- If forwarding fails at runtime, errors are logged periodically (not on every event) to avoid spam.
- If the queue is full, a warning is logged every 100 entries.

## Disabling Forwarding

At runtime:

```java
LogForwardingAppender.setEnabled(false);
```

Or configure with `<enabled>false</enabled>` in `logback.xml`.

## License

Apache License 2.0
