# DazzleDuck Logback

A Logback appender that forwards application logs to a DazzleDuck server in
Apache Arrow format — with zero-configuration auto-discovery.

## Overview

- Captures log events via standard SLF4J/Logback
- Buffers logs in-memory, spills to disk, then sends in Arrow format
- Supports batching, retries, and configurable flush intervals
- Supports column projection and date-based partitioning
- **Auto-discovers component config files — no `logback.xml` required**

## Requirements

- Java 11+
- Logback 1.3+

## Installation

```xml
<dependency>
    <groupId>io.dazzleduck.sql</groupId>
    <artifactId>dazzleduck-sql-logback</artifactId>
</dependency>
```

---

## Quick Start — Auto-discovery (no `logback.xml`)

Drop a `<component>-logback.xml` file into `src/main/resources/`.
`DazzleDuckLogbackConfigurator` is registered via Java SPI and runs automatically
when the JVM starts. It scans the classpath for any file whose name ends in
`-logback.xml`, parses it, and wires up the appender — including a console
appender and infinite-loop exclusion loggers.

**`controller-logback.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<appender name="CONTROLLER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
    <baseUrl>http://localhost:8081</baseUrl>
    <username>admin</username>
    <password>admin</password>
    <ingestionQueue>controller-logs</ingestionQueue>
    <project>*, CAST(timestamp AS DATE) AS date</project>
    <partitionBy>date</partitionBy>
</appender>
```

That is the entire configuration. No `logback.xml` needed.

---

## Multiple Services

### Separate modules — no extra config needed

Each service has its own module and its own `src/main/resources/`. Each JVM
sees only its own file:

```
controller-service/
  src/main/resources/
    controller-logback.xml      ← controller JVM picks this up

executor-service/
  src/main/resources/
    executor-logback.xml        ← executor JVM picks this up

standalone-service/
  src/main/resources/
    standalone-logback.xml      ← standalone JVM picks this up
```

Nothing else needed — the file name is the component identifier.

### Shared `src/main/resources/` — use the component property

When all components live in the same module, all files are on the same
classpath. Set the `dazzleduck.logback.component` system property at startup
to tell the configurator which file to load:

```
# start the controller
java -Ddazzleduck.logback.component=controller -jar myapp.jar

# start the executor
java -Ddazzleduck.logback.component=executor -jar myapp.jar

# start standalone
java -Ddazzleduck.logback.component=standalone -jar myapp.jar
```

With `controller` set, only `controller-logback.xml` is loaded — the other
files are ignored.

With `controller` set, only `controller-logback.xml` is loaded — the other
files are ignored. If the named file does not exist on the classpath, a warning
is logged and Logback falls through to its default configuration.

If the property is not set and multiple `*-logback.xml` files are on the
classpath, one appender is created per file. Use the optional `<logger>`
element to route each appender to a specific logger instead of root:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<appender name="CONTROLLER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
    <baseUrl>http://localhost:8081</baseUrl>
    <username>admin</username>
    <password>admin</password>
    <ingestionQueue>controller-logs</ingestionQueue>
    <project>*, CAST(timestamp AS DATE) AS date</project>
    <partitionBy>date</partitionBy>
    <!-- route this appender to a specific logger instead of root -->
    <logger>com.example.controller</logger>
</appender>
```

### Scenario reference

| Deployment | Action |
|------------|--------|
| Each component has its own module / classpath | Nothing — drop the file, auto-discovery handles it |
| All components share one module / classpath | Pass `-Ddazzleduck.logback.component=<name>` at startup |
| Multiple components in same JVM, each logging to its own queue | No property needed — all files are loaded, use `<logger>` to route |

---

## Component XML Reference

All properties that can appear inside the `<appender>` element:

| Element | Description | Default |
|---------|-------------|---------|
| `baseUrl` | DazzleDuck server URL | `http://localhost:8081` |
| `username` | Authentication username | `admin` |
| `password` | Authentication password | `admin` |
| `ingestionQueue` | Target ingestion queue name | `log` |
| `minBatchSize` | Min bytes to accumulate before sending | `1024` |
| `project` | Comma-separated SQL projection expressions | _(all columns)_ |
| `partitionBy` | Comma-separated partition column names | _(none)_ |
| `logger` | Logger name to attach this appender to | _(root logger)_ |

### Projection and Partitioning

Add derived columns with `project` and split Parquet output files with `partitionBy`:

```xml
<!-- Keep all columns and add a date column derived from timestamp -->
<project>*, CAST(timestamp AS DATE) AS date</project>
<!-- Write one Parquet file per date -->
<partitionBy>date</partitionBy>
```

Add a static host label:

```xml
<project>*, 'my-host' AS host, CAST(timestamp AS DATE) AS date</project>
<partitionBy>date</partitionBy>
```

---

## Log Schema

Each forwarded log entry contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `s_no` | Long | Global sequence number |
| `timestamp` | Timestamp | Event time (millisecond precision) |
| `level` | String | Log level (TRACE, DEBUG, INFO, WARN, ERROR) |
| `logger` | String | Logger name |
| `thread` | String | Thread name |
| `message` | String | Formatted log message |
| `mdc` | Map | MDC context key/value pairs |
| `marker` | String | Log marker (if present) |

---

## Structured Logging with MDC

MDC values are captured automatically in the `mdc` column:

```java
MDC.put("requestId", "REQ-123");
MDC.put("userId", "user-456");
try {
    logger.info("Processing request");
} finally {
    MDC.clear();
}
```

---

## Alternative Configuration Styles

Auto-discovery is the recommended approach, but two alternatives are available
when you need features such as environment-variable substitution (which requires
Logback's own property resolution).

### `logback.xml` with `configFile` reference

Keep a `logback.xml` but store each component's settings in its own file:

```xml
<!-- logback.xml -->
<appender name="CONTROLLER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
    <configFile>controller-logback.xml</configFile>
</appender>

<appender name="EXECUTOR" class="io.dazzleduck.sql.logback.LogForwardingAppender">
    <configFile>executor-logback.xml</configFile>
</appender>

<root level="INFO">
    <appender-ref ref="CONTROLLER"/>
    <appender-ref ref="EXECUTOR"/>
</root>
```

`configFile` is resolved first as a classpath resource, then as a filesystem path.
When `configFile` is set, all inline properties are ignored.

### Inline `logback.xml`

Configure everything directly in `logback.xml` — useful when environment-variable
substitution is needed:

```xml
<!-- logback.xml -->
<property name="BASE_URL" value="${DAZZLEDUCK_BASE_URL:-http://localhost:8081}" />
<property name="QUEUE"    value="${DAZZLEDUCK_QUEUE:-log}" />

<appender name="FORWARDER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
    <baseUrl>${BASE_URL}</baseUrl>
    <username>admin</username>
    <password>admin</password>
    <ingestionQueue>${QUEUE}</ingestionQueue>
    <project>*, CAST(timestamp AS DATE) AS date</project>
    <partitionBy>date</partitionBy>
</appender>

<root level="INFO">
    <appender-ref ref="FORWARDER"/>
</root>
```

> Define a `<property>` first and reference it — do not use `${VAR:-default}`
> directly inside `<baseUrl>`. The appender detects unresolved `${...}` strings
> and logs a clear error at startup.

---

## Programmatic Configuration

```java
LogForwarderConfig config = LogForwarderConfig.builder()
        .baseUrl("http://localhost:8081")
        .username("admin")
        .password("admin")
        .ingestionQueue("log")
        .project(List.of("*", "CAST(timestamp AS DATE) AS date"))
        .partitionBy(List.of("date"))
        .build();

LogForwarder forwarder = new LogForwarder(config);
Runtime.getRuntime().addShutdownHook(new Thread(forwarder::close));
```

---

## Excluded Packages

To prevent infinite loops, logs from these packages are never forwarded:

- `io.dazzleduck.sql.logback.Log*` (LogForwarder, LogForwardingAppender, etc.)
- `io.dazzleduck.sql.client`
- `org.apache.arrow`

When using auto-discovery these exclusions are configured automatically.
When using `logback.xml` directly, add them manually:

```xml
<logger name="io.dazzleduck.sql.logback.LogForwarder" level="INFO" additivity="false">
    <appender-ref ref="CONSOLE"/>
</logger>
<logger name="io.dazzleduck.sql.client" level="INFO" additivity="false">
    <appender-ref ref="CONSOLE"/>
</logger>
<logger name="org.apache.arrow" level="WARN" additivity="false">
    <appender-ref ref="CONSOLE"/>
</logger>
```

---

## Error Handling

| Condition | Behaviour |
|-----------|-----------|
| `dazzleduck.logback.component=controller` but `controller-logback.xml` not found | Warning logged; falls through to `logback.xml` |
| No `*-logback.xml` files found on classpath | Info logged; falls through to `logback.xml` |
| `*-logback.xml` found but unparseable | Error logged at startup; that appender is skipped |
| `baseUrl` not set | Error logged at startup; forwarding skipped |
| `baseUrl` contains unresolved `${...}` | Error logged with the raw value |
| Send failure at runtime | Error logged periodically (not on every event) |
| Queue full | Warning logged every 100 dropped entries |

---

## Disabling Forwarding

At runtime (affects all appender instances):

```java
LogForwardingAppender.setEnabled(false);
```

---

## License

Apache License 2.0
