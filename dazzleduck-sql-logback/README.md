# DazzleDuck Logback

A Logback appender that forwards application logs to a DazzleDuck server in
Apache Arrow format.

## Overview

- Captures log events via standard SLF4J/Logback
- Buffers logs in-memory, spills to disk, then sends in Arrow format
- Supports batching, retries, and configurable flush intervals
- Supports column projection and date-based partitioning
- Standard Logback XML configuration — works with any filename via `-Dlogback.configurationFile`

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

## Quick Start

Create a standard Logback XML file (any filename) in `src/main/resources/`:

**`myapp-logback.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>

    <shutdownHook/>

    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <appender name="LOG_FORWARDER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
        <baseUrl>http://localhost:8081</baseUrl>
        <username>admin</username>
        <password>admin</password>
        <ingestionQueue>app-logs</ingestionQueue>
        <project>*, CAST(timestamp AS DATE) AS date</project>
        <partitionBy>date</partitionBy>
    </appender>

    <!-- Exclude internal packages to prevent infinite loops -->
    <logger name="io.dazzleduck.sql.logback" level="INFO" additivity="false">
        <appender-ref ref="CONSOLE"/>
    </logger>
    <logger name="io.dazzleduck.sql.client" level="INFO" additivity="false">
        <appender-ref ref="CONSOLE"/>
    </logger>
    <logger name="org.apache.arrow" level="WARN" additivity="false">
        <appender-ref ref="CONSOLE"/>
    </logger>

    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
        <appender-ref ref="LOG_FORWARDER"/>
    </root>

</configuration>
```

Tell Logback which file to load at startup:

```bash
java -Dlogback.configurationFile=myapp-logback.xml -jar myapp.jar
```

Logback resolves the name as a classpath resource first, then as a filesystem path.

---

## Multiple Components — One `src/main/resources/`

When two components share the same module and resource directory, each component
gets its own XML file and its own `-Dlogback.configurationFile` at startup.

**File layout:**

```
src/main/resources/
    abx-logback.xml     ← component ABX
    fn-logback.xml      ← component FN
```

**Start each component separately:**

```bash
# Component ABX
java -Dlogback.configurationFile=abx-logback.xml -jar myapp.jar

# Component FN
java -Dlogback.configurationFile=fn-logback.xml -jar myapp.jar
```

Because each component is a separate JVM process, each reads only its own file.
No custom configurator or auto-discovery is needed.

---

## Setting the Property in Maven

### Running with `exec:java`

```bash
mvn exec:java \
  -Dexec.mainClass="com.example.Main" \
  -Dlogback.configurationFile=abx-logback.xml
```

### Maven Surefire (tests)

Via `systemPropertyVariables` in `pom.xml`:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <configuration>
        <systemPropertyVariables>
            <logback.configurationFile>abx-logback.xml</logback.configurationFile>
        </systemPropertyVariables>
    </configuration>
</plugin>
```

Or via `argLine`:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <configuration>
        <argLine>-Dlogback.configurationFile=abx-logback.xml</argLine>
    </configuration>
</plugin>
```

### Maven Exec Plugin in `pom.xml`

```xml
<plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>exec-maven-plugin</artifactId>
    <configuration>
        <mainClass>com.example.Main</mainClass>
        <systemProperties>
            <systemProperty>
                <key>logback.configurationFile</key>
                <value>abx-logback.xml</value>
            </systemProperty>
        </systemProperties>
    </configuration>
</plugin>
```

---

## File Resolution

`-Dlogback.configurationFile` accepts:

| Value | Resolved as |
|-------|-------------|
| `abx-logback.xml` | Classpath resource |
| `./config/abx-logback.xml` | Relative filesystem path |
| `/etc/myapp/abx-logback.xml` | Absolute filesystem path |
| `file:/etc/myapp/abx-logback.xml` | File URL |

---

## Appender XML Reference

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
| `configFile` | Path to a TypeSafe Config `.conf` file (overrides all inline properties) | _(none)_ |

### Projection and Partitioning

Add derived columns with `project` and split Parquet output by column with `partitionBy`:

```xml
<!-- Keep all columns, add a date column derived from timestamp -->
<project>*, CAST(timestamp AS DATE) AS date</project>
<!-- Write one Parquet file per date -->
<partitionBy>date</partitionBy>
```

Add a static host label:

```xml
<project>*, 'my-host' AS host, CAST(timestamp AS DATE) AS date</project>
<partitionBy>date</partitionBy>
```

### Using a TypeSafe Config File

Instead of inline properties you can point to a `.conf` file:

```xml
<appender name="LOG_FORWARDER" class="io.dazzleduck.sql.logback.LogForwardingAppender">
    <configFile>abx-logback.conf</configFile>
</appender>
```

`configFile` is resolved as a classpath resource first, then as a filesystem path.
When set, all inline properties are ignored.

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

## Excluded Packages

To prevent infinite loops, logs from these packages are never forwarded:

- `io.dazzleduck.sql.logback.Log*`
- `io.dazzleduck.sql.client`
- `org.apache.arrow`

Add the following to your logback XML to route these to console only:

```xml
<logger name="io.dazzleduck.sql.logback" level="INFO" additivity="false">
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

## Error Handling

| Condition | Behaviour |
|-----------|-----------|
| `baseUrl` not set and `configFile` not set | Error logged at startup; forwarding skipped |
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
