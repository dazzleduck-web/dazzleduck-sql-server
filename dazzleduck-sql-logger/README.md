# DazzleDuck SQL Logger

A drop-in SLF4J logging provider that sends logs directly to a DazzleDuck server in Apache Arrow format. Logs are batched, compressed, and stored as Parquet files for high-performance analytics.

## Features

- **SLF4J 2.0 Compatible** - Use standard SLF4J API, logs are automatically sent to DazzleDuck
- **Apache Arrow Format** - Efficient columnar serialization for fast analytics
- **Automatic Batching** - Logs are batched based on size and time for optimal throughput
- **MDC Support** - Full support for Mapped Diagnostic Context
- **Marker Support** - Log markers are captured and stored
- **Fault Tolerant** - Retry logic with disk spillover for backpressure handling

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Spring Boot Integration](#spring-boot-integration)
3. [Configuration](#configuration)
4. [Configuration Methods](#configuration-methods)
5. [Log Schema](#log-schema)
6. [Usage Examples](#usage-examples)
7. [Log File Tailing](#advanced-log-file-tailing)
8. [Troubleshooting](#troubleshooting)
9. [Architecture](#architecture)

---

## Quick Start

### 1. Add Dependency

```xml
<dependency>
    <groupId>io.dazzleduck.sql</groupId>
    <artifactId>dazzleduck-sql-logger</artifactId>
    <version>0.0.13-SNAPSHOT</version>
</dependency>
```

**Important:** This library IS your SLF4J provider. Do not include any other SLF4J provider (like `slf4j-simple`, `logback-classic`, or `log4j-slf4j-impl`) in your runtime classpath.

### 2. Configure

Create `application.conf` in your classpath (e.g., `src/main/resources/`):

```hocon
dazzleduck_logger {
  application_id   = "my-app-001"
  application_name = "MyApplication"
  application_host = "localhost"
  log_level        = "INFO"

  http {
    base_url               = "http://localhost:8081"
    username               = "admin"
    password               = "admin"
    target_path            = "logs/my-app"
    http_client_timeout_ms = 30000
  }

  min_batch_size       = 100
  max_batch_size       = 10000
  max_send_interval_ms = 5000
  retry_count          = 3
  retry_interval_ms    = 1000
  max_in_memory_bytes  = 10485760
  max_on_disk_bytes    = 104857600
  transformations      = []
  partition_by         = []
}
```

### 3. Use Standard SLF4J API

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyApplication {
    private static final Logger logger = LoggerFactory.getLogger(MyApplication.class);

    public static void main(String[] args) {
        logger.info("Application started");
        logger.debug("Debug value: {}", 42);
        logger.warn("Warning: {} items remaining", 5);
        logger.error("Error occurred", new RuntimeException("Something went wrong"));
    }
}
```

---

## Spring Boot Integration

Since this library replaces SLF4J's default provider, you must exclude Logback from Spring Boot.

### Step 1: Update `pom.xml`

```xml
<dependencies>
    <!-- Spring Boot with Logback excluded -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter</artifactId>
        <exclusions>
            <exclusion>
                <groupId>ch.qos.logback</groupId>
                <artifactId>logback-classic</artifactId>
            </exclusion>
        </exclusions>
    </dependency>

    <!-- For web applications -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
        <exclusions>
            <exclusion>
                <groupId>ch.qos.logback</groupId>
                <artifactId>logback-classic</artifactId>
            </exclusion>
        </exclusions>
    </dependency>

    <!-- DazzleDuck Logger -->
    <dependency>
        <groupId>io.dazzleduck.sql</groupId>
        <artifactId>dazzleduck-sql-logger</artifactId>
        <version>0.0.13-SNAPSHOT</version>
    </dependency>
</dependencies>
```

### Step 2: Create `src/main/resources/application.conf`

```hocon
dazzleduck_logger {
  application_id   = "spring-boot-app"
  application_name = "MySpringBootApplication"
  application_host = "localhost"
  log_level        = "INFO"

  http {
    base_url               = "http://localhost:8081"
    username               = "admin"
    password               = "admin"
    target_path            = "logs/spring-app"
    http_client_timeout_ms = 30000
  }

  min_batch_size       = 100
  max_batch_size       = 10000
  max_send_interval_ms = 5000
  retry_count          = 3
  retry_interval_ms    = 1000
  max_in_memory_bytes  = 10485760
  max_on_disk_bytes    = 104857600
  transformations      = []
  partition_by         = []
}
```

### Step 3: Use in Your Spring Boot Application

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class MyController {

    private static final Logger log = LoggerFactory.getLogger(MyController.class);

    @GetMapping("/hello")
    public String hello(@RequestParam String name) {
        log.info("Hello endpoint called with name: {}", name);
        return "Hello, " + name + "!";
    }

    @PostMapping("/process")
    public String process(@RequestBody String data) {
        log.debug("Processing data: {}", data);
        try {
            // Process data
            log.info("Data processed successfully");
            return "OK";
        } catch (Exception e) {
            log.error("Failed to process data", e);
            throw e;
        }
    }
}
```

### Step 4: Run Your Application

```bash
./mvnw spring-boot:run
```

**Note:** You will lose Spring Boot's colored console output since Logback is excluded. All logs go directly to DazzleDuck server.

---

## Configuration

### Full Configuration Reference

```hocon
dazzleduck_logger {
  # ─────────────────────────────────────────────────────────────────
  # Application Identification
  # These fields appear in every log record for filtering/grouping
  # ─────────────────────────────────────────────────────────────────
  application_id   = "my-app-001"      # Unique identifier
  application_name = "MyApplication"   # Human-readable name
  application_host = "localhost"       # Hostname or IP

  # ─────────────────────────────────────────────────────────────────
  # Log Level
  # Options: TRACE, DEBUG, INFO, WARN, ERROR
  # Logs below this level are discarded
  # ─────────────────────────────────────────────────────────────────
  log_level = "INFO"

  # ─────────────────────────────────────────────────────────────────
  # HTTP Connection Settings
  # Connection to the DazzleDuck server
  # ─────────────────────────────────────────────────────────────────
  http {
    base_url               = "http://localhost:8081"  # Server URL
    username               = "admin"                   # Auth username
    password               = "admin"                   # Auth password
    target_path            = "logs/my-app"            # Target table path
    http_client_timeout_ms = 30000                    # Request timeout (30s)
  }

  # ─────────────────────────────────────────────────────────────────
  # Batching Configuration
  # Controls how logs are batched before sending
  # ─────────────────────────────────────────────────────────────────
  min_batch_size       = 100      # Min records before sending
  max_batch_size       = 10000    # Max records per batch
  max_send_interval_ms = 5000     # Force send after 5 seconds of inactivity

  # ─────────────────────────────────────────────────────────────────
  # Retry Configuration
  # Controls retry behavior on failed sends
  # ─────────────────────────────────────────────────────────────────
  retry_count       = 3     # Number of retry attempts
  retry_interval_ms = 1000  # Delay between retries (1 second)

  # ─────────────────────────────────────────────────────────────────
  # Memory Management
  # Controls memory usage and disk spillover
  # ─────────────────────────────────────────────────────────────────
  max_in_memory_bytes = 10485760    # 10 MB in memory before spilling
  max_on_disk_bytes   = 104857600   # 100 MB max disk usage

  # ─────────────────────────────────────────────────────────────────
  # Data Transformations (optional)
  # SQL transformations applied before writing
  # ─────────────────────────────────────────────────────────────────
  transformations = []

  # ─────────────────────────────────────────────────────────────────
  # Partitioning (optional)
  # Columns to partition by when writing Parquet files
  # ─────────────────────────────────────────────────────────────────
  partition_by = []
}
```

---

## Configuration Methods

You can configure the logger using multiple methods. They are applied in order of priority (highest first):

### 1. System Properties (Highest Priority)

Override any config value with `-D` flags:

```bash
java -Ddazzleduck_logger.application_id=prod-app \
     -Ddazzleduck_logger.http.base_url=http://prod-server:8081 \
     -Ddazzleduck_logger.http.password=secret \
     -Ddazzleduck_logger.log_level=DEBUG \
     -jar myapp.jar
```

### 2. Environment Variables

Use environment variable substitution in your `application.conf`:

```hocon
dazzleduck_logger {
  application_id   = ${?APP_ID}
  application_name = ${?APP_NAME}
  application_host = ${?HOSTNAME}
  log_level        = ${?LOG_LEVEL}

  http {
    base_url = ${?DAZZLEDUCK_URL}
    username = ${?DAZZLEDUCK_USER}
    password = ${?DAZZLEDUCK_PASSWORD}
  }
}
```

Then set environment variables:

```bash
export APP_ID="my-prod-app"
export APP_NAME="MyProductionApp"
export DAZZLEDUCK_URL="http://prod-server:8081"
export DAZZLEDUCK_USER="admin"
export DAZZLEDUCK_PASSWORD="secret-password"
export LOG_LEVEL="WARN"
```

### 3. Include from External File

```hocon
include "/etc/myapp/dazzleduck.conf"

dazzleduck_logger {
  # Override specific settings
  application_id = "override-app-id"
}
```

### 4. Default `application.conf` (Lowest Priority)

The `application.conf` file in your classpath serves as the base configuration.

---

## Log Schema

Each log entry is stored with the following Arrow schema:

| Field | Type | Description |
|-------|------|-------------|
| `s_no` | Int64 | Global sequence number |
| `timestamp` | Utf8 | ISO-8601 timestamp |
| `level` | Utf8 | Log level (TRACE, DEBUG, INFO, WARN, ERROR) |
| `logger` | Utf8 | Logger name (usually class name) |
| `thread` | Utf8 | Thread name |
| `message` | Utf8 | Formatted log message (includes stack traces) |
| `mdc` | Utf8 | MDC context as JSON |
| `marker` | Utf8 | Log marker name |
| `application_id` | Utf8 | Application identifier |
| `application_name` | Utf8 | Application name |
| `application_host` | Utf8 | Host name |

---

## Usage Examples

### Basic Logging

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyService {
    private static final Logger log = LoggerFactory.getLogger(MyService.class);

    public void process() {
        log.trace("Entering process method");
        log.debug("Processing with params: {}", params);
        log.info("Process completed successfully");
        log.warn("Resource usage high: {}%", usage);
        log.error("Process failed", exception);
    }
}
```

### Using MDC (Mapped Diagnostic Context)

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class RequestHandler {
    private static final Logger log = LoggerFactory.getLogger(RequestHandler.class);

    public void handleRequest(String requestId, String userId) {
        // Set MDC context - these values appear in every log within this scope
        MDC.put("request_id", requestId);
        MDC.put("user_id", userId);
        MDC.put("correlation_id", UUID.randomUUID().toString());

        try {
            log.info("Processing request");
            // All logs here will include request_id, user_id, correlation_id
            doWork();
            log.info("Request completed");
        } finally {
            MDC.clear();  // Always clean up!
        }
    }
}
```

### Using Markers

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class AuditService {
    private static final Logger log = LoggerFactory.getLogger(AuditService.class);
    private static final Marker AUDIT = MarkerFactory.getMarker("AUDIT");
    private static final Marker SECURITY = MarkerFactory.getMarker("SECURITY");

    public void logUserLogin(String userId) {
        log.info(AUDIT, "User {} logged in", userId);
    }

    public void logSecurityEvent(String event) {
        log.warn(SECURITY, "Security event: {}", event);
    }
}
```

### Exception Logging

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataProcessor {
    private static final Logger log = LoggerFactory.getLogger(DataProcessor.class);

    public void process(String data) {
        try {
            // risky operation
            parseData(data);
        } catch (ParseException e) {
            // Stack trace is automatically included in the message field
            log.error("Failed to parse data: {}", data, e);
        }
    }
}
```

---

## Advanced: Log File Tailing

This module also supports tailing existing JSON log files and sending them to DazzleDuck.

### Log File Format

Log files must be JSON-per-line format:

```json
{"timestamp": "2024-01-01T10:00:00Z", "level": "INFO", "logger": "App", "thread": "main", "message": "Hello"}
{"timestamp": "2024-01-01T10:00:01Z", "level": "WARN", "logger": "App", "thread": "main", "message": "Warning"}
```

### Running the Log Processor

```bash
java -cp dazzleduck-sql-logger.jar io.dazzleduck.sql.logger.tailing.LogProcessorMain
```

Configure via `application.conf`:

```hocon
dazzleduck_logger {
  log_directory    = "/var/logs/"
  log_file_pattern = "*.log"
  poll_interval_ms = 30000

  # ... other settings as above
}
```

---

## Troubleshooting

### Multiple SLF4J Providers Warning

```
SLF4J: Class path contains multiple SLF4J providers.
```

**Solution:** You have multiple SLF4J implementations on the classpath. Remove other providers:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter</artifactId>
    <exclusions>
        <exclusion>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

### ClassCastException with LoggerContext

```
java.lang.ClassCastException: io.dazzleduck.sql.logger.ArrowSimpleLoggerFactory
cannot be cast to ch.qos.logback.classic.LoggerContext
```

**Solution:** Same as above - Logback is still on the classpath. Exclude it from all Spring Boot starters.

### Logs Not Appearing in DazzleDuck

1. **Check log level:** Ensure your log level is not filtering out logs:
   ```hocon
   log_level = "DEBUG"  # or lower
   ```

2. **Check server connectivity:**
   ```bash
   curl -X GET http://localhost:8081/health
   ```

3. **Check credentials:** Verify username/password in config

4. **Check target path:** Ensure the target path exists or can be created

### Configuration Not Loading

**Solution:** Ensure `application.conf` is in your classpath:
- For Maven: `src/main/resources/application.conf`
- For Gradle: `src/main/resources/application.conf`

Verify with:
```java
URL resource = getClass().getClassLoader().getResource("application.conf");
System.out.println("Config location: " + resource);
```

### High Memory Usage

**Solution:** Reduce batch sizes and memory limits:

```hocon
dazzleduck_logger {
  min_batch_size      = 50
  max_batch_size      = 1000
  max_in_memory_bytes = 5242880   # 5 MB
}
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Your Application                         │
│                                                             │
│   Logger.info("Hello")  Logger.error("Failed", ex)         │
│         │                        │                          │
└─────────┼────────────────────────┼──────────────────────────┘
          │                        │
          ▼                        ▼
┌─────────────────────────────────────────────────────────────┐
│                      SLF4J API                              │
│              (org.slf4j.Logger interface)                   │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              ArrowSLF4JServiceProvider                      │
│           (Registers as SLF4J 2.0 provider)                 │
│                          │                                  │
│                          ▼                                  │
│               ArrowSimpleLoggerFactory                      │
│            (Creates/manages logger instances)               │
│                          │                                  │
│                          ▼                                  │
│                  ArrowSimpleLogger                          │
│         (Formats log events, captures MDC/markers)          │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    HttpProducer                             │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   Batching  │→ │ Arrow IPC   │→ │   HTTP POST         │ │
│  │   (by size/ │  │ Serializer  │  │   with retries      │ │
│  │    time)    │  │             │  │                     │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
│         │                                                   │
│         ▼ (if memory full)                                 │
│  ┌─────────────┐                                           │
│  │ Disk Spill  │                                           │
│  │  (overflow) │                                           │
│  └─────────────┘                                           │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  DazzleDuck Server                          │
│                                                             │
│         HTTP Ingestion → Arrow Processing → Parquet        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Requirements

- **Java 21+**
- **DazzleDuck SQL Server** running with HTTP mode enabled

---

## License

Apache 2.0
