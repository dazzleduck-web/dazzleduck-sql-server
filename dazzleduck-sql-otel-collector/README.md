# dazzleduck-sql-otel-collector

An OTLP gRPC collector that receives OpenTelemetry signals (logs, traces, metrics) and writes them to Parquet files using Apache Arrow, with optional DuckLake catalog integration.

## Overview

```
OTLP Exporter
     │  gRPC (port 4317), JWT with x-dd-ingestion-queue claim
     ▼
OtelCollectorServer
     │
     ├── OtelLogService      ┐
     ├── OtelTraceService    ├─→ IngestionHandler → ParquetIngestionQueue → Parquet files (+ DuckLake)
     └── OtelMetricsService  ┘
```

Each incoming export request is:
1. Routed to an ingestion queue by the `x-dd-ingestion-queue` claim in the caller's JWT — there
   is no default queue; requests without the claim are rejected with `INVALID_ARGUMENT`
2. Flattened from the 3-level OTLP hierarchy (Resource → Scope → Record) into a flat Arrow batch
   and written to a temp Arrow file
3. Handed to the queue's `ParquetIngestionQueue`, which batches by size (`min_bucket_size`) and
   time (`max_delay_ms`) before flushing to Parquet — and, when a DuckLake provider is
   configured, registers the files with the catalog in the same post-ingestion transaction

Queues are never pre-created: the configured `IngestionHandler` is the single registry, so
queues added or removed at runtime (dynamic provider) become routable without a restart.

## Features

- **Three signal types**: logs, traces, metrics — each with a fully typed Arrow schema
- **JWT authentication**: Bearer token validation; supports local user map or login delegation to an external HTTP service
- **Batched writes**: Size + time-based flushing to minimize small Parquet files
- **DuckLake integration**: Optional post-ingestion task registration via `IngestionTaskFactory`
- **SQL transformations**: Derive columns (e.g. partition keys) before writing
- **Micrometer metrics**: Export counters, latency timers, and writer queue gauges
- **Health check**: Embedded HTTP `/health` endpoint with a graceful shutdown lifecycle (see [Health Check](#health-check))

## Configuration

Configuration is loaded from HOCON (`application.conf`) with environment variable and system property overrides.

```hocon
otel_collector {
    grpc_port = 4317

    health {
        port = 8081                       # GET /health
        shutdown_grace_period_ms = 2000   # MAINTENANCE/LB-drain window; 0 to skip
    }

    # Startup SQL run before any queue is created (load extensions, ATTACH DuckLake catalogs)
    startup_script_provider {
        content = "INSTALL arrow FROM community; LOAD arrow;"
        # script_location = "/config/startup.sql"
    }

    ingestion {
        min_bucket_size = 1048576   # 1 MB — flush when accumulated batch size exceeds this
        max_delay_ms    = 5000      # flush after this many ms even if min_bucket_size not reached
        queue_config_refresh_delay_ms = 120000
    }

    # One entry per ingestion queue. Without a provider class, batches are written
    # as plain Parquet under output_path. Per-queue keys: transformation,
    # partition_by, min_bucket_size, max_delay_ms, and (with DuckLake)
    # catalog / schema / table / additional_parameters.
    ingestion_task_factory_provider {
        ingestion_queue_table_mapping = [
            { ingestion_queue = "logs",    output_path = "./otel-logs" }
            { ingestion_queue = "traces",  output_path = "./otel-traces" }
            { ingestion_queue = "metrics", output_path = "./otel-metrics" }
        ]
    }

    # Authentication (required)
    authentication = "jwt"
    secret_key     = "base64-encoded-hmac-key"

    # Local users (Basic auth → JWT)
    users = [
        { username = admin, password = admin }
    ]

    # Optional: delegate Basic auth to an external login service
    # login_url = "http://localhost:8081/v1/login"

    # JWT expiration (default 1h)
    # jwt_token.expiration = 1h

    # Service name reported in metrics (default: "open-telemetry-collector")
    # service_name = "my-collector"
}
```

### Environment Variables

Any `otel_collector.*` environment variable overrides the corresponding config key:

```bash
otel_collector.grpc_port=4317
otel_collector.secret_key=<base64-key>
otel_collector.logs_output_path=/data/logs
otel_collector.service_name=my-collector
```

## Authentication

All gRPC calls must be authenticated.

**Step 1 — Login (Basic auth):**

```bash
# Returns a JWT in the Authorization response header
grpcurl -plaintext \
  -H "Authorization: Basic $(echo -n 'admin:admin' | base64)" \
  localhost:4317 opentelemetry.proto.collector.logs.v1.LogsService/Export
```

**Step 2 — Use the token:**

```bash
-H "Authorization: Bearer <jwt-token>"
```

**Login delegation:** Set `login_url` to forward Basic auth credentials to an external HTTP service (same pattern as the DazzleDuck Flight SQL server).

## Health Check

The collector runs a small embedded HTTP server (plain JDK `HttpServer`, no extra dependency)
exposing `GET /health`. It reports one of three statuses, each with a matching HTTP code so a
readiness probe or load balancer reacts correctly:

| Status | HTTP code | Meaning |
|--------|-----------|---------|
| `HEALTHY` | 200 | gRPC server is up and accepting requests |
| `MAINTENANCE` | 503 | Graceful shutdown in progress — draining, not accepting new traffic |
| `DOWN` | 503 | Shutdown complete (briefly visible right before the process exits) |

```json
{
  "status": "HEALTHY",
  "uptimeSeconds": 1234,
  "grpcPort": 4317,
  "knownQueues": 3,
  "batchesProcessed": 5821
}
```

Configure the port and shutdown grace period under `otel_collector.health`:

```hocon
otel_collector {
    health {
        port = 8081                    # GET /health
        shutdown_grace_period_ms = 2000   # MAINTENANCE/LB-drain window; 0 to skip
    }
}
```

On shutdown (SIGTERM via the JVM shutdown hook), the collector immediately flips to `MAINTENANCE`
— so a Kubernetes readiness probe or load balancer stops routing new traffic right away — then
stays in `MAINTENANCE` for `shutdown_grace_period_ms` before stopping the gRPC server itself.
Already-accepted in-flight calls then get up to 10s more (`awaitTermination`) to finish, during which
the flush scheduler is still running, so a batch buffered when shutdown begins is written to Parquet
and its export ack returned rather than the call being cut off mid-RPC.

## Arrow Schemas

All schemas flatten the OTLP 3-level hierarchy (Resource → Scope → Record). Resource and scope fields are promoted to top-level columns.

### Logs

| Column | Type | Notes |
|--------|------|-------|
| `timestamp` | Timestamp(ms) | |
| `observed_timestamp` | Timestamp(ms) | |
| `severity_number` | Int32 | |
| `severity_text` | Utf8 | |
| `body` | Utf8 | |
| `trace_id` | Utf8 | hex-encoded |
| `span_id` | Utf8 | hex-encoded |
| `flags` | Int32 | |
| `event_name` | Utf8 | |
| `attributes` | Map(Utf8, Utf8) | log record attributes |
| `resource_attributes` | Map(Utf8, Utf8) | resource attributes |
| `scope_name` | Utf8 | |
| `scope_version` | Utf8 | |

### Traces

| Column | Type | Notes |
|--------|------|-------|
| `trace_id` | Utf8 | |
| `span_id` | Utf8 | |
| `parent_span_id` | Utf8 | |
| `name` | Utf8 | |
| `kind` | Utf8 | INTERNAL / SERVER / CLIENT / PRODUCER / CONSUMER |
| `start_time_ms` | Timestamp(ms) | |
| `end_time_ms` | Timestamp(ms) | |
| `duration_ms` | Int64 | |
| `status_code` | Utf8 | |
| `status_message` | Utf8 | |
| `attributes` | Map(Utf8, Utf8) | |
| `resource_attributes` | Map(Utf8, Utf8) | |
| `scope_name` | Utf8 | |
| `scope_version` | Utf8 | |
| `events` | List(Struct(name, time_ms, attributes)) | |
| `links` | List(Struct(trace_id, span_id, attributes)) | |

### Metrics

Wide-table design — all metric types (GAUGE, SUM, HISTOGRAM, EXPONENTIAL_HISTOGRAM, SUMMARY) share one schema. Columns not applicable to a metric type are null.

| Column | Type | Notes |
|--------|------|-------|
| `name` | Utf8 | |
| `description` | Utf8 | |
| `unit` | Utf8 | |
| `metric_type` | Utf8 | GAUGE / SUM / HISTOGRAM / etc. |
| `start_time_ms` | Timestamp(ms) | |
| `time_ms` | Timestamp(ms) | |
| `attributes` | Map(Utf8, Utf8) | |
| `resource_attributes` | Map(Utf8, Utf8) | |
| `scope_name` | Utf8 | |
| `scope_version` | Utf8 | |
| `value_double` | Double | GAUGE / SUM scalar |
| `value_int` | Int64 | GAUGE / SUM integer |
| `count` | Int64 | HISTOGRAM |
| `sum` | Double | HISTOGRAM |
| `bucket_counts` | List(Int64) | HISTOGRAM |
| `explicit_bounds` | List(Double) | HISTOGRAM |
| `quantile_values` | List(Struct(quantile, value)) | SUMMARY |
| `is_monotonic` | Bool | SUM |
| `aggregation_temporality` | Utf8 | DELTA / CUMULATIVE |

## Micrometer Metrics

`OtelCollectorMetrics` publishes the following meters. When no `MeterRegistry` is configured, a `SimpleMeterRegistry` is used (metrics are active but not published externally).

### Common Tags (on every meter)

| Tag | Source |
|-----|--------|
| `service.name` | `otel_collector.service_name` config (default `"open-telemetry-collector"`) |
| `host.name` | `HOSTNAME` env var → `InetAddress.getLocalHost()` → `"unknown"` |
| `container.id` | `CONTAINER_ID` env var → `"unknown"` |

### Export Metrics

All meters are tagged with the ingestion `queue` id.

| Metric | Type |
|--------|------|
| `dazzleduck.otel.export.requests` | FunctionCounter |
| `dazzleduck.otel.export.records` | FunctionCounter |
| `dazzleduck.otel.export.errors` | FunctionCounter |
| `dazzleduck.otel.export.latency` | Timer (p50/p95/p99) |

`export.latency` covers the full RPC duration — from receiving the request to `onCompleted` or `onError`, including Arrow serialization and queue submission.

### Writer Metrics

Registered per queue when the queue is created, and unregistered when the queue is removed.

| Metric | Type |
|--------|------|
| `dazzleduck.otel.writer.bytes_written` | FunctionCounter |
| `dazzleduck.otel.writer.batches_written` | FunctionCounter |
| `dazzleduck.otel.writer.bytes_failed` | FunctionCounter |
| `dazzleduck.otel.writer.batches_failed` | FunctionCounter |
| `dazzleduck.otel.writer.write_failures` | FunctionCounter |
| `dazzleduck.otel.writer.producer_id_evictions` | FunctionCounter |
| `dazzleduck.otel.writer.data_phase_ms` | FunctionCounter |
| `dazzleduck.otel.writer.post_ingest_phase_ms` | FunctionCounter |
| `dazzleduck.otel.writer.pending_batches` | Gauge |
| `dazzleduck.otel.writer.pending_buckets` | Gauge |

### Registering a Real Registry

```java
CollectorProperties props = new CollectorConfig().toProperties();
props.setMeterRegistry(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
OtelCollectorServer server = new OtelCollectorServer(props);
server.start();
```

## DuckLake Integration

To register written Parquet files into a DuckLake catalog, set a provider `class` on
`ingestion_task_factory_provider` and add the DuckLake fields per queue (the catalog must be
attached by the startup script):

```hocon
otel_collector {
    startup_script_provider {
        content = "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:...' AS my_catalog (DATA_PATH 's3://...');"
    }

    ingestion_task_factory_provider {
        class = "io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionTaskFactoryProvider"
        ingestion_queue_table_mapping = [
            {
                ingestion_queue = "logs"
                catalog = "my_catalog"
                schema  = "main"
                table   = "logs"
                # Inline transformation (SELECT over the __this placeholder):
                # transformation = "CAST(timestamp AS DATE) AS date, severity_text AS level"
                # ... OR a view-based transformation — the view's definition is used, with
                # input_table rewritten to __this. Mutually exclusive with transformation;
                # editing the view updates the transformation at runtime, no restart:
                # view        = "my_catalog.main.logs_transform"
                # input_table = "my_catalog.main.raw_logs"
                # additional_parameters {   # optional per-batch watermark row, committed
                #     watermark_table                = "ingest_watermark"   # with the file registration
                #     watermark_timestamp_column     = "timestamp"
                #     watermark_min_timestamp_column = "min_timestamp"
                #     watermark_max_timestamp_column = "max_timestamp"
                #     watermark_row_count_column     = "row_count"
                #     watermark_group_columns        = "county,state"      # optional
                # }
            }
        ]
    }
}
```

For runtime add/remove of queues, use the dynamic provider backed by a SQLite registry:

```hocon
otel_collector.ingestion_task_factory_provider {
    class = "io.dazzleduck.sql.commons.ingestion.DynamicDuckLakeIngestionTaskFactoryProvider"
    db_path = "/var/data/ingestion-queues.db"
    config_load_interval_ms = 5000
}
```

Watermarks are not available for dynamically registered queues — the SQLite registry does not
store `additional_parameters`.

## Building

```bash
./mvnw clean package -pl dazzleduck-sql-otel-collector -am -DskipTests
```

## Running

```bash
java -cp target/dazzleduck-sql-otel-collector-*.jar \
     io.dazzleduck.sql.otel.collector.Main
```

Or with an external config file:

```bash
java -cp ... io.dazzleduck.sql.otel.collector.Main -c /etc/otel-collector.conf
```

## Docker

The collector is published as `dazzleduck/dazzleduck-otel-collector` (multi-arch). Local build via Jib:

```bash
./mvnw package -DskipTests jib:dockerBuild -pl dazzleduck-sql-otel-collector
# Apple Silicon:
./mvnw package -DskipTests jib:dockerBuild -pl dazzleduck-sql-otel-collector -Djib.architecture=arm64
```
