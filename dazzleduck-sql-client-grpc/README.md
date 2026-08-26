# DazzleDuck SQL Client (gRPC)

Arrow Flight SQL (gRPC) **ingestion** client — the Flight counterpart to
`dazzleduck-sql-client`. It reuses the same batching, disk-spill, retry, and backpressure
machinery (`ArrowProducer.AbstractArrowProducer`) but sends buckets via
`FlightSqlClient.executeIngest` instead of HTTP POST.

Targets **Java 11** bytecode.

## How It Works

`GrpcArrowProducer` accumulates rows/batches exactly like the HTTP producer, then streams each
bucket to the Flight SQL server as an Arrow ingest call. The target ingestion queue and any
other options are passed as ingest parameters — the queue key is `ingestion_queue`
(`Headers.QUERY_PARAMETER_INGESTION_QUEUE`), and partition columns travel as `x-dd-partition`.

Backpressure: a Flight `RESOURCE_EXHAUSTED` status raises `BackPressureException` (suggested
wait 5 seconds), mirroring the HTTP client's handling of `429`.

## Authentication

HTTP Basic on the first call, upgraded automatically to the server-issued Bearer token: the
client middleware (`auth/AuthUtils.createClientMiddlewareFactory`) captures the `Bearer` value
the server returns on the response headers and uses it for all subsequent calls. There is no
preconfigured-token mode in this client.

## Usage

```java
Schema schema = ...;

GrpcArrowProducer producer = new GrpcArrowProducer(
        schema,
        1024 * 1024,                    // minBatchSize
        16 * 1024 * 1024,               // maxBatchSize
        Duration.ofSeconds(2),          // maxSendInterval
        Clock.systemUTC(),
        3, 1000,                        // retryCount, retryIntervalMillis
        List.of(),                      // partitionBy
        10 * 1024 * 1024,               // maxInMemorySize
        1024L * 1024 * 1024,            // maxOnDiskSize
        allocator,
        Location.forGrpcInsecure("localhost", 59307),
        "admin", "admin",
        Map.of("ingestion_queue", "my_table"),  // ingest parameters
        Duration.ofSeconds(30));

try (producer) {
    producer.addRow(row);
}
```

There is no builder — a single positional constructor. Note: the final timeout parameter is
validated but not currently applied as a per-call gRPC deadline.

## Key Classes

| Class | Purpose |
|-------|---------|
| `GrpcArrowProducer` | The producer (`extends ArrowProducer.AbstractArrowProducer`) |
| `auth/AuthUtils` | Client middleware factory for the Basic-to-Bearer upgrade |

## Requirements

- Java 11+ (built and tested with JDK 21)
- Dependencies: `dazzleduck-sql-client` (base machinery), Arrow `flight-core` / `flight-sql`
