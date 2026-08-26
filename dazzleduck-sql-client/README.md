# DazzleDuck SQL Client

HTTP **ingestion** client for the DazzleDuck server, with Arrow-native batching, disk spill,
and backpressure handling. This module is write-path only — for queries, use plain HTTP against
`/v1/query` or a Flight SQL client.

Targets **Java 11** bytecode so it can be embedded in older applications. It is the transport
used by `dazzleduck-sql-logback`, `dazzleduck-sql-micrometer`, and `dazzleduck-sql-scrapper`.

## How It Works

`HttpArrowProducer` accumulates rows (`addRow(JavaRow)`) or pre-encoded Arrow batches
(`enqueue(byte[])`) into buckets. A background sender thread flushes a bucket when it reaches
`minBatchSize` or when `maxSendInterval` elapses, POSTing it as an Arrow IPC stream
(ZSTD-compressed by default) to:

```
POST {baseUrl}/v1/ingest?ingestion_queue={queue}
Content-Type: application/vnd.apache.arrow.stream
Authorization: Bearer <jwt>
x-dd-partition: <partition columns, when configured>
```

- **Buffering**: in-memory up to `maxInMemorySize`, then spills to temp files up to
  `maxOnDiskSize`; beyond that new elements are rejected
- **Backpressure**: a server `429` raises `BackPressureException` carrying the suggested wait
  from the `Retry-After` header (default 5 seconds)
- **Retries**: `retryCount` attempts with exponential backoff (multiplier 2, capped at 60 s)
- **Stats**: sent/dropped/retry/backpressure counters are exposed to subclasses

## Authentication

Two mutually exclusive modes, chosen by constructor:

1. **Login mode** — the producer POSTs `{username, password, claims}` to `{baseUrl}/v1/login`,
   caches the JWT, and refreshes it shortly before expiry; on `401`/`403` it re-logins (up to
   2 attempts)
2. **Preconfigured JWT** — pass the token directly; login is skipped entirely and a rejected
   token fails immediately (no re-login)

The optional `claims` map is forwarded at login and embedded in the JWT — used for row-level
security and for the `ingestion_queue` claim required in restricted access modes.

## Usage

```java
Schema schema = ...;  // Arrow schema of the rows you will send

HttpArrowProducer producer = new HttpArrowProducer(
        schema,
        "http://localhost:8081",
        "admin", "admin",
        "my_table",                     // ingestion queue
        Duration.ofSeconds(5),          // HTTP client timeout
        1024 * 1024,                    // minBatchSize
        16 * 1024 * 1024,               // maxBatchSize
        Duration.ofSeconds(2),          // maxSendInterval
        3, 1000,                        // retryCount, retryIntervalMillis
        List.of(),                      // partitionBy
        10 * 1024 * 1024,               // maxInMemorySize
        1024L * 1024 * 1024);           // maxOnDiskSize

try (producer) {
    producer.addRow(row);   // or producer.enqueue(arrowIpcBytes)
} // close() flushes pending data
```

There is no builder — the class exposes overloaded constructors: the variant above
(username/password, no claims), a JWT variant `(schema, baseUrl, jwt, ingestionQueue, ...)`,
and longer variants adding a claims map, a compression codec (`ZSTD` default or
`NO_COMPRESSION`), and a custom clock.

## Key Classes

| Class | Purpose |
|-------|---------|
| `ArrowProducer` | Interface + `AbstractArrowProducer` base: queueing, sender thread, flush timer, spill-to-disk elements, retry/stats machinery |
| `HttpArrowProducer` | Concrete HTTP implementation (JDK `java.net.http.HttpClient`) |
| `BackPressureException` | Carries `getSuggestedWaitMillis()` for the caller's retry pacing |

## SSL

`SslUtils` from `dazzleduck-sql-common` controls trust: set `DD_TRUST_SELF_SIGNED_CERTS` for
dev/test against self-signed certificates (never in production).

## Requirements

- Java 11+ (built and tested with JDK 21)
- Dependencies: `dazzleduck-sql-common`, Arrow (with ZSTD compression), SLF4J
