# DazzleDuck SQL Flight

The Arrow Flight SQL server implementation backed by DuckDB. This module contains the producer
hierarchy (one class per access mode), the gRPC authentication middleware, statement and cursor
lifecycle management, the ingestion bridge, metrics/audit recorders, and the adaptor that lets
the HTTP module reuse the exact same producer.

## Running Standalone

```bash
# Run through the runtime module so the Arrow --add-opens flags are applied
./mvnw exec:java -pl dazzleduck-sql-runtime \
  -Dexec.mainClass="io.dazzleduck.sql.flight.server.Main" \
  -Dexec.args="--conf warehouse=warehouse"
```

In normal deployments the server is started by `dazzleduck-sql-runtime`, which builds one
producer and shares it between Flight SQL and HTTP.

## Producers

`FlightSqlProducerFactory.build()` selects the producer from the `access_mode` config key:

| Access mode | Producer | Behavior |
|-------------|----------|----------|
| `COMPLETE` | `DuckDBFlightSqlProducer` | All SQL, no authorization |
| `READ_ONLY` | `SelectOnlyFlightSqlProducer` | Parses to AST, permits SELECT / set operations only; blocks prepared-statement updates |
| `RESTRICT_READ_ONLY` | `RestrictedReadOnlyFlightSqlProducer` | As above, plus blocks the raw-SQL schema probe (`getSchemaStatement`) that would bypass authorization |
| `RESTRICTED` | `RestrictedFlightSqlProducer` | Statement queries and bulk ingest only (catalog/schema/prepared-statement RPCs are unimplemented); adds split-based parallelization via `x-dd-split-size`; bulk ingest requires write access from JWT claims |

All producers implement `FlightSqlHttpProducer` (`FlightSqlProducer` + `HttpFlightAdaptor`),
which adds the HTTP-facing entry points: `acceptPutStatementBulkIngest` from an input stream,
`tryCancel`, and direct streaming as Arrow IPC (`getStreamStatementDirect`), TSV (`streamTsv`),
and JSONL (`streamJsonl`).

## Flight SQL RPC Coverage

Implemented: statement create/close/execute, prepared statements (except
`acceptPutPreparedStatementQuery`), bulk ingest, cancel, SQL info, catalogs, schemas, tables,
table types.

Unimplemented: type info, primary/exported/imported keys, cross reference, `listFlights`.

## Authentication

`AdvanceServerCallHeaderAuthMiddleware` wraps `AdvanceJWTTokenAuthenticator`:

1. A `Bearer` token is validated (signature verified unless `jwt_token.verify_signature = false`),
   checked for expiry, and each header listed in `jwt_token.claims.validate.headers` must match
   the corresponding JWT claim.
2. Otherwise `Basic` credentials are validated — against the `users` config list
   (`ConfBasedCredentialValidator`), or against an external login service when `login_url` is
   set (`HttpCredentialValidator`) — and a JWT is minted with one claim per header in
   `jwt_token.claims.generate.headers`. The token is returned on the outgoing headers, so
   clients (including JDBC) transparently switch from Basic to Bearer.

Authorization is delegated to the `SqlAuthorizer` implementations in `dazzleduck-sql-commons`
(see that module's README). Every `StatementHandle` is HMAC-signed with `secret_key`, so a
handle produced at plan time cannot be tampered with before execution, and all statement caches
are keyed by peer identity — one user cannot fetch or cancel another user's cursor.

## Statement and Cursor Lifecycle

- Prepared statements: Guava cache, max 4000, expire 10 minutes after access
- Statements/cursors: cache bounded by `max_cursors_total`, expiring after `cursor_ttl_ms`;
  per-identity limit `max_cursors_per_identity`; exceeding either returns `RESOURCE_EXHAUSTED`
- Query timeout: client `x-dd-query-timeout` (seconds) wins but is capped by
  `max_query_timeout_ms`; otherwise `query_timeout_ms` applies
- Results are streamed from DuckDB's native Arrow export with a child allocator per statement

## Ingestion

Bulk-ingest data is written to a temp Arrow file, validated, and handed to the
`ParquetIngestionQueue` for the target `ingestion_queue`. Saturation surfaces as
`RESOURCE_EXHAUSTED` (Flight) or HTTP 429 with `Retry-After`. Per-queue Micrometer meters are
registered on queue creation and unregistered on deletion.

## Named Queries

`namedquery/DefaultNamedQueryServiceAdaptor` renders Jinja templates (Jinjava) stored in the
table named by `named_query_table`, runs validators, and streams results through the same
output listeners as regular queries. The HTTP module exposes this at `/v1/named-query`.

## Metrics and Audit

- `MicroMeterFlightRecorder` — counters/gauges named `dazzleduck.flight.*`, tagged
  `service.name`, `host.name`, `container.id`, `producer.id`; defaults to a
  `LoggingMeterRegistry` when none is supplied
- `Auditor` — JSON `StatementAudit` records (START/END/CANCEL/ERROR/TIMEOUT with query, timings,
  bytes out) to the logger `dazzleduck.audit`
- `SqlProducerMBean` — running/open/completed/cancelled statement counts and details, consumed
  by the HTTP `/v1/ui` dashboard

## Configuration

Defaults ship in this module's `reference.conf` under the `dazzleduck_server` root:

| Key | Default |
|-----|---------|
| `flight_sql.port` / `flight_sql.host` | `59307` / `0.0.0.0` |
| `flight_sql.use_encryption` | `true` (TLS from `keystore` / `server_cert` classpath resources) |
| `flight_sql.data_processor_locations` | self — set to worker endpoints for distributed planning |
| `access_mode` | `COMPLETE` |
| `warehouse` | `${user.dir}/warehouse` |
| `secret_key` | dev placeholder — **change in production** |
| `query_timeout_ms` / `max_query_timeout_ms` | `120000` / `300000` |
| `cursor_ttl_ms` / `max_cursors_per_identity` / `max_cursors_total` | `60000` / `50` / `2000` |
| `ingestion.min_bucket_size` / `max_bucket_size` / `max_batches` / `max_pending_write` / `max_delay_ms` | 1 MB / 1 GB / 2048 / 256 MB / 2000 |
| `jwt_token.expiration` / `generation` / `verify_signature` | `60m` / `true` / `true` |
| `users` | `admin`/`admin` — **change in production** |
| `startup_script_provider.*` | installs and loads the community `arrow` extension |
| `ingestion_task_factory_provider.*` | NOOP provider writing under the warehouse |

The full request-header vocabulary (`x-dd-fetch-size`, `x-dd-split-size`,
`x-dd-arrow-compression`, ingestion headers, authorization claims) is defined in
`io.dazzleduck.sql.common.Headers` — see the root README.

## Key Files

```
src/main/java/io/dazzleduck/sql/flight/
├── server/
│   ├── Main.java                          # Standalone entry point
│   ├── FlightSqlProducerFactory.java      # Config-driven producer construction
│   ├── DuckDBFlightSqlProducer.java       # Base producer (~1500 lines)
│   ├── SelectOnlyFlightSqlProducer.java
│   ├── RestrictedReadOnlyFlightSqlProducer.java
│   ├── RestrictedFlightSqlProducer.java
│   ├── HttpFlightAdaptor.java             # Bridge used by dazzleduck-sql-http
│   ├── StatementHandle.java               # HMAC-signed statement handle
│   ├── ResultSetStreamUtil.java           # DuckDB → Arrow streaming
│   ├── ErrorHandling.java                 # Exception → Flight status mapping
│   ├── DirectOutputStreamListener.java    # Arrow IPC output
│   ├── TsvOutputStreamListener.java       # TSV output
│   ├── JsonOutputStreamListener.java      # JSON / JSONL output
│   └── auth2/                             # Authenticators, middleware, credential validators
├── namedquery/                            # Named-query adaptors (Jinjava)
├── context/SyntheticFlightContext.java    # Fabricates a Flight context from HTTP requests
├── optimizer/                             # QueryOptimizer SPI (NOOP default)
└── MicroMeterFlightRecorder.java / Auditor.java
```
