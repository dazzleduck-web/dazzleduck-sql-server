# DazzleDuck SQL Server

**DazzleDuck SQL Server** is a high-performance remote DuckDB server that supports both
[Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) (gRPC) and a RESTful
HTTP API. It enables multiple users to connect remotely and execute queries through JDBC,
ADBC, plain HTTP, or DuckDB itself.

### Key Features

- **Dual Protocol Support**: Arrow Flight SQL (gRPC, port `59307`) and RESTful HTTP API (port `8081`, versioned with `/v1`, HTTP/2 enabled)
- **Arrow-Native**: query results and ingestion both use Apache Arrow IPC (ZSTD-compressed by default); TSV and JSONL/NDJSON output are also available over HTTP
- **JWT Authentication**: all versioned HTTP endpoints and Flight SQL calls are authenticated; Basic credentials are upgraded to a server-issued JWT
- **Row-Level Security**: four access modes with filter injection scoped by JWT claims
- **Ingestion Pipeline**: batched Arrow-to-Parquet ingestion with backpressure, producer-id deduplication, DuckLake catalog registration, and per-group watermarks
- **Partition Pruning**: Hive, Delta Lake, and DuckLake pruning based on query predicates, plus split planning for distributed execution
- **Named Queries**: Jinja-templated SQL stored in a DuckDB table, executed by name over HTTP

## Modules

| Module | Description | Bytecode target |
|--------|-------------|-----------------|
| `dazzleduck-sql-runtime` | Entry point, startup orchestration, the `dazzleduck/dazzleduck` Docker image | 17 |
| `dazzleduck-sql-flight` | Arrow Flight SQL server (producers per access mode, auth middleware, named queries, output listeners) | 17 |
| `dazzleduck-sql-http` | HTTP REST API on Helidon 4 (reuses the same producer as Flight SQL) | 17 |
| `dazzleduck-sql-common` | Shared constants (`ConfigConstants`, `Headers`, `ContentTypes`), `SslUtils`, JWT claim extraction, Arrow row writers | 11 |
| `dazzleduck-sql-commons` | DuckDB utilities: connection pool, SQL AST transformations, authorization, ingestion queues, partition pruning, split planning | 21 |
| `dazzleduck-sql-client` | HTTP ingestion client (`HttpArrowProducer`) with Arrow batching, disk spill, and backpressure | 11 |
| `dazzleduck-sql-client-grpc` | gRPC/Flight SQL ingestion client (`GrpcArrowProducer`) | 11 |
| `dazzleduck-sql-login` | JWT login service (`LoginService`, `ProxyLoginService`) | 21 |
| `dazzleduck-sql-search` | Inverted-index construction for full-text search (indexing only) | 21 |
| `dazzleduck-sql-micrometer` | Micrometer registry that forwards application metrics as Arrow to the ingest endpoint | 21 |
| `dazzleduck-sql-logback` | Logback appender that forwards logs as Arrow to the ingest endpoint | 11 |
| `dazzleduck-sql-scrapper` | Prometheus endpoint scraper that forwards metrics as Arrow | 21 |
| `dazzleduck-sql-otel-collector` | OTLP gRPC collector (logs/traces/metrics to Parquet/DuckLake), the `dazzleduck/dazzleduck-otel-collector` image | 21 |
| `dazzleduck-sql-ducklake-compactor` | Scheduled DuckLake compaction and snapshot housekeeping, the `dazzleduck/ducklake-compactor` image | 21 |
| `dazzleduck-sql-examples` | docker-compose integration tests for the demos under `example/docker` | 17 |

Client-side artifacts (`dazzleduck-sql-client`, `dazzleduck-sql-client-grpc`,
`dazzleduck-sql-common`, `dazzleduck-sql-logback`) target JDK 11 bytecode so they can be
embedded in older applications. Everything is built and tested with **JDK 25**, the JVM the Docker images run.

## Dev Setup

### Requirements

- JDK 25 (build, test and runtime images)
- Maven wrapper (`./mvnw`)

```bash
# Build all modules
./mvnw clean package -DskipTests

# Start the server locally (no Docker needed)
./mvnw exec:java -pl dazzleduck-sql-runtime \
  -Dexec.args="--conf warehouse=warehouse --conf users.0.password='your password'"
```

The JVM flags required by Arrow (`--add-opens`) are configured automatically by the exec plugin.

To run a specific main class (e.g. for demos or tooling):

```bash
./mvnw exec:java -pl dazzleduck-sql-runtime \
  -Dexec.mainClass="io.dazzleduck.sql.SomeOtherMain" \
  -Dexec.args="[args...]"
```

## Getting Started with Docker

```bash
docker run -ti -p 59307:59307 -p 8081:8081 dazzleduck/dazzleduck:latest \
  --conf warehouse=/data \
  --conf users.0.password="your password"
```

The server runs both Arrow Flight SQL (gRPC) on port `59307` and the HTTP REST API on port `8081`.

Image build instructions (base image, Jib, multi-arch manifests) live in
[`dazzleduck-sql-runtime/docker/README.md`](dazzleduck-sql-runtime/docker/README.md).
Release publishing is scripted in `scripts/docker-publish.sh`, which also builds the
`dazzleduck/dazzleduck-otel-collector`, `dazzleduck/ducklake-compactor`, and
`dazzleduck/dazzleduck-sql-scrapper` images. The full release process — version bump, tests,
tag, images, GitHub release — is documented in [`RELEASE.md`](RELEASE.md).

## HTTP API

All endpoints use a `/v1` version prefix except `/health`.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/login` | POST | Authenticate and obtain a JWT token |
| `/v1/query` | GET/POST | Execute SQL — Arrow IPC (default), TSV, or JSONL depending on the `Accept` header |
| `/v1/plan` | GET/POST | Query execution plan with splits (`x-dd-split-size` header or query param) |
| `/v1/ingest` | POST | Ingest an Arrow IPC stream (`?ingestion_queue=` required) |
| `/v1/cancel` | GET/POST | Cancel a running query by statement `id` |
| `/v1/named-query` | GET/POST | List, inspect, and execute named queries (only registered when `named_query_table` is configured) |
| `/v1/ui` | GET | Metrics dashboard |
| `/health` | GET | Health check (unversioned, unauthenticated) |

### Output formats

The `Accept` header selects the response format on `/v1/query` and `/v1/named-query`:

| Accept value | Format |
|--------------|--------|
| _(default)_ | Arrow IPC stream (`application/vnd.apache.arrow.stream`), ZSTD-compressed; override with the `x-dd-arrow-compression` header (`zstd` or `none`) |
| `text/tab-separated-values` | TSV: header row + tab-separated values. Ideal for scripts and LLM agents |
| `application/jsonl` or `application/x-ndjson` | JSONL: one JSON object per row. Numbers/booleans/nulls keep JSON types; temporal values are ISO-8601 strings; lists/structs/maps are nested JSON |

Any response is gzip-compressed when the request asks for it with `Accept-Encoding: gzip` (curl's `--compressed`), which shrinks TSV and JSONL several times over. Without that header nothing changes. Requests to `/v1/ingest` may likewise send a gzip-compressed body with `Content-Encoding: gzip`.

### Useful request headers

Every header can also be passed as a URL query parameter.

| Header | Purpose |
|--------|---------|
| `x-dd-fetch-size` | Rows per Arrow batch (default 10000) |
| `x-dd-query-timeout` | Per-query timeout in seconds (capped by `max_query_timeout_ms`) |
| `x-dd-split-size` | Target split size in bytes for `/v1/plan` |
| `x-dd-arrow-compression` | `zstd` (default) or `none` |
| `x-dd-partition`, `x-dd-sort-order`, `x-dd-format`, `x-dd-producer-id`, `x-dd-producer-batch-id` | Ingestion options for `/v1/ingest` |

### Examples

```bash
# Get a token
curl -X POST 'http://localhost:8081/v1/login' \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}'

# Query (Arrow IPC by default)
curl -H "Authorization: Bearer $TOKEN" "http://localhost:8081/v1/query?q=select%201"

# Query as TSV
curl -H "Authorization: Bearer $TOKEN" \
  -H "Accept: text/tab-separated-values" \
  "http://localhost:8081/v1/query?q=select%201"

# Query as TSV, gzip-compressed on the wire (curl decompresses it)
curl --compressed -H "Authorization: Bearer $TOKEN" \
  -H "Accept: text/tab-separated-values" \
  "http://localhost:8081/v1/query?q=select%201"

# Ingest Arrow data
curl -X POST "http://localhost:8081/v1/ingest?ingestion_queue=my_table" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/vnd.apache.arrow.stream" \
  --data-binary "@data.arrow"
```

When the ingestion pipeline is saturated, `/v1/ingest` responds `429 Too Many Requests`
with a `Retry-After` header — clients should back off and resend the batch.

Query results can also be read directly from DuckDB:

```sql
INSTALL arrow FROM community; LOAD arrow;
CREATE SECRET http_auth (
    TYPE http,
    EXTRA_HTTP_HEADERS MAP {
        'Authorization': 'Bearer <jwt-token>'
    }
);
SELECT * FROM read_arrow(concat('http://localhost:8081/v1/query?q=', url_encode('select 1, 2, 3')));
```

## Connecting via Flight SQL JDBC

Download the [Apache Arrow Flight SQL JDBC driver](https://search.maven.org/search?q=a:flight-sql-jdbc-driver) and connect with:

```bash
jdbc:arrow-flight-sql://localhost:59307?database=memory&useEncryption=0&user=admin&password=admin
```

For instructions on setting up the JDBC driver in [DBeaver Community Edition](https://dbeaver.io),
see this [repo](https://github.com/voltrondata/setup-arrow-jdbc-driver-in-dbeaver).

**Note** — if you stop/restart the server and reconnect via JDBC with the same password, you may
get: "Invalid bearer token provided. Detail: Unauthenticated". The JDBC driver caches the bearer
token signed with the previous instance's secret key. Change the password (`users.0.password`)
in the new container and reconnect to force a fresh token.

## Connecting via the ADBC Python Flight SQL driver

The [ADBC Flight SQL driver](https://pypi.org/project/adbc-driver-flightsql/) keeps data in
columnar form end to end and avoids JDBC serialization overhead.

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install pandas pyarrow adbc_driver_flightsql
python
```

```python
import os
from adbc_driver_flightsql import dbapi, DatabaseOptions

with dbapi.connect(
    uri="grpc+tls://localhost:59307",
    db_kwargs={
        "username": os.getenv("DAZZLEDUCK_USERNAME", "admin"),
        "password": os.getenv("DAZZLEDUCK_PASSWORD", "admin"),
        DatabaseOptions.TLS_SKIP_VERIFY.value: "true",  # not needed with a CA-signed TLS cert
    },
) as conn:
    with conn.cursor() as cur:
        cur.execute("select * from generate_series(20)")
        print(cur.fetch_arrow_table())
```

## Named Queries

Named queries are pre-defined, parameterized SQL templates stored in a DuckDB table and executed
by name over HTTP. Templates use [Jinja2](https://jinja.palletsprojects.com) syntax via Jinjava.

### Setup

Enable the endpoint by setting `named_query_table` in your configuration:

```hocon
dazzleduck_server {
    named_query_table = "named_queries"
}
```

Create the table in DuckDB (all eight columns are required by the store):

```sql
CREATE TABLE named_queries (
    id                     BIGINT PRIMARY KEY,
    name                   VARCHAR UNIQUE,
    template               VARCHAR,
    validators             VARCHAR[],
    description            VARCHAR,
    parameter_descriptions MAP(VARCHAR, VARCHAR),
    preferred_display      VARCHAR,
    query_group            VARCHAR DEFAULT 'general'
);
```

Insert a template:

```sql
INSERT INTO named_queries VALUES (
    1,
    'top_sales',
    'SELECT * FROM sales WHERE region = ''{{ region }}'' LIMIT {{ limit | default(''10'') }}',
    NULL,
    'Returns top sales rows for a given region',
    MAP { 'region': 'Sales region name', 'limit': 'Maximum number of rows' },
    'table',
    'sales'
);
```

A complete seeded example lives in `example/seed/create_named_queries.sql` with the demo
stack under `example/docker/named-query-demo`.

### Executing a named query

```bash
curl -X POST http://localhost:8081/v1/named-query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "top_sales", "parameters": {"region": "WEST", "limit": "10"}}'
```

The response format follows the same `Accept` negotiation as `/v1/query` (Arrow IPC by
default, TSV or JSONL on request). Parameter validation failures return `400` with all
failures collected; an unknown name returns `404`.

### Listing and inspecting

```bash
# Paginated list (limit is capped at 200)
curl -H "Authorization: Bearer $TOKEN" "http://localhost:8081/v1/named-query?offset=0&limit=20"

# Full definition of one named query
curl -H "Authorization: Bearer $TOKEN" http://localhost:8081/v1/named-query/top_sales
```

Both return JSON including each query's `name`, `description`, parameter descriptions,
validator descriptions, `preferred_display`, and `query_group`.

### Parameter validators

Each named query may reference validator class names. Validators implement
`NamedQueryParameterValidator` from `dazzleduck-sql-common`:

```java
public class RegionValidator implements NamedQueryParameterValidator {
    @Override
    public void validate(Map<String, String> parameters) throws ParameterValidationException {
        String region = parameters.get("region");
        if (region == null || region.isBlank()) {
            throw new ParameterValidationException("'region' parameter is required");
        }
    }

    @Override
    public String description() {
        return "Requires a non-blank 'region' parameter";
    }
}
```

All validators run on every request and all failures are collected before returning HTTP 400.
Validator instances are cached (up to 500) to avoid repeated reflection overhead.

## Security and Access Modes

DazzleDuck SQL Server supports four access modes that control query permissions and external
access capabilities:

| Mode | Description | Query Types Allowed | External Access |
|-------|-------------|---------------------|-----------------|
| **COMPLETE** | Full access to all SQL operations | All (INSERT, UPDATE, DELETE, CREATE, DROP, etc.) | Enabled by default |
| **READ_ONLY** | Only SELECT queries allowed | SELECT, UNION, CTE, subqueries, joins, aggregates | Controlled by startup script |
| **RESTRICTED** | SELECT on one datasource; table/path/function scoped via JWT | SELECT on the authorized datasource only | Controlled by startup script |
| **RESTRICT_READ_ONLY** | SELECT on any table; mandatory per-table filter always injected | SELECT on any table (filter always applied) | Disabled by default |

### Configuring access mode

```hocon
dazzleduck_server {
    access_mode = COMPLETE  # COMPLETE | READ_ONLY | RESTRICTED | RESTRICT_READ_ONLY
}
```

### External access control

External access refers to DuckDB's ability to reach external files and functions
(`read_parquet`, `read_json`, `read_csv`, httpfs, cloud storage). In restricted modes it should
be disabled in the startup script:

```hocon
dazzleduck_server.startup_script_provider {
    content = """
        INSTALL arrow FROM community;
        LOAD arrow;

        -- Disable external access for read-only security
        SET enable_external_access = false;
        """
}
```

The startup script provider also supports `script_location` (path to a SQL file) and
substitutes `${ENV_VAR}` references from the environment.

### JWT claims for RESTRICTED mode

Project-specific JWT claims and HTTP headers use the `x-dd-` prefix to avoid colliding with
standard claim names. In `RESTRICTED` mode, the preferred way to grant access is the
`x-dd-access` JWT claim.

**`x-dd-access` claim — format: `[[type, name, projection, filter]]` (exactly one entry)**

| Element | Values | Description |
|---------|--------|-------------|
| `type` | `"table"`, `"path"`, `"function"` | Datasource kind (intra-claim discriminator, not a claim name) |
| `name` | table name / path prefix / function name | The authorized datasource |
| `projection` | `"*"` | Column restriction (reserved, must be `"*"`) |
| `filter` | SQL expression or `"true"` | Row-level filter; `"true"` = no restriction |

Examples:

```bash
# BASE_TABLE access with filter
-H 'x-dd-access: [["table","orders","*","tenant_id='\''abc'\''"]]'

# Path-prefix access (TABLE_FUNCTION)
-H 'x-dd-access: [["path","s3://bucket/tenant1/","*","true"]]'

# Named function access
-H 'x-dd-access: [["function","read_parquet","*","tenant_id='\''abc'\''"]]'
```

**Legacy claims (backward compatible):**

| Claim | Description |
|-------|-------------|
| `database` | Target database/catalog name (unprefixed — Flight SQL / JDBC interop) |
| `schema` | Target schema name (unprefixed — Flight SQL / JDBC interop) |
| `x-dd-table` | Authorized table name (BASE_TABLE) |
| `x-dd-path` | Authorized path prefix (TABLE_FUNCTION) |
| `x-dd-filter` | Optional row filter expression |

### JWT claims for RESTRICT_READ_ONLY mode

`RESTRICT_READ_ONLY` allows SELECT on any table but **always injects the filter** into every
base table reference via CTEs — including JOIN arms, WHERE subqueries, and EXISTS clauses.
Table functions and multi-statement queries are rejected. Tables not covered by the claim
get a `false` filter (they return no rows).

**`x-dd-access` claim — format: `[[type, name, projection, filter], ...]` (one or more `"table"` entries)**

```bash
# Single table
-H 'x-dd-access: [["table","orders","*","tenant_id='\''abc'\''"]]'

# Multiple tables with different filter columns
-H 'x-dd-access: [["table","orders","*","owner_id='\''alice'\''"],["table","items","*","region='\''us'\''"]]'
```

**Legacy — `x-dd-filter` + `x-dd-table` claims (single table only):**

```bash
-H "x-dd-table: orders" -H "x-dd-filter: tenant_id='abc'"
```

The filter is mandatory — requests without `access` or `filter` are rejected.

### Redirect authorization

A token carrying `x-dd-token-type: redirect` plus `x-dd-redirect_url` makes the server resolve
grants from an external endpoint: the authorizer sends a GET with the caller's bearer token and
expects a JSON response listing authorized tables/functions and row filters. Responses are
cached for five minutes.

## SSL / TLS Configuration

By default, all HTTP clients in DazzleDuck enforce strict certificate and hostname validation
using the JVM's default SSL context.

### Self-signed certificates (dev / test)

If you are running against a server with a self-signed certificate, set the
`DD_TRUST_SELF_SIGNED_CERTS` environment variable before starting the process:

```bash
export DD_TRUST_SELF_SIGNED_CERTS=true
```

When this variable is set (to any non-empty value), all internal HTTP clients — including
`HttpArrowProducer`, `RedirectAuthorizer`, `ProxyLoginService`, `HttpCredentialValidator`,
`AuthUtils`, `JwtServerInterceptor`, and `MetricsScraper` — will skip certificate validation
and hostname verification.

**Warning:** never set `DD_TRUST_SELF_SIGNED_CERTS` in production. Use a properly signed
certificate instead.

## HTTP Authentication

JWT authentication is always enforced on the versioned (`/v1`) HTTP endpoints — only `/health`
and `/v1/login` are open. Clients call the login API with username/password and use the
returned token on every subsequent request:

```bash
# Get the JWT token
curl -X POST 'http://localhost:8081/v1/login' \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}'

# Invoke an API with the token
curl -H "Authorization: Bearer <jwt-token>" -s "http://localhost:8081/v1/query?q=select%201"
```

Users are configured under `dazzleduck_server.users`; setting `login_url` instead delegates
`/v1/login` to an external login service. `jwt_token.verify_signature = false` disables
signature verification (tests/demos only).

## Ingestion Queue Routing

Every ingested batch is routed to a named **ingestion queue**, which maps to a target path (and
optionally a DuckLake table).

- **HTTP `/v1/ingest`**: the queue is the `ingestion_queue` URL query parameter. In restricted
  modes the JWT must also grant write access: `x-dd-access-type = WRITE` and an
  `ingestion_queue` claim matching the requested queue.
- **OTel collector (gRPC)**: the queue comes from the `x-dd-ingestion-queue` JWT claim; requests
  without it are rejected with `INVALID_ARGUMENT`. There is no default fallback.

```bash
# Login — the server embeds requested claims in the returned JWT
curl -X POST http://localhost:8081/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin", "claims": {"x-dd-ingestion-queue": "logs"}}'
```

## DuckLake Post-Ingestion Provider

After Arrow data is ingested and written as Parquet, DazzleDuck can automatically register those
files with a DuckLake catalog table via `ingestion_task_factory_provider` (disabled by default).

Set `class` to `io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionTaskFactoryProvider` and
configure `ingestion_queue_table_mapping` entries:

| Field | Required | Description |
|-------|----------|-------------|
| `ingestion_queue` | Yes | Queue name the producer targets |
| `catalog` | Yes | DuckLake catalog owning the target table |
| `schema` | Yes | Schema within the catalog |
| `table` | Yes | Target table name |
| `transformation` | No | SQL `SELECT` referencing placeholder table `__this`; omit to write all columns as-is |
| `view` + `input_table` | No | View-based transformation (see below); both fully qualified `catalog.schema.name`, must be set together, mutually exclusive with `transformation` |
| `additional_parameters` | No | Extra key/value pairs, including the watermark spec below |

Alternatively, `DynamicDuckLakeIngestionTaskFactoryProvider` reads queue mappings from a SQLite
registry (`db_path`), polled at `config_load_interval_ms`, so queues can be added and removed at
runtime without a restart.

### Transformation

`transformation` is a SQL `SELECT` that runs on each batch before it is persisted. The server
wraps it as:

```sql
WITH __this AS (SELECT * FROM read_parquet([...]) ORDER BY ...)
SELECT * FROM (<transformation>)
```

Because the transformation is nested as a subquery, it may declare its own `WITH` clause; those
CTEs can read `__this` (for example, parse JSON once and project several fields from it).

Common patterns:

```sql
-- Column subset
SELECT id, ts, msg FROM __this

-- Derived column
SELECT *, upper(level) AS level FROM __this

-- Row filter
SELECT * FROM __this WHERE level != 'DEBUG'

-- Add ingestion timestamp
SELECT id, ts, msg, current_timestamp AS ingested_at FROM __this

-- CTE: parse once, project several fields
WITH parsed AS (SELECT *, TRY_CAST(msg AS JSON) AS j FROM __this)
SELECT id, ts, msg, json_extract_string(j, '$.user.id') AS user_id FROM parsed
```

### View-Based Transformation

Instead of embedding the transformation SQL in configuration, a mapping can point at a DuckDB
view. The server reads the view's definition and rewrites every reference to `input_table`
into the `__this` placeholder — the view body becomes the transformation:

```hocon
ingestion_queue_table_mapping = [
    {
        ingestion_queue = "logs"
        catalog = "my_catalog"
        schema  = "main"
        table   = "logs"
        view        = "my_catalog.main.logs_transform"   # CREATE VIEW ... AS SELECT ... FROM my_catalog.main.raw_logs
        input_table = "my_catalog.main.raw_logs"         # the table the view reads; becomes __this
    }
]
```

`view` and `input_table` must both be set (and are mutually exclusive with `transformation`);
both must be fully qualified as `catalog.schema.name`. The resolution is validated at startup,
so a missing or malformed view fails fast. Because queue state refreshes when the DuckLake
catalog's schema version changes (which includes view DDL), altering the view updates the
transformation at runtime — no config change or restart needed. The dynamic SQLite provider
supports the same pair via its `view_name` / `input_table` registry columns.

### Data inlining must be off, and startup proves it

DuckLake can keep a small write in the catalog database instead of writing a Parquet file, when
`data_inlining_row_limit` is above zero — and **its default is 10**, so inlining is on unless
something says otherwise. The row is real and queryable, but it lives in the catalog until
`ducklake_flush_inlined_data` runs, and compaction is off by default, so on a default deployment it
never reaches storage.

That matters most where the catalog is shared. A production DuckLake catalog is Postgres or MySQL,
and inlined rows are *data* in it — so every batch writes data rows into a remote transactional
database on the insertion path, which is write amplification and contention on the one component
every writer shares. Many small files are a solved problem (`ducklake_merge_adjacent_files`); data
rows on every insert's critical path are not.

So any server configuring DuckLake ingestion (the query server and the OTel collector alike),
**when at least one of its queues configures a watermark table**:

1. sets `ducklake_default_data_inlining_row_limit = 0` for its DuckDB instance at startup, and
2. **checks the effective limit of every table it writes with SQL — the watermark tables — and
   refuses to start** when any is above zero, naming the table, the source that set it, and the
   statement that fixes it.

The condition matters, because step 1 is **instance-wide**: it moves the default for every DuckLake
attached to that DuckDB instance, including catalogs the server does not own, so every small
`INSERT`, `UPDATE` or `DELETE` on them becomes its own file too. With no watermark configured there
is nothing on the ingestion path to protect — target tables are registered files, which inlining
cannot touch — so the setting is left alone rather than changing other people's writes for no
benefit. It is logged when it is applied.

Step 2 exists because step 1 guarantees nothing on its own: a table-, schema- or catalog-scoped
option, and the `ATTACH` parameter, all override the instance setting. The check resolves all five
sources per table, in DuckLake's own order (table → schema → catalog → `ATTACH` → setting), and the
`ATTACH` parameter is **not** visible in `ducklake_options()` — it is read from
`duckdb_databases().options`, which is the trap any reimplementation falls into.

Target tables need no check: files are registered there, never inserted, and inlining cannot affect
a registration. Only the watermark insert is SQL.

```
Data inlining is enabled for 'my_catalog.main.ingest_watermark' (10 rows, from the ATTACH
parameter DATA_INLINING_ROW_LIMIT), so its rows would be written into the catalog database
instead of storage, on the insertion path of every batch. Fix the source: remove
DATA_INLINING_ROW_LIMIT from the ATTACH of 'my_catalog'. If that catalog is not yours to
change, exempt this table instead:
  CALL my_catalog.set_option('data_inlining_row_limit', 0, table_name => 'main.ingest_watermark');
```

The source's own fix comes first because a table option is stored in the shared catalog and so
applies to every process touching that table, whereas an `ATTACH` parameter is one line in this
server's startup script.

Two consequences worth expecting. **More, smaller files**, until compaction merges them — a
partition-pruning fixture in this repo went from 6 files to 9 when the rule landed, because its
inserts had been collapsing into inlined data. And **the watermark table gains data files of its
own**, in the same snapshot as the data it describes, so a query joining `ducklake_data_file` on
`begin_snapshot` without restricting to a table will now match watermark rows too.

Turning inlining off does not flush rows already inlined; that is
`ducklake_flush_inlined_data`, a compaction concern.

### Watermarks

A queue mapping can commit a watermark row per ingested batch, in the **same transaction** as
the DuckLake file registration. Each row carries the per-group MIN timestamp, MAX timestamp, and
row count. Configure under `additional_parameters`:

| Key | Required | Description |
|-----|----------|-------------|
| `watermark_table` | Yes | Watermark table (same catalog/schema as the target table) |
| `watermark_timestamp_column` | Yes | Source column both MIN and MAX are computed from |
| `watermark_min_timestamp_column` | Yes | Destination column for the MIN timestamp |
| `watermark_max_timestamp_column` | Yes | Destination column for the MAX timestamp |
| `watermark_row_count_column` | Yes | Destination column for the batch row count |
| `watermark_group_columns` | No | Comma-separated grouping columns; empty = one global row per batch |
| `watermark_snapshot_id_column` | Yes | Destination column for a lower bound on the DuckLake snapshot the batch committed in |
| `watermark_batch_id_column` | Yes | Destination column (`UUID`) for the batch id, which the appended snapshot correction attaches to |

A malformed spec (partial keys, blanks, typos in `watermark_*` keys) fails at startup rather
than per batch. Watermarks are not available for queues registered via the dynamic SQLite
provider, whose registry does not store `additional_parameters`.

### The snapshot id column

Every watermark row records the DuckLake snapshot its batch committed in, so the watermark table
must carry the column named by `watermark_snapshot_id_column`:

```sql
ALTER TABLE my_catalog.main.ingest_watermark ADD COLUMN min_commit_snapshot_id BIGINT;
```

The key is **required** whenever a watermark is configured; a spec without it fails at startup.
The column must be `BIGINT` and nullable — it is written on every insert, but leaving it nullable
lets an existing table be migrated without a rewrite.

**The value is a lower bound, not an exact id** — hence the `min_` prefix in the suggested column
name. The true snapshot is the recorded value or higher, never lower, so compare with `>=` / `<=`
rather than `=`, and **take the greatest value recorded for the batch**, which is what a correction
appends:

```sql
-- batches whose data is visible as of snapshot N
SELECT batch_id, max(min_commit_snapshot_id) AS commit_snapshot_id
FROM ingest_watermark WHERE batch_id IS NOT NULL
GROUP BY batch_id HAVING max(min_commit_snapshot_id) <= N
UNION ALL
-- rows written before the batch id column existed carry none; the earlier code corrected those
-- in place, so for them the recorded value is already exact and is read directly
SELECT NULL, min_commit_snapshot_id FROM ingest_watermark
WHERE batch_id IS NULL AND min_commit_snapshot_id <= N;
```

Two details in that query are each the difference between a right and a wrong answer, not
refinements:

**`max(...)` per batch.** Reading the raw column answers **too early** for a corrected batch: the
row as first written keeps its predicted id, and the exact id lives in the appended correction row.
A batch that predicted snapshot 4 but committed in 7 is reported by
`WHERE min_commit_snapshot_id <= 5` as visible at 5, when it is not.

**`WHERE batch_id IS NOT NULL`, with the legacy rows handled separately.** On a table migrated into
the batch id column, every row written before it carries NULL — and SQL groups those into a single
NULL "batch". Grouping them admits or excludes the entire history together on one `max`: with
historical batches at snapshots 2 and 3 the grouped form returns **one** row where there are two,
and a single historical batch at snapshot 9 would hide every earlier one from `N = 5`. Those rows
need no `max` anyway, because the code that wrote them corrected in place.

The bound holds unconditionally. DuckLake assigns the snapshot id at COMMIT and does not expose
the pending one, so what gets written is `max(snapshot_id) + 1` — the same formula DuckLake uses,
read just before the transaction opens. The committed id can only be that or higher, because
`max(snapshot_id)` never decreases (even aggressive `ducklake_expire_snapshots` retains the newest
snapshot, and ids are never reused) and a concurrent writer taking the id first merely pushes the
commit higher.

In practice the bound is tight: immediately after committing, the id is verified against
`begin_snapshot` of the batch's files. If a concurrent writer won the race, the exact id is
recorded by **appending a correction row** — the watermark table is append-only, and no row is ever
updated in place. That step improves accuracy rather than correctness: if it is skipped, or the
process dies before it runs, the column still satisfies its contract and no reader is misled.

### Reading the exact snapshot (`watermark_batch_id_column`)

A correction has to attach to something, and watermark rows carry no key — two batches in the same
group can aggregate identically — so the correction is keyed by batch id. The key is **required**
whenever a watermark is configured, so that there is one behaviour to reason about: the table is
always append-only and the correction is always an append.

**Migrating an existing watermark table.** Add the column, then the key. A queue whose
`watermark_table` is set but whose `watermark_batch_id_column` is missing fails at startup, naming
the key:

```sql
ALTER TABLE my_catalog.main.ingest_watermark ADD COLUMN batch_id UUID;
```

**Use `UUID`, not `VARCHAR`.** The value is a UUID, and the native type is 16 bytes where its text
is 36. The column is distinct on every row, so it compresses no better than its raw bytes and the
difference is close to the full ratio — measured over 50,000 rows, the column costs 1,001,297 bytes
as `UUID` against 1,986,297 as `VARCHAR`, so roughly 20 bytes per row against 40. In Parquet it is
written as `FIXED_LEN_BYTE_ARRAY(16)` with the standard `UUID` logical type, which Arrow and Spark
readers understand. A `VARCHAR` column still works if you have one already.

Do the `ALTER TABLE` first. Nothing verifies at startup that the column exists in the table, so
setting the key against a table without the column fails every batch at commit instead — see the
warning below.

Every row of a batch carries the same batch id, and the exact snapshot of a batch is the greatest
recorded value for that id:

```sql
SELECT batch_id, max(min_commit_snapshot_id) AS commit_snapshot_id
FROM ingest_watermark WHERE batch_id IS NOT NULL GROUP BY batch_id;
```

That is exact rather than a heuristic: a correction can only ever raise the value, and at most one
is written per batch.

A correction row carries **only** the batch id and the snapshot — group columns and aggregates are
NULL — so existing aggregate queries are unaffected by it: `sum(row_count)` and `max(max_ts)` skip
NULLs. Only a reader that wants the exact snapshot needs the per-batch rule above.

**Do not fold aggregates into that `GROUP BY batch_id`.** A batch holds one row per group, so
`any_value(row_count) ... GROUP BY batch_id` keeps one group and drops the rest — correct for a
watermark with no group columns, and silently short for every other kind:

```sql
-- WRONG for a grouped watermark: undercounts to a single group per batch
SELECT sum(row_count) FROM (
    SELECT batch_id, any_value(row_count) AS row_count
    FROM ingest_watermark GROUP BY batch_id);

-- Right: the counts need no deduplication, because corrections contribute NULL
SELECT sum(row_count) FROM ingest_watermark;
```

The batch id is needed for the *snapshot*, not for the counts. The one query it does change is
`count(*)`: correction rows are rows, so count the observations instead with
`count(row_count)`, or filter on `batch_id IS NULL` to see only corrections.

> **Column names are not verified against the table at startup.** A watermark column named in the
> config but absent from the table fails the batch at commit — `FilesNotRegisteredException`, no
> files registered, repeating for every batch — and the underlying error does not name the column.
> This applies to every `watermark_*` column, and `watermark_batch_id_column` is the one most
> likely to be missed, since it is the only one needing an `ALTER TABLE` on an existing table.

Without the column configured nothing changes and nothing breaks: no correction is written and the
column simply stays the lower bound it is specified as. The exact snapshot for a batch can still be
recovered at any time by reading `begin_snapshot` from `ducklake_data_file` for that batch's files.

Never predict this id yourself without the verification step: a lost race commits silently, with
no error.

## Publishing

Tagged releases (`v*`) publish all modules to Maven Central via the GitHub Actions release
workflow. Manual publishing:

```bash
export GPG_TTY=$(tty)
./mvnw -P release-sign-artifacts -DskipTests clean verify
./mvnw -P release-sign-artifacts -DskipTests deploy
```
