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
- **Ingestion Pipeline**: batched Arrow-to-Parquet ingestion with backpressure, producer-id deduplication, and DuckLake catalog registration
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

### Session variables (`x-dd-variables`)

The `x-dd-variables` claim works in every access mode. It is a JSON object of string key/values,
applied to the per-request DuckDB connection as `SET VARIABLE` and readable in SQL and in injected
row filters via `getvariable('name')`:

```
x-dd-variables = {"tenant_id":"acme","region":"us-east"}
```

So a filter can reference the value as data instead of a baked-in literal, e.g. an `x-dd-access`
entry of `["table","orders","*","tenant_id = getvariable('tenant_id')"]`.

- **Signed token only.** It is intentionally not a recognized request header, so a client cannot
  override it per request.
- **Values are quoted JSON strings.** A bare number or boolean (`{"n":42}`) is rejected with a hint
  to quote it, since all variables are applied as `VARCHAR` literals; cast for numeric or temporal
  comparisons (`getvariable('n')::INT`).
- **Names** must match `[A-Za-z_][A-Za-z0-9_]*` and are rendered as quoted identifiers, so a
  reserved word such as `table` works.
- **Limits:** values may not contain control characters, are capped at 4096 characters, and at
  most 64 variables may be sent. A malformed claim fails the request.
- **The claim value is JSON text**, i.e. a claim whose value is a string. A token minted with
  `x-dd-variables` as a nested JSON object is rejected at parse time, because claims are read as
  strings.

**Trust model — the login service owns the policy.** "Signed token only" means the value cannot be
overridden per request on an already-issued token. It does **not** mean the query server chose the
value. The value is decided at token issuance, and deciding it is the login service's job:

1. A client *requests* variables (a Flight connection property / call header, or the `claims` map
   in a `POST /v1/login` body).
2. `HttpCredentialValidator` forwards every `jwt_token.claims.generate.headers` entry —
   `x-dd-variables` included — to the configured `login_url` as the `claims` map.
3. The login service decides what to set, override, or reject, and signs only what it approves. A
   production one assigns variables from the authenticated identity (e.g. a fixed tenant per user).

The bundled `LoginService` is a **demo**: it signs the requested claims as given, with no policy.
Do not run it in production. Likewise, when the Flight server mints tokens itself
(`jwt_token.generation = true` with no `login_url`), `AdvanceJWTTokenAuthenticator` copies the
connection headers into the token unfiltered — also a development mode.

`SessionVariables.validate(...)` on the query server is a defence-in-depth hook (allowed names,
value constraints), not the primary control; it is an unimplemented placeholder today.

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
| `additional_parameters` | No | Reserved for per-queue extensions; no key is read today. Ingestion watermarks were removed, so a mapping with a `watermark_` key fails to load |
| `sidecars` | No | Tables written beside the target table from every batch, in the same transaction (see [Sidecars](#sidecars)) |

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
so a missing or malformed view fails fast. The view is re-read on every
`queue_config_refresh_delay_ms` tick, and right away after a failed write, so altering it updates
the transformation at runtime — no config change or restart needed. That holds wherever the view
lives, including a catalog other than the target table's. If a periodic re-read cannot resolve the
view (a brief catalog error, or the view dropped, renamed, or no longer reading `input_table` exactly
once), the queue keeps its last resolved transformation and logs a warning on each tick. If a write
then fails, the view is re-read at once; if it still does not resolve, only that queue is affected:
its batches are rejected with the reason until the view is fixed, and the first batch after the fix
is written. The dynamic SQLite provider supports the same pair via its `view_name` / `input_table`
registry columns.

### Ingestion variables

A query's session variables come from the caller's token (see
[Session variables](#session-variables-x-dd-variables)); the ingestion path has no caller at write
time, so an ingestion queue declares its own in `ingestion_queue_table_mapping`. They are applied as
`SET VARIABLE` on the connection that writes the queue's batches, so a `transformation` (and a
`partition_expression`) reads them with `getvariable('name')`:

```hocon
ingestion_queue_table_mapping = [{
    ingestion_queue = "log"
    catalog = "loglake", schema = "main", table = "log"
    transformation = "SELECT *, getvariable('env') AS env FROM __this"

    variables { env = "prod", retention_days = "30" }   # static, read once at startup
    variables_view              = "loglake.main.v_log_vars"  # key/value relation, reloaded
    variables_key_column        = "key"                      # default
    variables_value_column      = "value"                    # default
    variables_expiration_column = "expires_at"               # optional
}]
```

- **Reload.** The relation is read at startup and again on every `queue_config_refresh_delay_ms` —
  the copy of that key inside the `ingestion_task_factory_provider` block, which is what the handler
  reads (default 2 minutes); the dynamic provider uses `config_load_interval_ms` instead. It is the
  same tick that re-derives a view-based transformation, so a value changes without restarting the
  server. Rows are data, not schema, so the reload does not wait for a DuckLake schema change.
- **One queue per relation.** Every row is applied, and a key that is not a usable variable name
  fails the queue instead of being skipped. To keep several queues' variables in one table, give
  each queue a view selecting its own rows.
- **Precedence.** Where both sources define a name, the relation wins (the file holds the defaults).
- **Types.** All values are `VARCHAR`; cast for other types (`getvariable('retention_days')::INT`).
- **Expiry.** With `variables_expiration_column` configured, a row whose expiration has passed is
  treated as absent — the variable is no longer set, so the transformation reads the file's value
  for that name or `NULL`. A `NULL` expiration never expires. The comparison is made by DuckDB
  against its own `now()`, and takes effect on the refresh that follows it.
- **Validation.** Names and values go through the same validation and escaping as the JWT claim
  (`SqlVariables`, shared with `SessionVariables`).

A dynamic queue gets them the same way, from its SQLite registry row: the `variables_view`,
`variables_key_column`, `variables_value_column` and `variables_expiration_column` columns name the
relation (static pairs are config-file only). The registry stores only the relation's
`catalog.schema.table` path, so changing a value is a write to that relation — no `schema_version`
bump. The relation may be a table in the registry file itself, which the startup script must
`ATTACH` on the ingestion connection, since the registry loader's own attachment is private.
A SQLite column arrives as `VARCHAR`, so an expiration is cast for the comparison; text that will
not cast is an error, not a row that never expires.

### Sidecars

A queue may declare **sidecars**: tables beside its target table, each filled from every batch by a
query over the batch's rows, exposed as `__base`. A sidecar's files are registered in the same
DuckLake transaction as the batch's own, so a batch and its sidecars commit or roll back together.

```hocon
ingestion_queue_table_mapping = [{
    ingestion_queue = "log"
    catalog = "loglake", schema = "main", table = "log"
    transformation = "SELECT *, date_trunc('minute', ts) AS minute FROM __this"

    sidecars = [
        { table = "log_minute_counts"
          transformation = "SELECT minute, service, severity, count(*) AS n FROM __base GROUP BY ALL" }
        { table = "log_errors"
          transformation = "SELECT ts, service, body FROM __base WHERE severity = 'ERROR'" }
    ]
}]
```

- **`__base`** is the batch's rows as the target table gets them: after the queue's `transformation`
  and the table's partition projections (for example the `bucket` column of a `bucket(N, col)`
  partitioning). The queue's [ingestion variables](#ingestion-variables) apply, so a sidecar can read
  `getvariable('name')`.
- **Tables are created and checked at startup.** A sidecar table lives in the queue's catalog and
  schema. Startup runs each sidecar's query over an empty `__base`: a missing table is created with
  its columns, and an existing table whose columns differ in name or type fails startup, naming the
  table and the column. To change a sidecar's columns, change the table first (for example
  `ALTER TABLE ... ADD COLUMN`), then the query. The empty `__base` has the target table's columns,
  since a queue of this provider declares no input schema: a sidecar must read only columns the
  table has, and the transformation must produce every column a sidecar reads.
- **Every file is written with the table's column types,** selected by name and cast. A value that
  cannot be cast fails the batch.
- **Some types cannot be used.** Parquet stores `HUGEINT` and `UHUGEINT` as `DOUBLE`, and `TIMETZ` is
  not mapped back, so DuckLake would reject every file. A sidecar column of one of those types, at
  any depth, fails startup. `sum()` over `BIGINT` returns `HUGEINT`, so cast it:
  `sum(bytes)::BIGINT AS bytes`.
- **How a batch is written.** The batch is staged in a local directory next to the queue's input
  files. The base files are written there once, from the input with the transformation applied;
  each sidecar is then derived from those staged base files, so the base and every sidecar have the
  same rows even when the transformation is not deterministic (`now()`, `random()`). Only when every
  output exists is each file uploaded unchanged, in parallel, by DuckDB itself
  (`COPY (SELECT content FROM read_blob(file)) TO 'dest' (FORMAT blob)`), so uploads use DuckDB's
  own file systems and secrets: S3, GCS, Azure or a local path, with no other client or credentials.
  The staging directory is deleted when the write ends. A queue without sidecars writes as before,
  straight to its output path, with no staging.
- **Staging needs room.** The staging directory, next to the input files (or the system temp
  directory if that path has `=`, `*`, `?`, `[` or `{`, which DuckDB would misread), holds each
  in-flight batch's base and sidecar files. Each upload reads its file into memory as one value,
  with up to 16 at once, and DuckDB cannot read a file over 4 GB that way: keep the bucket size well
  below that.
- **Failures.** If a step fails, the others are cancelled and the batch fails with nothing
  registered. Before the upload nothing has left the staging directory; files already uploaded are
  deleted where they are local, and left for `ducklake_delete_orphaned_files` on object storage. A
  sidecar whose query returns no rows for a batch writes and registers no file.
- **A sidecar table dropped under a running queue** is created again after the next failed batch. One
  partitioned under a running queue fails that queue's batches with the reason until it is unpartitioned.

Not supported yet: partitioned sidecar tables, a sidecar query taken from a view
(`transformation_view`), and sidecars on queues of the dynamic SQLite provider. The design, including
local staging with an uploader for object storage, is in `doc/SIDECAR_VIEWS_SPEC.md`.

### Registration conflicts

A batch's files are registered with `ducklake_add_data_files` in one transaction. On stock DuckLake
(duckdb/ducklake#1895) its commit fails with a transaction conflict when a DELETE on the same table
commits meanwhile, such as the compactor's data retention job's. The registration is then retried in
a new transaction, up to 5 attempts with a short jittered backoff, instead of failing the batch back
to the client. The conflict is decided before DuckLake writes anything, so nothing of the failed
attempt was registered and the same files are registered again. When the attempts run out, the batch
fails as not registered, so its files are deleted and the client may resend it.

Only a conflict with a concurrent DELETE is retried. A conflict with an ALTER or DROP of the table is
not, since registering the same files under a changed schema could silently drop columns, and any
other commit failure has an unknown outcome, so retrying it could register the batch twice.

## Publishing

Tagged releases (`v*`) publish all modules to Maven Central via the GitHub Actions release
workflow. Manual publishing:

```bash
export GPG_TTY=$(tty)
./mvnw -P release-sign-artifacts -DskipTests clean verify
./mvnw -P release-sign-artifacts -DskipTests deploy
```
