# DazzleDuck SQL Server - Project Documentation

## Overview

High-performance remote DuckDB server with dual protocol support:
- **Arrow Flight SQL** (gRPC, port 59307)
- **RESTful HTTP API** (Helidon, port 8081)

JWT authentication, Arrow-native data transfers, Delta Lake and Hive partition pruning.

## Build & Development

**Requirements:** JDK 25 (build, tests, and the runtime images), client modules keep bytecode target 11, Maven wrapper (`./mvnw`)

```bash
# Build
./mvnw clean package install -DskipTests

# Run tests (all or specific module)
./mvnw test
./mvnw test -pl dazzleduck-sql-http

# Run locally
./mvnw exec:java -pl dazzleduck-sql-runtime -Dexec.mainClass="io.dazzleduck.sql.runtime.Main" -Dexec.args="--conf warehouse=warehouse"

# Docker
docker run -ti -p 59307:59307 -p 8081:8081 dazzleduck/dazzleduck:latest --conf warehouse=/data

# Docker image (local dev, Apple Silicon)
./mvnw package -DskipTests jib:dockerBuild -pl dazzleduck-sql-runtime -Djib.architecture=arm64
```

**Required JVM flags** (Arrow memory management on JDK 25):
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow"
```

## Project Structure

```
dazzleduck-sql-server/
├── dazzleduck-sql-runtime/           # Main entry point, server startup orchestration, Docker image
├── dazzleduck-sql-flight/            # Arrow Flight SQL server implementation (also named-query + output listeners)
├── dazzleduck-sql-http/              # HTTP REST API (Helidon 4, HTTP/2)
├── dazzleduck-sql-common/            # Shared constants (ConfigConstants, Headers, ContentTypes), SslUtils, JWT claim extraction (JDK 11)
├── dazzleduck-sql-commons/           # DuckDB utilities: connection pool, AST transformations, authorization, ingestion (JDK 21)
├── dazzleduck-sql-client/            # HTTP ingestion client, Arrow batching + backpressure (JDK 11)
├── dazzleduck-sql-client-grpc/       # gRPC/Flight SQL ingestion client (JDK 11)
├── dazzleduck-sql-login/             # JWT login service (LoginService / ProxyLoginService)
├── dazzleduck-sql-search/            # Inverted-index construction for full-text search (query side unimplemented)
├── dazzleduck-sql-micrometer/        # Micrometer StepMeterRegistry → Arrow → /v1/ingest
├── dazzleduck-sql-logback/           # Logback appender for log forwarding (JDK 11)
├── dazzleduck-sql-scrapper/          # Prometheus endpoint scraper → Arrow → /v1/ingest
├── dazzleduck-sql-otel-collector/    # OTLP gRPC collector (logs/traces/metrics → Parquet/DuckLake), port 4317
├── dazzleduck-sql-ducklake-compactor/# Scheduled DuckLake minor/major compaction + snapshot housekeeping
└── dazzleduck-sql-examples/          # docker-compose integration tests (Testcontainers, packaging=pom)
```

Note: `dazzleduck-sql-logger` was removed (2026-02); `dazzleduck-sql-logback` is its independent replacement.

## Module Details

### dazzleduck-sql-runtime
Entry point. `Main.java` (CLI/shutdown hooks) → `Runtime.java` (server lifecycle, starts both HTTP and Flight SQL).

### dazzleduck-sql-flight
Key files: `DuckDBFlightSqlProducer.java` (~1500 lines, core producer), `FlightSqlProducerFactory.java`, `ErrorHandling.java`, `ResultSetStreamUtil.java`.
Auth: `AdvanceJWTTokenAuthenticator.java`, `AdvanceBasicCallHeaderAuthenticator.java`. Metrics: `MicroMeterFlightRecorder.java`.

### dazzleduck-sql-http
Key files: `QueryService.java`, `IngestionService.java`, `PlanningService.java`, `JwtAuthenticationFilter.java`, `ParameterUtils.java`.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/login` | POST | Authenticate, get JWT token |
| `/v1/query` | GET/POST | Execute SQL — Arrow IPC (default), TSV (`Accept: text/tab-separated-values`), or JSONL/NDJSON (`Accept: application/jsonl` or `application/x-ndjson`) |
| `/v1/plan` | GET/POST | Query plan with splits (`x-dd-split-size` header or query param) |
| `/v1/ingest` | POST | Ingest Arrow data to Parquet (`?ingestion_queue=` required; 429 + `Retry-After` on backpressure) |
| `/v1/cancel` | GET/POST | Cancel running query by statement `id` |
| `/v1/named-query` | GET/POST | List / get-by-name / execute Jinja-templated named queries (only when `named_query_table` is configured) |
| `/v1/ui` | GET | Metrics dashboard (HTML) |
| `/health` | GET | Health check (unversioned, unauthenticated) |

TSV format: header row + tab-separated string values. Ideal for LLM agents and scripts.
JSONL format: one JSON object per row, per line (newline-delimited, no enclosing array). Numbers/booleans/nulls keep their JSON types; temporal values are ISO-8601 strings; lists/structs/maps are real nested JSON. Streamable and append-friendly.
Compression: any response is gzip-compressed when the request sends `Accept-Encoding: gzip`, and uncompressed otherwise (Helidon's `helidon-http-encoding-gzip`, found by service loading; it flushes per batch, so streaming is unchanged). Request bodies with `Content-Encoding: gzip` are decompressed too, e.g. for `/v1/ingest`.

### dazzleduck-sql-commons
Core DuckDB abstraction (JDK 21). Key classes:
- `ConnectionPool.java` — enum-singleton DuckDB connection (`connection.duplicate()` per use), Arrow reader, record mapping, `executeOnSingleton` for startup scripts
- `Transformations.java` (~2300 lines) — SQL ↔ JSON AST via `json_serialize_sql`, filter-CTE injection (RLS), LEFT-JOIN pruning, limit injection, table-reference collection
- `ExpressionFactory.java` / `ExpressionConstants.java` — build SQL AST nodes / AST string constants
- `Fingerprint.java` — SHA-256 of normalized query (literals replaced with placeholders; does not work with CTEs)
- `ingestion/` — `BulkIngestQueue` (batching, backpressure, producer-id dedup, drain), `ParquetIngestionQueue` (COPY-based writes, transformations via `__this` placeholder), `IngestionVariables` (per-queue `SET VARIABLE` values read by a transformation via `getvariable`, from the conf file, the SQLite registry, and/or a reloadable key/value relation with optional expiry), `WatermarkSpec` (per-group MIN/MAX timestamp + row count committed in the DuckLake post-ingestion transaction), `DuckLakeIngestionHandler`, `DynamicDuckLakeIngestionTaskFactoryProvider` (SQLite-backed queue registry)
- `authorization/` — `SqlAuthorizer` with `NOOPAuthorizer`, `SelectOnlyAuthorizer`, `RestrictedDatasourceOnlyAuthorizer`, `RestrictedReadOnlyAuthorizer`, `RedirectAuthorizer` (external `/resolve` endpoint)
- Partition pruning: `ducklake/DucklakePartitionPruning.java` (DuckLake metadata tables), `hive/HivePartitionPruning.java`, `delta/PartitionPruning.java` (Delta Kernel), `planner/SplitPlanner.java` + `planner/PartitionPrunerV2.java`
- `TableConfigProvider.java` — config overrides read from a key/value table
- `namedquery/` — named-query store, request/response models, validator cache

### dazzleduck-sql-common
Shared constants and small utilities (JDK 11): `ConfigConstants.java` (all config key constants — there is no `ConfigUtils`), `Headers.java` (all HTTP/Flight header + JWT claim constants + type extractors), `ContentTypes.java`, `SslUtils.java` (env-aware SSL via `DD_TRUST_SELF_SIGNED_CERTS`), `StartupScriptProvider.java` (env-var substitution in startup SQL), `auth/JwtClaimsExtractor.java`, `types/` Arrow row writers. (`CryptoUtils` lives in the flight module.)

### dazzleduck-sql-otel-collector
OTLP gRPC receiver (default port 4317) for logs/traces/metrics → flattened Arrow schemas → `ParquetIngestionQueue` → Parquet/DuckLake. JWT auth mandatory; queue routing via the `x-dd-ingestion-queue` JWT claim (no default fallback). Embedded HTTP server (health port, default 8081) serves `/health` (MAINTENANCE-aware graceful shutdown) and `/stats` (auto-refreshing per-queue ingestion dashboard, shared `StatsHtml` renderer with the main server's `/v1/ui`). Config root `otel_collector`.

### dazzleduck-sql-ducklake-compactor
Standalone service running `ducklake_merge_adjacent_files` (minor/major) plus snapshot expiry and file cleanup on schedules. Config root `dazzleduck_sql_compaction`; `/health` on port 8080 (always UP). Docker image `dazzleduck/ducklake-compactor`.

## Authorization & Access Modes

Four modes set via `access_mode` config:

| Mode | Permitted | Authorizer | External Access |
|------|-----------|------------|-----------------|
| **COMPLETE** | All SQL | none | enabled |
| **READ_ONLY** | SELECT only | `SELECT_ONLY_AUTHORIZER` | startup script |
| **RESTRICTED** | SELECT on one datasource scoped by JWT | `RESTRICTED_DATASOURCE_AUTHORIZER` | startup script |
| **RESTRICT_READ_ONLY** | SELECT any table; per-table CTE filter injected | `RESTRICT_READ_ONLY_AUTHORIZER` | disabled |

**Project-specific JWT claims and HTTP headers are namespaced with the `x-dd-` prefix**
to avoid collisions with standard claim names. The mapping is:
`x-dd-access`, `x-dd-access-type`, `x-dd-table`, `x-dd-filter`, `x-dd-path`,
`x-dd-function`, `x-dd-token-type`, `x-dd-redirect_url`, `x-dd-variables`. Connection-context
names `database` / `schema` stay unprefixed for Flight SQL / JDBC interop, and the URL
query parameter `ingestion_queue` also keeps its short form.

**JWT `x-dd-access` claim — RESTRICTED mode** (exactly one entry, preferred over legacy claims):
```
x-dd-access = [["table",    "orders",       "*", "tenant_id='abc'"]]
x-dd-access = [["path",     "s3://bucket/", "*", "true"]]
x-dd-access = [["function", "read_parquet", "*", "tenant_id='abc'"]]
```
Format: `[[type, name, projection, filter]]` — `projection` must be `"*"`, `filter` is a SQL WHERE expression.
The `type` values (`"table"` / `"path"` / `"function"`) are intra-claim discriminators, not claim names — they stay unprefixed.

Legacy separate claims: `x-dd-table`, `x-dd-path`, `x-dd-filter` (backward compatible).

**JWT `x-dd-access` claim — RESTRICT_READ_ONLY mode** (multiple tables supported):
```
x-dd-access = [["table","orders","*","owner_id='alice'"],["table","items","*","region='us'"]]
```
Filter is injected as a CTE for every base table reference (JOINs, subqueries, EXISTS — nothing bypasses it). Only `"table"` type supported; external access disabled.

**JWT `x-dd-variables` claim — session variables** (all access modes). A JSON object of string
key/values, applied to the per-request DuckDB connection as `SET VARIABLE` and readable in SQL
and in injected RLS filters via `getvariable('name')`:
```
x-dd-variables = {"tenant_id":"acme","region":"us-east"}
```
So a filter can reference the value as data instead of a baked-in literal, e.g. an `x-dd-access`
entry of `["table","orders","*","tenant_id = getvariable('tenant_id')"]`. Trusted from the
verified (signed) token **only** — it is intentionally not a recognized request header, so a
client cannot override it per request. Every value must be a **quoted JSON string** — a bare
number or boolean (`{"n":42}`) is rejected with a hint to quote it, since all variables are
applied as VARCHAR literals; cast for numeric/temporal comparisons (`getvariable('n')::INT`).
Variable names must match `[A-Za-z_][A-Za-z0-9_]*` and are rendered as quoted identifiers, so a
reserved word such as `table` works. Values may not contain control characters, are capped at 4096
characters, and at most 64 variables may be sent; a malformed claim fails the request.

The claim value is the JSON **text**, i.e. a claim whose value is a string. A token minted with
`x-dd-variables` as a nested JSON object is rejected at parse time, because claims are read as
strings.

**Trust model — the login service owns the policy.** "Verified-claim only" means the value cannot
be overridden per request on an already-issued token: the query path resolves it from the signed
claims, never from a request header. It does **not** mean the query server chose the value. The
value is decided at token issuance, and deciding it is the **login service's** job:

- A client *requests* variables (a Flight connection property / call header, or the `claims` map in
  a `POST /v1/login` body).
- `HttpCredentialValidator` forwards every `claims.generate.headers` entry — `x-dd-variables`
  included — to the configured `login_url` as the `claims` map.
- The login service decides what to set, override, or reject, and signs only what it approves. A
  production one assigns variables from the authenticated identity (e.g. a fixed tenant per user).

The bundled `LoginService` is a **demo**: it signs `loginRequest.claims()` as given, with no policy.
Do not run it in production. Likewise, when the Flight server mints tokens itself
(`jwt_token.generation = true` with no `login_url`), `AdvanceJWTTokenAuthenticator` copies the
connection headers into the token unfiltered — also a development mode.

`SessionVariables.validate(...)` on the query server is a defence-in-depth hook (allowed names,
value constraints), not the primary control; it is an unimplemented placeholder today.

**Ingestion variables — per queue, not per request.** The query path's session variables come from
the caller's token; the ingestion path has no caller at write time, so an ingestion queue declares
its own in `ingestion_queue_table_mapping`. They are applied as `SET VARIABLE` on the connection
that writes the queue's batches, so a `transformation` (and a `partition_expression`) reads them
with `getvariable('name')`:

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

The relation is read at startup and again on every `queue_config_refresh_delay_ms` — the copy of
that key inside the `ingestion_task_factory_provider` block, which is what the handler reads
(default 2 minutes); the dynamic provider uses `config_load_interval_ms` instead. It is the same
tick that re-derives a view-based transformation, so a value changes without restarting the server.
Rows are data, not schema, so that reload deliberately does not wait for a DuckLake schema change.
The relation holds that one queue's variables — every row is applied, and a key that is not a usable
variable name fails the queue instead of being skipped; to keep several queues' variables in one
table, give each queue a view selecting its own rows. Where both sources define a name the relation
wins (the file holds the defaults). All values are
`VARCHAR`; cast for other types (`getvariable('retention_days')::INT`). With
`variables_expiration_column` configured, a row whose expiration has passed is treated as absent —
the variable is no longer set, so the transformation reads the file's value for that name or
`NULL` — and a `NULL` expiration never expires; the comparison is made by DuckDB against its own
`now()`, and takes effect on the refresh that follows it. Names and values go through the same
validation and escaping as the JWT claim (`SqlVariables`, shared with `SessionVariables`).

A dynamic queue gets them the same way, from its SQLite registry row: the `variables_view`,
`variables_key_column`, `variables_value_column` and `variables_expiration_column` columns name the
relation (static pairs are config-file only). The registry stores only the relation's
`catalog.schema.table` path, so changing a value is a write to that relation — no `schema_version`
bump. The relation may be a table in the registry file itself, which the startup script must
`ATTACH` on the ingestion connection, since the registry loader's own attachment is private.
A SQLite column arrives as `VARCHAR`, so an expiration is cast for the comparison; text that will
not cast is an error, not a row that never expires.

**External access control** (for restricted modes):
```sql
SET enable_external_access = true;   -- in startup script to enable
SET enable_external_access = false;  -- default for restricted modes
```

## Configuration

TypeSafe Config (HOCON), `src/main/resources/application.conf` per module.

```hocon
dazzleduck_server = {
    warehouse = ${user.dir}"/warehouse"
    secret_key = "base64-encoded-key"
    access_mode = COMPLETE           # COMPLETE | READ_ONLY | RESTRICTED | RESTRICT_READ_ONLY
    networking_modes = [flight-sql, http]

    flight_sql.port = 59307
    http.port = 8081

    ingestion.min_bucket_size = 1048576
    ingestion.max_delay_ms = 2000

    jwt_token.expiration = 60m
    ticket_ttl_ms = 3600000          # signed Flight ticket lifetime (bound to the issuing user); allow for clock skew between nodes
    jwt_token.claims.generate.headers = [database, schema, x-dd-table, x-dd-filter, x-dd-access, x-dd-path, x-dd-function, x-dd-access-type, x-dd-variables]

    users = [{ username = admin, password = admin, groups = [admin, general] }]
}
```

**CLI override:** `--conf key=value` (e.g. `--conf warehouse=/data`)

Note: the JWT filter is always installed on versioned HTTP endpoints — the `http.authentication` key is read but no longer disables auth. Tests/demos that need to skip real tokens set `jwt_token.verify_signature = false` instead.

## Testing

**Frameworks:** JUnit 5, JMock, Testcontainers (MinIO, etc.)

**Required:** Use JDK 25 (`JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home`), the same JVM the images run. Arrow/Netty need `--enable-native-access=ALL-UNNAMED` and `--sun-misc-unsafe-memory-access=allow` on it; surefire gets them from the parent pom's `arrow.jvm.flags`. Delta Lake reads go through Hadoop, which needs 3.4.3+ (`hadoop.version`) on JDK 23+; older Hadoop calls `Subject.getSubject` and fails.

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home
./mvnw test
./mvnw test -pl dazzleduck-sql-http
./mvnw test -pl dazzleduck-sql-http -Dtest=QueryServiceTest
```

Patterns: `SharedTestServer` for server reuse, `MutableClock` for time-sensitive tests, `TestUtils.isEqual()` for result comparison.

**Stuck tests.** A hung test fails instead of stalling the build:
- Every test and lifecycle method times out after 5 minutes (`junit.default.timeout` in the parent pom). A test that legitimately needs longer sets its own `@Timeout`.
- On a timeout JUnit prints a thread dump to the test output before interrupting the test.
- A test JVM still running after `surefire.fork.timeout` seconds (1800) is killed ("There was a timeout in the fork").

Both can be tightened for a local run:

```bash
./mvnw test -pl dazzleduck-sql-http -Djunit.default.timeout="30 s" -Dsurefire.fork.timeout=300
```

The JUnit dump lists platform threads only. Helidon handles HTTP requests on virtual threads, so for a live hang in the HTTP server take a full dump of the surefire fork (`pgrep -f surefirebooter` gives its PID):

```bash
jcmd PID Thread.dump_to_file -format=text /tmp/threads.txt
```

Key test classes: `DuckDBFlightJDBCTest`, `FlightSqlProducerFactoryTest`, `QueryServiceTest`, `HttpMetricIntegrationTest`.

## API Usage Examples

```bash
# TSV query (plain text, best for scripts/LLMs)
curl -H "Accept: text/tab-separated-values" "http://localhost:8081/v1/query?q=select%201"

# Arrow IPC query (default, ZSTD-compressed binary)
curl -H "Authorization: Bearer <token>" "http://localhost:8081/v1/query?q=select%201"

# Login
curl -X POST http://localhost:8081/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin"}'

# Ingest Arrow data (routed by ingestion queue)
curl -X POST "http://localhost:8081/v1/ingest?ingestion_queue=my_table" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/vnd.apache.arrow.stream" \
  --data-binary "@file.arrow"

# Flight SQL JDBC
jdbc:arrow-flight-sql://localhost:59307?database=memory&useEncryption=0&user=admin&password=admin
```

## Troubleshooting

1. **Arrow Memory Error** — ensure JVM `--add-opens` flags are set (see Build section)
2. **Bearer Token Invalid** — token cached from previous instance; change password to force reissue
3. **Port in Use** — check for running instances on 59307 (Flight) or 8081 (HTTP)
4. **DuckDB Extension Not Found** — add to startup script:
   ```sql
   INSTALL arrow FROM community; LOAD arrow;
   ```

## Documentation & MDX Rules (IMPORTANT)

When editing any `.md` file:
- Write **Docusaurus-compatible MDX** — never raw HTML or Java generics in prose (`Map<String, String>`, `List<T>`)
- Wrap all code, types, and signatures in **fenced code blocks**
- No angle brackets (`< >`) in normal text — escape or move to code blocks
- Use Markdown tables/lists/headings over HTML

Violations break the documentation build.
