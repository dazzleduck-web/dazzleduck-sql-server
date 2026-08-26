# DazzleDuck SQL Server - Project Documentation

## Overview

High-performance remote DuckDB server with dual protocol support:
- **Arrow Flight SQL** (gRPC, port 59307)
- **RESTful HTTP API** (Helidon, port 8081)

JWT authentication, Arrow-native data transfers, Delta Lake and Hive partition pruning.

## Build & Development

**Requirements:** JDK 21 (server), JDK 11+ (client modules), Maven wrapper (`./mvnw`)

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

**Required JVM flags** (Arrow memory management):
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
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

### dazzleduck-sql-commons
Core DuckDB abstraction (JDK 21). Key classes:
- `ConnectionPool.java` — enum-singleton DuckDB connection (`connection.duplicate()` per use), Arrow reader, record mapping, `executeOnSingleton` for startup scripts
- `Transformations.java` (~2300 lines) — SQL ↔ JSON AST via `json_serialize_sql`, filter-CTE injection (RLS), LEFT-JOIN pruning, limit injection, table-reference collection
- `ExpressionFactory.java` / `ExpressionConstants.java` — build SQL AST nodes / AST string constants
- `Fingerprint.java` — SHA-256 of normalized query (literals replaced with placeholders; does not work with CTEs)
- `ingestion/` — `BulkIngestQueue` (batching, backpressure, producer-id dedup, drain), `ParquetIngestionQueue` (COPY-based writes, transformations via `__this` placeholder), `WatermarkSpec` (per-group MIN/MAX timestamp + row count committed in the DuckLake post-ingestion transaction), `DuckLakeIngestionHandler`, `DynamicDuckLakeIngestionTaskFactoryProvider` (SQLite-backed queue registry)
- `authorization/` — `SqlAuthorizer` with `NOOPAuthorizer`, `SelectOnlyAuthorizer`, `RestrictedDatasourceOnlyAuthorizer`, `RestrictedReadOnlyAuthorizer`, `RedirectAuthorizer` (external `/resolve` endpoint)
- Partition pruning: `ducklake/DucklakePartitionPruning.java` (DuckLake metadata tables), `hive/HivePartitionPruning.java`, `delta/PartitionPruning.java` (Delta Kernel), `planner/SplitPlanner.java` + `planner/PartitionPrunerV2.java`
- `TableConfigProvider.java` — config overrides read from a key/value table
- `namedquery/` — named-query store, request/response models, validator cache

### dazzleduck-sql-common
Shared constants and small utilities (JDK 11): `ConfigConstants.java` (all config key constants — there is no `ConfigUtils`), `Headers.java` (all HTTP/Flight header + JWT claim constants + type extractors), `ContentTypes.java`, `SslUtils.java` (env-aware SSL via `DD_TRUST_SELF_SIGNED_CERTS`), `StartupScriptProvider.java` (env-var substitution in startup SQL), `auth/JwtClaimsExtractor.java`, `types/` Arrow row writers. (`CryptoUtils` lives in the flight module.)

### dazzleduck-sql-otel-collector
OTLP gRPC receiver (default port 4317) for logs/traces/metrics → flattened Arrow schemas → `ParquetIngestionQueue` → Parquet/DuckLake. JWT auth mandatory; queue routing via the `x-dd-ingestion-queue` JWT claim (no default fallback). Embedded `/health` endpoint with MAINTENANCE-aware graceful shutdown. Config root `otel_collector`.

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
`x-dd-function`, `x-dd-token-type`, `x-dd-redirect_url`. Connection-context names
`database` / `schema` stay unprefixed for Flight SQL / JDBC interop, and the URL
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
    jwt_token.claims.generate.headers = [database, schema, x-dd-table, x-dd-filter, x-dd-access, x-dd-path, x-dd-function, x-dd-access-type]

    users = [{ username = admin, password = admin, groups = [admin, general] }]
}
```

**CLI override:** `--conf key=value` (e.g. `--conf warehouse=/data`)

Note: the JWT filter is always installed on versioned HTTP endpoints — the `http.authentication` key is read but no longer disables auth. Tests/demos that need to skip real tokens set `jwt_token.verify_signature = false` instead.

## Testing

**Frameworks:** JUnit 5, JMock, Testcontainers (MinIO, etc.)

**Required:** Use JDK 21 (`JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home`). JDK 25 causes test failures.

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home
./mvnw test
./mvnw test -pl dazzleduck-sql-http
./mvnw test -pl dazzleduck-sql-http -Dtest=QueryServiceTest
```

Patterns: `SharedTestServer` for server reuse, `MutableClock` for time-sensitive tests, `TestUtils.isEqual()` for result comparison.

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
