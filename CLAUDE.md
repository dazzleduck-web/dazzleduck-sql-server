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
├── dazzleduck-sql-runtime/     # Main entry point, server startup orchestration
├── dazzleduck-sql-flight/      # Arrow Flight SQL server implementation
├── dazzleduck-sql-http/        # HTTP REST API (Helidon-based)
├── dazzleduck-sql-common/      # Shared utilities, type handling, config
├── dazzleduck-sql-commons/     # DuckDB utilities, connection pool, transformations
├── dazzleduck-sql-client/      # HTTP client (JDK 11+)
├── dazzleduck-sql-client-grpc/ # gRPC/Flight SQL client (JDK 11+)
├── dazzleduck-sql-login/       # JWT authentication service
├── dazzleduck-sql-search/      # Full-text search indexing
├── dazzleduck-sql-micrometer/  # Micrometer metrics forwarding
├── dazzleduck-sql-logger/      # SLF4J Arrow logging provider
├── dazzleduck-sql-logback/     # Logback appender for log forwarding
└── dazzleduck-sql-scrapper/    # Prometheus metrics scraping
```

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
| `/v1/plan` | POST | Generate query execution plan |
| `/v1/ingest` | POST | Ingest Arrow data to Parquet |
| `/v1/cancel` | POST | Cancel running query |
| `/health` | GET | Health check |

TSV format: header row + tab-separated string values. Ideal for LLM agents and scripts.
JSONL format: one JSON object per row, per line (newline-delimited, no enclosing array). Numbers/booleans/nulls keep their JSON types; temporal values are ISO-8601 strings; lists/structs/maps are real nested JSON. Streamable and append-friendly.

### dazzleduck-sql-commons
Core DuckDB abstraction. Key classes:
- `ConnectionPool.java` — singleton DuckDB connection management, Arrow reader, bulk ingest
- `Transformations.java` (~600 lines) — SQL → JSON AST, CTE/subquery handling, fingerprinting
- `ExpressionFactory.java` — build SQL AST nodes programmatically
- `ExpressionConstants.java` — AST field/type string constants
- `Fingerprint.java` — SHA-256 of normalized query (literals replaced with placeholders)
- `BulkIngestQueue.java` / `ParquetIngestionQueue.java` — time+size-batched ingestion
- `SqlAuthorizer.java`, `JwtClaimBasedAuthorizer.java` — authorization framework
- Partition pruning: `DucklakePartitionPruning.java`, `HivePartitionPruning.java`, `SplitPlanner.java`

### dazzleduck-sql-common
Shared utilities: `ConfigUtils.java` (all config key constants), `Headers.java` (all HTTP/Flight header constants + type extractors), `SslUtils.java` (env-aware SSL via `DD_TRUST_SELF_SIGNED_CERTS`), `CryptoUtils.java`.

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
    http.authentication = "none"     # or "jwt"

    ingestion.min_bucket_size = 1048576
    ingestion.max_delay_ms = 2000

    jwt_token.expiration = 60m
    jwt_token.claims.generate.headers = [database,catalog,schema,table,filter,path,function]

    users = [{ username = admin, password = admin, groups = [admin, general] }]
}
```

**CLI override:** `--conf key=value` (e.g. `--conf warehouse=/data`, `--conf http.authentication=jwt`)

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

# Ingest Arrow data
curl -X POST "http://localhost:8081/v1/ingest?path=file1.parquet" \
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
