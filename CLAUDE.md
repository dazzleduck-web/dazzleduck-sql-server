# DazzleDuck SQL Server

Remote DuckDB server: **Arrow Flight SQL** (gRPC, port 59307) + **HTTP API** (Helidon 4, port 8081). JWT auth, Arrow-native transfer, DuckLake/Delta/Hive partition pruning.

## Build, Run, Test

JDK 25 for build, tests and images (`JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home`); client modules keep bytecode target 11.

```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow"
./mvnw clean package install -DskipTests
./mvnw test                                                    # full suite
./mvnw test -pl dazzleduck-sql-http                            # one module
./mvnw test -pl dazzleduck-sql-http -Dtest=QueryServiceTest    # one class
./mvnw exec:java -pl dazzleduck-sql-runtime -Dexec.mainClass="io.dazzleduck.sql.runtime.Main" -Dexec.args="--conf warehouse=warehouse"
./mvnw package -DskipTests jib:dockerBuild -pl dazzleduck-sql-runtime -Djib.architecture=arm64   # local image
```

- Surefire gets the Arrow/Netty JVM flags from the parent pom's `arrow.jvm.flags`.
- Delta Lake reads need Hadoop 3.4.3+ (`hadoop.version`) on JDK 23+ (older calls `Subject.getSubject`).
- Frameworks: JUnit 5, JMock, Testcontainers. Patterns: `SharedTestServer`, `MutableClock`, `TestUtils.isEqual()`.
- Hung tests: 5 min per test (`junit.default.timeout`, override with `@Timeout`; prints a thread dump), fork killed after `surefire.fork.timeout` (1800 s). Tighten locally with `-Djunit.default.timeout="30 s" -Dsurefire.fork.timeout=300`. The JUnit dump omits virtual threads (Helidon requests) — find the fork's PID with `pgrep -f surefirebooter` and run `jcmd PID Thread.dump_to_file -format=text /tmp/threads.txt`.

## Modules

| Module | Purpose |
|--------|---------|
| `runtime` | `Main` → `Runtime`; starts HTTP + Flight; Docker image |
| `flight` | `DuckDBFlightSqlProducer` (core), auth (`AdvanceJWTTokenAuthenticator`, `AdvanceBasicCallHeaderAuthenticator`), named-query + output listeners, `CryptoUtils` |
| `http` | `QueryService`, `IngestionService`, `PlanningService`, `JwtAuthenticationFilter`, `ParameterUtils` |
| `common` (JDK 11) | `ConfigConstants` (all config keys; no `ConfigUtils`), `Headers` (header/claim constants), `ContentTypes`, `SslUtils` (`DD_TRUST_SELF_SIGNED_CERTS`), `StartupScriptProvider`, `auth/JwtClaimsExtractor`, `types/` row writers |
| `commons` (JDK 21) | DuckDB core — see below |
| `client` (JDK 11) | HTTP ingestion client, Arrow batching + backpressure |
| `client-grpc` (JDK 11) | gRPC/Flight SQL ingestion client |
| `login` | `LoginService` (demo) / `ProxyLoginService` |
| `search` | Inverted-index build (query side unimplemented) |
| `micrometer`, `scrapper`, `logback` | Metrics registry / Prometheus scraper / log appender → Arrow → `/v1/ingest` |
| `otel-collector` | OTLP gRPC (4317) → Parquet/DuckLake; JWT mandatory, queue from `x-dd-ingestion-queue` claim (no fallback); `/health` + `/stats` on 8081; config root `otel_collector` |
| `ducklake-compactor` | Scheduled `ducklake_merge_adjacent_files` + snapshot expiry/cleanup; config root `dazzleduck_sql_compaction`; `/health` on 8080 |
| `examples` | docker-compose integration tests (packaging=pom) |

All modules are prefixed `dazzleduck-sql-`.

**commons key classes:** `ConnectionPool` (singleton DB, `duplicate()` per use, `executeOnSingleton`); `Transformations` (SQL ↔ JSON AST via `json_serialize_sql`, RLS filter-CTE injection, LEFT-JOIN pruning, limit injection); `ExpressionFactory`/`ExpressionConstants`; `Fingerprint` (normalized-query SHA-256, not CTE-safe); `ingestion/` (`BulkIngestQueue`, `ParquetIngestionQueue` with `__this` transformations, `IngestionVariables`, `Sidecar`/`SidecarTables` (per-batch tables over `__base`, registered with the batch), `DuckLakeIngestionHandler`, `DynamicDuckLakeIngestionTaskFactoryProvider` = SQLite queue registry); `authorization/` (`SqlAuthorizer` impls incl. `RedirectAuthorizer` → external `/resolve`); pruning in `ducklake/`, `hive/`, `delta/`, `planner/` (`SplitPlanner`, `PartitionPrunerV2`); `TableConfigProvider`; `namedquery/`.

## HTTP API

| Endpoint | Notes |
|----------|-------|
| `POST /v1/login` | Returns JWT |
| `GET/POST /v1/query` | Arrow IPC (default), TSV (`Accept: text/tab-separated-values`), JSONL (`application/jsonl` or `application/x-ndjson`; typed JSON, nested lists/structs, ISO-8601 temporals) |
| `GET/POST /v1/plan` | Splits via `x-dd-split-size` (header or query param) |
| `POST /v1/ingest` | `?ingestion_queue=` required; 429 + `Retry-After` on backpressure |
| `GET/POST /v1/cancel` | By statement `id` |
| `GET/POST /v1/named-query` | Jinja named queries; only if `named_query_table` set |
| `GET /v1/ui`, `GET /health` | Metrics dashboard; unauthenticated health |

Gzip responses when `Accept-Encoding: gzip` (flushes per batch); gzip request bodies (`Content-Encoding: gzip`) are accepted. JDBC: `jdbc:arrow-flight-sql://localhost:59307?database=memory&useEncryption=0&user=admin&password=admin`.

## Access Modes (`access_mode`)

| Mode | Permitted | Authorizer | External access |
|------|-----------|------------|-----------------|
| `COMPLETE` | All SQL | none | enabled |
| `READ_ONLY` | SELECT only | `SELECT_ONLY_AUTHORIZER` | via startup script |
| `RESTRICTED` | SELECT on one JWT-scoped datasource | `RESTRICTED_DATASOURCE_AUTHORIZER` | via startup script |
| `RESTRICT_READ_ONLY` | SELECT any table, per-table filter CTE on every base-table reference | `RESTRICT_READ_ONLY_AUTHORIZER` | disabled |

Project claims/headers use the `x-dd-` prefix (`x-dd-access`, `-access-type`, `-table`, `-filter`, `-path`, `-function`, `-token-type`, `-redirect_url`, `-variables`). `database`, `schema` and the `ingestion_queue` query param stay unprefixed.

`x-dd-access` = `[[type, name, "*", filter]]`, `type` ∈ `table`/`path`/`function`. RESTRICTED: exactly one entry (legacy `x-dd-table`/`x-dd-path`/`x-dd-filter` still accepted). RESTRICT_READ_ONLY: many entries, `table` only.

```
x-dd-access = [["table","orders","*","tenant_id='abc'"],["table","items","*","region='us'"]]
```

### Variables

`x-dd-variables` (per-request, from the signed token only) and per-queue ingestion variables (`variables`, `variables_view`, ...) are both applied as `SET VARIABLE` and read via `getvariable`; all values are VARCHAR. Full rules (limits, trust model, reload, expiry, dynamic queues) are in README.md under **Session variables** and **Ingestion variables**. Security invariants that keep `SET VARIABLE` injection-safe — preserve them when touching `SessionVariables`/`SqlVariables`: never accept a request-header override, string values only, names matching `[A-Za-z_][A-Za-z0-9_]*`, no control chars, max 4096 chars per value and 64 variables. The bundled `LoginService` signs client claims unfiltered — demo only.

## Configuration

HOCON, `src/main/resources/application.conf`, root `dazzleduck_server`; override with `--conf key=value`. Notable keys: `warehouse`, `secret_key`, `access_mode`, `networking_modes`, `flight_sql.port`, `http.port`, `ingestion.min_bucket_size`, `ingestion.max_delay_ms`, `jwt_token.expiration`, `jwt_token.claims.generate.headers`, `ticket_ttl_ms` (signed Flight ticket lifetime, bound to user; allow for clock skew), `users`.

JWT auth is always on for versioned HTTP endpoints (`http.authentication` no longer disables it); tests skip real tokens with `jwt_token.verify_signature = false`.

## Troubleshooting

- Arrow memory error → missing JVM flags above.
- Bearer token invalid → cached from previous instance; change password to force reissue.
- Port in use → a leftover server or test JVM holds 59307 (Flight) or 8081 (HTTP); `lsof -i :8081` finds it.
- Extension not found → `INSTALL arrow FROM community; LOAD arrow;` in startup script.

## Markdown Rules (IMPORTANT)

When editing any `.md` file (violations break the Docusaurus build):
- Write **Docusaurus-compatible MDX** — never raw HTML or Java generics in prose
- Wrap all code, types, and signatures in **fenced code blocks**
- No angle brackets in normal text — escape them or move them to code blocks
- Use Markdown tables/lists/headings over HTML
