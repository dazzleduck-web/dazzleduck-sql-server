# DazzleDuck SQL Common

Shared configuration keys, header constants, and small utilities used across DazzleDuck SQL
modules (package `io.dazzleduck.sql.common`).

This module deliberately targets **Java 11** bytecode and has a minimal dependency footprint
(TypeSafe Config, jjwt, Arrow vector, Jackson) so client-side artifacts can embed it in older
applications. Do not add server-only code or heavyweight dependencies here — that belongs in
`dazzleduck-sql-commons`.

## Contents

| Class | Purpose |
|-------|---------|
| `ConfigConstants` | Every HOCON config key constant used across the project (root `dazzleduck_server`, ingestion, JWT, HTTP client, batching, retry keys), plus `getWarehousePath` / `getTempWriteDir` helpers |
| `Headers` | Every HTTP/Flight header and JWT claim name constant, with typed value extractors |
| `ContentTypes` | Content-type constants: JSON, Arrow IPC stream, TSV, JSONL/NDJSON |
| `SslUtils` | `sslContext()` / `httpClient()` factories; the `DD_TRUST_SELF_SIGNED_CERTS` env var (read once at class load) switches them to trust-all variants for dev/test |
| `StartupScriptProvider` | SPI for loading startup SQL from config (`startup_script_provider` block: `class`, `content`, `script_location`), with `${ENV_VAR}` substitution that fails on undefined variables |
| `ConfigBasedStartupScriptProvider` | Default implementation: inline `content` concatenated with the file at `script_location` |
| `auth/JwtClaimsExtractor` | Parses JWT claims (optionally without signature verification) and flattens the configured claims for authorization |
| `auth/LoginRequest` / `auth/LoginResponse` | JSON DTOs for the `/v1/login` exchange |
| `NamedQueryParameterValidator` | SPI implemented by named-query parameter validators |
| `types/DataType` | DuckDB type model (`toSql()`, struct/field nesting, construction from a DuckDB AST cast) |
| `types/VectorSchemaRootWriter`, `types/JavaRow` | Write plain Java rows into Arrow vectors — the write path used by all the client-side producers |

## Header and Claim Names

Project-specific names carry the `x-dd-` prefix; `database`, `schema`, and the URL query
parameter `ingestion_queue` stay unprefixed for Flight SQL / JDBC interoperability.

| Constant | Wire value |
|----------|-----------|
| `HEADER_FETCH_SIZE` | `x-dd-fetch-size` |
| `HEADER_DATABASE` / `HEADER_SCHEMA` | `database` / `schema` |
| `HEADER_TABLE` / `HEADER_PATH` / `HEADER_FUNCTION` / `HEADER_FILTER` | `x-dd-table` / `x-dd-path` / `x-dd-function` / `x-dd-filter` |
| `HEADER_ACCESS` / `HEADER_ACCESS_TYPE` | `x-dd-access` / `x-dd-access-type` |
| `HEADER_SPLIT_SIZE` | `x-dd-split-size` |
| `HEADER_DATA_PARTITION` / `HEADER_DATA_FORMAT` / `HEADER_SORT_ORDER` | `x-dd-partition` / `x-dd-format` / `x-dd-sort-order` |
| `HEADER_PRODUCER_ID` / `HEADER_PRODUCER_BATCH_ID` | `x-dd-producer-id` / `x-dd-producer-batch-id` |
| `HEADER_QUERY_TIMEOUT` | `x-dd-query-timeout` |
| `HEADER_ARROW_COMPRESSION` | `x-dd-arrow-compression` |
| `HEADER_TOKEN_TYPE` | `x-dd-token-type` (values `inline` / `redirect`) |
| `HEADER_REDIRECT_URL` | `x-dd-redirect_url` (note: underscore — historical) |
| `HEADER_INGESTION_QUEUE` / `CLAIM_INGESTION_QUEUE` | `x-dd-ingestion-queue` |
| `QUERY_PARAMETER_INGESTION_QUEUE` | `ingestion_queue` |

## SSL for Dev / Test

```bash
export DD_TRUST_SELF_SIGNED_CERTS=true
```

Any non-empty value makes every HTTP client produced by `SslUtils` skip certificate validation
and hostname verification. Never set this in production.

## Requirements

- Java 11+ (bytecode target 11; built with JDK 21)
