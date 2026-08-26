# DazzleDuck SQL HTTP Module

This module provides HTTP REST API endpoints for the DazzleDuck SQL Server, built on **Helidon WebServer 4.x**.

## Overview

- **Framework**: Helidon WebServer 4.x with HTTP/2 (h2 when TLS is enabled, h2c otherwise, with HTTP/1.1 fallback)
- **API Version**: v1
- **Base Path**: `/v1` (except the health check)
- **Authentication**: JWT token-based, always enforced on all versioned endpoints
- **Data Formats**: Apache Arrow IPC (default, ZSTD-compressed), TSV, and JSONL/NDJSON via `Accept` negotiation

The module is a thin adapter over the same `DuckDBFlightSqlProducer` used by the Arrow Flight
SQL server: each HTTP request becomes a synthetic Flight call context, so authorization, access
modes, metrics, and ingestion behave identically on both protocols.

## API Endpoints

### Health Check

**Endpoint**: `GET /health`

Check server and database health status.

**Response**:
| Status | Description |
|--------|-------------|
| 200 OK | Database is up |
| 503 Service Unavailable | Database is down |

**Response Body**:
```json
{
  "status": "UP|DEGRADED",
  "uptime_seconds": 12345,
  "database": {
    "status": "UP|DOWN",
    "check": "SELECT 1",
    "error": "error message if DOWN"
  },
  "metrics": {
    "bytes_in": 1024.0,
    "bytes_out": 2048.0
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

---

### Login

**Endpoint**: `POST /v1/login`

Authenticate users and obtain JWT access tokens.

**Request Body**:
```json
{
  "username": "user@example.com",
  "password": "password123",
  "claims": {
    "custom_claim": "value"
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| username | string | Yes | User identifier |
| password | string | Yes | User password |
| claims | object | No | Custom JWT claims |

**Response**:
| Status | Description |
|--------|-------------|
| 200 OK | Authentication successful |
| 401 Unauthorized | Invalid credentials |

**Response Body**:
```json
{
  "accessToken": "<JWT token>",
  "username": "user@example.com",
  "tokenType": "Bearer"
}
```

---

### Query Execution

**Endpoint**: `GET|POST /v1/query`

Execute SQL queries and return results in Apache Arrow format.

#### GET Request
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| q | string | Yes | URL-encoded SQL query |
| id | long | No | Statement ID (auto-generated if omitted) |

**Example**: `GET /v1/query?q=SELECT%20*%20FROM%20users&id=123`

#### POST Request
```json
{
  "query": "SELECT * FROM users",
  "id": 123
}
```

**Response**:
| Status | Description |
|--------|-------------|
| 200 OK | Query executed successfully |
| 400 Bad Request | Invalid query or parameters |
| 500 Internal Server Error | Execution error |
| 504 Gateway Timeout | Query timeout exceeded |

**Response format** is selected by the `Accept` request header:

| Accept | Content-Type returned | Body |
|--------|----------------------|------|
| _(default)_ | `application/vnd.apache.arrow.stream` | Arrow IPC stream, ZSTD-compressed (override with `x-dd-arrow-compression: none`) |
| `text/tab-separated-values` | `text/tab-separated-values; charset=utf-8` | Header row + tab-separated values |
| `application/jsonl` or `application/x-ndjson` | `application/jsonl; charset=utf-8` | One JSON object per row |

**Other request headers** (each also accepted as a URL query parameter):

| Header | Description |
|--------|-------------|
| `x-dd-fetch-size` | Rows per Arrow batch (default 10000) |
| `x-dd-query-timeout` | Per-query timeout in seconds; must be non-negative and below the server's `max_query_timeout_ms` |
| `x-dd-arrow-compression` | `zstd` (default) or `none` |

- **Timeout**: Default 120 seconds (`query_timeout_ms`)

---

### Query Planning

**Endpoint**: `GET|POST /v1/plan`

Get query execution plan with endpoint locations and statement handles for distributed query execution.

#### GET Request
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| q | string | Yes | URL-encoded SQL query |
| id | long | No | Statement ID |

#### POST Request
```json
{
  "query": "SELECT * FROM users",
  "id": 123
}
```

**Headers**:
| Header | Type | Required | Description |
|--------|------|----------|-------------|
| `x-dd-split-size` | long | No | Target split size in bytes for partitioning (default: 1GB); also accepted as a query parameter |

**Response**:
| Status | Description |
|--------|-------------|
| 200 OK | Plan retrieved successfully |
| 500 Internal Server Error | Planning error |

**Response Body**:

Returns an array of `PlanResponse` objects, one per query split/partition:

```json
[
  {
    "endpoints": ["http://0.0.0.0:8081"],
    "descriptor": {
      "statementHandle": {
        "query": "SELECT * FROM users",
        "queryId": 123,
        "producerId": "uuid-string",
        "splitSize": 1073741824,
        "queryChecksum": "base64-checksum"
      }
    }
  }
]
```

| Field | Type | Description |
|-------|------|-------------|
| endpoints | string[] | HTTP endpoint URLs where this split can be executed |
| descriptor.statementHandle.query | string | The SQL query (potentially modified for this split) |
| descriptor.statementHandle.queryId | long | Unique identifier for the query |
| descriptor.statementHandle.producerId | string | UUID of the producer that created this plan |
| descriptor.statementHandle.splitSize | long | Size of this split in bytes (-1 if not split) |
| descriptor.statementHandle.queryChecksum | string | Base64-encoded checksum for query validation |

**Example - Multiple Splits**:

When using `split_size` header to partition large queries:

```bash
curl -X POST http://localhost:8081/v1/plan \
  -H "Content-Type: application/json" \
  -H "x-dd-split-size: 1" \
  -d '{"query": "SELECT * FROM read_parquet(...)"}'
```

Response with multiple partitions:
```json
[
  {
    "endpoints": ["http://0.0.0.0:8081"],
    "descriptor": {
      "statementHandle": {
        "query": "SELECT * FROM read_parquet(['file1.parquet'])",
        "queryId": 1,
        "producerId": "abc-123",
        "splitSize": 254,
        "queryChecksum": "..."
      }
    }
  },
  {
    "endpoints": ["http://0.0.0.0:8081"],
    "descriptor": {
      "statementHandle": {
        "query": "SELECT * FROM read_parquet(['file2.parquet'])",
        "queryId": 2,
        "producerId": "abc-123",
        "splitSize": 312,
        "queryChecksum": "..."
      }
    }
  }
]
```

---

### Query Cancellation

**Endpoint**: `GET|POST /v1/cancel`

Cancel a running query by statement ID.

#### GET Request
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| q | string | No | SQL query (usually empty) |
| id | long | Yes | Statement ID to cancel |

#### POST Request
```json
{
  "query": "",
  "id": 123
}
```

**Response**:
| Status | Description |
|--------|-------------|
| 200 OK | Query cancelled successfully |
| 400 Bad Request | Missing statement ID |
| 500 Internal Server Error | Cancellation error |

**Response Body**: Plain text message
- `"query cancel successfully."`
- `"failed to cancel query."`

---

### Data Ingestion

**Endpoint**: `POST /v1/ingest`

Bulk ingest data in Arrow format into tables.

**Query Parameters**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| ingestion_queue | string | Yes | Target ingestion queue (cannot start with "/" or contain ".."). In restricted access modes the JWT must grant write access to this queue |

**Headers**:
| Header | Type | Required | Description |
|--------|------|----------|-------------|
| Content-Type | string | Yes | Must be `application/vnd.apache.arrow.stream` |
| x-dd-format | string | No | Data format (default: "parquet") |
| x-dd-partition | string | No | Partition columns (CSV format, URL-encoded) |
| x-dd-project | string | No | Projection expressions (CSV format, URL-encoded) |
| x-dd-producer-id | string | No | Producer identifier |
| x-dd-producer-batch-id | long | No | Producer batch ID |
| x-dd-sort-order | string | No | Sort order columns (CSV format, URL-encoded) |

**Request Body**: Binary Apache Arrow IPC stream

**Example**:
```http
POST /v1/ingest?ingestion_queue=my_table HTTP/1.1
Content-Type: application/vnd.apache.arrow.stream
x-dd-format: parquet
x-dd-partition: year,month

[binary Arrow stream data]
```

**Response**:
| Status | Description |
|--------|-------------|
| 200 OK | Ingestion completed |
| 400 Bad Request | Invalid parameters |
| 415 Unsupported Media Type | Wrong content type |
| 429 Too Many Requests | Ingestion backpressure — retry after the number of seconds in the `Retry-After` response header |
| 500 Internal Server Error | Ingestion error |

---

### Metrics Dashboard (UI)

**Endpoint**: `GET /v1/ui`

Web-based monitoring dashboard for real-time metrics and query management.

| Route | Description |
|-------|-------------|
| `GET /v1/ui/` | HTML dashboard page |
| `GET /v1/ui/styles.css` | Dashboard CSS styles |
| `GET /v1/ui/script.js` | Dashboard JavaScript |
| `GET /v1/ui/api/metrics` | Metrics data (HTML tables) |

**Dashboard Features**:
- Application metrics (start time, statement counts)
- Network metrics (bytes in/out, arrow batches)
- Running statements with details
- Open prepared statements
- Running bulk ingestion status
- Query cancellation support

---

### Named Queries

**Endpoint**: `/v1/named-query` — registered only when `named_query_table` is set in the
configuration.

| Route | Method | Description |
|-------|--------|-------------|
| `/v1/named-query` | GET | Paginated list (`offset` default 0, `limit` default 20, max 200), JSON |
| `/v1/named-query/{name}` | GET | Full named-query definition, JSON; `404` if unknown |
| `/v1/named-query` | POST | Execute — body `{"name": "...", "parameters": {...}}`; response follows the same `Accept` negotiation as `/v1/query` (Arrow IPC / TSV / JSONL) |

Errors: missing/blank `name` → `400`; validator failures → `400` with all failures collected;
unknown template → `404`; timeout → `504`.

See the root `README.md` for the named-query table schema, template syntax, and examples.

---

## Header Value Parsing

Several headers accept multiple values in CSV format. These headers are parsed using RFC 4180-compliant CSV parsing.

### CSV-Parsed Headers

| Header | Constant | Description |
|--------|----------|-------------|
| `x-dd-partition` | `HEADER_DATA_PARTITION` | Partition columns |
| `x-dd-project` | `HEADER_DATA_PROJECT` | Projection expressions |
| `x-dd-sort-order` | `HEADER_SORT_ORDER` | Sort order columns |

### Partition Header Limitations

The `x-dd-partition` header is treated as **column references only**. Values are automatically wrapped in double quotes for SQL safety.

**Supported:**
- Simple column names: `year`, `month`, `user_id`
- Column names with underscores: `created_at`, `order_id`

**Not Supported:**
- Expressions: `year + 1`, `EXTRACT(year FROM date)`
- Functions: `UPPER(name)`, `DATE_TRUNC('month', ts)`
- Table-qualified names: `table.column`
- Aliases: `column AS alias`

**Examples:**

```
# Valid
x-dd-partition: year,month,day

# Invalid - these will be treated as literal column names (quoted as-is)
x-dd-partition: YEAR(date)           # Becomes "YEAR(date)" - not a function call
```

For complex partitioning logic, use the `x-dd-project` header to create derived columns first, then reference those columns in the partition header.

### Sort Order Header

The `x-dd-sort-order` header accepts **column references with optional sort direction** (ASC or DESC). Column names are automatically wrapped in double quotes for SQL safety.

**Supported:**
- Simple column names: `created_at`, `id`
- Column names with direction: `created_at DESC`, `id ASC`
- Multiple columns: `created_at DESC,id ASC`
- Mixed (with and without direction): `created_at DESC,id`

**Not Supported:**
- Expressions: `year + 1`, `EXTRACT(year FROM date)`
- Functions: `UPPER(name)`, `DATE_TRUNC('month', ts)`
- NULLS FIRST/LAST modifiers
- Table-qualified names: `table.column`

**Examples:**

```
# Valid
x-dd-sort-order: created_at
x-dd-sort-order: created_at DESC
x-dd-sort-order: created_at DESC,id ASC
x-dd-sort-order: year,month DESC,day

# Invalid - these will be treated as literal column names
x-dd-sort-order: created_at NULLS FIRST    # Becomes "created_at NULLS FIRST"
```

For complex sorting logic, use the `x-dd-project` header to create derived columns first, then reference those columns in the sort order header.

### Projection Header

The `x-dd-project` header supports **expressions** and is validated against SQL injection patterns.

**Supported:**
- Column references: `col1`, `col2`
- Arithmetic expressions: `col1 + col2`, `price * quantity`
- Function calls: `UPPER(name)`, `CONCAT(first, last)`
- CASE expressions: `CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END`
- Aliases: `col1 + col2 AS total`
- String literals: `'constant'`

**Blocked (SQL Injection Protection):**
- SQL keywords (surrounded by whitespace): `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `DROP`, `UNION`, `FROM`, `WHERE`, etc.
- SQL comments: `--`, `/*`, `*/`
- Statement separators: `;`

### Parsing Rules

1. **Simple comma-separated values**: Values are split by commas
   ```
   x-dd-partition: year,month,day
   ```
   Result: `["year", "month", "day"]`

2. **Quoted values**: Use double quotes for values containing commas or spaces
   ```
   x-dd-project: "col1 + col2",col3,"CASE WHEN x > 1 THEN 'a' ELSE 'b' END"
   ```
   Result: `["col1 + col2", "col3", "CASE WHEN x > 1 THEN 'a' ELSE 'b' END"]`

3. **Escaped quotes**: Use double quotes to escape quotes within quoted values
   ```
   x-dd-project: "concat(col1, "" - "", col2)"
   ```
   Result: `["concat(col1, \" - \", col2)"]`

4. **Whitespace handling**: Leading and trailing whitespace is trimmed from each value
   ```
   x-dd-partition:  year , month , day
   ```
   Result: `["year", "month", "day"]`

5. **Empty values**: Empty strings and blank values are ignored
   ```
   x-dd-partition: year,,month
   ```
   Result: `["year", "month"]`

6. **Null or blank header**: Returns an empty array
   ```
   x-dd-partition:
   ```
   Result: `[]`

### URL Encoding

When passing header values in query parameters or HTTP headers, remember to URL-encode special characters:

| Character | Encoded |
|-----------|---------|
| `,` | `%2C` |
| `"` | `%22` |
| ` ` (space) | `%20` or `+` |

**Example**:
```bash
curl -X POST "http://localhost:8081/v1/ingest?ingestion_queue=my_table" \
  -H "Content-Type: application/vnd.apache.arrow.stream" \
  -H "x-dd-partition: year,month" \
  -H "x-dd-project: col1,\"col2 + col3\",col4" \
  --data-binary @data.arrow
```

---

## Authentication

### JWT Authentication (Always Enforced)

All versioned API endpoints require a valid JWT Bearer token. The authentication filter is
always installed — the `http.authentication` config key is read but no longer disables it.
For tests and demos, set `jwt_token.verify_signature = false` to skip signature verification
instead.

**Protected Endpoints**:
- `/v1/query`
- `/v1/plan`
- `/v1/ingest` (additionally requires write access to the requested `ingestion_queue`)
- `/v1/cancel`
- `/v1/ui`
- `/v1/named-query`

**Unprotected Endpoints**:
- `/health` - Health check (always accessible)
- `/v1/login` - Authentication endpoint (for obtaining tokens)

**Authorization Header**:
```
Authorization: Bearer <JWT token>
```

When `login_url` is configured, `/v1/login` proxies credentials to that external login service
instead of validating against the local `users` list.

### CORS Configuration

- **Default Allow-Origin**: `["https://dazzleduck-ui.netlify.app"]` (configurable via `http.allow-origin`)
- **Allowed Methods**: GET, POST
- **Allowed Headers**: Content-Type, Authorization, x-dd-arrow-compression

---

## Configuration

All keys live under the `dazzleduck_server` HOCON root.

| Key | Description | Default |
|-----|-------------|---------|
| `http.host` | Server host | `0.0.0.0` |
| `http.port` | Server port | `8081` |
| `http.allow-origin` | CORS allow-origin list | `["https://dazzleduck-ui.netlify.app"]` |
| `http.tls.enabled` | Enable TLS (activates HTTP/2 h2) | `false` |
| `warehouse` | DuckDB warehouse path | `${user.dir}/warehouse` |
| `secret_key` | Base64-encoded JWT/HMAC secret key | dev placeholder — change in production |
| `access_mode` | `COMPLETE`, `READ_ONLY`, `RESTRICTED`, `RESTRICT_READ_ONLY` | `COMPLETE` |
| `query_timeout_ms` | Default query timeout | `120000` |
| `max_query_timeout_ms` | Upper bound for client `x-dd-query-timeout` | `300000` |
| `jwt_token.expiration` | JWT token expiration | `60m` |
| `jwt_token.verify_signature` | Verify JWT signatures | `true` |
| `jwt_token.claims.generate.headers` | Headers embedded as JWT claims on login | see flight `reference.conf` |
| `named_query_table` | Table holding named queries; enables `/v1/named-query` | unset |
| `login_url` | External login service; makes `/v1/login` a proxy | unset |
| `ingestion.*` | Ingestion queue tuning (`min_bucket_size`, `max_delay_ms`, `max_pending_write`, ...) | see flight `reference.conf` |

---

## Content Types

| Content-Type | Usage |
|--------------|-------|
| `application/json` | JSON requests/responses |
| `application/vnd.apache.arrow.stream` | Arrow IPC streaming format (default query results, ingestion body) |
| `text/tab-separated-values` | TSV query results |
| `application/jsonl` / `application/x-ndjson` | JSONL/NDJSON query results |
| `text/html` | UI dashboard pages |
| `text/css` | CSS stylesheets |
| `application/javascript` | JavaScript |

---

## Error Handling

### HTTP Status Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad Request - Validation/parse errors |
| 401 | Unauthorized - Authentication failure |
| 403 | Forbidden - Authorization failure |
| 404 | Not Found |
| 409 | Conflict |
| 415 | Unsupported Media Type |
| 500 | Internal Server Error |
| 501 | Not Implemented |
| 503 | Service Unavailable - Database down |
| 504 | Gateway Timeout - Query timeout |

---

## Quick Start Examples

### Execute a Query (curl)

```bash
# JWT authentication is required by default
# First, login to get a token
curl -X POST http://localhost:8081/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "secret"}'

# Use the returned token in subsequent requests
curl -X POST http://localhost:8081/v1/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"query": "SELECT * FROM users"}' \
  --output result.arrow
```

### Check Health Status

```bash
curl http://localhost:8081/health
```

### Ingest Data

```bash
# JWT authentication is required by default
curl -X POST "http://localhost:8081/v1/ingest?ingestion_queue=my_table" \
  -H "Content-Type: application/vnd.apache.arrow.stream" \
  -H "Authorization: Bearer <token>" \
  -H "x-dd-format: parquet" \
  --data-binary @data.arrow
```

---

## Module Structure

```
dazzleduck-sql-http/
└── src/main/java/io/dazzleduck/sql/http/server/
    ├── Main.java                        # Application entry point (also embeddable via Main.start)
    ├── QueryService.java                # Query execution endpoint (Arrow/TSV/JSONL)
    ├── HealthCheckService.java          # Health check endpoint
    ├── PlanningService.java             # Query planning endpoint
    ├── CancelService.java               # Query cancellation endpoint
    ├── IngestionService.java            # Data ingestion endpoint
    ├── NamedQueryService.java           # Named-query endpoints (conditional)
    ├── UIService.java                   # Metrics dashboard UI
    ├── AbstractQueryBasedService.java   # Base service for query endpoints
    ├── ControllerService.java           # Synthetic Flight context + error mapping
    ├── JwtAuthenticationFilter.java     # JWT authentication filter
    ├── ParameterUtils.java              # Header-or-query-param extraction
    ├── FlightToHttpEndpointMapper.java  # Flight locations to HTTP endpoint URLs
    └── model/                           # QueryRequest, PlanResponse, Descriptor, HttpConfig
```

Content-type constants live in `io.dazzleduck.sql.common.ContentTypes`.
