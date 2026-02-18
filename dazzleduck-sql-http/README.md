# DazzleDuck SQL HTTP Module

This module provides HTTP REST API endpoints for the DazzleDuck SQL Server, built on **Helidon WebServer 4.x**.

## Overview

- **Framework**: Helidon WebServer
- **API Version**: v1
- **Base Path**: `/v1` (except health check and UI)
- **Authentication**: JWT token-based (required for all versioned endpoints by default)
- **Data Format**: Apache Arrow IPC streaming format for query results

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

- **Content-Type**: `application/vnd.apache.arrow.stream`
- **Body**: Binary Apache Arrow IPC stream
- **Timeout**: Default 120 seconds (configurable)

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
| split_size | long | No | Target split size in bytes for partitioning (default: 1GB) |

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
  -H "split_size: 1" \
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
| ingestion_queue | string | Yes | Target table path (cannot start with "/" or contain "..") |

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
curl -X POST "http://localhost:8080/v1/ingest?ingestion_queue=my_table" \
  -H "Content-Type: application/vnd.apache.arrow.stream" \
  -H "x-dd-partition: year,month" \
  -H "x-dd-project: col1,\"col2 + col3\",col4" \
  --data-binary @data.arrow
```

---

## Authentication

### JWT Authentication (Required)

All versioned API endpoints require a valid JWT Bearer token for authentication.

**Protected Endpoints**:
- `/v1/query`
- `/v1/plan`
- `/v1/ingest`
- `/v1/cancel`
- `/v1/ui`

**Unprotected Endpoints**:
- `/health` - Health check (always accessible)
- `/v1/login` - Authentication endpoint (for obtaining tokens)

**Authorization Header**:
```
Authorization: Bearer <JWT token>
```

**Note**: To disable authentication (not recommended for production), set `http.authentication` to `"none"` in your configuration.

### CORS Configuration

- **Default Allow-Origin**: `*` (configurable)
- **Allowed Methods**: GET, POST
- **Allowed Headers**: Content-Type, Authorization

---

## Configuration

| Key | Description | Default |
|-----|-------------|---------|
| `http.host` | Server host | localhost |
| `http.port` | Server port | 8080 |
| `http.authentication` | Auth mode ("none" or "jwt") | jwt |
| `jwt_token.expiration` | JWT token expiration | - |
| `jwt_token.claims.generate.headers` | Headers to extract as JWT claims | - |
| `jwt_token.claims.validate.headers` | Headers to validate | - |
| `allow-origin` | CORS allow-origin value | * |
| `warehouse_path` | DuckDB warehouse path | - |
| `secret_key` | Base64-encoded JWT secret key | - |

---

## Content Types

| Content-Type | Usage |
|--------------|-------|
| `application/json` | JSON requests/responses |
| `application/vnd.apache.arrow.stream` | Arrow IPC streaming format |
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
curl -X POST http://localhost:8080/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "secret"}'

# Use the returned token in subsequent requests
curl -X POST http://localhost:8080/v1/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"query": "SELECT * FROM users"}' \
  --output result.arrow
```

### Check Health Status

```bash
curl http://localhost:8080/health
```

### Ingest Data

```bash
# JWT authentication is required by default
curl -X POST "http://localhost:8080/v1/ingest?ingestion_queue=my_table" \
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
    ├── Main.java                        # Application entry point
    ├── QueryService.java                # Query execution endpoint
    ├── HealthCheckService.java          # Health check endpoint
    ├── PlanningService.java             # Query planning endpoint
    ├── CancelService.java               # Query cancellation endpoint
    ├── IngestionService.java            # Data ingestion endpoint
    ├── UIService.java                   # Metrics dashboard UI
    ├── AbstractQueryBasedService.java   # Base service for query endpoints
    ├── JwtAuthenticationFilter.java     # JWT authentication filter
    ├── QueryRequest.java                # Query request model
    ├── ContentTypes.java                # Content type constants
    ├── HttpConfig.java                  # HTTP configuration
    └── HttpException.java               # Exception base class
```
