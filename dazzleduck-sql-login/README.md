# DazzleDuck SQL Login

JWT authentication and token-issuing service. It is used two ways:

1. **As a library** — `dazzleduck-sql-http` registers `LoginService` (or `ProxyLoginService`)
   at `POST /v1/login` on the main server.
2. **As a standalone service** — `io.dazzleduck.sql.login.Main` starts its own Helidon server
   (default port 8080) exposing the same endpoint, configured under the separate HOCON root
   `dazzleduck_login_service`.

The module has no DuckDB dependency.

## Endpoint

### `POST /v1/login`

Request:

```json
{
  "username": "admin",
  "password": "admin",
  "claims": { "x-dd-table": "orders", "x-dd-filter": "tenant_id = 'abc'" }
}
```

`claims` is an optional string-to-string map; the server embeds each entry as a JWT claim.
Claim names understood downstream are listed in the root README (`database`, `schema`,
`x-dd-table`, `x-dd-filter`, `x-dd-path`, `x-dd-function`, `x-dd-access`,
`x-dd-ingestion-queue`, ...).

Response `200`:

```json
{ "accessToken": "<jwt>", "username": "admin", "tokenType": "Bearer" }
```

Any failure (bad credentials or otherwise) returns `401` with an empty body.

## Implementations

| Class | Behavior |
|-------|----------|
| `LoginService` | Validates credentials against the configured `users` list (SHA-256 hashed passwords, constant-time comparison) and mints an HMAC-signed JWT with the requested claims and `jwt_token.expiration` |
| `ProxyLoginService` | Forwards the raw JSON body to an external login URL and relays the backend's status and body verbatim. Selected by the HTTP module when `dazzleduck_server.login_url` is set |
| `ProxyResolveAccessService` | Mock `/resolve` endpoint for redirect-authorization tests — returns a hard-coded grant set. Test fixture only; do not use in production |

## Configuration (standalone mode)

HOCON root `dazzleduck_login_service` (this module's `reference.conf`):

```hocon
dazzleduck_login_service {
    http.port = 8080
    http.host = "localhost"
    users = [{ username = admin, password = admin, groups = [admin, general] }]
    jwt_token.expiration = 60m
    secret_key = "..."   # base64 HMAC key — must match the server that verifies the tokens
}
```

The `secret_key` must be shared with whatever verifies the issued tokens (the Flight/HTTP
server's `dazzleduck_server.secret_key`). The bundled default is a well-known dev key —
**change it in production**.

## Security Notes

- Requested claims are embedded **as sent** — there is no server-side allow-list. Do not expose
  this login service to callers who should not be able to choose their own authorization claims;
  in that situation issue tokens from a trusted external service and use `login_url` /
  preconfigured tokens instead.
- Tokens are HMAC-signed (`jjwt`); expiration granularity is minutes.

## Requirements

- Java 21
