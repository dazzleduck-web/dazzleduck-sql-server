# DazzleDuck SQL Runtime

The process launcher and startup orchestrator — this module produces the published
`dazzleduck/dazzleduck` Docker image. It owns no query logic: it loads and validates
configuration, runs the startup SQL script, builds **one shared** `DuckDBFlightSqlProducer`,
and hands that same producer to the HTTP server and/or the Flight SQL server.

## Startup Sequence

`Main.main` → `Runtime.start(args)`:

1. Parse `--conf key=value` overrides (repeatable) and merge them over the classpath
   `reference.conf` defaults and system properties
2. Validate the warehouse path (local paths are created if missing and must be writable
   directories; `s3://` paths get basic bucket-name validation)
3. Load and execute the startup script (`startup_script_provider`) on the singleton DuckDB
   connection — extensions and attached catalogs are inherited by all connections
4. Start the servers listed in `networking_modes` — `flight-sql` (port 59307) and/or `http`
   (port 8081) — sharing one producer and allocator
5. A JVM shutdown hook closes the servers, producer, and allocator

## Running

```bash
# From source
./mvnw exec:java -pl dazzleduck-sql-runtime \
  -Dexec.args="--conf warehouse=warehouse --conf users.0.password='your password'"

# Docker
docker run -ti -p 59307:59307 -p 8081:8081 dazzleduck/dazzleduck:latest \
  --conf warehouse=/data \
  --conf users.0.password="your password"
```

The Arrow `--add-opens` JVM flags are preconfigured by the exec plugin and the container
entrypoint.

The container entrypoint also supports running an arbitrary main class: if the first argument
starts with `io.dazzleduck.`, that class is run instead of the server (used by the demo
containers). Optional jars dropped into `/app/extra` (e.g. Hadoop for Delta Lake support) are
added to the classpath.

## Configuration

This module's own `reference.conf` contributes exactly one key:

```hocon
dazzleduck_server = { networking_modes = [flight-sql, http] }
```

Valid `networking_modes` values are `flight-sql` and `http`; an empty or unknown value fails
startup. Every other key (`warehouse`, `secret_key`, `access_mode`, ports, JWT, ingestion, ...)
comes from the flight and http modules' `reference.conf` files — see those modules' READMEs and
the root README.

## Docker Image

Built with Jib on top of a custom base image (`dazzleduck/base-jre`) that bundles the JRE and a
platform-stripped DuckDB JDBC driver. Build instructions, base-image rationale, and multi-arch
publishing live in [`docker/README.md`](docker/README.md).

## Key Files

```
src/main/java/io/dazzleduck/sql/runtime/
├── Main.java      # CLI entry point, banner, shutdown hook, exit codes
└── Runtime.java   # Config validation, startup script, server lifecycle
```

The module also publishes a test-jar containing `SharedTestServer`, reused by other modules'
integration tests.
