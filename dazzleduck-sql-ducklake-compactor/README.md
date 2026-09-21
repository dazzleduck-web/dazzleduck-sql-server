# DazzleDuck DuckLake Compaction Service

A background service that runs minor and major compaction on DuckLake catalogs to keep Parquet file counts manageable and reclaim storage from expired snapshots.

## Overview

DuckLake writes small Parquet files on each insert/update. Without compaction, query performance
degrades as the file count grows. This service compacts files via any number of configurable
**tiers**, each a self-contained compaction level: its own file-size range, schedule, file-count
cap, enable flag, and DuckDB connection settings. The bundled default is two tiers, "minor" and
"major", matching what used to be hardcoded:

- **minor** — merges adjacent small files below `8MB`. Runs frequently (every 1 minute).
- **major** — merges files in `[8MB, 64MB)`. Runs less frequently (every 1 hour).

There's nothing special about having exactly two — add, remove, rename, or resize tiers freely, as
long as every **enabled** tier's `[min_file_size, max_file_size)` range is disjoint from every
other enabled tier's. That's enforced at startup (refuses to start on overlapping ranges or
duplicate tier names) and is what lets tiers run **concurrently with no locking between them**:
since each only ever touches files in its own range, two tiers can never race on the same file.

Every tier is scheduled at a **fixed rate**, not a fixed delay: a cycle that takes time `T` waits
`frequency - T` before the next one starts, rather than a full `frequency` after completion
regardless of `T`. A cycle that runs longer than its frequency has no negative wait — the next one
starts immediately.

## Configuration

All settings live under the `dazzleduck_sql_compaction` HOCON root in `application.conf` and can be overridden at runtime with `--conf key=value`.

| Key | Default | Description |
|-----|---------|-------------|
| `databases` | `[]` | DuckLake catalog names to compact (must be attached via startup script) |
| `compaction_tiers` | see below | Compaction tiers, keyed by tier name (see below) |
| `housekeeping_frequency` | `5 minutes` | How often to expire snapshots and delete orphaned files |
| `housekeeping_connection_settings` | `[]` | Raw SQL run on housekeeping's connection right after opening it — independent of any tier |
| `snapshot_retention` | `15 minutes` | Expire snapshots older than this during housekeeping |
| `health_port` | `8080` | Port for the `GET /health` endpoint |
| `startup_script_provider` | — | How to load the startup SQL (attach catalogs, load extensions) |
| `config_provider` | — | Optional: overlay config values read from a table (see below) |

The bundled defaults live in this module's `application.conf` (not `reference.conf`).

### Compaction tiers

```hocon
compaction_tiers {
  minor {
    enabled = true
    frequency = 1 minute
    min_file_size = 0
    max_file_size = 8MB
    max_compacted_files = 1000   # 0 = unbounded
    connection_settings = ["SET memory_limit='2GB'", "SET threads=2"]
  }
  major {
    enabled = true
    frequency = 1 hour
    min_file_size = 8MB
    max_file_size = 64MB
    max_compacted_files = 1000
    connection_settings = ["SET memory_limit='8GB'", "SET threads=4"]
  }
}
```

Tiers are **keyed by name**, and the key is the tier's name — there is no `name` field. That is
what makes a tier individually overridable: HOCON merges objects field-by-field but replaces lists
wholesale, so

```bash
--conf 'dazzleduck_sql_compaction.compaction_tiers.minor.frequency = 10 seconds'
```

changes one cadence and leaves every other field of `minor` — and every other tier — intact. The
same spelling works from a `config_provider` table (see below). Tiers are always processed in
`min_file_size` order regardless of key order, so logs and the per-tier file-count gauges read in
band order.

A **list** of tiers, each carrying its own `name` field, is still accepted so configs written
against 0.2.19 keep working; with a list, declaration order is preserved and per-tier overrides are
not possible.

Per-tier fields:

| Field | Description |
|-------|-------------|
| `enabled` | Set `false` to turn this tier off entirely, for all databases, without removing it from config |
| `frequency` | How often this tier runs |
| `min_file_size` / `max_file_size` | This tier only touches files in `[min_file_size, max_file_size)`. Always required, including `0` for the lowest tier |
| `max_compacted_files` | Caps files merged per cycle (passed through as `ducklake_merge_adjacent_files`'s own `max_compacted_files`); `0` = unbounded, and a catalog with more eligible files than the cap just finishes over several ticks instead of one |
| `connection_settings` | Optional (default none). Raw SQL run on this tier's own connection right after opening it, e.g. to set `memory_limit`/`threads` differently per tier. Optional because it is the tier's only list-valued field, and a key/value override table can carry only scalars — so a whole tier can be declared from such a table |

Disabling a tier (`enabled = false`) is the direct way to turn it off. Simply raising a tier's
`frequency` to a very large value is **not** equivalent — with the old hardcoded minor/major design
that could starve the other tier of any chance to run at all; with independent per-tier schedules
that's no longer true, but `enabled` is still the explicit way to say "don't run this."

### Connections

Each tier, plus housekeeping, gets its own **real, independent DuckDB connection** — not
`io.dazzleduck.sql.commons.ConnectionPool`, whose `getConnection()` returns duplicates of one shared
process-wide instance. That matters because DuckDB's `memory_limit`, `threads`, and
`temp_directory` are all **GLOBAL**-scoped (confirmed via `duckdb_settings()`): a `SET` on one
duplicate silently changes it for every other duplicate of the same instance, which would make two
concurrently-running tiers with different `connection_settings` race and clobber each other's
global config instead of each getting its own value. Each connection independently re-runs the
startup script before applying its own `connection_settings`, so the script must be safe to run
more than once (plain `ATTACH`/`INSTALL`/`LOAD` are; one-time DDL like `CREATE TABLE` is not).

**This means a local file-based DuckLake catalog (`ATTACH 'ducklake:/path/...'`) cannot have more
than one tier enabled at a time** (nor a tier running alongside housekeeping) — DuckDB only allows
one attach of a given local catalog file at a time (`Unique file handle conflict`, verified
empirically), and separate raw connections trying to attach the same file concurrently will hit
that error as soon as more than one is opened. Server-backed catalogs (Postgres) have no such
restriction and are what any real multi-tier deployment wanting per-tier connection isolation
should use.

### Startup Script

Configure under `startup_script_provider`:

```hocon
dazzleduck_sql_compaction.startup_script_provider {
  script_location = "/config/startup.sql"
}
```

Or pass inline content:

```hocon
dazzleduck_sql_compaction.startup_script_provider {
  content = "ATTACH 'ducklake:...' AS mydb (DATA_PATH 's3://...');"
}
```

The startup script runs before configuration overrides are read, so catalogs referenced by the
config-provider table must be attached here.

### Configuration Overrides from a Table

Settings can be overlaid from a two-column key/value table (readable after the startup script
has attached its catalog):

```hocon
dazzleduck_sql_compaction.config_provider {
  table = "mydb.main.compactor_config"
  # key_column   = "config_key"   (default)
  # value_column = "value"        (default)
  # prefix       = ""             (optional key prefix filter)
}
```

Only scalar keys can be overridden this way, because every table value is read as a string and
HOCON will not widen a string to a list. That still rules out `databases` and
`housekeeping_connection_settings`, but **individual tier fields are scalars and can be set**, since
tiers are keyed by name rather than listed:

| Row key | Effect |
|---------|--------|
| `compaction_tiers.minor.frequency` | retunes one cadence; the tier's other fields are untouched |
| `compaction_tiers.minor.enabled` | turns one tier off while leaving it configured, so its file-count gauge still reports its band |
| `compaction_tiers.mega.min_file_size` (+ the other scalars) | declares a whole new tier — `connection_settings` is optional precisely so this is possible |

A tier's `connection_settings` is still a list and so cannot be set from the table; it stays in the
file. Note also that overrides are read **once at startup**, so a changed row takes effect on the
next restart — this centralizes the values, it does not make them live.

A configured table that cannot be read is a fatal startup error by design — silently falling
back to file defaults would hide a broken override source.

## Health Check

`GET /health` on `health_port` (default 8080) returns uptime plus per-database stats, with one
nested object per configured tier (`totalCompactions`, `currentFiles`, `nextExecutionTime`) plus
whole-catalog totals (`totalFailedCycles`, `totalFilesCompacted`, `lastSuccessTime`,
`currentTotalFiles`). Note: the status is always `UP` while the process is running — it does not
reflect failing compaction cycles.

## Build

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home

# Build fat JAR
./mvnw clean package -pl dazzleduck-sql-ducklake-compactor

# Run tests
./mvnw test -pl dazzleduck-sql-ducklake-compactor

# Build Docker image (Jib, no daemon required)
./mvnw jib:dockerBuild -pl dazzleduck-sql-ducklake-compactor
```

## Running

```bash
java -jar target/dazzleduck-sql-ducklake-compactor-*.jar \
  --conf 'dazzleduck_sql_compaction.databases=[mydb]' \
  --conf 'dazzleduck_sql_compaction.startup_script_provider.script_location=/config/startup.sql'
```

## Docker

```bash
docker run \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  -v /config:/config \
  dazzleduck/ducklake-compactor:latest \
  --conf 'dazzleduck_sql_compaction.databases=[mydb]' \
  --conf 'dazzleduck_sql_compaction.startup_script_provider.script_location=/config/startup.sql'
```

## Metrics

Micrometer metrics are emitted via the logging registry by default:

| Metric | Tags | Description |
|--------|------|-------------|
| `ducklake.compaction.duration` | `type` (tier name, or `housekeeping`), `step` (merge/expire/cleanup), `database` | Time per compaction step |
| `ducklake.compaction.cycles` | `tier`, `database` | Successful compaction cycles for this tier |
| `ducklake.compaction.failures` | `type` (tier name, or `housekeeping`), `database` | Cycles that ended in an exception |
| `ducklake.files.compacted` | `database` | Total files compacted, across all tiers |
| `ducklake.files.total` | `database` | Total active Parquet files |
| `ducklake.files.by_tier` | `tier`, `database` | Active files in this tier's file-size range |
