# DazzleDuck DuckLake Compaction Service

A background service that runs minor and major compaction on DuckLake catalogs to keep Parquet file counts manageable and reclaim storage from expired snapshots.

## Overview

DuckLake writes small Parquet files on each insert/update. Without compaction, query performance degrades as the file count grows. This service runs two compaction strategies on a schedule:

- **Minor compaction** — merges adjacent small files using `ducklake_merge_adjacent_files`. Runs frequently (default: every 1 minute).
- **Major compaction** — merges all files, expires old snapshots, and cleans up deleted files. Runs less frequently (default: every 1 hour).

## Configuration

All settings live under the `dazzleduck_sql_compaction` HOCON root in `application.conf` and can be overridden at runtime with `--conf key=value`.

| Key | Default | Description |
|-----|---------|-------------|
| `databases` | `[]` | DuckLake catalog names to compact (must be attached via startup script) |
| `minor_compaction_frequency` | `1 minute` | How often to run minor compaction |
| `major_compaction_frequency` | `1 hour` | How often to run major compaction |
| `minor_compaction_max_size` | `8MB` | Only merge files smaller than this |
| `major_compaction_max_size` | `64MB` | Only compact files smaller than this during major pass |
| `housekeeping_frequency` | `5 minutes` | How often to expire snapshots and delete orphaned files |
| `snapshot_retention` | `15 minutes` | Expire snapshots older than this during housekeeping |
| `health_port` | `8080` | Port for the `GET /health` endpoint |
| `startup_script_provider` | — | How to load the startup SQL (attach catalogs, load extensions) |
| `config_provider` | — | Optional: overlay config values read from a table (see below) |

The bundled defaults live in this module's `application.conf` (not `reference.conf`).

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

A configured table that cannot be read is a fatal startup error by design — silently falling
back to file defaults would hide a broken override source.

## Health Check

`GET /health` on `health_port` (default 8080) returns uptime plus per-database compaction
counters (total minor/major compactions, files compacted, last/next execution time, current
small/medium/total file counts). Note: the status is always `UP` while the process is running —
it does not reflect failing compaction cycles.

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
| `ducklake.compaction.duration` | `type` (minor/major/housekeeping), `step` (merge/expire/cleanup), `database` | Time per compaction step |
| `ducklake.compaction.minor` | `database` | Total minor compactions run |
| `ducklake.compaction.major` | `database` | Total major compactions run |
| `ducklake.files.compacted` | `database` | Total files compacted |
| `ducklake.files.total` | `database` | Total active Parquet files |
| `ducklake.files.small` | `database` | Files below `minor_compaction_max_size` |
| `ducklake.files.medium` | `database` | Files between minor and major thresholds |
