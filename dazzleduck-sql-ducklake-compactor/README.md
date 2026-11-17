# DazzleDuck DuckLake Compaction Service

A background service that runs minor and major compaction on DuckLake catalogs to keep Parquet file counts manageable and reclaim storage from expired snapshots.

## Overview

DuckLake writes small Parquet files on each insert/update. Without compaction, query performance
degrades as the file count grows. This service merges files in **tiers**: each tier is a band of
file sizes `[min_file_size, max_file_size)` with a target its merges write, so outputs move up the
chain. The bundled default is three tiers (see [DYNAMIC_COMPACTION_SPEC.md](DYNAMIC_COMPACTION_SPEC.md)):

| Tier | Band | Target | Groups per call (static cap) |
|------|------|--------|------------------------------|
| minor | `[0, 8MB)` | `16MB` | 12 |
| major | `[8MB, 64MB)` | `64MB` | 16 |
| mega | `[64MB, 256MB)` | `256MB` | 5 |

Files of `256MB` or more are final. Add, remove, rename, or resize tiers freely, as long as every
**enabled** tier's band is disjoint from every other enabled tier's: that is checked at startup,
and it is what lets tiers run concurrently, since each only ever touches files in its own band.

Each enabled `(database, tier)` runs **one loop thread**. A pass:

1. **Refreshes**: one query over the catalog metadata lists the tables with work, grouped the way
   `ducklake_merge_adjacent_files` groups files (schema version, partition, partition values).
   Files with deletes and tables with `auto_compact = false` are left out, as DuckLake does.
2. **Ranks** them by files waiting, most first; tables carried over from the previous pass go first.
3. **Calls** each table in turn, in one transaction with one commit:

   ```sql
   SET SESSION ducklake_target_file_size = '<target> bytes';
   CALL ducklake_merge_adjacent_files('<db>', '<table>', schema := '<schema>',
        min_file_size := <min>, max_file_size := <max>, max_compacted_files := <cap>);
   ```

4. **Ends** when every table was called or `pass_time_limit` has passed; the rest carry over. After
   a pass that merged nothing it sleeps `idle_sleep_max`; after one that merged, the next refresh
   starts once 5 seconds have passed since this one, so the catalog sees at most one refresh per tier
   and database every 5 seconds.

The session target bounds every merge group (so a call's peak memory and each output file) and
overrides table and catalog options, so no table falls back to DuckLake's 512 MB default. It needs
DuckDB 1.5.4 or later; a tier whose `SET` fails does not start. The cap is sized per call, never
above the tier's static cap (see [Call sizing](#call-sizing)).

A failure costs one table's call: the table is retried on the next pass, and after 3 failures in a
row it backs off for 15 minutes. A call the catalog's idle-in-transaction timeout killed is retried
with half the cap, down to 1; each commit doubles it again, but never back to a cap the timeout
killed. A killed call of a single group halves the table's files per group instead. These learnt
limits expire an hour after the last timeout, so a slow spell on the catalog does not shrink a busy
table's calls for good. A table runs at most one job at a time across tiers, data
retention and the delete-file rewrite (a **table lease**): a loop skips a table another job holds,
and retention and the rewrite wait for it.

## Configuration

All settings live under the `dazzleduck_sql_compaction` HOCON root in `application.conf` and can be overridden at runtime with `--conf key=value`.

| Key | Default | Description |
|-----|---------|-------------|
| `databases` | `[]` | DuckLake catalog names to compact (must be attached via startup script) |
| `compaction_tiers` | see below | Compaction tiers, keyed by tier name (see below) |
| `housekeeping_frequency` | `5 minutes` | How often to expire snapshots and delete orphaned files |
| `housekeeping_connection_settings` | `[]` | Raw SQL run on housekeeping's connection right after opening it — independent of any tier |
| `snapshot_retention` | `15 minutes` | Expire snapshots older than this during housekeeping |
| `rewrite_deletes_enabled` | `true` | Each housekeeping cycle first runs `ducklake_rewrite_data_files` on each table in turn, under the table's lease, rewriting data files with enough deleted rows so their delete files are dropped. A table another job holds is skipped until the next cycle, and waited for after 3 skips in a row |
| `rewrite_delete_threshold` | unset | Fraction (0–1) of a file's rows that must be deleted before it is rewritten; unset uses the catalog's `rewrite_delete_threshold` option (DuckLake default 0.95) |
| `data_retention` | off | Deletes expired rows of the tables listed in a policy table (see [Data retention](#data-retention)) |
| `health_port` | `8080` | Port for the `GET /health` endpoint |
| `startup_script_provider` | — | How to load the startup SQL (attach catalogs, load extensions) |
| `config_provider` | — | Optional: overlay config values read from a table (see below) |

The bundled defaults live in this module's `application.conf` (not `reference.conf`).

### Compaction tiers

```hocon
compaction_tiers {
  minor {
    enabled = true
    min_file_size = 0
    max_file_size = 8MB
    target_file_size = 16MB
    max_compacted_files = 12
    min_files = 4
    max_wait = 5 minutes
    memory_share = 0.4
    connection_settings = ["SET enable_external_file_cache = false"]
  }
  major {
    enabled = true
    min_file_size = 8MB
    max_file_size = 64MB
    target_file_size = 64MB
    max_compacted_files = 16
    memory_share = 0.3
    connection_settings = ["SET enable_external_file_cache = false"]
  }
  mega {
    enabled = true
    min_file_size = 64MB
    max_file_size = 256MB
    target_file_size = 256MB
    max_compacted_files = 5
    memory_share = 0.3
    connection_settings = ["SET enable_external_file_cache = false"]
  }
}
```

Sizes are HOCON byte sizes, where `MB` is 10^6 bytes and `MiB` is 2^20.

The names `housekeeping` and `retention` (in any case) are reserved: they tag the metrics of those
jobs, so a tier with either name is refused at startup.

Tiers are **keyed by name**, and the key is the tier's name — there is no `name` field. That is
what makes a tier individually overridable: HOCON merges objects field-by-field but replaces lists
wholesale, so

```bash
--conf 'dazzleduck_sql_compaction.compaction_tiers.minor.max_compacted_files = 8'
```

changes one field and leaves every other field of `minor` — and every other tier — intact. The
same spelling works from a `config_provider` table (see below). Tiers are always processed in
`min_file_size` order regardless of key order, so logs and the per-tier file-count gauges read in
band order.

A **list** of tiers, each carrying its own `name` field, is still accepted so configs written
against 0.2.19 keep working; with a list, declaration order is preserved.

> **Migrating from the list shape:** don't add a per-tier override until the file is keyed by name.
> Against a list, `compaction_tiers.minor.max_compacted_files` is an object that *replaces* the
> whole list rather than merging into it, leaving one tier holding only the overridden field —
> startup then fails naming the incomplete tier and pointing back here.

Per-tier fields:

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | required | Set `false` to turn this tier off entirely, for all databases, without removing it from config |
| `min_file_size` / `max_file_size` | required | This tier only touches files in `[min_file_size, max_file_size)`. Always required, including `0` for the lowest tier |
| `target_file_size` | required | `ducklake_target_file_size` for this tier's calls. At least `max_file_size`: DuckLake skips a file as large as the target, so the files between them would never merge |
| `max_compacted_files` | required | The static cap: the most groups (output files) one call merges. Positive |
| `max_files_per_call` | `6000` | The most input files a call is sized to merge, about 4.5 minutes at the measured rate |
| `call_time_limit` | `10 minutes` | The longest a call is sized to run |
| `min_files` | `2` | A table is merged once one of its groups has this many files |
| `max_wait` | none | ... or once a group's second-oldest file has waited this long |
| `pass_time_limit` | `2 minutes` | After this, a pass starts no new call; the rest go first next pass |
| `idle_sleep_max` | `10 seconds` | Sleep after a pass that found no work |
| `threads_per_cpu` | `4` | DuckDB threads per available CPU (`Runtime.availableProcessors()`, which reads the container limit) for each of this tier's instances, divided among the databases, at least 1. Every tier gets them; there is no CPU share. Merging small files from an object store costs two GETs per file, so its time is about the latency x 2 x files / threads: in a load test at about 25 ms per request, 1,000 files of 8 KB took 13.9 s on 4 threads, 6.9 s on 8 and 3.5 s on 16, and no DuckDB setting changed the request count. More threads also mean more connections; see "Many small files and ephemeral ports" below. `cpu_share`, used before, is ignored with a warning; a tier's memory, which used to follow it, now follows `memory_share`, so a config that set only `cpu_share` should set `memory_share` to keep its split. An instance gets no more threads than leave half of a group's memory (80% of `memory_limit`) for the group, at 8 MiB a thread: a container with a memory limit but no CPU limit sees all of the node's CPUs. A `SET threads` in `connection_settings` wins, and groups are sized for it |
| `memory_share` | an equal part of what the other tiers leave | Share of the tiers' memory for this tier: unless `connection_settings` sets one, each instance gets a `memory_limit` of 50% of the process's memory (the container's limit, else the machine's) x its share, divided among the databases. Separate from `cpu_share` because minor merges many tiny files and needs threads, while major and mega merge few large files and need memory. Housekeeping, retention and the file-count gauges get 8%, 8% and 2%, also divided among the databases. Shares add up to at most 1. Each thread holds its own read buffers, so a merge group is sized to 80% of the `memory_limit` less 8 MiB per thread |
| `connection_settings` | none | Raw SQL run on this tier's own connection right after opening it. A `SET threads`, `SET memory_limit` or `SET temp_directory` here overrides the computed one. Optional because it is the tier's only list-valued field, and a key/value override table can carry only scalars — so a whole tier can be declared from such a table |

`frequency`, the timer of earlier releases, is no longer used; a config that still sets it starts.

**Minor's admission threshold.** Any two files in a group merge, so without a threshold a slow
table would rewrite its leftover file on almost every pass, committing a snapshot every few seconds.
`min_files = 4` and `max_wait = 5 minutes` wait for a few files, or for a new file to have waited
5 minutes. The wait is timed on a group's second-oldest file, because its oldest is usually the
leftover of the last merge. DuckLake has no partition filter, so once a table is admitted every
partition holding a leftover plus one new file is merged too.

#### Call sizing

From the table's files waiting and average file size, each call gets:

```text
files_per_group = min(ceil(target / average_file_size), files_waiting)
file_cap        = max(1, floor(max_files_per_call / files_per_group))
time_limit      = min(call_time_limit, 0.8 x idle_in_transaction_session_timeout)
time_cap        = max(1, floor(time_limit / (files_per_group x sec_per_file)))
cap             = min(static_cap, time_cap, max(file_cap, busy_partitions + 1))
```

`sec_per_file` is 0.045 s for a table with a sort key (measured) and 0.09 s without one (a guess).
`busy_partitions` are the table's groups with a file committed in the last 10 minutes: DuckLake
takes partitions in a fixed order, so a cap at or below them could give every slot to the same
partitions on every call. When `busy_partitions + 1` is above the static or time cap, the table is
reported as starved.

The catalog's `idle_in_transaction_session_timeout` is read from Postgres when a tier's connection
opens, so an `options=-cidle_in_transaction_session_timeout=600000` in the ATTACH string counts. A
value of 0 (Postgres's default) or a SQLite or DuckDB-file catalog means no limit. A group's memory
is sized per file as a fixed cost (0.12 MiB with a sort key, 0.85 MiB without) plus 4 times the
file's bytes, which dominates for files of megabytes. A tier is refused at startup only when not even
two of its band's smallest files fit in a group within 80% of the timeout and of its instance's
`memory_limit` (mega in a small container, say); when one full group does not fit (3,200 tiny files
take about 144 s), it starts with a warning. An unsorted table is also twice as slow per file and
needs about 7 times the fixed memory, so its full group may not fit in 80% of the timeout or of the
instance's `memory_limit`. Its calls then run with a lower target, so a group holds only the files
that fit (10 MB files in a 256 MiB major instance: 5 a group, a 50 MB target), and with `max_file_size` at half that target, so the outputs of earlier such calls (merged
tiny files compress several-fold) are not merged again pass after pass; they stay in the band and are
merged once the table can be merged at the tier's own target. A lowered call that merged nothing is not repeated until the
table's waiting files change. A call that runs out of memory or spill space halves the table's files
per group for an hour, as a timed-out single group does. A table of which not even two
files fit is skipped and reported (`oversized`). A sort key, a longer timeout, more memory or larger
files avoid both. If the timeout of a
Postgres catalog cannot be read, the tier retries opening its connection rather than run without it.

The images set `MALLOC_ARENA_MAX=2`: most compactor memory is native `malloc` memory outside
DuckDB's `memory_limit`, kept by glibc's per-thread arenas. Every DuckDB instance also runs with
`allocator_background_threads = true` and `allocator_flush_threshold = '64MB'`, so DuckDB's own
allocator returns memory a merge freed instead of keeping it across calls; an instance's own
settings can override them. Each call records RSS before and after; `rss_growth_per_file` is what a
tier's recent calls left behind per input file merged. RSS is the whole process's, so tiers merging
at the same time add to each other's figure.

### Data retention

A separate housekeeping job deletes rows older than each table's retention. Which tables expire, by
which column and after how long is read from a policy table (or a view) on every cycle, so a change
applies from the next cycle without a restart:

```sql
CREATE TABLE lake.main.retention_policy (
  table_name  VARCHAR,  -- catalog.schema.table
  time_column VARCHAR,  -- a DATE or TIMESTAMP column holding the row's age (UTC)
  retention   VARCHAR   -- a duration such as '30 days' or '12h', or 'unlimited'
);
INSERT INTO lake.main.retention_policy VALUES
  ('lake.main.logs',   'ts',         '30 days'),
  ('lake.main.ai_txn', 'event_date', 'unlimited');
```

```hocon
data_retention {
  enabled = true
  frequency = 1 hour
  policy_table = "lake.main.retention_policy"
  connection_settings = ["SET enable_external_file_cache = false"]
  max_files_per_cycle = 1000   # 0 = no cap
}
```

A retention is a lower bound, not an exact cut: the job deletes whole data files, and leaves the
rest for a later cycle. For every table in the policy table whose catalog is a configured database
(no other table is touched), it reads the min/max statistics of the time column for each live data
file from the catalog, and lowers the cutoff to a file boundary, one that no file has rows on both
sides of. It then runs:

```sql
DELETE FROM lake.main.logs WHERE ts < CAST('2026-09-01 00:00:00.000000' AS TIMESTAMP)
```

That DELETE matches whole files, so DuckLake drops them without writing delete files or rewriting
anything. The exception is a file holding NULLs or without statistics for the time column (below):
its expired rows are deleted with a delete file, which housekeeping's delete-file rewrite cleans up.
The DELETE reads the time column of the files it drops (and of those exceptions); every other file
is pruned by its statistics. Before it, the job checks that a row older than the boundary exists, and
runs no DELETE when none does.

- A file that straddles the cutoff holds back the files above its minimum until its newest row
  expires. Partitioning the table by time, for example by day, keeps files from straddling. How
  far the boundary lags the cutoff is the `ducklake.retention.boundary_lag` gauge (seconds), and a
  lag longer than the retention itself, such as from one row stamped far in the future, is logged
  as a warning.
- One table's DELETE drops at most `max_files_per_cycle` files, the oldest, so the first run on a
  table with a long backlog drains it over several cycles rather than in one transaction that
  could outlast the catalog's idle-transaction timeout. Files that overlap so that none of them can
  be dropped alone are dropped together, past the cap.
- A file whose time column holds a value before year 1, from a corrupt value, is treated like one
  without statistics.
- Rows whose time column is NULL are kept. A file holding NULLs cannot be dropped whole, so it does
  not move the boundary; its expired rows are deleted from it, with a delete file when inlining is
  off (below).
- A data file without statistics for the time column, such as one written before the column was
  added, does not move the boundary either, and is logged. Its expired rows are deleted the same way.
- Keep DuckLake data inlining off for the tables retention deletes from: attach the catalog with
  `DATA_INLINING_ROW_LIMIT 0`, and leave no `data_inlining_row_limit` option above 0 set with
  `set_option` on the catalog, the table's schema or the table, which takes precedence over the
  `ATTACH` parameter. Otherwise the rows retention deletes from a file it cannot drop whole are
  recorded as inlined deletes in the catalog database instead of a delete file. Nothing checks this
  at startup.
- Rows that are still inlined in the catalog, from a time inlining was on, are deleted too.
- Files holding NULLs or without statistics keep their statistics after their expired rows are
  deleted, so each cycle's check for a row older than the boundary scans them until housekeeping's
  delete-file rewrite replaces them.
- The statistics are read and the DELETE runs in one transaction, so a file committed meanwhile is
  never partly deleted; the commit conflicts instead, and the next cycle retries.
- A failed table does not stop the others. Each cycle opens its own connection, running the
  startup script, and closes it at the end; shutdown closes it at once, aborting a running DELETE,
  which rolls back.
- `retention` is a whole number and a unit of at least seconds: `s`, `min`, `h`, `d`, `w`, or the
  unit spelled out (`30 days`, `90 minutes`). A bare number and `m` are rejected rather than read
  as milliseconds and minutes, as they would be in the config file.
- The statistics are read from `__ducklake_metadata_<catalog>`, the name DuckLake gives the metadata
  catalog by default, as the rest of the compactor does; a catalog attached with `METADATA_CATALOG`
  is not supported.
- Rows that cannot be used (a name that is not a plain identifier, an unknown duration) are logged
  and skipped. A table that fails, such as one without the named column, does not stop the others.
- The dropped files are removed from storage by the regular housekeeping job once they fall out of
  `snapshot_retention`.
- Metrics: `ducklake.retention.rows_deleted` and `ducklake.retention.boundary_lag` (tagged
  `database`, `table`), the
  `ducklake.compaction.duration` timer with `type=retention`, and `ducklake.compaction.failures`
  with `type=retention` for a cycle with a failed table or an unreadable policy table.

Without the duckdb/ducklake#1895 fix, which the DuckLake patch has from `v1.5.6-dazzleduck.3`,
every such DELETE conflicts with any concurrent insert into the table. Under continuous ingestion
it then keeps failing and is only retried on the next cycle. With the fix it conflicts only with
inserted files whose column statistics could hold rows older than the cutoff: late data, older than
the retention.

The fix has two halves, and each runs in the process that commits second. The compactor's half
covers an insert that commits while the retention DELETE is open. The other half covers a DELETE
that commits while an insert is open, and runs in the ingesting process: the SQL server or the
OTel collector. Those load stock DuckLake unless their image bakes in the patched extension too.
With stock DuckLake there, an ingestion batch whose registration is open when a retention DELETE
on the same table commits fails with a transaction conflict and is reported to the client as
failed, which must resend it. Registration takes milliseconds and retention commits once per table
per cycle, so this is rare but not impossible.

### Spill directory

DuckDB's default `temp_directory` for an in-memory database is `.tmp` in the working directory, and
DuckDB creates only the last level of a configured one. Both fail late, only when a merge needs to
spill (usually a sorted group): a read-only root filesystem, or a missing parent directory. So every
DuckDB instance of the compactor spills to its own directory under one root, `spill_directory`
(default `/tmp/ducklake-compactor`). Each tier, housekeeping, retention and the file-count gauges get
a directory per database, for example `/tmp/ducklake-compactor/lake/tier-minor`. Each directory is
created and probed (a file written and deleted) when its instance opens, and a tier whose directory
is not writable does not start, with the reason in `/health`. Instances never share a directory,
since DuckDB cleans up its temp directory when an instance closes.

Each instance's `max_temp_directory_size` is split like its `memory_limit`, from a budget of the
free space under the root at startup, or `spill_max_size` when that is smaller. Set `spill_max_size`
on Kubernetes, where an `emptyDir` volume's `sizeLimit` is not visible as free space and going over
it evicts the pod. Without a cap no instance gets less than 256 MiB; under a cap there is no minimum,
so the sizes always add up to less than it. An instance whose `connection_settings` set
`temp_directory` keeps it.

A process killed while spilling (an out-of-memory kill, say) leaves its spill files behind, and
DuckDB removes them only when an instance spills to the same directory again; an `emptyDir` keeps
them across container restarts. So the first time the compactor uses an instance's directory it
deletes any `duckdb_temp_*` files left in it. Set
`spill_directory = ""` to leave DuckDB's defaults.

```hocon
spill_directory = "/tmp/ducklake-compactor"
spill_max_size = 20GB
```

`/health` reports the root, its budget, the sizes given to the instances opened so far
(`allocatedBytes`), the bytes spilled under it now and the free space. It alerts when spill uses
more than 80% of the sizes given (merges that need more fail), or when the free space is less than
the instances may still spill (the disk fills first).

### Many small files and ephemeral ports

Merging tiny files from an object store opens many short HTTP connections, each of which holds a
local port in `TIME_WAIT` for a while after it closes. In a load test, 4 DuckDB threads exhausted
the ephemeral port range of a container, and httpfs failed with `Could not connect to server`. If
that happens, widen `net.ipv4.ip_local_port_range` and enable `net.ipv4.tcp_tw_reuse` for the
container (on Kubernetes, as pod `sysctls`), or lower the tier's `threads_per_cpu`. With the default
of 4 threads per CPU this matters more than it did with fewer threads.

### Catalog queries on a PostgreSQL catalog

DuckDB reads a PostgreSQL catalog's tables through the postgres scanner, which copies them in `ctid`
ranges and pushes down only simple column-versus-constant filters. So the compactor runs its own
catalog queries inside PostgreSQL, through `postgres_query`, and only their results come back:
tier discovery (a few rows per table) and the file-count gauges (one row). Data retention looks up
the table and column ids first and then filters by them, so only that table's file rows are copied.
Without this, every pass copied the catalog rows of every table in the lake: at about 1.5 million
live files, the minor tier's discovery ran out of its 512 MB `memory_limit`. The PostgreSQL schema
that holds the catalog is the one with `ducklake_snapshot` (the connection's current schema when
several lakes share a database). DuckDB-file and SQLite catalogs run the same queries in DuckDB.

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
| `compaction_tiers.minor.max_compacted_files` | retunes one field; the tier's other fields are untouched |
| `compaction_tiers.minor.enabled` | turns one tier off while leaving it configured, so its file-count gauge still reports its band |
| `compaction_tiers.mega.min_file_size` (+ the other scalars) | declares a whole new tier — `connection_settings` is optional precisely so this is possible |

A tier's `connection_settings` is still a list and so cannot be set from the table; it stays in the
file. Note also that overrides are read **once at startup**, so a changed row takes effect on the
next restart — this centralizes the values, it does not make them live.

A configured table that cannot be read is a fatal startup error by design — silently falling
back to file defaults would hide a broken override source.

## Health Check

`GET /health` on `health_port` (default 8080) returns uptime, an `alerts` list, and per-database
stats: one nested object per configured tier (`totalCompactions`, `currentFiles`, `loop`, and
`telemetry` for its latest table call) plus whole-catalog totals (`totalFailedCycles`,
`totalFilesCompacted`, `totalFilesRewritten`, `lastSuccessTime`, `currentTotalFiles`,
`unexpiredSnapshots`). A tier's `loop` shows its `state` (`STARTING`, `RUNNING`, `REFUSED` with the
reason, `STOPPED`), its last pass, queue, carry-over, back-offs, starved and stale tables, the
tables it skips as `oversized` and those it merges with a lowered target (`reduced`), the catalog
timeout it sizes calls by, its threads and `duckdb_memory()`.

`loop.tables` has one entry per table the tier tracks (listed by its latest refresh, or backed off):
files and bytes waiting in mergeable groups, the age of its oldest waiting file, busy partitions,
whether it is sorted, its last call and outcome (`SUCCESS` merged, `EMPTY` merged nothing,
`FAILED`), its last success (a call that completed without an error), consecutive failures, the end
of its back-off, the cap and files per group a timeout has taught the tier (`null` for none), and
its flags (`stale`, `starved`, `oversized`, `reduced`, `backoff`). A table with nothing left to merge
and no back-off drops off the list, and its history with it.

`alerts` lists the conditions that hold now: a tier with no completed pass in 15 minutes (a pass
that finds no work counts), a tier that did not start, a table backed off after 3 failures, a
starved table, a table skipped because not even two of its files fit in one group within the
catalog's timeout and the instance's memory, a table whose oldest file with something to merge has waited over 30 minutes, a band
whose file count rose for 30 minutes, unexpired snapshots growing across 3 housekeeping runs, and
RSS above 75% of the container's memory limit, and spill above 80% of its budget or free space
under the spill directory below what the instances may still spill. The status is always `UP` while the process is
running; watch `alerts` (or the metrics below) instead.

When alerting on metrics, `last_success_age` alone is not enough: a success is a table call that
completed, so it grows without bound while the catalog has nothing to merge. Alert on `pass_age`
(the loops are alive), and on `tier_last_success_age` only while the tier's `queue` is above zero
(it has work but no call completes).

## Build

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home

# Build fat JAR
./mvnw clean package -pl dazzleduck-sql-ducklake-compactor

# Run tests
./mvnw test -pl dazzleduck-sql-ducklake-compactor

# Build Docker image (Jib, no daemon required); bakes in the patched DuckLake extension
# (see DUCKLAKE_PATCH.md), so the download step must be explicitly enabled here
./mvnw jib:dockerBuild -pl dazzleduck-sql-ducklake-compactor -Dducklake.extension.download.skip=false
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

## Telemetry

The service can ship both its meters and its log lines to the DazzleDuck OTel collector over OTLP
gRPC. Both exports are off by default and independent of each other: either can be on without the
other. Both are configured in the file or via environment variables only; the config-provider
table overrides described above do not apply to them.

### Two tokens, two queues

The collector routes every export by the token's `x-dd-ingestion-queue` claim, regardless of
signal, and its log and metric queues have different schemas. So the two exports need **two
different tokens**: one whose claim names a metrics queue and one whose claim names a logs queue.
Mint each with the login endpoint, passing the queue in the `claims` map (see the root README's
"Ingestion Queue Routing" section), and configure the collector's `ingestion_queue_table_mapping` with
one entry per queue. Using the same token for both is refused at startup.

| Variable | Config key | Default |
|----------|------------|---------|
| `DD_METRICS_ENABLED` | `metrics.enabled` | `false` |
| `DD_METRICS_OTLP_ENDPOINT` | `metrics.endpoint` | `http://localhost:4317` (the collector's gRPC port, not its health port) |
| `DD_METRICS_OTLP_TOKEN` | `metrics.token` | none; required when metrics are enabled |
| `DD_LOGS_ENABLED` | `logs.enabled` | `false` |
| `DD_LOGS_OTLP_ENDPOINT` | `logs.endpoint` | the metrics endpoint |
| `DD_LOGS_OTLP_TOKEN` | `logs.token` | none; required when logs are enabled |
| `DD_LOGS_LEVEL` | `logs.level` | `INFO` |

`metrics.service_name` (default `ducklake-compactor`) is reported as the `service.name` resource
attribute on both signals. An enabled export with no token is a fatal startup error, since the
collector would reject every request and the signal would silently never land.

```bash
docker run \
  -e DD_METRICS_ENABLED=true -e DD_METRICS_OTLP_TOKEN=... \
  -e DD_LOGS_ENABLED=true -e DD_LOGS_OTLP_TOKEN=... \
  -e DD_METRICS_OTLP_ENDPOINT=http://collector:4317 \
  dazzleduck/ducklake-compactor:latest \
  --conf 'dazzleduck_sql_compaction.databases=[mydb]'
```

### Logs

All code logs through SLF4J with Logback as the backend. With `logs.enabled = true` an
OpenTelemetry appender is attached to the root logger next to the console appender, so console
output is unchanged and every line at `logs.level` or above is also exported as an OTLP log
record. In the collector's log table the logger name lands in `scope_name`, the rendered message
in `body`, the level in `severity_text`, and a logged exception in the `exception.type`,
`exception.message` and `exception.stacktrace` attributes. The exporter batches records, so a
line shows up in the collector within a few seconds; shutdown flushes whatever is still queued.

Things worth knowing:

- With logs on and metrics off, the fallback logging meter registry prints every meter once a
  minute at INFO, and those lines are exported too. Enable metrics or set `DD_LOGS_LEVEL=WARN`.
- Log export is set up before the rest of startup, so a failure later in startup (bad startup
  script, unreadable config-provider table, port in use) is logged and flushed before the process
  exits and the reason a pod is crash-looping is in the log table. A failure in the export setup
  itself (missing token, bad level, unreachable collector) can only be read from the console.
- Credentials are masked in every exported record, in the body and in every string attribute
  including `exception.message` and `exception.stacktrace`. This matters because DuckDB repeats
  the startup script in its errors: a failed Postgres or DuckLake-on-Postgres `ATTACH` reports the
  whole connection string with its password, and a parser error quotes the statement. Every
  compaction connection re-runs the script, so these errors can recur on every cycle. Masked:
  quoted values of secret-like names (`SECRET`, `KEY_ID`, `SESSION_TOKEN`, `password`,
  `s3_secret_access_key`, ...), unquoted `password=...`-style and URL query values, `user:password@`
  in URIs, `Bearer` tokens, and parser echo lines (`LINE n: ...`) as a whole. Console output is not
  masked. Masking works on patterns, so keep credentials in those forms (or in DuckDB secrets
  created from environment variables) rather than in free text.

### Metrics

Micrometer metrics are emitted via the logging registry by default, and over OTLP when
`metrics.enabled = true`:

| Metric | Tags | Description |
|--------|------|-------------|
| `ducklake.compaction.duration` | `type` (tier name, or `housekeeping`), `step` (discover/merge/rewrite_deletes/expire/cleanup), `database` | Time per step |
| `ducklake.compaction.cycles` | `tier`, `database` | Successful table calls of this tier |
| `ducklake.compaction.cycles_by_outcome` | `tier`, `database`, `outcome` (SUCCESS/EMPTY/FAILED) | Table calls by outcome |
| `ducklake.compaction.failures` | `type` (tier name, or `housekeeping`/`retention`), `database` | Calls, refreshes and cycles that ended in an exception |
| `ducklake.compaction.failures_by_class` | `class`, `database` | Failures by cause, including `PLANNING` |
| `ducklake.compaction.commits` | `tier`, `database` | Calls that committed a merge, one snapshot each; its rate is the commits per minute expiry must keep up with |
| `ducklake.compaction.passes` / `pass_age` | `tier`, `database` | Completed passes, and seconds since the last one |
| `ducklake.compaction.rss_growth_per_file` | `tier`, `database` | Process RSS the tier's recent calls left behind per input file merged: memory kept outside `memory_limit` |
| `ducklake.compaction.tier_last_success_age` | `tier`, `database` | Seconds since one of this tier's calls completed without an error (`last_success_age` is the same across the database's tiers) |
| `ducklake.compaction.queue` / `carry_over` / `backoffs` | `tier`, `database` | Tables the latest pass listed, left for the next pass, and backed off |
| `ducklake.compaction.max_compacted_files` / `cap` | `tier`, `database` | The static cap, and the cap the latest call was given |
| `ducklake.compaction.table.files_waiting` / `oldest_wait` / `starved` / `backoff` | `tier`, `database`, `table` | One table's state in one tier |
| `ducklake.snapshots.unexpired` | `database` | Snapshots left after housekeeping's latest expiry |
| `ducklake.process.rss_bytes` / `memory_limit_bytes` | — | Process RSS and the container's memory limit |
| `ducklake.files.compacted` | `database` | Total files compacted, across all tiers |
| `ducklake.files.rewritten` | `database` | Total data files rewritten by housekeeping to drop their delete files (`rewrite_deletes` step) |
| `ducklake.files.total` | `database` | Total active Parquet files |
| `ducklake.files.by_tier` | `tier`, `database` | Active files in this tier's file-size range |
