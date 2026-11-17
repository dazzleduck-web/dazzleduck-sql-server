# Dynamic Compaction Spec

Minor compaction must never get stuck: every unit of work should be small, bounded in memory and time, and limited to one table if it fails. In one observed failure it went 22 hours without a single successful run while the small-file backlog grew to about 98k files.

**Status.** Phase 1 is implemented: the per-table scheduler (PR #542), its resource budget, spill directory and catalog queries run inside PostgreSQL (PR #544), and the patched DuckLake it runs on (`v1.5.6-dazzleduck.5`, see "What the compactor needs from DuckLake"). This document describes it as built. Phases 2 and 3 are not implemented.

## Problem and goals

| Observed failure | Cause | Rule adopted |
| --- | --- | --- |
| OOM-killed every ~14 min, even at double the memory | Groups grew to the 512 MB default target: one partition = one 33k-file merge | Always set the target; limit files per call, not only bytes |
| Commit failed after 8 min (TRANSACTION_CONFLICT) | One catalog-wide transaction raced tables other writers rewrite every few seconds | One short transaction per table |
| Planning error failed the whole cycle | One table's internal error (hive path) aborted every table | Isolate failures per table |
| Backlog grew with a cap of 1 | DuckLake took partitions in a fixed order, so a busy partition took every slot | Keep the cap above busy partitions; DuckLake lists the oldest chunks first |
| 12–18 s per run even for 3 files | Every run re-planned every small file | Plan only tables with work; wait when idle |
| RSS kept climbing | glibc arenas and DuckDB's allocator keep freed memory, outside `memory_limit` | `MALLOC_ARENA_MAX=2`, allocator flushing, and a budget for memory outside the limit |
| Discovery ran out of 512 MB at ~1.5M files | Through the postgres scanner, the compactor's catalog queries copied every table's catalog rows into DuckDB | Run them inside PostgreSQL |
| Every merge failed with `Failed to get compaction file list` | DuckLake aggregated the partition values of every file in the catalog for each call | Patched DuckLake reads the compacted table's only |
| A table with a 569k-file backlog failed whatever its cap | DuckLake listed every candidate file of the band before applying `max_compacted_files` | Patched DuckLake lists only the files a capped call can use |
| Calls killed mid-merge | The catalog's idle-in-transaction timeout closed the session the rewrite left idle in a transaction | Patched DuckLake releases the catalog transaction during the rewrite |
| Container OOM-killed with RSS 1.2 GB above a 322 MB limit | The limits took 68% of the container and left the rest for the JVM's heap (25% by default) and DuckDB's memory outside its limit | Reserve the JVM, budget memory outside each limit |

**Goals:** each table call takes seconds in steady state and minutes during a backlog, peak memory stays under a fixed budget that adds up to the container, one table's failure never discards another's work, and the backlog drains without manual tuning. **Non-goals:** changing how writers size their files. Changing DuckLake was a non-goal too, but every fix it needed is now a backport or a patch in the fork the images ship.

## The unit of work

The unit of work is one table call: one `ducklake_merge_adjacent_files` call on one table, with one commit. Two limits bound every call, and the compactor never breaks either of them:

1. **`ducklake_target_file_size`** is set with `SET SESSION` on the connection before every call, to the tier's target or a lower one (see "Groups that do not fit"). It bounds each group, so it bounds the call's peak memory and each output file. The session setting overrides table and catalog options, so no table can fall back to DuckLake's 512 MB default. The compactor refuses to start a tier without a target, and reads the setting back in bytes after setting it: a mismatch refuses the tier. It needs DuckDB 1.5.4 or later (DuckLake commit `961c9799`), so a tier whose `SET` fails at startup does not start.
2. **Groups per table per call** (the tier's static cap) is the most `max_compacted_files` a call is ever given. Every rule below can only lower the cap for a call, never raise it.

Together they bound a call's input: at most static cap × target bytes, one group in memory at a time.

## Tier chain and band edges

Bands are contiguous, so every file belongs to one tier and outputs move up the chain. Files of 256 MB or more are final.

| Tier | Input band [min, max) | Target | Static cap | `memory_share` | `min_files`, `max_wait` |
| --- | --- | --- | --- | --- | --- |
| minor | [0, 8 MB) | 16 MB | 12 | 0.4 | 4, 5 min |
| major | [8 MB, 64 MB) | 64 MB | 16 | 0.3 | 2, none |
| mega | [64 MB, 256 MB) | 256 MB | 5 | 0.3 | 2, none |

The caps keep each call at about 1 GB of input or less (minor 12 × 16 MB, major 16 × 64 MB, mega 5 × 256 MB). A backlog drains over consecutive passes, each committing its own batch.

- **Startup checks:** bands must not overlap, every enabled tier has a target of at least its band max (with a lower target, DuckLake skips in-band files at or above it and they never merge), a positive static cap, `min_files` of at least 2, `memory_share` values adding up to at most 1 and `threads_per_cpu` of at least 1.
- **Overshoot:** DuckLake checks a group's total before adding each file, so outputs overshoot the target by at most one input file (minor outputs stay under 24 MB).
- **Compression:** tiny-file merges compress heavily (3,000 files, 16 MB in, 139 KB out), so their outputs stay in minor's band until they reach 8 MB.

**At most one leftover file per band.** Every in-band file is below the target, so a group's second file is always added: any two files in the same band, partition and schema version merge. Once caught up, each partition holds at most one file per band per schema version, plus final files. Files with a delete file or inlined deletions are skipped by DuckLake, so a workload with row-level deletes keeps more leftovers until housekeeping rewrites them.

**Minor admission threshold.** Because any two files merge, a slow table would merge one new tiny file into its leftover on almost every pass, rewriting the leftover and committing a snapshot every few seconds. So minor admits a table when one of its groups has at least `min_files` (4) eligible files, or the second-oldest file in one of its groups has waited `max_wait` (5 min). The oldest file in a group is usually the leftover from the last merge: timing it would admit the table as soon as one new file arrived. Both stay low because every small file waiting is one more file each query on that partition opens. Major and mega keep the 2-file trigger: their inputs arrive slowly and are already large. Admission is per table, not per partition: DuckLake has no partition filter, so once a table is admitted, every partition holding a leftover plus one new file may be merged too.

## How one table call works

`ducklake_merge_adjacent_files` on one table, as the patched DuckLake runs it:

1. **Lists candidate files:** live files of the table in the size band, below the target, without a delete file or inlined deletions. With `max_compacted_files` set, it first numbers each (schema version, partition) group's files into chunks of the target by the bytes before them, drops chunks of fewer than 2 files, and keeps only the oldest 2 × cap chunks across groups; only their files are listed in full, with paths, partition values and row id statistics. Partition values are read for the compacted table only.
2. **Groups them** by schema version, `partition_id` and partition values. Within each group it walks the files in order and cuts them into merge groups: a merge group keeps adding files until its total reaches the target. Each merge group becomes one output file.
3. **Stops** after `max_compacted_files` merge groups, counted across the table's partitions. A single file left at the end of a partition also uses a slot without merging anything.
4. **Rewrites:** one plan with a branch per merge group (UNION ALL). The merge groups run one after another, whatever the thread count: each reads its files, writes one Parquet file and frees its buffers before the next starts. In autocommit, the catalog transaction is committed once the merge is planned and reopened for the commit, so the rewrite holds no catalog transaction.
5. **Commits once:** every merge group lands in one snapshot, or none does. A commit that loses the race for the next snapshot id is retried up to `ducklake_max_retry_count` (20) times, about 6 minutes at most.

So a merge group's size sets the call's peak memory, while the number of groups multiplies the time:

```text
call_time ≈ groups × (files_per_group × sec_per_file + bytes_per_group ÷ throughput)
```

## Call sizing

**Why a file limit too.** The target bounds a group's bytes, not its files. Tiny files make a full minor group about 3,200 files (16 MB ÷ 5 KB), about 144 s at the measured 0.045 s per file, so 12 full groups would take about 29 minutes. Minor time depends almost entirely on file count, so each call also gets a limit on input files, `max_files_per_call`. Major and mega groups are a few large files, and their static caps already bound them.

**The cap for one call.** From the discovery query's files waiting and average file size:

```text
files_per_group = min(ceil(target ÷ average_file_size), files_waiting)
file_cap        = max(1, floor(MAX_FILES_PER_CALL ÷ files_per_group))
time_limit      = min(CALL_TIME_LIMIT, 0.8 × IDLE_TXN_TIMEOUT)   -- CALL_TIME_LIMIT alone when there is no timeout
time_cap        = max(1, floor(time_limit ÷ (files_per_group × sec_per_file)))
cap             = min(static_cap, time_cap, max(file_cap, busy_partitions + 1))
```

- In steady state groups are a few files and the cap is the static cap (12 for minor).
- During a minor backlog of 5 KB files the file cap is 1 or 2, so a call merges one or two full groups and commits in minutes.
- The cap is never above the static cap, so the two limits of the unit of work always hold.

**Busy partitions.** A busy partition always has work. Before the patched listing, DuckLake walked partitions in hash-map order, so a cap at or below the number of busy partitions could give every slot to the same partitions on every call. So, within the static cap and the time cap, the cap is at least busy partitions + 1, even past the file cap. When the static or time cap wins, the table raises a starvation alert. With the patched DuckLake, a capped call's candidates are the oldest chunks across all groups, so a busy partition no longer starves older backlog; the floor stays as a second guard. The safety net remains the alert on a table's oldest small file older than 30 min.

**Memory per group.** A group's peak memory is files per group × memory per file. Memory per file has two parts: a fixed cost of 0.12 MiB for a table with a sort key and 0.85 MiB without one (measured, 1,000 small files), and `BYTES_EXPANSION` (4) × the file's bytes, for decompression, the sort and the writer's row-group buffer, which dominate for files of megabytes. A group may use 80% of the instance's `memory_limit` (`MEMORY_LIMIT_SHARE`). Threads are not charged against it: a load test measured about the same memory at 1, 2 and 8 threads. So a full minor group of tiny files needs about 385 MiB sorted and 2.7 GiB unsorted, and 10 MB files fit four to a group in major's 220 MiB at 2 GiB. The discovery query flags tables without a sort key.

**Groups that do not fit.** A full group can be too slow to commit within 80% of the catalog's timeout, or too large for 80% of the instance's memory. So before each call the most files a group may hold is worked out at the table's own rate and memory per file (its average file's), and capped by what the table has learnt (see "Failures"). When a full group at the tier's target holds more, the call runs with a lower session target: those files × the average file size (1,600 tiny files, 8 MB; 4 files of 10 MB, 40 MB instead of 64 MB). Such a call also runs with `max_file_size` at half its lowered target, so it leaves the outputs of earlier lowered calls alone (see "Merged outputs that stay in their band"). When a lowered call merges nothing, the table is not called again until its waiting files change. A table of which not even 2 files fit is skipped and reported as oversized. A sort key, a longer timeout, more memory or larger files avoid both.

**Startup check.** A tier is refused only when not even two of its band's smallest files fit in a group within 80% of the timeout (at the sorted rate) or 80% of its instance's `memory_limit`: then no table of it could ever be merged (mega in a 2 GiB container, say). When one full group of its smallest files does not fit, it starts with a warning and its calls run with lowered targets.

**The timeout after `.5`.** Sizing to 80% of the idle-in-transaction timeout dates from when the rewrite held the catalog transaction open. With the patched DuckLake it no longer does, so the timeout now only has to hold the planning and the commit; sizing calls to it is conservative and could be relaxed. `CALL_TIME_LIMIT` still bounds how long one call holds a tier's loop.

## Resources

Each (database, tier) has its own DuckDB instance, as do each database's housekeeping, data retention and file-count gauges, so `threads`, `memory_limit` and `temp_directory`, which are global to an instance, never race.

### Threads

One table call uses about 1 to 2 cores, whatever its thread count: its groups run one after another and each writes its Parquet file on one thread. Extra threads read, decode and sort a group's input files in parallel. Merging small files from an object store costs two GETs per file (footer, then data), so a call's time is about latency × 2 × files ÷ threads: at about 25 ms per request, 1,000 files of 8 KB took 13.9 s on 4 threads, 6.9 s on 8 and 3.5 s on 16, and no DuckDB 1.5 prefetch or cache setting changed it by more than 3%. Threads are the lever, not CPUs.

- Every tier's instance gets `threads_per_cpu` (4) × the CPUs available to the process (`Runtime.availableProcessors()`, which reads the container's quota, rounded up: 1.5 CPUs read as 2, so 8 threads) ÷ databases, at least 1. There is no CPU share; a config that still sets `cpu_share` starts with a warning.
- A pod with a memory limit but no CPU limit sees all of the node's CPUs, so an instance gets no more threads than take half of a group's memory at `PER_THREAD_BYTES` (8 MiB) each.
- A `SET threads` in an instance's `connection_settings` wins, and the instance reports the threads it actually runs with.

### Memory

The process's memory (the container's limit, else the machine's) is split as follows:

1. **The JVM** is reserved first: its maximum heap, at most 20% of the memory, plus 128 MiB for metaspace, code cache and thread stacks. The images cap the heap at 15%: `-XX:MaxRAMPercentage=15` in the Jib image, `-R:MaximumHeapSizePercent=15` in the native image, whose default is 80%.
2. **The rest, the DuckDB pool,** is split by share: the tier instances 70% × their `memory_share`, housekeeping 12%, data retention 12%, the file-count gauges 4%, each divided among the databases.
3. **Each slice** holds an instance's `memory_limit` and what DuckDB allocates outside it: freed memory its allocator keeps (up to its 64 MB flush threshold), glibc arenas and fragmentation, budgeted as 25% of the limit plus 64 MiB. So the limit is (slice − 64 MiB) ÷ 1.25, at least 64 MiB.

The default limits therefore always fit. An instance whose `connection_settings` set a `memory_limit` keeps it; when explicit limits, their allowances and the JVM's whole heap exceed the process's memory, the compactor warns at startup and `/health` alerts.

The defaults, with the shipped `memory_share` values and one database:

| Container | JVM reserve | Pool | minor | major, mega | housekeeping, retention | file counts |
| --- | --- | --- | --- | --- | --- | --- |
| 2 GiB, 2 CPUs | 435 MiB | 1,613 MiB | 310 MiB | 220 MiB | 104 MiB | 64 MiB |
| 8 GiB, 4 CPUs | 1,357 MiB | 6,835 MiB | 1,480 MiB | 1,097 MiB | 605 MiB | 168 MiB |

At 2 GiB, major merges 10 MB files four to a group and mega is refused at startup (two 64 MB files need about 512 MB). At 8 GiB, mega merges 64 MB files three to a group, with a lowered target of 192 MB.

**Memory hygiene.** Most memory outside DuckDB's `memory_limit` was native `malloc` memory in glibc arenas (926 MiB of a 1.25 GiB process in 28 arenas), so the images set `MALLOC_ARENA_MAX=2`. DuckDB's bundled allocator also kept memory a merge freed (about 0.3 MiB per merged input file, until a 4 GiB container was OOM-killed), so every instance runs with `allocator_background_threads = true` and `allocator_flush_threshold = '64MB'`, which an instance's own settings can override. Each call records the process's RSS before and after, and the tier exports what its recent calls left behind per input file (`rss_growth_per_file`) to calibrate `BYTES_EXPANSION` and the allowance outside the limit against. RSS is the whole process's, so tiers merging at the same time add to each other's figure.

### Spill

Every instance spills to its own directory under `spill_directory` (default `/tmp/ducklake-compactor`): `<root>/<database>/tier-<name>`, `housekeeping`, `retention` and `file-counts`. DuckDB's default is `.tmp` in the working directory, and it creates only the last level of a configured directory, so both failed late, when a merge first spilled.

- Each directory is created and probed (a file written and deleted) when its instance opens; a tier whose directory is not writable does not start. Instances never share one, since DuckDB cleans up its temp directory on close.
- A process killed while spilling leaves its spill files, and DuckDB removes them only on its next spill; an `emptyDir` keeps them across restarts. So the first time the process uses a directory, it deletes the `duckdb_temp_*` files left in it.
- Each instance's `max_temp_directory_size` is split like its memory, from 68% of the free space under the root at startup (at least 256 MiB each), or of `spill_max_size` when that is smaller (no minimum, so the sizes stay under the cap; going over an `emptyDir`'s `sizeLimit` evicts the pod). The rest of the disk stays for its other users. An instance whose settings set `temp_directory` keeps it; `spill_directory = ""` leaves DuckDB's defaults.
- `/health` reports the root, its budget, the sizes given out, the bytes spilled and the free space, and alerts above 80% of the sizes given out or when the free space is less than the instances may still spill.

## Catalog queries

The compactor's own catalog queries (tier discovery, the file-count gauges, data retention's file ranges) read DuckLake's catalog tables. Through the postgres scanner, which copies a PostgreSQL table in `ctid` ranges and pushes down only column-versus-constant filters, a query that filters with `IN (SELECT ...)` or joins on names copies every table's catalog rows into DuckDB on every pass. So, on a PostgreSQL catalog:

- **Discovery** and **the file-count gauges** run inside PostgreSQL through `postgres_query`, and only their result comes back: a few rows per table, one row of counts. The PostgreSQL discovery query returns the same rows as the DuckDB one (tested on partitions, schema versions and delete files); a NULL partition value is keyed by `chr(1)`, since PostgreSQL text cannot hold `chr(0)`.
- **Data retention** looks up the table and column ids first, from the small table, schema and column catalogs, and filters the data-file and column-statistics scans by them as literals, which the scanner pushes down. Its min/max conversions stay in DuckDB.
- **The schema** is the one holding `ducklake_snapshot`. When several lakes share a database, it is the schema whose `ducklake_metadata` `data_path` is the lake's (the metadata schema is not exposed in SQL); a schema whose catalog cannot be read is skipped. When no schema matches, the DuckDB queries run, with a warning.
- **A failed lookup** (a catalog briefly unreachable) is not settled for good: discovery fails and the loop reconnects and retries, and the counts skip that refresh, rather than run the DuckDB queries that copy the catalog.

DuckDB-file and SQLite catalogs run the same queries in DuckDB.

**The catalog's timeout.** It is read from PostgreSQL's `pg_settings` through `postgres_query` on the metadata catalog, whose connection carries the ATTACH string's options, so an `options=-cidle_in_transaction_session_timeout=600000` there counts (`SHOW` cannot be used: `postgres_query` wraps its query in `COPY (SELECT ...)`). The metadata catalog is hidden from `duckdb_databases()`, so the backend is recognised by the DuckLake catalog's own path, which starts with `postgres:`. If a PostgreSQL catalog's value cannot be read, a timeout in the tier's `connection_settings` is used; without one, the tier retries opening its connection rather than size calls as if there were no limit. 0 (PostgreSQL's default) or a catalog without the setting means no timeout limit.

## The loop

Phase 1 replaces the catalog-wide call with per-table calls run one at a time per tier. One loop thread per (database, tier):

1. **Refresh.** One discovery query lists the tables with work.
2. **Rank** by outstanding files, most first; ties go to the older oldest file. Tables carried over from the previous pass go first.
3. **Call** each table in turn: take its lease, size the call (Call sizing), `SET SESSION ducklake_target_file_size`, run `CALL ducklake_merge_adjacent_files(catalog, table, schema := …, min_file_size := …, max_file_size := …, max_compacted_files := cap)`, release the lease, and record the outcome. A table whose lease is held elsewhere is skipped this pass.
4. **End the pass** when every listed table has been called or `pass_time_limit` (2 min) has passed. A running call finishes; tables not yet called carry over. The first call of a pass always runs.
5. **Wait.** After a pass that merged nothing (no work, or every table backed off, held, oversized or failing), sleep `idle_sleep_max` (10 s). After a pass that merged, the next refresh starts once `MIN_REFRESH_INTERVAL` (5 s) has passed since this one, which bounds the catalog load to one discovery query per tier and database every 5 s.

**Discovery query.** It groups files exactly as `ducklake_merge_adjacent_files` does: by table, schema version (from `ducklake_schema_versions`, in force at each file's `begin_snapshot`), `partition_id` and partition values. It reads live files in the band, skips files with a delete file, applies `auto_compact` at its narrowest scope (table, schema, catalog; only `'true'` enables merging, as in DuckLake) and reads the sort key. File ages come from each file's first snapshot; a file whose first snapshot has been expired is as old as the oldest snapshot left. It lists tables with at least 2 eligible files in one group (for minor, the admission threshold), and returns per table the files and bytes waiting in groups of at least 2, the oldest waiting file (each group's second-oldest, for the same reason as the admission threshold), busy partitions (groups with a file committed in the last 10 min) and the sort-key flag. Files whose deletes are inlined in the catalog are counted, though DuckLake skips them; the worst that costs is a call that merges nothing.

**Table leases.** A table runs at most one job at a time, across all tiers and jobs: merges, the data-retention `DELETE` and the delete-file rewrite. Each job takes the table's lease (an in-process set of busy tables) and releases it when it commits or fails. Tier loops skip a held table. Retention waits for it, and while it waits the loops skip that table too, so a busy table cannot keep the lease from it. The delete-file rewrite skips a held table, and after `SKIPS_BEFORE_WAIT` (3) skipped runs in a row waits for it. Leases live in one process, so Phase 1 assumes one compactor process per catalog; a conflict therefore means another writer, and tables other writers rewrite continuously get `auto_compact = false`.

**Retention and housekeeping.** Data retention and the delete-file rewrite run per table under the lease, on their own timers. The rewrite calls `ducklake_rewrite_data_files` on every live table, since inlined deletes count toward the threshold too; a call that rewrites nothing commits no snapshot. Snapshot expiry and old-file cleanup (`ducklake_cleanup_old_files`, files of expired snapshots only) stay catalog-wide. Every table call commits one snapshot, so the compactor exports commits per minute and alerts when unexpired snapshots keep growing.

**Failures.** A failure costs one table's call and nothing else; the table is retried on the next pass, and after 3 failures in a row it backs off for 15 min and raises an alert. A conflict or other failure does not change the table's sizing; a resource failure does:

- **The catalog's timeout killed a call:** the per-file rates were too fast for this table, so its cap is halved, down to 1. Each call that commits doubles it again, but never back up to a cap the timeout killed. When the timeout kills a call of a single group, the table's files per group are halved instead (down to 2), through a lower session target.
- **Out of memory or spill space:** the table's files per group are halved the same way, since one group is in memory at a time.
- **Expiry:** these learnt limits expire an hour after the last such failure (`LEARNED_LIMITS_EXPIRE`), and are forgotten when the table leaves the discovery list. They are not persisted, so a restart forgets them too.

**Connections.** A failure that can leave the tier's connection unusable (a lost catalog connection, a killed transaction, a failed catalog lookup, an INTERNAL or FATAL DuckDB error) makes the loop reopen it on the next pass and carry the rest of the pass over.

**Catalog options load once.** `set_option` changes (`target_file_size`, `auto_compact`) are read only at attach, so they need a restart. The session target does not depend on them.

**Telemetry and alerts.** Each call records table, tier, cap and static cap, busy partitions, files and bytes waiting before the call, files in and out (from the merge's result row, which has no byte counts), duration, RSS before and after, the instance's `memory_limit`, the timeout it was sized to, and the outcome and failure class. `/health` shows per tier its state (STARTING, RUNNING, REFUSED with its reason, STOPPED), last pass, queue, carry-over, back-offs, starved, stale, oversized and reduced tables, each tracked table's files waiting, last call, outcome, last success, failures and learnt limits, its threads, the timeout it sizes calls by, and `duckdb_memory()`; and the spill status. A success is a call that completed without an error. Alerts:

- a tier that did not start, or has no completed pass in 15 min (a pass that finds no work counts);
- a table backed off after 3 failures, starved, oversized, or whose oldest small file has waited over 30 min;
- a band whose file count rose for 30 min, and unexpired snapshots growing across 3 housekeeping runs;
- RSS above 75% of the container's limit, and the JVM plus the instances' memory limits and allowances exceeding the process's memory;
- spill above 80% of the sizes given out, or free space below what the instances may still spill.

## What the compactor needs from DuckLake

The images ship a patched DuckLake extension (`ducklake.patch.release`, built from `dazzleduck-web/ducklake`, branch `backport/v1.5.6-dazzleduck`; see DUCKLAKE_PATCH.md), not the stock one, which fails the compaction stress test. The compactor relies on:

| Behaviour | Where it comes from | Without it |
| --- | --- | --- |
| Compaction conflicts checked per file, not per table | duckdb/ducklake#1453, #1482, #1484 (backported) | Concurrent tiers on one table always conflict; two concurrent deletes can bring deleted rows back |
| A `DELETE` conflicts only with inserts its filter could match | duckdb/ducklake#1949, the fix for #1895 (patch) | Data retention's `DELETE` fails against continuous ingestion |
| Partition values read for the compacted table only | duckdb/ducklake#1410, part of #1707 (backported in `.4`) | Every call aggregates the whole catalog's partition values and runs out of memory on large catalogs |
| The catalog transaction released during the rewrite | duckdb/ducklake#1580 (backported in `.5`) | The idle-in-transaction timeout kills long merges |
| A capped call lists only the oldest chunks it can fill, skipping files with deletes or inlined deletions | patch (`.5`) | The list of a large backlog alone runs out of memory, whatever the cap |
| `SET SESSION ducklake_target_file_size` | DuckLake `961c9799` (DuckDB 1.5.4 and later) | Groups grow to the catalog's target |
| Row id pruning statistics | patch | Not needed by the compactor itself |

Commits retry up to `ducklake_max_retry_count` (20) times, set in every instance's `connection_settings`: with writers and compaction committing at once, the default of 10 ran out. A commit that keeps losing can retry for about 6 minutes, so any job deleting orphaned files must use an `older_than` well above that.

## Implementation

| Item | Where |
| --- | --- |
| Tier settings and startup checks; `memory_share` resolution | `CompactionTier`, `CompactionConfig` |
| One loop per (database, tier): refresh, rank, call, pass limit and carry-over, wait, retry and back-off, learnt limits, oversized and reduced tables, published status | `TierLoop` |
| Opening an instance: threads (capped by memory, read back), `memory_limit`, spill directory, the session target read back, the catalog's timeout, the startup check; discovery; the per-table call | `DuckDbTierCompactor` |
| The cap, groups that do not fit, the startup check | `CallSizing` |
| Catalog queries inside PostgreSQL, the schema lookup | `CatalogSql`, `DuckDbTierCompactor.discoverySqlPostgres`, `CompactionService.fileCountsSql` |
| Memory budget and over-commit check | `MemoryBudget` |
| Spill directories, sizes, leftovers and alerts | `SpillBudget` |
| Table leases | `TableLeases`, taken by `TierLoop`, `DuckLakeDataRetention` and `DuckLakeHousekeeper` |
| Per-table delete-file rewrite; unexpired snapshots after expiry | `DuckLakeHousekeeper` |
| Per-call telemetry, metrics, `/health`, alerts, `/ui` | `CompactionRun`, `CompactionRunLog`, `CompactionState`, `CompactionService`, `HealthServer`, `CompactionHtml` |
| Allocator settings on every instance | `RawConnections` |
| `MALLOC_ARENA_MAX=2`, the heap cap | the Jib image and `Dockerfile.native` (pom) |

**Tests that guard the load-test fixes.** `CatalogScaleTest` generates a million file rows into a PostgreSQL catalog and runs discovery, the counts and a merge in a 64 MB instance (the old discovery query, and a merge on `.3`, run out of memory there). `IdleCatalogTransactionTest` checks that PostgreSQL kills no catalog session during a merge longer than a 500 ms idle timeout (it does on `.4`). `CompactionStressTest` (the `stress` profile) runs writers, readers, merges, retention and housekeeping concurrently against PostgreSQL on the pinned patched release and checks that no row is lost or duplicated.

**Merged outputs that stay in their band.** A group stops on its input bytes, and its output can come out smaller than its inputs, so an output may land back in its own band:

- **Major and mega** (target = band max): an output just under the band max merges with the next file in its group and leaves the band, at the cost of one extra rewrite.
- **Minor:** merged tiny files compress several-fold, so minor outputs usually land back in the minor band and are merged again as more files arrive, until one reaches 8 MB. The admission threshold keeps how often this happens in check.
- **Calls with a lowered target** compress the same way, so their outputs would be grouped with the next tiny files and rewritten pass after pass; such a call runs with `max_file_size` at half its lowered target, and larger outputs wait for a call at the tier's own target.

**Known gaps.**

- **Learnt limits** are lost on restart: after a container OOM kill, the next start plans full groups again.
- **The constants** `BYTES_EXPANSION`, the allowance outside the limit, `PER_THREAD_BYTES` and the unsorted `sec_per_file` are starting values, to calibrate with `rss_growth_per_file` and load tests.
- **Timeout sizing** is conservative since `.5` (see "The timeout after `.5`").
- **`ResourceSampler.rssPeakBytes()`** reads the process's lifetime high-water mark, so it cannot measure one call; Phase 2's reservations need DuckDB's profiler or `duckdb_memory()` sampled during the call.
- **One stress run** on `.5` failed (seed `163677237091470220`, three minutes); not yet investigated. One-minute runs can also fail the check that housekeeping rewrote a file, since that rewrite happens a few times in 80+ attempts.

## Phase 2: parallel scheduling

Phase 2 uses more cores through concurrent calls on different tables. The two limits on the unit of work stay as they are.

- **Worker pool:** up to K workers per tier, each on its own connection to the tier's DuckDB instance, so all calls share one `memory_limit` and thread pool. K starts at 2 and is at most the tier's CPUs. A table is never split across workers; the lease already prevents two calls on one table.
- **Admission:** a call reserves its estimated peak memory (Call sizing) and about 1 core (2 for a sorted table with full groups), and starts only when both fit in what running calls leave. Reservations are estimates, not measured RSS: RSS cannot be split between concurrent calls.
- **Solo tables:** a call that runs long while others run beside it marks its table solo; a solo table runs alone until 3 solo successes.
- **Adaptive K:** lower K by 1 when parallel calls run more than 1.5× slower than solo, raise it by 1, up to the tier's CPUs, after 10 passes without a slow call.
- **Learned constants:** per table, learn `sec_per_file` (minor) and throughput (major, mega) as a moving average, to size reservations and to replace `MAX_FILES_PER_CALL` with a per-table limit.
- **Instance recycling:** reattach a tier's instance when RSS is above 60% of the container limit, once its workers are idle; a reattach costs about 1 s.

## Phase 3 (optional): learned time-based caps

If the fixed per-file rates prove too blunt, learn `group_time` per table, adjusted on time failures, and use it in the time cap in place of the constant `sec_per_file` and of `MAX_FILES_PER_CALL`. The cap is still never above the static cap. Two starting values for that model are guesses: `sec_per_file` for unsorted tables (0.09 s, 2× sorted) and throughput for major and mega (50 MB/s).

## Constants

| Constant | Value | Source |
| --- | --- | --- |
| Static caps (minor, major, mega) | 12, 16, 5 | About 1 GB of input per call at most |
| sec_per_file, sorted / unsorted | 0.045 / 0.09 s | Sorted measured (~22 files/s, 4 threads); unsorted a guess at 2× sorted |
| per_file_cost, sorted / unsorted | 0.12 / 0.85 MiB | Measured, 1,000 small files |
| BYTES_EXPANSION | 4 | Memory per byte of an input file, on top of the fixed cost; a starting value |
| MEMORY_LIMIT_SHARE | 80% | Share of an instance's `memory_limit` one group may use |
| CALL_TIME_LIMIT | 10 min | The longest a call is sized to run; below the 15-min pass alert |
| MAX_FILES_PER_CALL (`max_files_per_call`) | 6,000 | About 4.5 min at the sorted rate |
| IDLE_TXN_TIMEOUT | read when a tier's connection opens | From `pg_settings`; 0 or none means no limit |
| TIMEOUT_SHARE | 80% | Share of the timeout a group is sized to |
| `min_files`, `max_wait` (minor) | 4, 5 min | Each waiting small file is one more file a query opens |
| `pass_time_limit` | 2 min | After it, no new call starts until the next refresh |
| MIN_REFRESH_INTERVAL | 5 s | The least time between two refreshes of a tier for one database |
| `idle_sleep_max` | 10 s | The sleep after a pass that merged nothing |
| Back-off | 3 failures in a row, 15 min | |
| LEARNED_LIMITS_EXPIRE | 1 hour | After the last timeout, out-of-memory or spill failure |
| BUSY_WINDOW | 10 min | A group with a file committed this recently is busy |
| Stale | 30 min | A table's oldest waiting file older than this raises an alert |
| `threads_per_cpu` | 4 | DuckDB threads per available CPU, divided among the databases |
| PER_THREAD_BYTES, THREAD_MEMORY_SHARE | 8 MiB, 50% | Cap threads at half of a group's memory; not charged against the group |
| JVM reserve | heap, at most 20%, + 128 MiB | The images cap the heap at 15% |
| Pool shares | tiers 70% × `memory_share`, housekeeping 12%, retention 12%, file counts 4% | Of the process's memory less the JVM, divided among the databases |
| Outside the limit | 25% of `memory_limit` + 64 MiB | Allocator retention, glibc arenas, fragmentation; a starting value |
| MINIMUM memory_limit | 64 MiB | No instance gets less |
| Allocator | background threads, 64 MB flush threshold | On every instance |
| Spill | `/tmp/ducklake-compactor`; 68% of free space or `spill_max_size`; at least 256 MiB an instance unless capped | One directory per instance |
| Spill alerts | above 80% of the sizes given out; free space below what may still spill | |
| `ducklake_max_retry_count` | 20 | About 6 min of commit retries at most |
| SKIPS_BEFORE_WAIT (housekeeping) | 3 | Housekeeping runs that may skip a held table before the rewrite waits |
| K (Phase 2) | starts at 2, at most the tier's CPUs | Also bounds catalog connections, request rate and commit retries |

## Decisions

- Every call runs with a session `ducklake_target_file_size` at or below the tier's target and a cap never above the tier's static cap; nothing raises either limit.
- Default band edges are 8 MB / 64 MB / 256 MB, and mega is enabled by default (it is refused at startup when its instance cannot hold two of its files).
- Recommend a sort key on every table, so its merges take the cheaper sorted path (0.12 vs 0.85 MiB per file).
- Phase 1 sizes calls with fixed limits and only learns from resource failures; learning rates waits for Phase 2 or 3.
- Threads come from `threads_per_cpu` for every tier, not a CPU share: merges of small files wait on object-store round trips, so threads, not CPUs, set their speed. More cores are used through more concurrent table calls (Phase 2), not more groups per call.
- Memory is budgeted for the whole container: the JVM, every instance's limit and its memory outside the limit.
- The compactor's catalog queries run where the catalog is: inside PostgreSQL for a PostgreSQL catalog.
- DuckLake fixes the compactor needs go into the patched release the images pin, backported from upstream where upstream has them.
