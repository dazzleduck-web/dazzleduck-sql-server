# Dynamic Compaction Spec

Minor compaction must never get stuck: every unit of work should be small, bounded in memory and time, and limited to one table if it fails. In one observed failure it went 22 hours without a single successful run while the small-file backlog grew to about 98k files.

**Status:** Phase 1 is implemented (PR #542); this document describes it as built. Phases 2 and 3 are not implemented.

## Problem and goals

| Observed failure | Cause | Rule adopted |
| --- | --- | --- |
| OOM-killed every ~14 min, even at double the memory | Groups grew to the 512 MB default target: one partition = one 33k-file merge | Always set the target; limit files per call, not only bytes |
| Commit failed after 8 min (TRANSACTION_CONFLICT) | One catalog-wide transaction raced tables other writers rewrite every few seconds | One short transaction per table |
| Planning error failed the whole cycle | One table's internal error (hive path) aborted every table | Isolate failures per table |
| Backlog grew with a cap of 1 | DuckLake takes partitions in a fixed order, so a busy partition took every slot | Keep the cap above busy partitions, within the static cap |
| 12–18 s per run even for 3 files | Every run re-plans every small file | Plan only tables with work; wait when idle |
| RSS kept climbing | glibc arenas keep freed memory, outside DuckDB's `memory_limit` | `MALLOC_ARENA_MAX=2` |

**Goals:** each table call takes seconds in steady state and minutes during a backlog, always under the catalog's idle-in-transaction timeout, peak memory stays under a fixed budget, one table's failure never discards another's work, and the backlog drains without manual tuning. **Non-goals:** changing DuckLake, and changing how writers size their files.

## The unit of work

The unit of work is one table call: one `ducklake_merge_adjacent_files` call on one table, in one transaction, with one commit. Two limits bound every call, and the compactor never breaks either of them:

1. **`ducklake_target_file_size`** is set with `SET SESSION` on the connection before every call, to the tier's target. It bounds each group, so it bounds the call's peak memory and each output file. The session setting overrides table and catalog options, so no table can fall back to DuckLake's 512 MB default. The compactor refuses to start a tier without a target, and refuses a call if the setting is not in place. The setting needs DuckDB 1.5.4 or later (DuckLake commit `961c9799`; verified on 1.5.6.0, which this repo ships, and on 2.0.0.1), so a tier whose `SET` fails at startup does not start.
2. **Groups per table per call** (the tier's static cap) is the most `max_compacted_files` a call is ever given. It bounds how many groups one call may merge. Every rule below can only lower the cap for a call, never raise it.

Together they bound a call's input: at most static cap × target bytes, one group in memory at a time. The rest of this spec sizes calls inside those limits and schedules them.

## Tier chain and band edges

Bands are contiguous, so every file belongs to one tier and outputs move up the chain. Files of 256 MB or more are final.

| Tier | Input band [min, max) | Target (`ducklake_target_file_size`) | Groups per table per call (static cap) |
| --- | --- | --- | --- |
| minor | [0, 8 MB) | 16 MB | 12 |
| major | [8 MB, 64 MB) | 64 MB | 16 |
| mega | [64 MB, 256 MB) | 256 MB | 5 |

These caps keep each call at about 1 GB of input or less (minor 12 × 16 MB, major 16 × 64 MB, mega 5 × 256 MB). A backlog drains over consecutive passes, each committing its own batch.

- Startup checks: bands must not overlap, every tier has a target, and each target must be at least its band max. With a lower target, DuckLake skips in-band files at or above it, and they never merge.
- DuckLake checks a group's total before adding each file, so outputs overshoot the target by at most one input file (minor outputs stay under 24 MB).
- Tiny-file merges compress heavily (3,000 files, 16 MB → 139 KB), so their outputs stay in minor's band until they reach 8 MB.

**At most one leftover file per band.** Every in-band file is below the target, so a group's second file is always added: any two files in the same band, partition and schema version merge (7.9 + 7.9 MB → 15.8 MB). Once caught up, each partition holds at most one file per band per schema version, plus final files. This assumes no delete files: DuckLake skips files that carry them, so a workload with row-level deletes keeps more leftovers until they are rewritten.

**Minor admission threshold.** Because any two files merge, a slow table would merge one new tiny file into its leftover on almost every pass, rewriting the leftover and committing a snapshot every few seconds until it reaches 8 MB. So minor admits a table when one of its groups has at least `MINOR_MIN_FILES` (4) eligible files, or the second-oldest file in one of its groups reaches `MINOR_MAX_WAIT` (5 min). The oldest file in a group is usually the leftover from the last merge: timing it would admit the table as soon as one new file arrived and rewrite the leftover every pass. Timed on the second-oldest file, a new small file waits about 5 min at most once another file joins it. Both stay low because every small file waiting is one more file each query on that partition has to open. Major and mega keep the 2-file trigger: their inputs arrive slowly and are already large. Admission is per table, not per partition: DuckLake has no partition filter, so once a table is admitted, every partition holding a leftover plus one new file is merged too. Tables with many partitions still rewrite some leftovers early, a known limitation until DuckLake gains a partition filter.

## Call sizing

**How one table call works.** `ducklake_merge_adjacent_files` on one table:

1. Lists the table's files in the band that carry no deletes, and splits them by partition and schema version.
2. Within each partition, walks the files in order and cuts them into groups: a group keeps adding files until its total reaches the target. Each group becomes one output file.
3. Stops after `max_compacted_files` groups (the cap), counted across all of the table's partitions; in a partition with at least 2 files, a single file left at the end also uses a slot without merging anything (a partition with only one file uses none).
4. Builds one plan with a branch per group (UNION ALL). The groups run one after another, whatever the thread count: each reads its files, writes one Parquet file and frees its buffers before the next starts.
5. Commits once: every group lands in one snapshot, or none does.

So a group's size sets the call's peak memory (one group is in memory at a time), while the number of groups multiplies the time. The transaction stays open for all of it:

```text
call_time ≈ groups × (files_per_group × sec_per_file + bytes_per_group ÷ throughput)
```

**Why a file limit too.** The target bounds a group's bytes, not its files. Tiny files make a full minor group about 3,200 files (16 MB ÷ 5 KB), about 144 s at the measured 0.045 s per file, so 12 full groups would hold the transaction open for about 29 minutes. Minor time depends almost entirely on file count, so each call also gets a limit on input files, `MAX_FILES_PER_CALL`. Major and mega groups are a few large files, and their static caps already bound them.

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
- The cap is never above the static cap, so `ducklake_target_file_size` and groups per call always hold.

**Busy partitions and starvation.** DuckLake v1.5 walks partitions in hash-map order, which ignores file age and changes only as partitions come and go, and a busy partition always has work, so a cap at or below the number of busy partitions can give every slot to the same partitions on every call: the table's other partitions never compact. So, within the static cap, the cap is at least busy partitions + 1, even when that goes past the file limit, but never past the time cap: what fits in `CALL_TIME_LIMIT` (10 min), or in 80% of the catalog's idle-in-transaction timeout if that is shorter, at the table's per-file rate. When busy partitions + 1 is above the static cap or the time cap, those still win and the table raises a starvation alert. With full groups of tiny files the time cap is 4 groups for a sorted table (about 10 minutes at 144 s each) and 2 for an unsorted one at the guessed 0.09 s per file, so no call is sized past 10 minutes and a tier's loop is never held longer than that.

The floor is only a partial guard. It assumes one group per busy partition, but during a backlog a busy partition yields several full groups and can still take every slot. The safety net is the alert on a table's oldest small file older than 30 min; the real fix is upstream: a partition filter or oldest-first groups in DuckLake.

**Memory.** A call's peak memory is one group: files per group × a per-file cost. That cost is 0.12 MiB for a table with a sort key and 0.85 MiB without one (measured, 1,000 small files), so a full minor group of tiny files needs about 385 MiB sorted but about 2.7 GiB unsorted. DuckLake cannot limit files per group, so unsorted tables with tiny files need a sort key or a larger `memory_limit`; the discovery query flags tables without a sort key.

**Groups that do not fit.** For the same reason, one full group can be too slow to commit or too large for the instance. The startup check (below) uses the sorted rate, so an unsorted table of tiny files can pass it and still need twice as long per group: 3,200 files take 288 s, past 80% of a 180 s timeout, and about 2.7 GiB. The time cap cannot go below 1 group. So before each call the most files a group may hold is worked out at the table's own rate and per-file cost: within 80% of the catalog's idle-in-transaction timeout and 80% of the instance's `memory_limit`. When a full group at the tier's target holds more, the call runs with a lower session target: those files × the average file size (1,600 files, 8 MB, in the example). Its outputs are smaller and stay in the band, to be merged again later, but the table makes progress instead of being killed call after call. Such a call also runs with `max_file_size` at half its lowered target, so it leaves the outputs of earlier lowered calls alone (see "Merged outputs that stay in their band" below). Only a table of which not even 2 files fit is skipped and reported as oversized. A sort key, a longer timeout, more memory or larger files avoid both.

## Threads and cores

One table call uses about 1 to 2 cores, whatever its thread count, because its groups run one after another and each group writes its Parquet file on one thread. Extra threads read and decode a group's input files, and sort them for a sorted table, in parallel. Measured on local disk, one call merging 16 groups:

| DuckDB threads | Call time, unsorted | Average cores in use, unsorted | Average cores in use, sorted |
| --- | --- | --- | --- |
| 1 | 1.4 s | 0.6 | 0.7 |
| 2 | 1.0 s | 1.0 | 1.1 |
| 4 | 0.8 s | 1.5 | 1.7 |
| 8 | 0.7 s | 1.9 | 2.1 |

With one thread the call used only 0.6 of a core: the rest was I/O wait, which is far longer on an object store. So:

- **Threads per instance** are set at startup to `THREADS_PER_CPU` (2) × the instance's CPUs, not fixed in `connection_settings`. A tier's CPUs are its `cpu_share` of the CPUs available to the process, which Java's `Runtime.availableProcessors()` reads from the container limit. Each (database, tier) has its own DuckDB instance, so the shares are a process budget: they add up to at most 1 (defaults minor 0.5, major 0.25, mega 0.25), and a tier's CPUs are divided among the databases. Each instance gets at least 1 thread. A tier without a `cpu_share` gets an equal part of what the others leave.
- **Memory per instance** follows the same budget. An instance whose `connection_settings` set no `memory_limit` gets `MEMORY_SHARE` (50%) of the process's memory (the container's limit, else the machine's) × its `cpu_share` ÷ databases. The compactor's other DuckDB instances get fixed slices too, divided among the databases: housekeeping 8%, data retention 8%, the file-count gauges 2%. That leaves about a third for the JVM and for what DuckDB allocates outside `memory_limit`. An explicit `SET threads` or `SET memory_limit` in an instance's settings wins.
- **More cores only help through more concurrent calls** on different tables (Phase 2). More groups per call do not run in parallel.

## Phase 1: per-table scheduler

Phase 1 replaces the catalog-wide call with per-table calls run one at a time per tier. It fixes every observed failure without learning or concurrency.

**The loop.** One loop thread per (database, tier):

1. **Refresh.** One discovery query lists the tables with work (below).
2. **Rank** by outstanding files, most first; ties go to the older oldest file. Tables carried over from the previous pass go first.
3. **Call** each table in turn: take the table's lease, `SET SESSION ducklake_target_file_size`, compute the cap (Call sizing), run `CALL ducklake_merge_adjacent_files(catalog, table, schema := …, min_file_size := …, max_file_size := …, max_compacted_files := cap)`, commit, release the lease, and record the outcome. A table whose lease is held elsewhere is skipped this pass.
4. **End the pass** when every listed table has been called or `PASS_TIME_LIMIT` (2 min) has passed. A running call finishes; tables not yet called carry over. The first call of a pass always runs, so a refresh slower than the limit cannot stall the tier.
5. **Wait.** After a pass that merged nothing, sleep `idle_sleep_max` (10 s) before the next refresh. That covers a pass that found no work, and one whose tables were all backed off, held by another job, oversized or failing; refreshing again at once would only repeat the query. After a pass that merged, the next refresh starts once `MIN_REFRESH_INTERVAL` (5 s) has passed since this one. Each refresh is one query over the catalog's file metadata, so this bounds the load to one query per tier and database every 5 s. A table below the minor admission threshold becomes due within `MINOR_MAX_WAIT`, so a fixed 10 s sleep is precise enough.

The pass limit keeps one slow table from holding up the rest: three tables with several minutes of work each would otherwise make small busy tables wait for all of them.

**Table leases.** A table runs at most one job at a time, across all tiers and jobs: merges, the data-retention `DELETE` and the delete-file rewrite. Each job takes the table's lease (an in-process set of busy tables) before it starts and releases it when it commits or fails. Tier loops skip a table whose lease is taken. Retention waits for it, and while it waits the loops skip that table too, so a busy table cannot keep the lease from it. The delete-file rewrite skips a held table, so one long merge does not hold up expiry and cleanup behind it; after `REWRITE_SKIPS_BEFORE_WAIT` (3) skipped runs in a row it waits for the table like retention does. Leases live in one process, so Phase 1 assumes one compactor process per catalog. A merge and a retention `DELETE` on the same files would conflict without the lease; with it, the compactor's own jobs never race.

**Retention and housekeeping.** The data-retention job and the delete-file rewrite run per table under the lease, on their existing timers. The rewrite calls `ducklake_rewrite_data_files` on every live table, not only those with delete files: deletes can also be inlined in the catalog, and they count toward the threshold too. A per-table call that rewrites nothing commits no snapshot. Snapshot expiry and orphan-file cleanup do not touch live files and stay catalog-wide. Every table call commits one snapshot, so expiry and cleanup must keep up: the compactor exports commits per minute and alerts when unexpired snapshots keep growing.

**Discovery query.** One round trip per pass over the catalog metadata, grouping files exactly as `ducklake_merge_adjacent_files` does: by table, schema version, `partition_id` and partition values. It reads `ducklake_data_file` (live files in the band), `ducklake_file_partition_value` (partition values), `ducklake_schema_versions` (the schema version in force at each file's `begin_snapshot`, as DuckLake reads it), `ducklake_delete_file` (DuckLake skips a file with any delete file), `ducklake_metadata` (`auto_compact`) and `ducklake_sort_info` with `end_snapshot IS NULL` (sort key). File ages come from `ducklake_snapshot.snapshot_time` of each file's `begin_snapshot`; a file whose first snapshot has been expired is at least as old as the oldest snapshot left, so that is its age. Files whose deletes are still inlined in the catalog are counted, though DuckLake skips them; the worst that costs is a call that merges nothing. `auto_compact` can be set for the catalog, a schema or a table, and the query applies the narrowest one, as DuckLake does (only the value `'true'` enables merging), so it never lists a table the call would skip. It lists tables with at least 2 eligible files in one group (for minor, the admission threshold), `auto_compact` not false, and not in back-off. Per table it returns files and bytes waiting (in groups of at least 2), the oldest waiting file's time, busy partitions (groups with a file committed in the last 10 min) and the sort-key flag. The oldest waiting file is each group's second-oldest, for the same reason as the admission threshold: a group's oldest is usually the leftover, and timing it would report a healthy table as stale.

**Failures.** A failure costs one table's call and nothing else. The table is retried on the next pass. After 3 failures in a row it backs off for 15 min and raises an alert. A conflict or other failure does not change the table's cap. A call the catalog's idle-in-transaction timeout killed does: the per-file rates behind its sizing were too fast for this table, so its cap is halved, down to 1. Each call that commits doubles the limit again, but never back up to a cap the timeout has killed: a table whose real rate is slower than the constants settles below it, instead of alternating between a killed call and a committed one. When the timeout kills a call of a single group, one group is itself too slow, so the table's files per group are halved instead (down to 2), through a lower session target as for groups that do not fit. These limits expire an hour after the last timeout (`LEARNED_LIMITS_EXPIRE`): a busy table never leaves the discovery list, so one slow spell on the catalog would otherwise shrink its calls until a restart. A timeout after that learns them again. They are also forgotten when the table leaves the discovery list. A conflict between the compactor's own jobs cannot happen under the leases, so a conflict means another writer; tables that other writers rewrite continuously get `auto_compact = false`. A table that leaves the discovery list (dropped, or nothing left to merge) is forgotten along with its failure count.

**Connections.** A failure that can leave the tier's connection unusable makes the loop reopen it on the next pass and carry the rest of the pass over: a lost catalog connection, a killed transaction, or an INTERNAL or FATAL DuckDB error, which invalidates the DuckDB instance.

**Memory hygiene.** Most compactor memory is native `malloc` memory outside DuckDB's `memory_limit` (926 MiB of a 1.25 GiB process in 28 glibc arenas; Java heap 56 MiB). `MALLOC_ARENA_MAX=2` bounds it, and container memory stays well above `memory_limit`. DuckDB's bundled allocator can also keep memory a merge freed: with its defaults (no background thread, a 128 MiB flush threshold), an object-store workload saw RSS grow by about 0.3 MiB per merged input file and stay after the commit, until a 4 GiB container was OOM-killed after a handful of calls. Every instance therefore runs with `allocator_background_threads = true` and `allocator_flush_threshold = '64MB'`, which an instance's own settings can override. The per-file costs above are one group's peak, measured locally; each call records RSS before and after, and the tier exports what its recent calls left behind per input file (`rss_growth_per_file`), to calibrate them against. RSS is the whole process's, so tiers merging at the same time add to each other's figure: it is an average over the tier's last calls, best read when one tier is busy and, in Phase 2, to admit calls on measured rather than assumed cost.

**Catalog options load once.** `set_option` changes (`target_file_size`, `auto_compact`) are read only at attach, so they need a restart. The session target does not depend on them.

**Telemetry and alerts.** Each call records table, tier, cap, duration, files in and out (from the merge's result row, which has no byte counts), bytes waiting before the call (from the discovery query) and the outcome. `/health` shows each tier's state (STARTING, RUNNING, REFUSED with its reason, STOPPED), pass, queue, carry-over, back-offs, starved, stale and oversized tables, tables merged with a lowered target, threads, the timeout it sizes calls by, and `duckdb_memory()`. A success is a call that completed without an error, recorded per database and per tier; a pass with nothing to do counts toward the pass alert only. Alerts: a tier that did not start; no completed pass for a tier in 15 min, which is above the longest pass, `PASS_TIME_LIMIT` plus one call sized to at most `CALL_TIME_LIMIT` (a pass that finds no work counts, so an idle tier does not alert); 3 consecutive failures for a table; starvation for a table; an oversized table; small-file count rising for 30 min; a table's oldest small file older than 30 min; unexpired snapshots growing across housekeeping runs; RSS above 75% of the container limit.

**Phase 1 checklist** (all done in PR #542)

- [x] Tier config: `target_file_size` (required), static cap, `cpu_share`, `pass_time_limit`, `idle_sleep_max`, minor's `min_files` and `max_wait`, `max_files_per_call`
- [x] Startup checks: bands do not overlap, every tier has a target at least its band max, CPU shares add up to at most 1, and the catalog's idle-in-transaction timeout is read and, when set, fits at least one full group per tier
- [x] Threads and memory per instance from `cpu_share`, `Runtime.availableProcessors()` and the number of databases; `MALLOC_ARENA_MAX=2`
- [x] Per-table call with `SET SESSION ducklake_target_file_size` before every call, and the cap from Call sizing (static, file and time caps with the busy-partition floor)
- [x] Discovery query with the minor admission threshold, busy partitions and sort-key flag
- [x] Loop per tier: refresh → rank → call in turn, pass time limit with carry-over, idle sleep
- [x] Table leases shared by merges, retention and the delete-file rewrite
- [x] Retry, then back-off after 3 failures
- [x] Per-call telemetry, `/health` and the alerts above

## Phase 1 in the compactor code

Before Phase 1, each enabled tier ran on a fixed-rate timer per database and issued one catalog-wide `CALL ducklake_merge_adjacent_files(db, min_file_size, max_file_size, max_compacted_files)`, with two band-wide COUNTs around it for telemetry. Phase 1 replaces that as follows:

| Item | Where |
| --- | --- |
| Tier settings and startup checks (`target_file_size` required and at least `max_file_size`, positive static cap, `cpu_share` adding up to at most 1, `min_files` at least 2); `frequency` is no longer read | `CompactionTier`, `CompactionConfig` |
| One loop thread per (database, tier): refresh, rank, call, pass limit with carry-over, wait, retry and back-off, cap halving after a timeout, oversized tables, published status | `TierLoop` |
| Opening a tier's instance: threads and `memory_limit`, `SET SESSION ducklake_target_file_size` read back in bytes (a mismatch refuses the tier), the catalog's timeout from `pg_settings` and the full-group check; the discovery query; the per-table call | `DuckDbTierCompactor` |
| The cap of Call sizing, the full-group and per-table group checks | `CallSizing` |
| Table leases | `TableLeases`, taken by `TierLoop`, `DuckLakeDataRetention` and `DuckLakeHousekeeper` |
| Per-table delete-file rewrite; unexpired snapshots counted after expiry | `DuckLakeHousekeeper` |
| `PLANNING` failure class | `CompactionRun.classify` |
| One telemetry record per call (table, cap, files and bytes waiting, files in and out); per-tier and per-table metrics; `/health` loop status and alerts; `/ui` | `CompactionRun`, `CompactionRunLog`, `CompactionState`, `CompactionService`, `HealthServer`, `CompactionHtml` |
| `MALLOC_ARENA_MAX=2` | the compactor's Jib image and `Dockerfile.native` |

**Reading the catalog's timeout.** It is read from Postgres's `pg_settings` through `postgres_query` on the metadata catalog. The connection that runs it carries the ATTACH string's options, so an `options=-cidle_in_transaction_session_timeout=600000` there counts. `SHOW` cannot be used, because `postgres_query` wraps its query in `COPY (SELECT ...)`. The metadata catalog is hidden from `duckdb_databases()`, so the backend is recognised by the DuckLake catalog's own `path`, which starts with `postgres:`; a catalog attached through a secret shows its resolved path too. If a Postgres catalog's value cannot be read, a timeout set in the tier's `connection_settings` is used. Without one, the tier retries opening its connection rather than size calls as if there were no limit.

**Merged outputs that stay in their band.** A group stops on its *input* bytes, once they reach the target, and its output can come out smaller than its inputs, so an output may land back in its own band and be merged again later. Every tier sees this:

- **Major and mega** (target = band max): outputs come out a little smaller than their inputs and may land just under the band max. Such an output merges with the next file in its group and leaves the band. A test sees two of them converge in 2 passes, leaving at most one file in the band: the leftover rule above, at the cost of at most one extra rewrite. A target slightly above the band max would avoid that rewrite.
- **Minor** (target 16 MB, band up to 8 MB): merged tiny files compress several-fold (3,000 files, 16 MB in, 139 KB out), so minor outputs usually land back in the minor band and are merged again as more files arrive, until one reaches 8 MB. Raising minor's target would not help much: its outputs grow by what each merge adds, not by the target. The minor admission threshold keeps how often this happens in check.
- **Calls with a lowered target** compress the same way, so their outputs land far below the lowered target. Grouped with the next tiny files on the next pass, the same output would be rewritten pass after pass. Such a call therefore also runs with `max_file_size` at half its lowered target: an output of that size or more is left for when the table can be merged at the tier's own target.

**Gaps found in the current code.** `ResourceSampler.rssPeakBytes()` reads `VmHWM`, the process's lifetime high-water mark. It only rises, so it cannot measure one call; per-call memory needs DuckDB's query profiler or `duckdb_memory()` sampled during the call. It matters for Phase 2's reservations, not for Phase 1.

## Phase 2: parallel scheduling

Phase 2 uses more cores through concurrent calls on different tables. The two limits on the unit of work stay as they are.

- **Worker pool:** up to K workers per tier, each on its own connection to the tier's DuckDB instance, so all calls share one `memory_limit` and thread pool. K starts at 2 and is at most the tier's CPUs. A table is never split across workers; the lease already prevents two calls on one table.
- **Admission:** a call reserves its estimated peak memory (Call sizing) and about 1 core (2 for a sorted table with full groups); it starts only when both fit in what running calls leave. Reservations are estimates, not measured RSS: freed memory stays in glibc arenas, and RSS cannot be split between concurrent calls.
- **Solo tables:** a call that runs long while others run beside it marks its table solo; a solo table runs alone until 3 solo successes.
- **Adaptive K:** lower K by 1 when parallel calls run more than 1.5× slower than solo, raise it by 1, up to the tier's CPUs, after 10 passes without a slow call.
- **Learned constants:** per table, learn `sec_per_file` (minor) and throughput (major, mega) as a moving average, to size reservations and to replace `MAX_FILES_PER_CALL` with a per-table limit.
- **Instance recycling:** reattach a tier's instance when RSS is above 60% of the container limit, once its workers are idle; a reattach costs about 1 s.

## Phase 3 (optional): learned time-based caps

If the fixed per-file rates prove too blunt, learn `group_time` per table, adjusted on time failures, and use it in the time cap in place of the constant `sec_per_file` and of `MAX_FILES_PER_CALL`. The cap is still never above the static cap. Two starting values for that model are guesses: `sec_per_file` for unsorted tables (0.09 s, 2× sorted) and throughput for major and mega (50 MB/s).

## Constants

| Constant | Value | Source |
| --- | --- | --- |
| Static caps (minor, major, mega) | 12, 16, 5 | About 1 GB of input per call at most; the time cap may lower them further |
| sec_per_file, sorted / unsorted | 0.045 / 0.09 s | Sorted measured (~22 files/s, 4 threads); unsorted a guess at 2× sorted. Phase 1 uses these in the time cap; Phase 2 learns them per table |
| IDLE_TXN_TIMEOUT | read when a tier's connection opens | The catalog database's idle-in-transaction timeout, from `pg_settings`. A tier whose single full group of its smallest files would not fit in 80% of it, at the sorted rate, does not start. A value of 0 (Postgres's default, disabled) or a catalog without the setting (SQLite, a DuckDB file) means no timeout limit: `CALL_TIME_LIMIT` and the static and file caps still apply |
| CALL_TIME_LIMIT | 10 min | The longest a call is sized to run, so one call never holds a tier's loop longer; below the 15-min pass alert |
| MAX_FILES_PER_CALL | 6,000 | About 4.5 min at the measured sorted rate (0.045 s per file, 4 threads); about 9 min if unsorted tables take twice as long (a guess) |
| MINOR_MIN_FILES | 4 | Proposed; each waiting small file is one more file a query opens |
| MINOR_MAX_WAIT | 5 min | Proposed; the longest a small file waits before it is merged |
| PASS_TIME_LIMIT | 2 min | Proposed; after it, no new call starts until the next refresh |
| MIN_REFRESH_INTERVAL | 5 s | The least time between two refreshes of a tier for one database, even after a pass that merged |
| IDLE_SLEEP_MAX | 10 s | The sleep after a pass that merged nothing |
| Back-off | 3 failures in a row, 15 min | A timed-out call also halves the table's cap, down to 1; commits double it again, up to below the smallest cap the timeout killed |
| MEMORY_SHARE | 50% | Share of the process's memory the tier instances get together, split by `cpu_share` and database, when `connection_settings` set no `memory_limit`; housekeeping 8%, retention 8%, file counts 2% |
| LEARNED_LIMITS_EXPIRE | 1 hour | After the last timeout, a table's learnt cap and files-per-group limits are forgotten |
| Allocator | background threads, 64 MB flush threshold | Set on every DuckDB instance, so memory a merge freed is returned between calls |
| MEMORY_LIMIT_SHARE | 80% | Share of an instance's `memory_limit` one group is sized to use, at the per-file cost |
| REWRITE_SKIPS_BEFORE_WAIT | 3 | Housekeeping runs that may skip a held table before the rewrite waits for it |
| THREADS_PER_CPU | 2 | Proposed; threads mostly wait on object-store reads |
| per_file_cost, sorted / unsorted | 0.12 / 0.85 MiB | Measured, 1,000 small files |
| MEM_BUDGET (minor, Phase 2) | 1 GiB | Example: minor `memory_limit` with a 4 GiB container limit |
| K (Phase 2) | starts at 2, at most the tier's CPUs | Also bounds catalog connections, request rate and commit retries |

## Decisions

- Every call runs with the tier's `ducklake_target_file_size` set in its session and a cap never above the tier's groups per call; nothing raises either limit.
- Default band edges are 8 MB / 64 MB / 256 MB.
- Recommend a sort key on every table, so its merges take the cheaper sorted path (0.12 vs 0.85 MiB per file).
- Mega is enabled by default.
- Phase 1 sizes calls with fixed limits (static cap, files per call, busy-partition floor); learning waits for Phase 2 or 3.
- DuckDB threads per instance are twice its CPUs, with `cpu_share` a budget for the whole process; more cores are used through more concurrent table calls, not more groups per call.
