# Dynamic Compaction Spec

Minor compaction must never get stuck: every unit of work should be small, bounded in memory and time, and limited to one table if it fails. In one observed failure it went 22 hours without a single successful run while the small-file backlog grew to about 98k files.

## Problem and goals

| Observed failure | Cause | Rule adopted |
| --- | --- | --- |
| OOM-killed every ~14 min, even at double the memory | Groups grew to the 512 MB default target: one partition = one 33k-file merge | Always set the target; limit files per call, not only bytes |
| Commit failed after 8 min (TRANSACTION_CONFLICT) | One catalog-wide transaction raced tables other writers rewrite every few seconds | One short transaction per table |
| Planning error failed the whole cycle | One table's internal error (hive path) aborted every table | Isolate failures per table |
| Backlog grew with a cap of 1 | DuckLake takes partitions in a fixed order, so a busy partition took every slot | Keep the cap above busy partitions, within the static cap |
| 12–18 s per run even for 3 files | Every run re-plans every small file | Plan only tables with work; wait when idle |
| RSS kept climbing | glibc arenas keep freed memory, outside DuckDB's `memory_limit` | `MALLOC_ARENA_MAX=2` |

**Goals:** each table call takes seconds in steady state and minutes at most during a backlog, peak memory stays under a fixed budget, one table's failure never discards another's work, and the backlog drains without manual tuning. **Non-goals:** changing DuckLake, and changing how writers size their files.

## The unit of work

The unit of work is one table call: one `ducklake_merge_adjacent_files` call on one table, in one transaction, with one commit. Two limits bound every call, and the compactor never breaks either of them:

1. **`ducklake_target_file_size`** is set with `SET SESSION` on the connection before every call, to the tier's target. It bounds each group, so it bounds the call's peak memory and each output file. The session setting overrides table and catalog options, so no table can fall back to DuckLake's 512 MB default. The compactor refuses to start a tier without a target, and refuses a call if the setting is not in place.
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

**Minor admission threshold.** Because any two files merge, a slow table would merge one new tiny file into its leftover on almost every pass, rewriting the leftover and committing a snapshot every few seconds until it reaches 8 MB. So minor admits a table when one of its groups has at least `MINOR_MIN_FILES` (4) eligible files, or its oldest eligible file reaches `MINOR_MAX_WAIT` (5 min), so no small file waits longer than that. Both stay low because every small file waiting is one more file each query on that partition has to open. Major and mega keep the 2-file trigger: their inputs arrive slowly and are already large.

## Call sizing

**How one table call works.** `ducklake_merge_adjacent_files` on one table:

1. Lists the table's files in the band that carry no deletes, and splits them by partition and schema version.
2. Within each partition, walks the files in order and cuts them into groups: a group keeps adding files until its total reaches the target. Each group becomes one output file.
3. Stops after `max_compacted_files` groups (the cap), counted across all of the table's partitions; a partition's leftover single file also uses a slot without merging anything.
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
cap             = min(static_cap, max(file_cap, busy_partitions + 1))
```

- In steady state groups are a few files and the cap is the static cap (12 for minor).
- During a minor backlog of 5 KB files the file cap is 1 or 2, so a call merges one or two full groups and commits in minutes.
- The cap is never above the static cap, so `ducklake_target_file_size` and groups per call always hold.

**Busy partitions and starvation.** DuckLake v1.5 picks partitions in a fixed order and a busy partition always has work, so a cap at or below the number of busy partitions can give every slot to the same partitions on every call: the table's other partitions never compact. So, within the static cap, the cap is at least busy partitions + 1, even when that goes past the file limit. When busy partitions + 1 is above the static cap, the static cap still wins and the table raises a starvation alert. Minor's static cap is 12 so that even this worst case, 12 full groups of tiny files at about 144 s each, stays near 29 minutes, under a 30-minute idle-in-transaction timeout. The real fix is upstream: a partition filter or oldest-first groups in DuckLake.

**Memory.** A call's peak memory is one group: files per group × a per-file cost. That cost is 0.12 MiB for a table with a sort key and 0.85 MiB without one (measured, 1,000 small files), so a full minor group of tiny files needs about 385 MiB sorted but about 2.7 GiB unsorted. DuckLake cannot limit files per group, so unsorted tables with tiny files need a sort key or a larger `memory_limit`; the discovery query flags tables without a sort key.

## Threads and cores

One table call uses about 1 to 2 cores, whatever its thread count, because its groups run one after another and each group writes its Parquet file on one thread. Extra threads read and decode a group's input files, and sort them for a sorted table, in parallel. Measured on local disk, one call merging 16 groups:

| DuckDB threads | Call time, unsorted | Average cores in use, unsorted | Average cores in use, sorted |
| --- | --- | --- | --- |
| 1 | 1.4 s | 0.6 | 0.7 |
| 2 | 1.0 s | 1.0 | 1.1 |
| 4 | 0.8 s | 1.5 | 1.7 |
| 8 | 0.7 s | 1.9 | 2.1 |

With one thread the call used only 0.6 of a core: the rest was I/O wait, which is far longer on an object store. So:

- **Threads per tier** are set at startup to `THREADS_PER_CPU` (2) × the tier's CPUs, not fixed in `connection_settings`. The tier's CPUs are its `cpu_share` of the CPUs available to the process, which Java's `Runtime.availableProcessors()` reads from the container limit. Each tier has its own DuckDB instance, so the shares must add up to at most 1 (for example minor 0.5, major 0.25, mega 0.25).
- **More cores only help through more concurrent calls** on different tables (Phase 2). More groups per call do not run in parallel.

## Phase 1: per-table scheduler

Phase 1 replaces the catalog-wide call with per-table calls run one at a time per tier. It fixes every observed failure without learning or concurrency.

**The loop.** One loop thread per (database, tier):

1. **Refresh.** One discovery query lists the tables with work (below).
2. **Rank** by outstanding files, most first; ties go to the older oldest file. Tables carried over from the previous pass go first.
3. **Call** each table in turn: take the table's lease, `SET SESSION ducklake_target_file_size`, compute the cap (Call sizing), run `CALL ducklake_merge_adjacent_files(catalog, table, schema := …, min_file_size := …, max_file_size := …, max_compacted_files := cap)`, commit, release the lease, and record the outcome. A table whose lease is held elsewhere is skipped this pass.
4. **End the pass** when every listed table has been called or `PASS_TIME_LIMIT` (2 min) has passed. A running call finishes; tables not yet called carry over. If the refresh found no work, sleep up to 10 s, since writers may commit new files every few seconds.

The pass limit keeps one slow table from holding up the rest: three tables with several minutes of work each would otherwise make small busy tables wait for all of them.

**Table leases.** A table runs at most one job at a time, across all tiers and jobs: merges, the data-retention `DELETE` and the delete-file rewrite. Each job takes the table's lease (an in-process set of busy tables) before it starts and releases it when it commits or fails. A merge and a retention `DELETE` on the same files would otherwise conflict, and two jobs of the compactor's own then never race.

**Retention and housekeeping.** The data-retention job and the delete-file rewrite run per table under the lease, on their existing timers. Snapshot expiry and orphan-file cleanup do not touch live files and stay catalog-wide. Every table call commits one snapshot, so expiry and cleanup must keep up: the compactor exports commits per minute and alerts when unexpired snapshots keep growing.

**Discovery query.** One round trip per pass over the catalog metadata: `ducklake_data_file` (live files in the band), `ducklake_file_partition_value` (partition grouping), `ducklake_snapshot.schema_version` through `begin_snapshot` (schema grouping), `ducklake_delete_file` (skip files with live deletes), `ducklake_metadata` (`auto_compact` per table) and `ducklake_sort_info` with `end_snapshot IS NULL` (sort key). It lists tables with at least 2 eligible files in one group (for minor, the admission threshold), `auto_compact` not false, and not in back-off. Per table it returns files and bytes waiting, average file size, the oldest file's time, busy partitions (files committed in the last 10 min) and the sort-key flag.

**Failures.** A failure costs one table's call and nothing else. The table is retried on the next pass with the same cap. After 3 failures in a row it backs off for 15 min and raises an alert. A conflict between the compactor's own jobs cannot happen under the leases; if one does, it is logged as a bug. Tables that other writers rewrite continuously get `auto_compact = false`.

**Memory hygiene.** Most compactor memory is native `malloc` memory outside DuckDB's `memory_limit` (926 MiB of a 1.25 GiB process in 28 glibc arenas; Java heap 56 MiB). `MALLOC_ARENA_MAX=2` bounds it, and container memory stays well above `memory_limit`.

**Catalog options load once.** `set_option` changes (`target_file_size`, `auto_compact`) are read only at attach, so they need a restart. The session target does not depend on them.

**Telemetry and alerts.** Each call records table, tier, cap, duration, files and bytes in and out (from the merge's result row) and the outcome. `/health` shows each tier's pass, queue, carry-over and back-offs, and `duckdb_memory()`. Alerts: minor `last_success_age` over 15 min; 3 consecutive failures for a table; starvation for a table; small-file count rising for 30 min; a table's oldest small file older than 30 min; unexpired snapshots growing across housekeeping runs; RSS above 75% of the container limit.

**Phase 1 checklist**

- [ ] Tier config: `target_file_size` (required), static cap, `cpu_share`, `pass_time_limit`, `idle_sleep_max`, minor's `min_files` and `max_wait`, `max_files_per_call`
- [ ] Startup checks: bands do not overlap, every tier has a target at least its band max, CPU shares add up to at most 1
- [ ] Threads per tier from `cpu_share` and `Runtime.availableProcessors()`; `MALLOC_ARENA_MAX=2`
- [ ] Per-table call with `SET SESSION ducklake_target_file_size` before every call and the cap never above the static cap
- [ ] Discovery query with the minor admission threshold, busy partitions and sort-key flag
- [ ] Loop per tier: refresh → rank → call in turn, pass time limit with carry-over, idle sleep
- [ ] Table leases shared by merges, retention and the delete-file rewrite
- [ ] Retry, then back-off after 3 failures
- [ ] Per-call telemetry, `/health` and the alerts above

## Phase 1 in the compactor code

Today each enabled tier runs on a fixed-rate timer per database (`CompactionService.start`, `scheduleAtFixedRate(runTier, frequency)`). `runTier` counts the band's files across the catalog, calls `TierCompactor.compact(database, tier)`, counts again and records a `CompactionRun`. `DuckDbTierCompactor` issues one catalog-wide `CALL ducklake_merge_adjacent_files(db, min_file_size, max_file_size, max_compacted_files)` on one raw DuckDB connection per (database, tier), opened with the tier's `connection_settings`. Housekeeping (`DuckLakeHousekeeper`) and data retention (`DuckLakeDataRetention`) run on their own fixed-delay timers.

| Item | Today | Change |
| --- | --- | --- |
| Configuration | Per-tier `connection_settings` (including `threads`), `max_compacted_files`, `frequency` | Add a required `target_file_size`, `cpu_share`, `pass_time_limit`, `idle_sleep_max`, `max_files_per_call`, and minor's `min_files` and `max_wait`; `max_compacted_files` becomes the static cap; set `threads` from `cpu_share`; `frequency` stops being a timer |
| Startup checks | Band overlap is checked in `CompactionConfig` | Also require `target_file_size` of at least `max_file_size` on every tier, and CPU shares adding up to at most 1 |
| Per-table call | Catalog-wide call | `compact(database, tier, schema, table, cap)`: `SET SESSION ducklake_target_file_size`, then `CALL ducklake_merge_adjacent_files(db, table, schema := …, min_file_size := …, max_file_size := …, max_compacted_files := cap)`; DuckLake still honors `auto_compact = false` on a per-table call |
| Discovery query | `queryActiveFiles`: one band-wide COUNT and SUM, nothing per table | New query on the catalog metadata |
| Loop | Fixed-rate timer per (database, tier) | One loop thread per (database, tier) on the existing connection |
| Leases | None | An in-process set of busy tables, taken by the tier loops, `DuckLakeDataRetention` and the per-table delete-file rewrite |
| Housekeeping | Catalog-wide `ducklake_rewrite_data_files(db)`, then expiry and cleanup | Rewrite per table under the lease; expiry and cleanup unchanged |
| Failures | `CompactionRun.classify` yields `COMMIT_TIMEOUT`, `TRANSACTION_CONFLICT`, `OUT_OF_MEMORY`, `SPILL_EXCEEDED`, `CATALOG_UNAVAILABLE`, `OTHER` | Keep the classes for telemetry and add `PLANNING`; per-table consecutive-failure count and back-off |
| Telemetry | `CompactionRun` per tier, plus two band-wide COUNTs around every call | Add table and cap; take files from the merge's result row and drop the COUNTs; add a table dimension to `CompactionRunLog`, `/ui` and the exported metrics |

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

If the file limit proves too blunt, replace it with a time-based cap, still never above the static cap: `cap = min(static_cap, max(floor(CALL_TIME_LIMIT ÷ group_time), busy_partitions + 1))`, with `group_time` learned per table and adjusted on time failures. Two starting values for that model are guesses: `sec_per_file` for unsorted tables (0.09 s, 2× sorted) and throughput for major and mega (50 MB/s).

## Constants

| Constant | Value | Source |
| --- | --- | --- |
| Static caps (minor, major, mega) | 12, 16, 5 | About 1 GB of input per call at most; minor's 12 full groups of tiny files stay under a 30-min idle-in-transaction timeout |
| MAX_FILES_PER_CALL | 6,000 | About 4.5 min at the measured sorted rate (0.045 s per file, 4 threads); about 9 min if unsorted tables take twice as long (a guess) |
| MINOR_MIN_FILES | 4 | Proposed; each waiting small file is one more file a query opens |
| MINOR_MAX_WAIT | 5 min | Proposed; the longest a small file waits before it is merged |
| PASS_TIME_LIMIT | 2 min | Proposed; after it, no new call starts until the next refresh |
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
- DuckDB threads per tier are twice the tier's CPUs; more cores are used through more concurrent table calls, not more groups per call.
