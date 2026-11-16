# Dynamic Compaction Spec

Minor compaction must never get stuck: every batch should be small, bounded in memory and time, and limited to one table if it fails. In one observed failure it went 22 hours without a single successful run while the small-file backlog grew to about 98k files.

## Problem and goals

| Observed failure | Cause | Rule adopted |
| --- | --- | --- |
| OOM-killed every ~14 min, even at double the memory | Groups grew to the 512 MB default target: one partition = one 33k-file merge | Limit files per batch, not only bytes |
| Commit failed after 8 min (TRANSACTION_CONFLICT) | One catalog-wide transaction raced tables other writers rewrite every few seconds | One short transaction per table |
| Planning error failed the whole cycle | One table's internal error (hive path) aborted every table | Isolate failures per table |
| Backlog grew with a cap of 1 | DuckLake takes partitions in a fixed order, so a busy partition took every slot | Keep the cap above busy partitions |
| 12–18 s per run even for 3 files | Every run re-plans every small file | Plan only tables with work; wait when idle |
| RSS kept climbing | glibc arenas keep freed memory, outside DuckDB's `memory_limit` | `MALLOC_ARENA_MAX=2`; recycle the instance |

**Goals:** each table call takes seconds in steady state and about 10 min at most during a backlog, peak memory stays under a fixed budget, one table's failure never discards another's work, and the backlog drains without manual tuning. **Non-goals:** changing DuckLake, and changing how writers size their files.

## Tier chain and band edges

Bands are contiguous, so every file belongs to one tier and outputs move up the chain. Files of 256 MB or more are final.

| Tier | Input band [min, max) | Target (`ducklake_target_file_size`) | Groups per table per call |
| --- | --- | --- | --- |
| minor | [0, 8 MB) | 16 MB | 16 |
| major | [8 MB, 64 MB) | 64 MB | 16 |
| mega | [64 MB, 256 MB) | 256 MB | 5 |

These static caps keep each call near 1 GB of input (major 16 × 64 MB, mega 5 × 256 MB), a few minutes at most. A backlog drains over consecutive passes, each committing its own batch, and the time-based cap lowers it while groups are full.

- The target is a session setting on each tier's own DuckDB instance. It overrides table and catalog options and does not affect writers or other tiers.
- Startup checks: bands must not overlap, and each target must be at least its band max. With a lower target, DuckLake skips in-band files at or above it, and they never merge.
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

So a group's size sets the call's peak memory (one group is in memory at a time) and that group's time, while the number of groups multiplies the time. The transaction stays open for all of it:

```text
call_time ≈ groups × (files_per_group × sec_per_file + bytes_per_group ÷ throughput)
```

For example, 4 full minor groups of 3,200 files at 0.045 s per file take about 4 × 144 s ≈ 10 min, while 16 groups of 3 files take about 2 s.

Memory is bounded statically by the bands and targets; time is not, so the cap on groups comes from a time limit.

| Quantity | Formula | Worst case (minor, 5 KB files) |
| --- | --- | --- |
| Files per group | min(target ÷ average input size, files waiting) | 16 MB ÷ 5 KB ≈ 3,200 |
| Call peak memory | files per group × per_file_cost | ≈ 385 MiB sorted; ≈ 2.7 GiB unsorted |
| Group time | files per group × sec_per_file + bytes per group ÷ throughput | ≈ 144 s for a full minor group |

```text
time_cap = clamp(floor(CALL_TIME_LIMIT ÷ group_time), 1, static_cap)
cap      = max(time_cap, busy_partitions + 1)   while that call's estimate stays under IDLE_TXN_TIMEOUT
```

In steady state a group takes well under a second and the cap is the static cap (16); with full minor groups the time cap is 4, with half-full groups 8. The bytes term matters for major and mega, whose groups are a few large files.

**Busy-partition floor.** DuckLake v1.5 picks partitions in a fixed order and a busy partition always has work, so a cap at or below the number of busy partitions can give every slot to the same partitions on every call: the table's other partitions never compact. That is starvation, not slower rotation. So the cap never drops below busy partitions + 1, even when that runs past CALL_TIME_LIMIT, as long as the estimate stays under the catalog's idle-in-transaction timeout. If even that does not fit, the time cap applies and the table raises a starvation alert. The real fix is upstream: a partition filter or oldest-first groups in DuckLake.

**Learning.** sec_per_file and throughput start from the constants below, chosen by whether the table has a sort key, then are learned per table as a moving average over successful calls. One duration cannot fit both terms, so minor learns sec_per_file (its bytes term is negligible) and major and mega learn throughput (their file term is negligible). Calls that ran beside others are recorded apart from solo calls, so the master can see what concurrency costs. Two starting values are guesses, not measurements: sec_per_file for unsorted tables and throughput for major and mega. The first caps of unsorted tables and of every major and mega table come from them, so those first calls may run long or short until learning corrects them.

| Constant | Value | Source |
| --- | --- | --- |
| CALL_TIME_LIMIT | 10 min | A third of IDLE_TXN_TIMEOUT |
| IDLE_TXN_TIMEOUT | 30 min | The catalog database's idle-in-transaction timeout in the measured setup |
| per_file_cost, sorted table | 0.12 MiB | Measured, 1,000 small files |
| per_file_cost, unsorted table | 0.85 MiB | Measured, same files without a sort |
| sec_per_file, sorted table | 0.045 s (4 threads) | Measured, ~22 files/s |
| sec_per_file, unsorted table | 0.09 s | **Guess**: 2× sorted |
| throughput (major, mega) | 50 MB/s | **Guess** |
| MINOR_MIN_FILES | 4 | Proposed; each waiting small file is one more file a query opens |
| MINOR_MAX_WAIT | 5 min | Proposed; the longest a small file waits before it is merged |
| PASS_TIME_LIMIT | 2 min | Proposed; after it, no new call starts until the next refresh |
| THREADS_PER_CPU | 2 | Proposed; threads mostly wait on object-store reads |
| MEM_BUDGET (minor) | 1 GiB | Example: minor `memory_limit` with a 4 GiB container limit |
| K (concurrent table calls per tier) | starts at 2, at most the tier's CPUs | Also bounds catalog connections, request rate and commit retries |

## Execution model

A master scheduler per tier dispatches table calls to a pool of up to K workers. Each worker holds its own connection to the tier's single DuckDB instance, so all calls share one `memory_limit` and thread pool. The unit of work is one table: one call, one transaction, one commit.

1. **Refresh.** One catalog query lists the tables with work: at least 2 files in the tier's band in one partition and schema version (for minor, the admission threshold), with no delete files (DuckLake's own grouping), `auto_compact` not false, and not in back-off. For each it returns the files and bytes waiting, their average size, the oldest file's age, the busy partitions (written in the last 10 min), and whether the table has a sort key.
2. **Rank** by outstanding files, most first; ties go to the older oldest file. Tables carried over from the previous pass go first.
3. **Dispatch.** Compute the table's cap (Call sizing) and estimate the call's peak memory and cores (Threads and cores). Start it on a free worker if both fit in what is left (MEM_BUDGET minus the reservations of running calls, the tier's cores minus theirs), no solo table is running, and no retention or housekeeping job holds the table. A solo table starts only when no other call runs. Otherwise wait for a call to finish, release its reservation, and re-check.
4. **Run**, on the worker: `SET SESSION ducklake_target_file_size` (workers share one instance), then `CALL ducklake_merge_adjacent_files(catalog, table, min_file_size := …, max_file_size := …, max_compacted_files := cap)`, commit, and record duration, peak memory, files and bytes in and out, whether other calls ran alongside, and the outcome.
5. **Next pass.** A pass ends when every listed table has run or PASS_TIME_LIMIT has passed; running calls finish, and tables not yet called carry over to the next pass. Then refresh and start again. If the refresh finds no work, sleep up to 10 s: writers may commit new files every few seconds.

The pass limit keeps one slow table from holding up the rest: with one worker and three tables at about 10 min each, an unbounded pass would last 30 min while small busy tables wait.

**Threads and cores.** One table call uses about 1 to 2 cores, whatever its thread count, because its groups run one after another and each group writes its Parquet file on one thread. Extra threads read and decode a group's input files, and sort them for a sorted table, in parallel. Measured on local disk, one call merging 16 groups:

| DuckDB threads | Call time, unsorted | Average cores in use, unsorted | Average cores in use, sorted |
| --- | --- | --- | --- |
| 1 | 1.4 s | 0.6 | 0.7 |
| 2 | 1.0 s | 1.0 | 1.1 |
| 4 | 0.8 s | 1.5 | 1.7 |
| 8 | 0.7 s | 1.9 | 2.1 |

With one thread the call used only 0.6 of a core: the rest was I/O wait, which is far longer on an object store. So:

- **Threads per tier** are set at startup to THREADS_PER_CPU × the tier's CPUs, not fixed in `connection_settings`. The tier's CPUs are its `cpu_share` of the CPUs available to the process, which Java's `Runtime.availableProcessors()` reads from the container limit. Each tier has its own DuckDB instance, so the shares must add up to at most 1 (for example minor 0.5, major 0.25, mega 0.25).
- **A call reserves 1 core**, or 2 for a sorted table with full groups, not the tier's thread count.
- **More cores only help through more concurrent calls** on different tables (K workers). Two calls on the same table would conflict at commit, and more groups per call do not run in parallel.

**Adjusting on failure.** The master classifies each failure and changes the table's state before its next call:

| Failure | Adjustment |
| --- | --- |
| Time: the call ran over CALL_TIME_LIMIT, or the catalog ended the idle transaction | group_time estimate × 2, so the time cap halves |
| Time, while other calls ran in the tier | as above, and the table is marked solo |
| TRANSACTION_CONFLICT with another process | retry on the next pass with the same cap: a smaller batch does not avoid another writer |
| Memory: OOM, or the call's peak above its reservation | reservation × 2; an unsorted table is flagged for a sort key |
| Planning error | skip for this pass |
| 3 failures in a row, conflicts included | back off 15 min and raise an alert |

A success under half of CALL_TIME_LIMIT lets the estimate decay back toward the learned average, so caps recover after a backlog.

**Solo tables and concurrency.** A solo table runs with no other call in its tier: the master lets running calls finish, runs it alone, then resumes parallel dispatch. It returns to parallel after 3 successful solo calls. The master also compares each table's learned sec_per_file in parallel against solo: when parallel calls are more than 1.5× slower across the tier, it lowers K by 1, and raises it by 1, up to the tier's CPUs, after 10 passes without a time failure.

Reservations are estimates from Call sizing (doubled after a memory failure), not measured RSS: freed memory stays in glibc arenas, and RSS cannot be split between concurrent calls. A large table holds one worker while smaller tables keep moving, and one table is never split across workers. Calls on different tables never conflict logically; DuckLake retries a snapshot-id collision at commit.

**Why one table per call.** A catalog-wide `merge_adjacent_files` runs every table's merge in one transaction, one table after another. It adds no parallelism, and any one table's conflict or error discards all of them.

## Snapshots and the compactor's other jobs

**Snapshot churn.** Every table call commits one snapshot, so the commit rate is roughly tables with work per pass. The minor admission threshold keeps quiet tables from committing every pass. Snapshot expiry and orphan-file cleanup must keep up with that rate: the scheduler exports commits per minute, and an alert fires when unexpired snapshots keep growing across housekeeping runs.

**Retention and housekeeping.** The same compactor runs the data-retention job (a time-based `DELETE` per table) and housekeeping (`ducklake_rewrite_data_files`, then expiry and cleanup). A merge and a retention `DELETE` on the same files conflict, and the old failure rule would have read that as "another writer" and halved the cap for no reason. So both jobs go through the scheduler as per-table work: a table never has a merge and a retention or rewrite job running at the same time, and expiry and cleanup run between passes. A conflict between the compactor's own jobs should then never happen; if one does, it is logged as a bug, not counted as a table failure.

## Failure handling and memory hygiene

Each failure costs at most one table's call; the master classifies it and adjusts that table (Execution model). Tables that other writers rewrite continuously get `auto_compact = false`.

**Memory hygiene.** Most compactor memory is native `malloc` memory outside DuckDB's `memory_limit` (926 MiB of a 1.25 GiB process in 28 glibc arenas; Java heap 56 MiB). `MALLOC_ARENA_MAX=2` bounds it. Also recycle a tier's instance when RSS is above 60% of the container limit, once its workers are idle; a reattach costs about 1 s. There is no pass-count trigger: at 10 s passes, recycling every 50 passes would reattach every 8 minutes whatever the memory use.

**Catalog options load once.** `set_option` changes (`target_file_size`, `auto_compact`) are read only at attach, so they need a restart or an instance recycle.

**Alerts:** minor `last_success_age` over 15 min; 3 consecutive `FAILED` outcomes for a table; a starvation alert for a table; small-file count rising for 30 min; a table's oldest small file older than 1 hour; unexpired snapshots growing across housekeeping runs; RSS above 75% of the container limit.

## Rollout

Phase 1 ships the scheduler with one worker and the learned time-based cap. Phase 2 adds parallel dispatch, admission, solo tables and an adaptive K on top of it.

**Phase 1: configuration and scheduler**

- [ ] Configuration: minor cap 16, major cap 16, per-tier `SET SESSION ducklake_target_file_size` (16 / 64 / 256 MB), bands 8 / 64 / 256 MB, container memory well above `memory_limit`, `MALLOC_ARENA_MAX=2`, `auto_compact = false` on tables other writers rewrite continuously
- [ ] Threads per tier = THREADS_PER_CPU × the tier's `cpu_share` of the available CPUs, set at startup
- [ ] Startup checks: bands do not overlap, each target is at least its band max, and the CPU shares add up to at most 1
- [ ] The refresh flags any table without a sort key (one is recommended on every table)
- [ ] Per-table call: one table, one transaction, replacing the catalog-wide call
- [ ] Work discovery query: per tier, the tables with work, with files and bytes waiting, average size, oldest file age, busy partitions and sort key; the minor admission threshold
- [ ] Master scheduler loop (refresh → rank → dispatch → next pass) with one worker, PASS_TIME_LIMIT and carry-over
- [ ] Time-based cap with the busy-partition floor: starting constants by sort key, per-table learning, failure classification and adjustment
- [ ] Per-table state: telemetry per call, learned estimates, failure count and back-off
- [ ] Retention and housekeeping as per-table work through the scheduler, never beside a merge on the same table
- [ ] Idle sleep of at most 10 s; `duckdb_memory()` and scheduler state per tier on `/health`; the alerts above

**Phase 2: parallel scheduling** (`CompactionService` and `DuckDbTierCompactor`)

- [ ] Worker pool of up to K, each worker on its own connection to the tier's instance
- [ ] Resource accounting and admission: per-tier memory budget and cores, about 1 core per call (2 for sorted tables with full groups); reserve on dispatch, release on finish
- [ ] Solo tables: a time failure under concurrency makes the table run alone until 3 solo successes
- [ ] Adaptive K, up to the tier's CPUs: lower when parallel calls run more than 1.5× slower than solo, raise after 10 passes without a time failure
- [ ] Instance recycling when RSS is above 60% of the container limit, once workers are idle

## Phase 1 in the compactor code

Today each enabled tier runs on a fixed-rate timer per database (`CompactionService.start`, `scheduleAtFixedRate(runTier, frequency)`). `runTier` counts the band's files across the catalog, calls `TierCompactor.compact(database, tier)`, counts again and records a `CompactionRun`. `DuckDbTierCompactor` issues one catalog-wide `CALL ducklake_merge_adjacent_files(db, min_file_size, max_file_size, max_compacted_files)` on one raw DuckDB connection per (database, tier), opened with the tier's `connection_settings`. Housekeeping (`DuckLakeHousekeeper`) and data retention (`DuckLakeDataRetention`) run on their own fixed-delay timers.

| Phase 1 item | Today | Change |
| --- | --- | --- |
| Configuration | Per-tier `connection_settings` (including `threads`), `max_compacted_files`, `frequency` | Make `target_file_size` a tier field applied with `SET SESSION`; add `cpu_share`, `call_time_limit`, `pass_time_limit`, `idle_sleep_max`, and minor's `min_files` and `max_wait`; set `threads` from `cpu_share` and `Runtime.availableProcessors()`; `frequency` stops being a timer |
| Startup checks | Band overlap is checked in `CompactionConfig` | Require `target_file_size` and check it is at least `max_file_size` (without it DuckLake's 512 MB default applies); check the CPU shares add up to at most 1 |
| Per-table call | Catalog-wide call | `compact(database, tier, schema, table, cap)` issuing `CALL ducklake_merge_adjacent_files(db, table, schema := …, min_file_size := …, max_file_size := …, max_compacted_files := cap)`; DuckLake still honors `auto_compact = false` on a per-table call |
| Work discovery query | `queryActiveFiles`: one band-wide COUNT and SUM, nothing per table | New query on the catalog metadata (below) |
| Scheduler loop | Fixed-rate timer per (database, tier) | One loop thread per (database, tier): refresh → rank → dispatch → next pass, PASS_TIME_LIMIT with carry-over, idle sleep up to 10 s, a worker pool of 1 on the existing connection |
| Time-based cap, learning, failure adjustment | None; `CompactionRun.classify` already yields `COMMIT_TIMEOUT`, `TRANSACTION_CONFLICT`, `OUT_OF_MEMORY`, `SPILL_EXCEEDED`, `CATALOG_UNAVAILABLE`, `OTHER` | Reuse the classifier and add `PLANNING` (DuckLake internal errors); treat a successful call over the limit as a time signal; apply the busy-partition floor; new in-memory per-table state: learned sec_per_file or throughput, cap factor, consecutive failures, back-off |
| Retention and housekeeping | Own timers, catalog-wide `ducklake_rewrite_data_files(db)`, retention `DELETE` per table, no coordination with merges | Run retention and rewrite per table through the scheduler, never beside a merge on the same table; run expiry and cleanup between passes |
| Per-table telemetry | `CompactionRun` per tier, plus two band-wide COUNTs around every call | Add table, cap, bytes in and out, and whether other calls ran alongside; take files processed and created from the merge's result row and drop the COUNTs; add a table dimension to `CompactionRunLog`, `/ui` and the exported metrics |
| `/health` and alerts | `/health`, `/ui` and OTLP export exist | Add scheduler state (pass, queue, carry-over, back-offs) and `duckdb_memory()` per tier; export consecutive failures per table, starvation, commits per minute, oldest small file age and last success per tier |

**Discovery query.** One round trip per pass over the catalog metadata: `ducklake_data_file` (live files in the band), `ducklake_file_partition_value` (partition grouping), `ducklake_snapshot.schema_version` through `begin_snapshot` (schema grouping), `ducklake_delete_file` (skip files with live deletes), `ducklake_metadata` (`auto_compact` per table) and `ducklake_sort_info` with `end_snapshot IS NULL` (sort key). It lists only tables with at least 2 eligible files in one group (for minor, the admission threshold), and per table returns files and bytes waiting, average size, the oldest file's time, busy partitions (files committed in the last 10 min) and the sort-key flag.

**Gaps found in the current code.**

- `ResourceSampler.rssPeakBytes()` reads `VmHWM`, the process's lifetime high-water mark. It only rises, so it cannot measure one call; per-call memory needs DuckDB's query profiler or `duckdb_memory()` sampled during the call.
- Housekeeping still runs a catalog-wide `ducklake_rewrite_data_files(db)`, with the same conflict exposure as the old merge; Phase 1 moves it to per-table calls through the scheduler.

## Decisions

- Default band edges are 8 MB / 64 MB / 256 MB.
- Recommend a sort key on every table, so its merges take the cheaper sorted path (0.12 vs 0.85 MiB per file).
- Mega is enabled by default.
- Memory is bounded statically by bands, targets and sort keys; time is bounded by a cap learned per table, with a floor of busy partitions + 1.
- DuckDB threads per tier are twice the tier's CPUs; a call reserves about 1 core, and more cores are used through more concurrent table calls, not more groups per call.
