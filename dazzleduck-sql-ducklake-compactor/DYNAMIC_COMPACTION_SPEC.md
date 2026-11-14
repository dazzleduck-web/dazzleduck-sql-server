# Dynamic Compaction Spec

Minor compaction must never get stuck: every batch should be small, bounded in memory and time, and limited to one table if it fails. In one observed failure it went 22 hours without a single successful run while the small-file backlog grew to about 98k files.

## Problem and goals

| Observed failure | Cause | Rule adopted |
| --- | --- | --- |
| OOM-killed every ~14 min, even at double the memory | Groups grew to the 512 MB default target: one partition = one 33k-file merge | Limit files per batch, not only bytes |
| Commit failed after 8 min (TRANSACTION_CONFLICT) | One catalog-wide transaction raced tables other writers rewrite every few seconds | One short transaction per table |
| Planning error failed the whole cycle | One table's internal error (hive path) aborted every table | Isolate failures per table |
| Backlog grew with a cap of 1 | DuckLake takes partitions in a fixed order, so a busy partition took every slot | Leave room past busy partitions |
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

## Call sizing

**How one table call works.** `ducklake_merge_adjacent_files` on one table:

1. Lists the table's files in the band that carry no deletes, and splits them by partition and schema version.
2. Within each partition, walks the files in order and cuts them into groups: a group keeps adding files until its total reaches the target. Each group becomes one output file.
3. Stops after `max_compacted_files` groups (the cap), counted across all of the table's partitions; a partition's leftover single file also uses a slot without merging anything.
4. Builds one plan with a branch per group (UNION ALL). DuckDB runs the branches one after another: each reads its files, writes one Parquet file and frees its buffers before the next starts. Threads speed up work inside a group, not across groups.
5. Commits once: every group lands in one snapshot, or none does.

So a group's size sets the call's peak memory (one group is in memory at a time) and that group's time, while the number of groups multiplies the time. The transaction stays open for all of it:

```math
\text{call\_time} \approx \text{groups} \times \left(\text{files\_per\_group} \times \text{sec\_per\_file} + \frac{\text{bytes\_per\_group}}{\text{throughput}}\right)
```

For example, 4 full minor groups of 3,200 files at 0.045 s per file take about 4 × 144 s ≈ 10 min, while 16 groups of 3 files take about 2 s.

Memory is bounded statically by the bands and targets; time is not, so the cap on groups comes from a time limit.

| Quantity | Formula | Worst case (minor, 5 KB files) |
| --- | --- | --- |
| Files per group | min(target ÷ average input size, files waiting) | 16 MB ÷ 5 KB ≈ 3,200 |
| Call peak memory | files per group × per_file_cost | ≈ 385 MiB sorted; ≈ 2.7 GiB unsorted |
| Group time | files per group × sec_per_file + bytes per group ÷ throughput | ≈ 144 s for a full minor group |

```math
\text{cap} = \operatorname{clamp}\left(\left\lfloor \frac{\text{CALL\_TIME\_LIMIT}}{\text{group\_time}} \right\rfloor,\ 1,\ \text{static\_cap}\right)
```

In steady state a group takes well under a second and the cap is the static cap (16); with full minor groups it is 4, with half-full groups 8. When the limit allows fewer groups than busy partitions + 1, the limit wins: an over-long call loses all its work, while a low cap only slows rotation. The bytes term matters for major and mega, whose groups are a few large files.

**Learning.** sec_per_file and throughput start from the constants below, chosen by whether the table has a sort key, then are learned per table as a moving average over successful calls. One duration cannot fit both terms, so minor learns sec_per_file (its bytes term is negligible) and major and mega learn throughput (their file term is negligible). Calls that ran beside others are recorded apart from solo calls, so the master can see what concurrency costs.

| Constant | Value | Source |
| --- | --- | --- |
| CALL_TIME_LIMIT | 10 min | A third of the catalog database's idle-in-transaction timeout (30 min in the measured setup) |
| per_file_cost, sorted table | 0.12 MiB | Measured, 1,000 small files |
| per_file_cost, unsorted table | 0.85 MiB | Measured, same files without a sort |
| sec_per_file, sorted table | 0.045 s (4 threads) | Measured, ~22 files/s |
| sec_per_file, unsorted table | 0.09 s | Not measured; starts at 2× sorted |
| throughput (major, mega) | 50 MB/s | Not measured; starting guess |
| MEM_BUDGET (minor) | 1 GiB | Example: minor `memory_limit` with a 4 GiB container limit |
| K (concurrent table calls per tier) | 4 | Bounds catalog connections, request rate and commit retries |

## Execution model

A master scheduler per tier dispatches table calls to a pool of up to K workers. Each worker holds its own connection to the tier's single DuckDB instance, so all calls share one `memory_limit` and thread pool. The unit of work is one table: one call, one transaction, one commit.

1. **Refresh.** One catalog query lists the tables with work: at least 2 files in the tier's band in one partition and schema version, with no delete files (DuckLake's own grouping), `auto_compact` not false, and not in back-off. For each it returns the files and bytes waiting, their average size, the oldest file's age, the busy partitions (written in the last 10 min), and whether the table has a sort key.
2. **Rank** by outstanding files, most first; ties go to the older oldest file.
3. **Dispatch.** Compute the table's cap (Call sizing) and estimate the call's peak memory and cores (1 for a few files, up to the tier's threads for full groups). Start it on a free worker if both fit in what is left (MEM_BUDGET minus the reservations of running calls, the tier's cores minus theirs) and no solo table is running. A solo table starts only when no other call runs. Otherwise wait for a call to finish, release its reservation, and re-check.
4. **Run**, on the worker: `SET SESSION ducklake_target_file_size` (workers share one instance), then `CALL ducklake_merge_adjacent_files('<catalog>', '<table>', min_file_size := …, max_file_size := …, max_compacted_files := cap)`, commit, and record duration, peak memory, files and bytes in and out, whether other calls ran alongside, and the outcome.
5. **Next pass.** When every listed table has finished, refresh and start again. If the refresh finds no work, sleep up to 10 s: writers may commit new files every few seconds.

**Adjusting on failure.** The master classifies each failure and changes the table's state before its next call:

| Failure | Adjustment |
| --- | --- |
| Time: the call ran over CALL_TIME_LIMIT, or the catalog ended the idle transaction | group_time estimate × 2, so the cap halves |
| Time, while other calls ran in the tier | as above, and the table is marked solo |
| TRANSACTION_CONFLICT | cap halved for the next call: another writer is rewriting the table |
| Memory: OOM, or the call's peak above its reservation | reservation × 2; an unsorted table is flagged for a sort key |
| Planning error | skip for this pass |
| 3 failures in a row | back off 15 min and raise an alert |

A success under half of CALL_TIME_LIMIT lets the estimate decay back toward the learned average, so caps recover after a backlog.

**Solo tables and concurrency.** A solo table runs with no other call in its tier: the master lets running calls finish, runs it alone, then resumes parallel dispatch. It returns to parallel after 3 successful solo calls. The master also compares each table's learned sec_per_file in parallel against solo: when parallel calls are more than 1.5× slower across the tier, it lowers K by 1, and raises it by 1 after 10 passes without a time failure.

Reservations are estimates from Call sizing (doubled after a memory failure), not measured RSS: freed memory stays in glibc arenas, and RSS cannot be split between concurrent calls. A large table holds one worker while smaller tables keep moving, and one table is never split across workers. Calls on different tables never conflict logically; DuckLake retries a snapshot-id collision at commit.

**Why one table per call.** A catalog-wide `merge_adjacent_files` runs every table's merge in one transaction, one table after another. It adds no parallelism, and any one table's conflict or error discards all of them.

**DuckLake v1.5 limits.** No partition filter or ordering, and the group cap is per table, so a cap below busy partitions + 1 lets one busy partition take every slot. The upstream fix is a partition filter or oldest-first groups.

## Failure handling and memory hygiene

Each failure costs at most one table's call; the master classifies it and adjusts that table (Execution model). Tables that other writers rewrite continuously get `auto_compact = false`.

**Memory hygiene.** Most compactor memory is native `malloc` memory outside DuckDB's `memory_limit` (926 MiB of a 1.25 GiB process in 28 glibc arenas; Java heap 56 MiB). `MALLOC_ARENA_MAX=2` bounds it. Also recycle the minor instance after 50 passes or above 60% of the container limit, once its workers are idle; a reattach costs about 1 s.

**Catalog options load once.** `set_option` changes (`target_file_size`, `auto_compact`) are read only at attach, so they need a restart or an instance recycle.

**Alerts:** minor `last_success_age` > 15 min; 3 consecutive `FAILED` outcomes for a table; small-file count rising for 30 min; a table's oldest small file older than 1 hour; RSS > 75% of the container limit.

## Rollout

Phase 1 ships the scheduler with one worker and the learned time-based cap. Phase 2 adds parallel dispatch, solo tables and an adaptive K on top of it.

**Phase 1: configuration and scheduler**

- [ ] Configuration: minor cap 16, major cap 16, minor threads 4, per-tier `SET SESSION ducklake_target_file_size` (16 / 64 / 256 MB), bands 8 / 64 / 256 MB, container memory well above `memory_limit`, `MALLOC_ARENA_MAX=2`, `auto_compact = false` on tables other writers rewrite continuously
- [ ] Startup checks: bands do not overlap, and each target is at least its band max
- [ ] The refresh flags any table without a sort key (one is recommended on every table)
- [ ] Per-table call: one table, one transaction, replacing the catalog-wide call
- [ ] Work discovery query: per tier, the tables with work, with files and bytes waiting, average size, oldest file age, busy partitions and sort key
- [ ] Master scheduler loop (refresh → rank → dispatch → next pass) with one worker
- [ ] Time-based cap: starting constants by sort key, per-table learning, failure classification and adjustment
- [ ] Resource accounting: per-tier memory and core budget, reserve on dispatch and release on finish
- [ ] Per-table state: telemetry per call, learned estimates, failure count and back-off
- [ ] Idle sleep of at most 10 s; `duckdb_memory()` and scheduler state per tier on `/health`; the alerts above

**Phase 2: parallel scheduling** (`CompactionService` and `DuckDbTierCompactor`)

- [ ] Worker pool of up to K, each worker on its own connection to the tier's instance
- [ ] Admission: dispatch while the estimated memory and cores fit
- [ ] Solo tables: a time failure under concurrency makes the table run alone until 3 solo successes
- [ ] Adaptive K: lower when parallel calls run more than 1.5× slower than solo, raise after 10 passes without a time failure
- [ ] Instance recycling after 50 passes or above 60% of the container limit, once workers are idle

## Phase 1 in the compactor code

Today each enabled tier runs on a fixed-rate timer per database (`CompactionService.start`, `scheduleAtFixedRate(runTier, frequency)`). `runTier` counts the band's files across the catalog, calls `TierCompactor.compact(database, tier)`, counts again and records a `CompactionRun`. `DuckDbTierCompactor` issues one catalog-wide `CALL ducklake_merge_adjacent_files('<db>', min_file_size, max_file_size, max_compacted_files)` on one raw DuckDB connection per (database, tier), opened with the tier's `connection_settings`.

| Phase 1 item | Today | Change |
| --- | --- | --- |
| Configuration | Per-tier `connection_settings`, `max_compacted_files`, `frequency` | Make `target_file_size` a tier field applied with `SET SESSION`; add `call_time_limit` and `idle_sleep_max`; `frequency` stops being a timer |
| Startup checks | Band overlap is checked in `CompactionConfig` | Require `target_file_size` and check it is at least `max_file_size`; without it DuckLake's 512 MB default applies |
| Per-table call | Catalog-wide call | `compact(database, tier, schema, table, cap)` issuing `CALL ducklake_merge_adjacent_files('<db>', '<table>', schema := …, min_file_size := …, max_file_size := …, max_compacted_files := cap)`; DuckLake still honors `auto_compact = false` on a per-table call |
| Work discovery query | `queryActiveFiles`: one band-wide COUNT and SUM, nothing per table | New query on the catalog metadata (below) |
| Scheduler loop | Fixed-rate timer per (database, tier) | One loop thread per (database, tier): refresh → rank → dispatch → next pass, idle sleep up to 10 s, a worker pool of 1 on the existing connection |
| Time-based cap, learning, failure adjustment | None; `CompactionRun.classify` already yields `COMMIT_TIMEOUT`, `TRANSACTION_CONFLICT`, `OUT_OF_MEMORY`, `SPILL_EXCEEDED`, `CATALOG_UNAVAILABLE`, `OTHER` | Reuse the classifier and add `PLANNING` (DuckLake internal errors); treat a successful call over the limit as a time signal; new in-memory per-table state: learned sec_per_file or throughput, cap factor, consecutive failures, back-off |
| Resource accounting | None; `ResourceSampler.memoryLimitBytes` parses the tier's `memory_limit` | Per-tier memory and core budget from `memory_limit` and `threads`; reserve on dispatch, release on finish |
| Per-table telemetry | `CompactionRun` per tier, plus two band-wide COUNTs around every call | Add table, cap, bytes in and out, and whether other calls ran alongside; take files processed and created from the merge's result row and drop the COUNTs; add a table dimension to `CompactionRunLog`, `/ui` and the exported metrics |
| `/health` and alerts | `/health`, `/ui` and OTLP export exist | Add scheduler state (pass, queue, back-offs) and `duckdb_memory()` per tier; export consecutive failures per table, oldest small file age and last success per tier |

**Discovery query.** One round trip per pass over the catalog metadata: `ducklake_data_file` (live files in the band), `ducklake_file_partition_value` (partition grouping), `ducklake_snapshot.schema_version` through `begin_snapshot` (schema grouping), `ducklake_delete_file` (skip files with live deletes), `ducklake_metadata` (`auto_compact` per table) and `ducklake_sort_info` with `end_snapshot IS NULL` (sort key). It lists only tables with at least 2 eligible files in one group, and per table returns files and bytes waiting, average size, the oldest file's time, busy partitions (files committed in the last 10 min) and the sort-key flag.

**Gaps found in the current code.**

- `ResourceSampler.rssPeakBytes()` reads `VmHWM`, the process's lifetime high-water mark. It only rises, so it cannot measure one call; per-call memory needs DuckDB's query profiler or `duckdb_memory()` sampled during the call.
- Housekeeping (`DuckLakeHousekeeper`) still runs a catalog-wide `ducklake_rewrite_data_files('<db>')`, with the same conflict exposure as the old merge. It is outside Phase 1 and should move to per-table calls next.

## Decisions

- Default band edges are 8 MB / 64 MB / 256 MB.
- Recommend a sort key on every table, so its merges take the cheaper sorted path (0.12 vs 0.85 MiB per file).
- Mega is enabled by default.
- Memory is bounded statically by bands, targets and sort keys; time is bounded by a cap learned per table.
