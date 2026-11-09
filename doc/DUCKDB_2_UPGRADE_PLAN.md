# DuckDB 2.0 Upgrade Plan

## Overview

DazzleDuck runs DuckDB 1.5.6 (`duckdb_jdbc` `1.5.6.0`) with a patched DuckLake
extension, release `v1.5.6-dazzleduck.2` of
[`dazzleduck-web/ducklake`](https://github.com/dazzleduck-web/ducklake). DuckDB
2.0.0 was released on 2026-10-21 and pins DuckLake `417a9117` (branch
`v2.0-cyanoptera`).

The plan moves to DuckDB 2.0 with a new DuckLake fork, then builds three features
on top of it. Five tasks, in order:

| # | Task | Spec | Depends on |
|---|------|------|------------|
| 1 | Partition-scoped delete conflicts in DuckLake 2.0 | [duckdb/ducklake#1895](https://github.com/duckdb/ducklake/issues/1895) | none |
| 2 | Row-id file pruning in DuckLake 2.0, fork release, DuckDB 2.0 bump | [DuckLake patch](../dazzleduck-sql-ducklake-compactor/DUCKLAKE_PATCH.md) | 1 |
| 3 | Aggregation sidecars | [Sidecar views spec](https://github.com/dazzleduck-web/dazzleduck-sql-server/pull/526) (PR #526, `doc/SIDECAR_VIEWS_SPEC.md`) | 2 |
| 4 | Aggregation background task | none yet | 1, 3 |
| 5 | Live aggregation view | [Live aggregation spec](https://github.com/dazzleduck-web/dazzleduck-sql-server/pull/527) (PR #527, `LIVE_AGGREGATION_SPEC.md`) | 2, 4 |

---

## What Carries Over From the 1.5 Fork

| 1.5 fork patch | On DuckLake 2.0 | Action |
|---|---|---|
| File-level compaction conflicts ([#1453](https://github.com/duckdb/ducklake/pull/1453)) | Upstream checks compaction conflicts per file (`CheckFileConflicts`) | Drop; port the fork's tests to confirm |
| Concurrent deletes replacing a delete file ([#1482](https://github.com/duckdb/ducklake/pull/1482)) | Upstream conflicts deletes of the same data file | Drop; port the fork's tests to confirm |
| Compaction-vs-delete at file level ([#1484](https://github.com/duckdb/ducklake/pull/1484)) | Covered by `CheckFileConflicts` | Drop; port the fork's tests to confirm |
| Row-id stats and pruning (`4f0ee303`, `6963bc8b`) | Missing: no per-file row-id stats | Port (Task 2) |
| Row ids kept by `merge_adjacent_files` (`2e7b2d11`) | Fixed upstream: a sorted table's merge keeps row ids (verified on `417a9117`) | Drop |
| none | DELETE conflicts with every concurrent insert into the table | Fix (Task 1) |

---

## Task 1: Partition-Scoped Delete Conflicts

**Problem.** DuckLake checks a DELETE against concurrent inserts per table: a
transaction that deletes from a table fails if any other transaction inserted into
it, and the other way round, even when the delete only touches a partition the
insert never writes. Deleting a closed hour while ingestion appends to the current
hour always fails one of the two. Tasks 4 and 5 both delete while ingestion runs.

**Fix**, in the style of Iceberg's `OverwriteFiles` conflict-detection filter:

1. At plan time, keep the filters pushed into the delete's scan
   (`DuckLakeDelete::PlanDelete`, via `FindDeleteSource`).
2. At commit, test each concurrently inserted file against those filters with its
   min/max column stats (`CheckStatistics` returning `FILTER_ALWAYS_FALSE`). A file
   that cannot match is not a conflict.
3. Keep the table-level rule where nothing can be proven: dynamic filters, deletes
   without a pushed-down filter, and the staged and server-side commit paths.
4. For an insert committing after a concurrent delete, track the tables the
   inserting transaction read.

**State.** Issue filed with a repro verified on DuckDB 2.0.0. Worktree
`~/git/ducklake-delete-conflict`, branch `fix/delete-conflict-file-stats` at
`417a9117`, builds. No code yet.

**Size.** About 350–450 lines of C++ and 300 lines of tests.

**Done when** the repro passes, the existing test suite passes, and a PR is open
upstream.

---

## Task 2: Row-Id Pruning, Fork Release and DuckDB Bump

**Port the row-id pruning.** Each data file's row-id range is stored as a stats row
in `ducklake_file_column_stats` (`column_id = 2147483540`), so a filter or join on
`rowid` skips files whose range cannot match.
`ducklake_backfill_row_id_stats(...)` fills the row in for older files. Background
is in [discussion #1389](https://github.com/duckdb/ducklake/discussions/1389).
Port `4f0ee303` and `6963bc8b` onto the Task 1 branch and their test
`rowid_stats_pruning.test`.

Row-id stats also help Task 1: a delete filtered on `rowid` can then be proven not
to touch files inserted concurrently, since their row ids are all higher.

**Release the fork.** Branch `backport/v2.0.0-dazzleduck` from the commit DuckDB
2.0.0 pins, with Tasks 1 and 2 on top, and publish `v2.0.0-dazzleduck.1` with the
existing workflow. The steps are in
[Updating the patch](../dazzleduck-sql-ducklake-compactor/DUCKLAKE_PATCH.md#updating-the-patch-eg-a-duckdb-version-bump).

**Bump DazzleDuck.**

- `duckdb.version` to `2.0.0.x` and the three `ducklake.*` pins in
  `dazzleduck-sql-ducklake-compactor/pom.xml` (`ducklake.extension.cache.version`
  becomes `v2.0.0`).
- Native images: check `jni-config.json` for JNI lookups added in the 2.0 driver;
  1.5.6 needed one.
- Full test suite, `CompactionStressTest`, and the Docker and Jib builds.
- S3 writes: DuckDB 2.0 makes two extra HEAD requests per written file (measured in
  the sidecar benchmark). Check the ingestion path's latency.

**Done when** DazzleDuck runs on DuckDB 2.0 with the new fork release.

---

## Task 3: Aggregation Sidecars

Spec: [PR #526](https://github.com/dazzleduck-web/dazzleduck-sql-server/pull/526),
`doc/SIDECAR_VIEWS_SPEC.md`.

Each ingestion queue can declare several sidecars. Each sidecar is a SQL view over
the base rows of a batch, written to its own table in the same commit as the base
table, for example per-minute counts by service. The spec covers the
configuration, the direct and staged write paths, the `FileUploader`
interface, S3 authentication and the benchmark.

**State.** Spec only; the PR is open. No code yet.

**Scope.** Start with the default mode (no uploader): parallel `COPY`s from the
source relation. Add the staged pipeline with an uploader next. Re-run the
benchmark on DuckDB 2.0.

---

## Task 4: Aggregation Background Task

No spec yet; to be written.

A sidecar table that stores `count(*)` per hour gets one row per batch for each
hour and dimension set. Once an hour is closed, a background task re-aggregates
its partition to one row per dimension set.

**One run, per closed partition, in one transaction:**

1. Aggregate the partition's rows with mergeable measures (`sum` of counts, `min`,
   `max`, sketches).
2. Delete the partition.
3. Insert the aggregated rows.
4. Commit.

**Why it needs Task 1.** Step 2 is a DELETE on a table ingestion keeps inserting
into. Without partition-scoped conflicts, every run conflicts with ingestion. With
them, a delete of a closed hour does not conflict with files written for the
current hour.

**To decide in the spec:**

- When an hour counts as closed: a fixed delay after the hour ends, or a
  watermark from ingestion. Late rows for a closed hour are aggregated again on
  the next run.
- Several intervals: roll hourly into daily and daily into weekly, each its own
  table or the same table.
- Change data feed: deleting and re-inserting changes `table_changes` output for
  the rolled-up rows. Note it for consumers.
- Scheduling: `dazzleduck-sql-ducklake-compactor` already schedules DuckLake
  maintenance and is a natural home, alongside the live view's roll-up job
  (Task 5).

---

## Task 5: Live Aggregation View

Spec: [PR #527](https://github.com/dazzleduck-web/dazzleduck-sql-server/pull/527),
`LIVE_AGGREGATION_SPEC.md`.

A view over two tables gives an exact aggregate at any time: `agg` holds rolled-up
rows stamped with a row-id watermark, and `tail` holds raw rows. The view merges
`agg` with the `tail` rows above the watermark. A roll-up job moves fresh rows
into `agg`.

**Depends on:**

- Task 2. Without row-id pruning the result is still correct, but
  `rowid > w` reads every `tail` file.
- Stable row ids under `merge_adjacent_files`: upstream on 2.0.
- Task 4. The roll-up job is a background aggregation task and should share its
  scheduling.
- Task 1, for retention: deleting `tail` rows already in `agg` while ingestion
  runs.

**Open items** from the spec review: late rows widening the merge and multiple
intervals, retention, and access modes (RESTRICTED and RESTRICT_READ_ONLY).
