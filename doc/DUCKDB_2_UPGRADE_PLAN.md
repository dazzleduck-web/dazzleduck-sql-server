# DuckDB 2.0 Upgrade Plan

## Overview

DazzleDuck runs DuckDB 1.5.6 (`duckdb_jdbc` `1.5.6.0`) with a patched DuckLake
extension, release `v1.5.6-dazzleduck.2` of
[`dazzleduck-web/ducklake`](https://github.com/dazzleduck-web/ducklake). DuckDB
2.0.0 was released on 2026-10-21 and pins DuckLake `417a9117` (branch
`v2.0-cyanoptera`).

The plan moves to DuckDB 2.0 with a new DuckLake fork, builds three features on top
of it, and reshapes the otel-collector's attribute columns. Six tasks, in order:

| # | Task | Spec | Depends on |
|---|------|------|------------|
| 1 | Partition-scoped delete conflicts in DuckLake 2.0 | [duckdb/ducklake#1895](https://github.com/duckdb/ducklake/issues/1895) | none |
| 2 | Row-id file pruning in DuckLake 2.0, fork release, DuckDB 2.0 bump | [DuckLake patch](../dazzleduck-sql-ducklake-compactor/DUCKLAKE_PATCH.md) | 1 |
| 3 | Otel attributes as one struct per attribute set | [Attribute struct spec](https://github.com/dazzleduck-web/dazzleduck-sql-server/pull/530) (PR #530, `doc/OTEL_ATTRIBUTE_STRUCT_SPEC.md`) | 2 |
| 4 | Aggregation sidecars | [Sidecar views spec](https://github.com/dazzleduck-web/dazzleduck-sql-server/pull/526) (PR #526, `doc/SIDECAR_VIEWS_SPEC.md`) | 2, 3 |
| 5 | Aggregation background task | none yet | 1, 4 |
| 6 | Live aggregation view | [Live aggregation spec](https://github.com/dazzleduck-web/dazzleduck-sql-server/pull/527) (PR #527, `LIVE_AGGREGATION_SPEC.md`) | 1 (retention), 2, 5 |

---

## What Carries Over From the 1.5 Fork

| 1.5 fork patch | On DuckLake 2.0 | Action |
|---|---|---|
| File-level compaction conflicts ([#1453](https://github.com/duckdb/ducklake/pull/1453)) | Upstream checks compaction conflicts per file (`CheckFileConflicts`) | Drop; port the fork's tests to confirm |
| Concurrent deletes replacing a delete file ([#1482](https://github.com/duckdb/ducklake/pull/1482)) | Upstream conflicts deletes of the same data file | Drop; port the fork's tests to confirm |
| Compaction-vs-delete at file level ([#1484](https://github.com/duckdb/ducklake/pull/1484)) | Covered by `CheckFileConflicts` | Drop; port the fork's tests to confirm |
| Row-id stats and pruning (`4f0ee303`, `6963bc8b`) | Missing: no per-file row-id stats | Port (Task 2) |
| Row ids kept by `merge_adjacent_files` (`2e7b2d11`) | Fixed upstream for both cases the patch covers, verified on `417a9117`: rows an `UPDATE` preserved, and a sorted table's merge. Stock 1.5.5 renumbers both | Drop |
| none | DELETE conflicts with every concurrent insert into the table | Fix (Task 1) |

---

## Task 1: Partition-Scoped Delete Conflicts

**Problem.** DuckLake checks a DELETE against concurrent inserts per table: a
transaction that deletes from a table fails if any other transaction inserted into
it, and the other way round, even when the delete only touches a partition the
insert never writes. Deleting a closed hour while ingestion appends to the current
hour always fails one of the two. Tasks 5 and 6 both delete while ingestion runs.

**Fix**, in the style of Iceberg's `OverwriteFiles` conflict-detection filter:

1. At plan time, keep the filters pushed into the delete's scan
   (`DuckLakeDelete::PlanDelete`, via `FindDeleteSource`).
2. At commit, test each concurrently inserted file against those filters with its
   min/max column stats (`CheckStatistics` returning `FILTER_ALWAYS_FALSE`). A file
   that cannot match is not a conflict.
3. Keep the table-level rule where nothing can be proven: dynamic filters and
   deletes without a pushed-down filter. The same goes for DuckLake's server-side
   commit, which a metadata server that runs the commit retry loop itself selects
   (`RetrialsServerSide`); it rebuilds the conflict state from staged tables and
   would need the filters staged too. That is DuckLake's own commit mode, unrelated
   to the local staging in Task 4: files registered with `ducklake_add_data_files`
   commit through the ordinary path and get the narrower check.
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

Row-id stats also help Task 1, for a delete whose `rowid` predicate has an upper
bound (`rowid <= w`, `rowid < w`). Row ids are allocated at commit, so a file
committed concurrently starts above every row id the deleting transaction could
see. The check compares the filter's upper bound with each such file's minimum row
id, not just the presence of a `rowid` filter: a lower bound (`rowid > x`), a range
above the snapshot, an `IN` list or a join on `rowid` proves nothing and keeps the
table-level rule unless the file's range is shown to miss it.

**Release the fork.** Branch `backport/v2.0.0-dazzleduck` from the commit DuckDB
2.0.0 pins, with Tasks 1 and 2 on top, and publish `v2.0.0-dazzleduck.1` with the
existing workflow. The steps are in
[Updating the patch](../dazzleduck-sql-ducklake-compactor/DUCKLAKE_PATCH.md#updating-the-patch-eg-a-duckdb-version-bump).

**Bump DazzleDuck.**

- `duckdb.version` to `2.0.0.x` and the three `ducklake.*` pins in
  `dazzleduck-sql-ducklake-compactor/pom.xml` (`ducklake.extension.cache.version`
  becomes `v2.0.0`).
- Native images: check both `jni-config.json` files, in
  `dazzleduck-sql-ducklake-compactor` and `dazzleduck-sql-otel-collector`, for JNI
  lookups added in the 2.0 driver; 1.5.6 needed one in each.
- Base image: build and publish `dazzleduck/base-jre:...-noble-duckdb-2.0.0.x` with
  `scripts/build-base-image.sh` before the Jib builds. The runtime, compactor,
  otel-collector and scrapper images all pin it by `duckdb.version`, and fail with
  image-not-found until it exists.
- Full test suite, `CompactionStressTest`, and the Docker and Jib builds.
- S3 writes: DuckDB 2.0 makes two extra HEAD requests per written file (measured in
  the sidecar benchmark). Check the ingestion path's latency.

**Done when** DazzleDuck runs on DuckDB 2.0 with the new fork release.

---

## Task 3: Otel Attributes as One Struct per Attribute Set

Spec: [PR #530](https://github.com/dazzleduck-web/dazzleduck-sql-server/pull/530),
`doc/OTEL_ATTRIBUTE_STRUCT_SPEC.md`.

**Today** the otel-collector writes each attribute set as six top-level maps, one
per OTLP value type: `attributes` (strings) and `attributes_int`,
`attributes_double`, `attributes_bool`, `attributes_bytes`, `attributes_json`, and
the same six for `resource_attributes`. The string map sits at its original
position and the ten typed maps are appended at the end of the table, so one
attribute set is spread across the schema.

**Proposed:** one column per attribute set, a struct with one map per value type.

```sql
attributes STRUCT(
    string MAP(VARCHAR, VARCHAR),
    "int"    MAP(VARCHAR, BIGINT),
    "double" MAP(VARCHAR, DOUBLE),
    bool   MAP(VARCHAR, BOOLEAN),
    bytes  MAP(VARCHAR, BLOB),
    "json"   MAP(VARCHAR, JSON)
)
```

`resource_attributes` gets the same type, and so do the `attributes` of span
events and links. Each signal loses ten top-level columns: logs go from 23 to 13,
traces from 26 to 16 and metrics from 29 to 19 (plus `claims` where enabled, which
stays a plain map).

**Queries** read `attributes.int['http.status_code']` or
`resource_attributes.string['service.name']`. Checked on DuckDB 1.5.5: the field
names need no quoting in a query, only in DDL, where DuckDB prints `"int"`,
`"double"` and `"json"` quoted. Reading one field reads only that field's Parquet
columns (the scan projects `attributes.int` alone), so the struct costs nothing
over separate maps for a query that uses one type.

**Work:**

- `OtelSchemaFields`: build the struct field in place of the six maps;
  `AttributeType` names the struct's children.
- `AttributeMapsWriter`: write into the struct's child vectors. The column index
  constants `COL_TYPED_ATTRIBUTES` and `COL_TYPED_RESOURCE_ATTRIBUTES` go away.
- `attributes.string_copy` keeps its meaning: the `string` field also holds a text
  copy of every typed value.
- Schemas, events and links, README, `reference.conf`, and tests. The sidecar spec's
  attribute examples (Task 4) move to the new names.

**Migration.** DuckLake cannot alter a `MAP` column into a `STRUCT`, and a struct
file sent to a flat table fails registration. The collector ships the flattening as
a per-queue `attribute_layout` (`auto`, `struct` or `flat`); `auto`, the default,
follows the target table, so existing tables keep receiving today's columns with no
change. A queue's own transformation runs after the flattening and keeps working.

**Never null.** OTLP attribute sets are `repeated KeyValue`, so the struct is never
null and a type without attributes is an empty map; this changes span events and
links, which today write null when they have no attributes.

**Verify on DuckDB 2.0:** struct field pruning, DuckLake column stats and file
pruning on the struct's nested fields, and adding a field to the struct later (a
new OTLP value type) as a DuckLake schema change.

**Why in this plan.** It changes the schema new tables get; shipping it after the
2.0 upgrade, in the same release, gives consumers one set of changes instead of two,
and it lands before Tasks 4 and 6 build views over the attribute columns.

---

## Task 4: Aggregation Sidecars

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

## Task 5: Aggregation Background Task

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

**Conflicts a run must handle:**

- **A late row for the hour being rolled up.** It lands in the same partition the
  run deletes, so Task 1 cannot separate them, and the run fails at commit. The run
  retries; the retry re-reads the partition, so the late row is included.
- **Compaction of the same hour.** A closed hour is what minor and major compaction
  merge, and DuckLake checks compaction against a delete per file. Whichever commits
  second fails when both touch the hour's files. Either the compactor does not
  compact partitions waiting for a roll-up, or the two are scheduled so they never
  overlap on a partition; a retry covers the rest.

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
  (Task 6).

---

## Task 6: Live Aggregation View

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
- Task 5. The roll-up job is a background aggregation task and should share its
  scheduling.
- Task 1, for retention: deleting `tail` rows already in `agg` while ingestion
  runs.

**Open items** from the spec review: late rows widening the merge and multiple
intervals, retention, and access modes (RESTRICTED and RESTRICT_READ_ONLY).
