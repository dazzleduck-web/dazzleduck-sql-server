# Live Aggregation over DuckLake — Design Specification

## Overview

A **live aggregate** is a DuckLake view that always returns the aggregation of
every committed raw row — including rows committed a second ago and late rows
for days that were already rolled up — without re-aggregating the raw data on
every query.

It is built from two tables and one view:

| Object | Content | Written by |
|---|---|---|
| `tail` | Raw rows, append-only. DuckLake assigns each row a `rowid` at commit. | Ingestion |
| `agg` | One row per group, holding mergeable measures, stamped with `row_id_watermark` | Roll-up job |
| `events_v` | The live result: `agg` plus every `tail` row `agg` has not absorbed yet | Nobody: a view |

A periodic **roll-up job** folds new `tail` rows into `agg`. Queries see the
same answer before and after the job runs; the job only moves work from query
time to background time.

**Status:** design validated with SQL prototypes against DuckDB v1.5.5 and the
`dazzleduck-web/ducklake` fork, Postgres catalog. No DazzleDuck code yet; see
[Integration](#integration-proposed) and [Open Questions](#open-questions).

---

## Worked Example

Raw data has a day, a group key and a value. The aggregation rule is
`count, sum, min, max` of `x` per `(day, k)`.

```sql
CALL lake.set_option('data_inlining_row_limit', 0);

CREATE TABLE lake.tail(day DATE, k VARCHAR, x BIGINT);
ALTER TABLE lake.tail SET PARTITIONED BY (day);

-- row_id_watermark: the tail row id the roll-up that wrote the row had aggregated up to
CREATE TABLE lake.agg(day DATE, k VARCHAR, cnt BIGINT, sum_x BIGINT, min_x BIGINT, max_x BIGINT,
                      row_id_watermark BIGINT);
ALTER TABLE lake.agg SET PARTITIONED BY (day);
```

A user queries the view like a table:

```sql
SELECT day, sum(sum_x) FROM lake.events_v WHERE day >= DATE '2026-10-01' GROUP BY day;
```

---

## Concepts

**Watermark (`w`).** Every `tail` row with `rowid <= w` is in `agg`; every row
with `rowid > w` is not. `w` is the largest `row_id_watermark` among the `agg`
files live at the query's snapshot, read from catalog stats (no Parquet read).

**Fresh rows.** `tail` rows with `rowid > w`. The roll-up job keeps this set
small.

**Boundary (`min_day`).** The earliest `day` among fresh rows. Days before it
hold no fresh rows, so their `agg` rows are final. Days on or after it may hold
fresh rows (new data or late data), so they are re-aggregated at query time.

**Mergeable measures.** Every stored measure must be combinable from partial
results: `count` and `sum` merge by `sum`, `min` by `min`, `max` by `max`.
`avg` is stored as `sum` plus `count`. Distinct counts need a mergeable sketch.

---

## The View

```sql
CREATE OR REPLACE VIEW lake.events_v AS
WITH st AS MATERIALIZED (SELECT lake.tail_state() AS s)          -- state lookup runs once per query
-- before the boundary: already aggregated, pass through
SELECT day, k, cnt, sum_x, min_x, max_x
FROM lake.agg
WHERE day < (SELECT s.min_day FROM st)
UNION ALL
-- from the boundary on: aggregated rows plus fresh raw rows, rolled up together
SELECT day, k, sum(cnt)::BIGINT, sum(sum_x)::BIGINT, min(min_x), max(max_x)
FROM (
    SELECT day, k, cnt, sum_x, min_x, max_x
    FROM lake.agg
    WHERE day >= (SELECT s.min_day FROM st)
    UNION ALL
    SELECT day, k, 1, x, x, x
    FROM lake.tail
    WHERE rowid > (SELECT s.w FROM st)
)
GROUP BY day, k;
```

`MATERIALIZED` matters: a macro is expanded at every reference, so without the
CTE the catalog lookup would run three times per query.

---

## The State Macro

Returns `{w, min_day}`. The watermark comes from catalog metadata only; the
boundary reads only the fresh `tail` files.

```sql
-- agg_table_id / wm_column_id: catalog ids of lake.agg and agg.row_id_watermark, passed as literals so
-- Postgres applies the filters itself. Both are checked against the ids looked up by name.
CREATE OR REPLACE MACRO lake.tail_state(agg_table_id := 21, wm_column_id := 7) AS (
    WITH snap AS (SELECT id AS s FROM lake.current_snapshot()),
    wm_col AS (     -- column id of agg.row_id_watermark
        SELECT c.table_id, c.column_id
        FROM __ducklake_metadata_lake.ducklake_column c
        JOIN __ducklake_metadata_lake.ducklake_table t ON t.table_id = c.table_id AND t.end_snapshot IS NULL
        JOIN __ducklake_metadata_lake.ducklake_schema sc ON sc.schema_id = t.schema_id AND sc.end_snapshot IS NULL
        WHERE sc.schema_name = 'main' AND t.table_name = 'agg'
          AND c.column_name = 'row_id_watermark' AND c.end_snapshot IS NULL
    ),
    agg_files AS (  -- agg files visible at this query's snapshot, with their row_id_watermark max
        SELECT f.data_file_id, TRY_CAST(s.max_value AS BIGINT) AS wm
        FROM __ducklake_metadata_lake.ducklake_data_file f
        JOIN wm_col ON f.table_id = wm_col.table_id
        LEFT JOIN __ducklake_metadata_lake.ducklake_file_column_stats s
               ON s.data_file_id = f.data_file_id AND s.table_id = f.table_id AND s.column_id = wm_col.column_id
              AND s.table_id = agg_table_id AND s.column_id = wm_column_id    -- literals: pushed into the Postgres scan
        WHERE f.table_id = agg_table_id                                       -- literal: pushed into the Postgres scan
          AND f.begin_snapshot <= (SELECT s FROM snap)
          AND (f.end_snapshot IS NULL OR f.end_snapshot > (SELECT s FROM snap))
    ),
    -- Max over live files is the latest roll-up's watermark: a row stamped w is only ever deleted by a
    -- later roll-up, which writes its replacement stamped higher. Delete files therefore cannot make it wrong.
    agg_max AS (
        SELECT count(*) AS n_files, count(wm) AS n_with_stat, coalesce(max(wm), -1) AS w
        FROM agg_files
    ),
    w AS (
        -- a wrong id would leave agg_files (or their stats) empty and silently reset w to -1, so check both
        SELECT CASE WHEN (SELECT table_id FROM wm_col) IS DISTINCT FROM agg_table_id
                      OR (SELECT column_id FROM wm_col) IS DISTINCT FROM wm_column_id
                    THEN error('tail_state: agg_table_id / wm_column_id do not match the catalog; update the macro defaults')
                    WHEN n_files > n_with_stat
                    THEN error('tail_state: an agg data file has no row_id_watermark stat')
                    ELSE w END AS w
        FROM agg_max
    )
    SELECT struct_pack(
        w       := (SELECT w FROM w),
        -- earliest day among rows not yet rolled up; reads only the fresh tail files
        min_day := coalesce((SELECT min(day) FROM lake.tail WHERE rowid > (SELECT w FROM w)), DATE '9999-12-31')
    ) AS tail_state);
```

Notes:

- `__ducklake_metadata_lake` is the metadata catalog DuckLake attaches for a
  lake attached as `lake`; it follows the attach name.
- The name lookup matches schema and table name. Table names are unique only
  within a schema, so with a name-only match two live aggregations in
  different schemas, each with an `agg` table, would return two rows and break
  the scalar subqueries in the guard.
- The two id defaults are the catalog ids of `agg` and its `row_id_watermark`
  column. They change when `agg` is recreated; the guard then fails every query
  until the macro is re-created with the new ids:

```sql
SELECT c.table_id, c.column_id
FROM __ducklake_metadata_lake.ducklake_column c
JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id)
JOIN __ducklake_metadata_lake.ducklake_schema sc USING (schema_id)
WHERE sc.schema_name = 'main' AND t.table_name = 'agg' AND c.column_name = 'row_id_watermark'
  AND c.end_snapshot IS NULL AND t.end_snapshot IS NULL AND sc.end_snapshot IS NULL;
```

---

## The Roll-up Job

```sql
BEGIN;
SET VARIABLE w_old = (lake.tail_state()).w;
SET VARIABLE w_new = (SELECT coalesce(max(rowid), getvariable('w_old')) FROM lake.tail);
CREATE OR REPLACE TEMP TABLE fresh AS
    SELECT day, k, 1::BIGINT AS cnt, x AS sum_x, x AS min_x, x AS max_x
    FROM lake.tail WHERE rowid > getvariable('w_old') AND rowid <= getvariable('w_new');
-- every merged row is stamped with w_new, so the max stamp in agg is always the latest watermark
CREATE OR REPLACE TEMP TABLE merged AS
    SELECT day, k, sum(cnt)::BIGINT cnt, sum(sum_x)::BIGINT sum_x, min(min_x) min_x, max(max_x) max_x,
           getvariable('w_new')::BIGINT AS row_id_watermark
    FROM (SELECT day, k, cnt, sum_x, min_x, max_x FROM lake.agg WHERE day IN (SELECT DISTINCT day FROM fresh)
          UNION ALL SELECT * FROM fresh)
    GROUP BY day, k;
DELETE FROM lake.agg WHERE day IN (SELECT DISTINCT day FROM fresh);
INSERT INTO lake.agg SELECT * FROM merged;
COMMIT;
```

It rewrites whole days of `agg`: every day touched by fresh rows is deleted and
re-inserted, merged with the fresh rows. A run with no fresh rows writes
nothing and leaves `w` unchanged.

---

## Correctness

The view returns exactly the aggregation of all `tail` rows at the query's
snapshot. The argument rests on these facts, each verified against the fork:

1. **Row ids follow commit order.** DuckLake allocates row ids at commit, so a
   row committed after the job's snapshot always gets a row id above that
   snapshot's `max(rowid)`. A job never skips a row that commits while it runs.
2. **Row ids are stable.** `merge_adjacent_files` keeps the row-id-to-row
   mapping, so compacting `tail` never moves a row across the watermark.
3. **The job is atomic.** It reads `tail`, rewrites `agg` and stamps `w_new` in
   one transaction; a query sees either all of a run or none of it.
4. **The watermark is exact.** Every row a run writes carries `w_new`, and a
   row stamped `w` is only deleted by a later run that writes its replacement
   stamped higher. So the largest stamp among live `agg` files is the latest
   run's `w_new`, whether or not some files carry delete files.
5. **The lookup is snapshot-consistent.** The macro filters `agg` files by
   `begin_snapshot` and `end_snapshot` against `current_snapshot()`, the same
   snapshot the view's `agg` and `tail` scans read. A run committing mid-query
   is invisible to both.
6. **The cut-over is exact.** Days before `min_day` hold no fresh row, so their
   `agg` rows are complete. Days from `min_day` on are rebuilt from their `agg`
   rows plus every fresh row, and the measures merge exactly.

Measured: 0 differences from a full `GROUP BY` over `tail`, and no duplicate
`(day, k)` rows, in every state tested:

| State | `w` | `min_day` |
|---|---|---|
| Nothing rolled up | -1 | first day |
| After a roll-up | 299999 | 9999-12-31 |
| New rows for the last days | unchanged | 10-28 |
| Late rows for an old day | unchanged | 10-03 |
| Roll-up touching one day only | 300279 | 9999-12-31 |
| After `merge_adjacent_files` | unchanged | unchanged |
| After `expire_snapshots` | unchanged | unchanged |
| Files added with `ducklake_add_data_files`, row-id stat missing | unchanged | correct |
| Roll-up running concurrently with ingestion (tested with an earlier variant whose job also wrote only `agg`) | — | — |

---

## Concurrency

| Pair | Outcome |
|---|---|
| Roll-up job and ingestion into `tail` | No conflict: the job writes only `agg` |
| Roll-up job and queries on `events_v` | No conflict; queries read a snapshot |
| Roll-up job and `merge_adjacent_files` | No conflict |
| Two roll-up jobs | Expected conflict (not tested): a `DELETE` on `agg` conflicts with any concurrent insert into `agg`, so one fails at commit. Run one job at a time per `agg` table. |

---

## Performance

Measured on a Postgres catalog, about 300k `tail` rows over 30 days, fresh rows
in 1–3 days.

| Query part | Reads |
|---|---|
| Watermark lookup | 1 `ducklake_data_file` row and 1 `ducklake_file_column_stats` row per live `agg` file; no Parquet |
| Boundary | Only the fresh `tail` files (1–3) |
| Pass-through branch | `agg` files for days before `min_day`, pruned by user filters |
| Merge branch | `agg` files from `min_day` on, plus the fresh `tail` files |

`tail_state()` median latency: 31 ms. A full scan of `events_v`: about 40 ms.

**Filter pushdown.** User filters on `day`, on `k` and through joins reach every
`agg` and `tail` scan and prune files. The `rowid > w` filter is a runtime
filter (its value comes from a subquery); it still prunes `tail` files before
DuckLake lists them, because the scalar subquery is evaluated below the
`GROUP BY`. Verified stable over 60 runs at 1, 4 and 16 threads.

**Catalog pushdown.** The DuckDB Postgres scanner pushes only comparisons with
constants. That is why the macro passes the table and column ids as literals.
The snapshot comparisons on `ducklake_data_file` use subquery values and are
applied in DuckDB, after the rows for `agg` arrive.

---

## Requirements

1. **Data inlining off** (`CALL lake.set_option('data_inlining_row_limit', 0)`).
   Passing `DATA_INLINING_ROW_LIMIT` to `ATTACH` is not persisted. With inlining
   on, `agg` rows would have no data file and the watermark lookup would miss
   them.
2. **Row-id file stats** (the `dazzleduck-web/ducklake` fork). They let
   `rowid > w` prune `tail` files. Without them the result is still correct, but
   every query reads all of `tail`. Files added by `ducklake_add_data_files` on
   a build without derived row-id stats lack them; `ducklake_backfill_row_id_stats`
   fills them in.
3. **Mergeable measures only** in `agg`.
4. **One roll-up job at a time** per `agg` table.
5. **`agg` is written only by the roll-up job.** A row written without the
   current watermark stamp would break fact 4.

---

## Rejected Alternatives

| Alternative | Why rejected |
|---|---|
| One table with `aggregate = true/false` files per partition | A rewrite job's `DELETE` conflicts table-wide with every concurrent insert, so the job starves under continuous ingestion |
| A query rewrite in DazzleDuck | Ruled out during design |
| A DuckDB optimizer extension | A C++ extension to build and maintain, for plans a view already produces |
| A plain `UNION ALL` of `agg` and fresh rows, no cut-over | Late rows for a rolled-up day produce a second row for the same group |
| `SET VARIABLE` for `w` and `min_day` | Same pruning as the macro, but every caller must set it in the same transaction, and it can go stale |
| Catalog-only lookup of `min_day` from `tail` file stats | Breaks when a file lacks row-id or `day` stats; reading the few fresh files is exact and as fast |
| A separate append-only watermark table | `max()` is answered from stats only until the first delete on that table; after any cleanup it scans |
| `max(row_id_watermark)` via a normal scan of `agg` | `agg` has deletes, so DuckDB reads every `agg` file on every query |
| Watermark in the roll-up commit's snapshot metadata | `expire_snapshots` deletes it; the watermark then falls back to -1 and totals are double counted silently |

---

## Integration (proposed)

Nothing below is built. A DazzleDuck feature would generate and run the SQL
above from a declaration:

```hocon
live_aggregations = [{
    catalog = "lake", schema = "main"
    tail_table = "tail"
    agg_table  = "agg"
    view_name  = "events_v"
    group_by   = ["day", "k"]
    boundary_column = "day"          # partition column used for the cut-over
    measures = [
        { name = "cnt",   function = "count", column = "*" }
        { name = "sum_x", function = "sum",   column = "x" }
        { name = "min_x", function = "min",   column = "x" }
        { name = "max_x", function = "max",   column = "x" }
    ]
    rollup_interval = 1m
}]
```

Candidate responsibilities:

- **DDL generation.** Create `agg`, the macro (with the resolved table and
  column ids) and the view from the declaration.
- **Roll-up scheduling.** Run the job on `rollup_interval`, one at a time per
  `agg`. `dazzleduck-sql-ducklake-compactor` already schedules DuckLake
  maintenance and is a natural home.
- **Id refresh.** Re-create the macro when `agg` is recreated.

---

## Test Plan

Prototype scripts that drove the measurements above are the basis:

1. Exactness after each state in the [Correctness](#correctness) table:
   compare `events_v` with a full `GROUP BY` over `tail` (both directions of
   `EXCEPT ALL`) and assert no duplicate group keys.
2. Roll-up running while ingestion commits: both succeed, result exact.
3. Guard: stale table id and stale column id each raise the macro's error, and
   an `agg` table of the same name in another schema does not affect it.
4. Pruning: profile `events_v` with a `day` filter and assert the file counts
   per scan.

---

## Open Questions

1. **Boundary column.** The cut-over needs a column correlated with arrival,
   such as `day`. What if the group key has no such column?
2. **Several group keys and measures.** Generating the merge expression per
   measure type (`sum`, `min`, `max`, sketches, `avg` as two columns).
3. **Id literals.** Resolve them at generation time (proposed), or accept
   reading every catalog file row without pushdown?
4. **Retention.** Pruning old `tail` rows that are already in `agg` (rows with
   `rowid <= w`) without breaking pruning or the cut-over.
5. **Job failure.** A failed run leaves `agg` unchanged and the view still
   exact; only the fresh set grows. Is an alert on fresh-set size enough?
6. **Late rows widen the merge.** One late row for an old day moves `min_day`
   back, so every query re-aggregates all of `agg` from that day onward until
   the next roll-up. It is bounded by `rollup_interval`, but an alternative is
   to merge only the days that actually have fresh rows, at the cost of the
   range-based file pruning. To be explored together with making the design
   generic over several intervals (hourly, daily, weekly and so on).
