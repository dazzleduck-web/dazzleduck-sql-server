# DuckLake Data Inlining Must Be Off — Design Specification

Status: **superseded for ingestion; the rule still matters for retention.** Ingestion watermarks
were removed (Task 7 of `doc/DUCKDB_2_UPGRADE_PLAN.md`). Their tables were the only ones the
collector writes with SQL, so the startup check over them was removed with them, and nothing on the
ingestion path is inlinable: registering a file never is.

What this spec measured still applies to the compactor's data retention job, which runs `DELETE` on
ingested tables. Inlining covers deletes: at the default limit, deleting a few rows of a Parquet file
puts them in the catalog's `ducklake_inlined_delete_<n>` tables instead of a delete file. Retention
drops whole files where it can, which writes neither, but the rows it deletes from a file holding
NULL times, or without statistics, go to the catalog. Keep the effective limit at 0 for the tables
retention deletes from: attach with `DATA_INLINING_ROW_LIMIT 0`, and set no option above 0 with
`set_option` on the catalog, schema or table, which takes precedence over the attach parameter (see
the resolution order below). No check enforces it today.

Earlier status: accepted; partly implemented. The open questions were settled (see the last section):
the collector refuses to start, the watermark's `INSERT` becomes a write-then-register, and its
conflict-path `UPDATE` becomes an append. The append landed in #514; the inlining rule itself and the
sidecar conversion did not.

The rule guards a live fault, not a hypothetical one: with #514 merged the watermark path still
writes with SQL — one `INSERT` per batch for its rows, plus an `INSERT` for a correction on the rare
conflict — and both are inlinable at the default limit. One trap survives into the implementation:
there are five places inlining can be switched on and they are not all visible in the same view.

## The problem

DuckLake can keep a small write in the catalog database instead of writing a Parquet file. The row
is then real and queryable — reads merge inlined data with the files — but it lives in the catalog,
which may be SQLite, and only reaches storage when `ducklake_flush_inlined_data` runs.

That breaks the premise the whole ingestion path is built on: data reaches object storage as
Parquet, and the catalog holds metadata about it.

**The decisive reason is the distributed catalog.** A production DuckLake catalog is Postgres or
MySQL, shared by every writer. Inlined rows are *data* in that store, so with inlining on, every
batch writes data rows into a remote transactional database on the insertion path — write
amplification and contention on the one component every writer shares, growing with ingest rate.
That is a scalability limit, not an untidiness. Inlining is a convenience for a single-process
catalog in a file; it is not an option once the catalog is shared.

Two things compound it:

- **It is on by default.** `ducklake_default_data_inlining_row_limit` defaults to **10**, so any
  write of ten rows or fewer is inlined unless something says otherwise. A one-row insert produced
  no file and one row in the catalog's inlined-data table.
- **Nothing flushes it on a default deployment.** Collector compaction, which is what would call
  `ducklake_flush_inlined_data`, is `enabled = false` by default, so those rows stay in the catalog
  indefinitely.

The counter-argument — that turning inlining off makes every small write its own file — is answered
by compaction: many small files are a known, solved problem (`ducklake_merge_adjacent_files`),
whereas data rows in a shared remote catalog are on the critical path of every insert. The costs
are not comparable.

## The rule

1. **The collector turns inlining off for its own instance** at startup:
   `SET GLOBAL ducklake_default_data_inlining_row_limit = 0`, which applies to every connection of
   the instance.
2. **Startup checks the effective limit for every table the collector writes with SQL** — which is
   the **watermark tables**, where each batch inserts its rows and may insert a snapshot
   correction — and **refuses to start** when any is above zero, naming the table, the source that
   set it, and the statement that fixes it.

**Only tables the collector inserts into need checking.** Inlining applies to `INSERT`, `UPDATE` and
`DELETE` plans, not to registering a file: `ducklake_add_data_files` with a one-row file at the
default limit of 10 registered it as a data file and put nothing in the catalog's inlined-data
tables. So target tables are exempt, and so is the attribute type registry — it writes Parquet and
registers it, which inlining cannot touch. (That also removes the "it defeats a write-then-register
design" motivation an earlier draft of this gave, and the dependency
`doc/ATTRIBUTE_TYPE_REGISTRY_SPEC.md` declared on this spec: there is none.)

**The limit covers deletes, not just inserts.** At the default limit a `DELETE` of a row living in a
Parquet file wrote no delete file — the delete went into a catalog table
(`ducklake_inlined_delete_<n>`); at limit 0 the same delete wrote a delete file. That is what makes
this matter wherever a `DELETE` or `UPDATE` reaches a DuckLake table. The watermark no longer issues
one — #514 replaced its conflict-path `UPDATE` with an append — so for this spec the live surface is
the two `INSERT`s that remain (see below). The delete measurement is kept because it is what makes
the rule correct for any DML a later change introduces, and because it is the reason the old
`UPDATE` was the worst shape of the fault.

Step 1 alone guarantees nothing, which is the point of step 2 — see below.

## Where inlining comes from, and what each source is visible in

**Five** sources, in resolution order. All measured, because the visibility differs and that is the
trap:

| # | Source | How it is set | Where it is readable |
|---|---|---|---|
| 1 | Table option | `CALL c.set_option('data_inlining_row_limit', n, table_name => 's.t')` | `ducklake_options(c)`, scope `TABLE`, `scope_entry` `s.t` |
| 2 | **Schema option** | `CALL c.set_option('data_inlining_row_limit', n, schema => 's')` | `ducklake_options(c)`, scope `SCHEMA`, `scope_entry` `s` |
| 3 | Catalog option | `CALL c.set_option('data_inlining_row_limit', n)` | `ducklake_options(c)`, scope `GLOBAL` |
| 4 | **Attach parameter** | `ATTACH 'ducklake:...' AS c (DATA_PATH '...', DATA_INLINING_ROW_LIMIT n)` | **`duckdb_databases().options`** — *not* in `ducklake_options()` |
| 5 | Instance setting | `SET [GLOBAL] ducklake_default_data_inlining_row_limit = n` (default **10**) | `current_setting(...)`, `duckdb_settings()` |

**Most specific wins**, matching `TryGetConfigOption` in DuckLake's `ducklake_catalog.cpp`. Verified
at the three boundaries that matter:

- Attach parameter 10 with the instance setting at 0 → **inlined**. So the attach parameter beats
  the setting, and step 1 of the rule does not cover it.
- Table option 0 with the attach parameter at 10 → **a Parquet file was written**. So a table-level
  zero does rescue a catalog attached with inlining on.
- Schema option 0 on `s2` with the attach parameter at 10 → an insert into `s2.t` **wrote a file**
  while `main.t` **inlined**. So the schema scope is real and sits between table and catalog.

A resolver that goes straight from table to catalog is wrong in both directions: a schema-level zero
is missed, which refuses a table that is fine, and a schema-level 10 under a catalog-level zero
passes a table that inlines.

Three consequences for the check:

- **It must read `duckdb_databases().options`, not just `ducklake_options()`.** An operator who puts
  `DATA_INLINING_ROW_LIMIT` on the `ATTACH` in the startup script — a thoroughly plausible place —
  is invisible to `ducklake_options()` entirely. A check that consulted only that view would pass a
  collector whose rows all go to the catalog.
- **It must include the schema scope**, between table and catalog.
- **It must resolve per table, not per catalog.** A catalog shared with other tools may legitimately
  want inlining for their tables; rejecting it outright would be wrong when the collector's own
  tables can be exempted with a table- or schema-level zero, which demonstrably works even against
  an attach parameter.

Options are resolved per session/attachment, so changing them under a running process does not take
effect — which means the limit the check reads at startup is the limit the writes will use.

## What it changes for a deployment that already runs

Not nothing, and this was measured by turning the rule on against the existing suite rather than
reasoned about. Two existing tests changed behaviour, and both are the rule working:

- A partition-pruning fixture went from **6 files to 9**. Its own comment recorded the old
  behaviour — "the 7 inserts collapse into 4 parquet files (grouped by partition) + 2 manual = 6" —
  and that collapse *was* inlining, plus the `ducklake_flush_inlined_data` the fixture calls to undo
  it. With inlining off each insert is its own file. So an operator should expect more, smaller
  files immediately after this lands, until compaction merges them. That is the cost named in the
  motivation, now with a number against it.
- A watermark test's join started matching every row twice, because the watermark table now has
  **data files of its own** in the same snapshot as the data it describes. Nothing was wrong with
  the rows; a query that joined `ducklake_data_file` on `begin_snapshot` without restricting to a
  table was relying on the watermark's rows not being files. Worth knowing for anyone who has
  written such a query against a watermark table.

A third thing the same exercise exposed, which is about the mechanism rather than the rule: the
pruning fixture had no ingestion provider and never asked about inlining, yet its result depended on
whether something else in the process had already called `validate()`. An instance-wide setting
changed by whoever starts first is order-dependent, and a test or a co-tenant catalog can be on the
receiving end. The fixture now states the limit it needs. See the note under Scope.

## The watermark becomes a write-then-register

Decided in review. The watermark rows stop being an `INSERT ... VALUES` inside the ingestion
transaction and become what the attribute type registry already does: the rows are written as a
Parquet file *before* the transaction, and `ducklake_add_data_files` registers that file *inside*
it, alongside the batch's own data files. Atomicity is unchanged — registration and the data files
still commit or roll back together — and the mechanism is the one `SidecarRows` already provides, so
this unifies the two tables rather than adding a second way of doing it.

Four things were measured before accepting it, because two of them constrain the implementation and
one of them retires an argument this spec was making.

**1. The sidecar file's column types must match the table's.** `ducklake_add_data_files` does not
cast. Registering an all-VARCHAR file — which is exactly what `SidecarRows.copySql` produces today,
since it renders values as string literals in a `VALUES` list — into a table with `TIMESTAMP` and
`BIGINT` columns fails:

```
Invalid Input Error: Failed to map column "min_ts" from file "dd_varchar.parquet" to the
column in table "wm"
* Expected one of TIMESTAMP_NS, TIMESTAMP, TIMESTAMP_MS, TIMESTAMP_S, found type VARCHAR
```

The registry got away with it because its columns are all strings. The watermark's are not, so the
sidecar writer needs the target table's column types in its projection. The same typed file
registers and reads back correctly. This is the one real piece of work in the conversion, and it is
a change to `SidecarRows`, which both tables then share.

**It is now two column types, not one.** The batch id below is a `UUID` (16 bytes against 36 as
text, measured at 1,001,297 bytes against 1,986,297 over 50,000 rows — the column is distinct on
every row, so nothing compresses it back). A UUID column is written to Parquet as
`FIXED_LEN_BYTE_ARRAY(16)` with the standard UUID logical type; registering that file into a `UUID`
column works and reads back as `UUID`, while the same file with the id as `VARCHAR` is rejected with
`Expected UUID, found type VARCHAR`. So typed projection is a hard requirement of the conversion
rather than a tidiness — a VARCHAR-rendering sidecar now fails on timestamps *and* on the batch id.

**2. Rows that arrive as a registered file are still addressable by `rowid`** — measured when the
conflict path was still an `UPDATE`, to prove it could key on a sidecar's rows: a one-row sidecar
registered in snapshot 2 came back from `ducklake_table_insertions('c','main','wm',2,2)` with
`rowid = 0`, and an `UPDATE` keyed on it changed exactly that row. Superseded by point 3 — nothing
in the design uses it now — and kept only because it is the kind of fact that is expensive to
rediscover if a later change wants to address registered rows.

**3. The tightening `UPDATE` is gone; the correction is appended.** Landed in #514, which this spec
assumes. Rather than editing the row in place, the conflict path appends a correction carrying the
exact snapshot id, and a reader takes the latest snapshot per batch id. That makes the watermark
table append-only with duplicates tolerated — the registry's model — and it needed one column the
table did not have, which was the whole of the design question.

**A correction is indistinguishable from a second batch without an identity.** The rows carry no
key; the old `UPDATE` keyed on DuckLake's `rowid` precisely because two batches in the same group
can aggregate identically (same minute, same count), which is a documented property of the table.
Measured on three rows — batch `b1` corrected from snapshot 4 to 7, and batch `b2` with identical
aggregates — where the truth is 200 ingested rows:

| Reader rule | `rows_ingested` | |
|---|---|---|
| Latest snapshot **per batch id** | **200** | correct |
| Group by the value columns, no batch id | 100 | wrong: merges two distinct batches |
| Raw `sum(row_count)`, as a consumer writes it today | 300 | wrong: counts the correction |

So the append needs **`watermark_batch_id_column`**, a `UUID` column, **required** whenever a
watermark is configured. Optional was tried first and rejected in review: it meant two shapes of
table and two post-commit behaviours differing only on the rare conflict path — the branch nobody
exercises, which is the worst place to put a choice. Required means one behaviour: the table is
always append-only and the correction is always an append. A watermark table that predates the key
must add the column before its queue will start, which is a startup error naming the key.

The `UPDATE` path is deleted outright rather than kept as a fallback, which also retires the
dependency on `ducklake_table_insertions` and on `rowid` stability.

**This half is implemented**, in #514, ahead of the sidecar conversion that the rest of this section
describes — it needed neither the inlining disable nor `SidecarRows`, so it stands alone. What
remains unimplemented here is the write-then-register itself.

**The correction row should carry only the identity and the snapshot**, with the aggregate columns
NULL, rather than repeating the whole row. Both work with "latest snapshot per batch", but a sparse
correction keeps every query a consumer writes today correct — `sum(row_count)` ignores NULLs, and
`max(max_ts)` likewise — so only a reader that wants the *exact* snapshot needs the per-batch rule.
The alternative, repeating the full row, silently breaks an existing `sum(row_count)` on the rare
occasions a correction exists, which is the worst shape for a bug: right almost always. A
correction is not an observation of ingested data, so a row that states only what it corrects is
also the more honest record.

**What the append costs the reader, which this spec did not account for.** Implementing it (#514)
turned up two ways a consumer query goes wrong that the in-place `UPDATE` had hidden, and both were
found by reading the change back rather than by reasoning about it. Recorded here because they are
the real price of the append, and because the spec as written implied the only change was on the
write side:

- **"Visible as of snapshot N" must read the per-batch maximum.** The row as first written keeps its
  *predicted* id forever — the `UPDATE` used to overwrite it — so the exact id now lives only in the
  correction. A batch that predicted 4 and committed in 7 is reported by
  `WHERE snapshot_id <= 5` as visible at 5, when it is not. This is a regression in the documented
  consumer query, not an inherited limitation.
- **…but that maximum must exclude rows with no batch id.** SQL collapses every id-less row into a
  single NULL batch, so on a migrated table a max over that group admits or excludes the whole
  history together: historical batches at snapshots 2 and 9 lose the one at 2 entirely for `N = 5`.
  Those rows need no maximum, since the code that wrote them corrected in place.

Neither is fatal and both are now documented and tested, but the shape is worth noting for the
remaining conversion: **the append makes the write simpler and the correct read less obvious.** The
sparse correction row is what keeps the damage contained to the snapshot column — `sum(row_count)`,
`max(max_ts)` and `count(row_count)` all stay correct read directly, on new and migrated tables
alike.

**4. The append does NOT remove the SQL writes, and an earlier revision of this spec claimed it
did.** Correcting that here, because the claim was load-bearing in the wrong direction: it was used
to argue the startup check guards nothing.

What the append changed is the *kind* of statement on the conflict path, from `UPDATE` to `INSERT`.
It did not change that the watermark path writes with SQL. With #514 merged, each batch with a
watermark still issues:

| Write | Statement | Inlinable at the default limit |
|---|---|---|
| The batch's watermark rows | `INSERT ... VALUES`, inside the ingest transaction | yes |
| A snapshot correction, on the rare conflict | `INSERT ... VALUES`, after commit | yes |

So **the check is fully load-bearing today**, on the most ordinary path there is — one insert per
batch per queue, not a rare repair. The earlier reasoning conflated the append with the
write-then-register conversion; only the latter removes the SQL, and only if it covers the
correction as well as the rows. Until then an operator who enables inlining for a watermark table
gets exactly the fault this spec exists to prevent, on every batch.

What the append did remove is the `UPDATE`, and with it the worst *shape* of the fault. That
measurement stands and is worth keeping: at the default limit the old tightening put **one row in
`ducklake_inlined_data_<n>` and one in `ducklake_inlined_delete_<n>`** and left the registered file
unexpired, so the file said `snapshot_id = 4` where the table read `2` — a registered Parquet file
contradicting the table it belongs to. An inlined `INSERT` is merely a row in the wrong store; an
inlined `UPDATE` was a contradiction. Both are prevented by the same rule.

**When the check does become forward-looking:** once the sidecar conversion covers both writes in
the table above, nothing on the ingestion path issues row-level SQL and the check guards only a
write someone else makes into these tables, or a DML path added later. That is a reason to keep it
cheap and well-tested, not a reason to drop it — but the spec should not pretend the condition has
already arrived.

## Failure message

Naming the table and quoting the remedy, since the fix is not guessable:

```
Data inlining is enabled for 'my_catalog.main.ingest_watermark' (10 rows, from the ATTACH
parameter DATA_INLINING_ROW_LIMIT), so its rows would be written into the catalog database
instead of storage, on the insertion path of every batch. Fix the source: remove
DATA_INLINING_ROW_LIMIT from the ATTACH of 'my_catalog'. If that catalog is not yours to
change, exempt this table instead:
  CALL my_catalog.set_option('data_inlining_row_limit', 0, table_name => 'main.ingest_watermark');
```

The source is named because the remedy follows from it, and the source's **own** fix comes first: a
table option is stored in the shared catalog and so applies to every process touching that table,
whereas an attach parameter is one line in this collector's startup script. Advising the stored
option for a local misconfiguration would be a global change to fix a local one.

## Why fail startup rather than warn

**Decided in review: it refuses to start.** The failure is silent and slow otherwise. Nothing errors, writes succeed, queries return correct
results — and the catalog grows without bound while the operator believes data is in object
storage. By the time it is noticed, the catalog holds rows that have to be flushed before anything
can be migrated or compacted. A collector that refuses to start names the problem at the one moment
someone is watching.

The counter-argument is that this makes the collector refuse to start over a configuration that
"works". That is accepted on the grounds that it works in a way nobody asked for; the fix is one
statement and the message contains it.

**An earlier revision of this spec said this premise had weakened, and that was wrong** — the
correction is under point 4 above. Replacing the conflict-path `UPDATE` with an append did not stop
the watermark writing with SQL: its rows are still inserted on every batch, and the correction is
still an insert. So the check guards the ordinary path, not a rare repair, and the case for failing
startup is the one it always was.

What the append did remove is the worst shape of the fault — an inlined `UPDATE` left a registered
file whose contents contradicted the table, where an inlined `INSERT` merely puts a row in the wrong
store. Both are prevented here.

## Scope

In: `DuckLakeInlining` in commons (disable, resolve the effective limit across all five sources,
render the remedy), the startup check over the watermark tables, and a test per source.

Also in, now that review has decided it: converting the watermark rows to a write-then-register, and
its conflict path to an append. That is typed-column support in `SidecarRows` (finding 1 above), the
watermark's sidecar write ahead of the transaction with its registration inside it, the required
`watermark_batch_id_column`, and the appended correction in place of `updateSnapshotIdSql` — which
is deleted, along with its use of `ducklake_table_insertions`.

**Status: the append half has landed** (#514), with the batch id column, the sparse correction, the
`UUID` type and the reader rules above. What is left in this spec is the write-then-register itself:
typed-column support in `SidecarRows` for both `TIMESTAMP` and `UUID`, and the watermark's sidecar
write — **for the correction as well as the rows**, since leaving the correction as an `INSERT`
keeps one inlinable write on the path and so keeps the check load-bearing. It is the same mechanism
either way: a one-row sparse sidecar registered after commit instead of an `INSERT`. That part depends on `SidecarRows` landing with the registry
(`doc/ATTRIBUTE_TYPE_REGISTRY_SPEC.md`) — the dependency runs that way only, and it is a sequencing
one, not a design one.

Documentation is in scope, not a follow-up: the snapshot id column's contract changes for every
consumer. It stays a lower bound, but the exact value now comes from the latest correction for that
batch rather than from a value patched in place, so `WatermarkSpec`'s javadoc and the ingestion
documentation both need the reader rule written down.

Also in, as stated rather than discovered: the disable is **instance-wide and cannot be narrower**.
A per-connection `SET` was tried and does not exist in practice — `ConnectionPool` hands out
duplicates of one instance, and setting it on a duplicate moved it for all of them. So the default
moves for every catalog the process has attached, including ones it does not own. Acceptable,
because it only moves a default that every scoped option overrides, but it is a side effect that
belongs in the log line and in this spec rather than in a surprise. And an attach value that is not
a number must be reported as unknown rather than read with a cast that yields NULL and silently
falls through to the instance setting.

Out: anything about existing inlined rows. If a deployment already has them, the fix is
`ducklake_flush_inlined_data` — a compaction concern, and the collector should say so rather than
try to flush on someone's behalf.

## What review settled

1. **Refusing to start is right**, not a warning plus a metric. Decided on the inlined tightening
   that leaves a file disagreeing with its table — see the caveat under that section, since
   decision 3 removes that fault from this code and so weakens the premise.
2. **The watermark's `INSERT` becomes a write-then-register**, with the measured constraints in its
   own section above. In scope now rather than deferred.
3. **The conflict path's `UPDATE` becomes an append**, with the reader taking the latest snapshot
   per batch id. It needs `watermark_batch_id_column` — a required `UUID` column — to tell a
   correction from a second batch, and it makes the watermark table append-only like the registry,
   after which nothing on the ingestion path issues a row-level write. Implemented in #514,
   including the two reader-side corrections that implementing it exposed.

Settled earlier in review, with the evidence folded in above: the schema scope as a fifth source;
that registration is immune to inlining, so only tables written with SQL need checking; that the
limit covers deletes as well as inserts; and that the file churn from disabling inlining is
compaction's job, whereas data rows in a shared remote catalog are on every insert's critical path.

Nothing is left open. An earlier revision raised one consequence for review — that decision 3 left
the check guarding no write this code performs — and it was mistaken: the watermark still inserts
its rows and its corrections with SQL. The correction is under point 4 of the watermark section, and
*Why fail startup rather than warn* now states the case as it stands.
