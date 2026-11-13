# OTel Attribute Type Registry — Design Specification

Status: **agreed, not yet merged.** This is the design to review; the implementation follows as its
own PR. Everything below has been prototyped against the real ingestion path and the numbers are
measured, not estimated — so the open points are the design's, not unknowns about feasibility. The
one item left is a confirmation rather than a question (see the last section).

Builds on the typed attribute maps shipped in 0.2.24 (#506).

## Problem

The collector writes each attribute to the map column its OTLP value type selects —
`attributes`, `attributes_int`, `attributes_double`, `attributes_bool`, `attributes_bytes`,
`attributes_json` (shipped in 0.2.24). The same attribute name can arrive with different types
over time: a service sends `x` as a string in one release and as an int in the next, so its values
move from `attributes` to `attributes_int`. Nothing in the data records that, so finding `x` means
searching every map.

Goals: answer "which types has `x` had, and when did each first appear?" without scanning data;
let a tool pick the right typed map or build a unified view; and cost no measurable collector
throughput.

Non-goals: enforcing or coercing types (ingestion stays schema-on-read — a type change is
recorded, not rejected); value statistics; exact first-seen times across a fleet.

## The design

| | |
|---|---|
| **Where** | Whatever table each stream's own config names — `attribute_types_table` in that queue's mapping, resolved in the queue's catalog and schema |
| **How** | A Parquet file written before the transaction, then registered — the same semantics as the data itself, not an `INSERT` |
| **When** | Registered by the same transaction that registers the batch's data files |
| **Created by** | The collector, **at startup**, before the gRPC server accepts traffic; startup fails if it cannot be |
| **Inlining** | Cannot affect these rows — registering a file is not an insert (see below) |
| **Shape** | Append-only, duplicates tolerated, nothing ever updated or deleted |
| **Dedup** | A per-process cache of committed mappings, read from the tables at startup |
| **Scope** | DuckLake queues only; opt-in per stream |

### Key and table

A mapping is `(ingestion_queue, signal, attribute_scope, attribute_name, value_type)`:

```sql
CREATE TABLE <attribute_types_table> (
    ingestion_queue  VARCHAR     NOT NULL,
    signal           VARCHAR     NOT NULL,   -- logs | traces | metrics
    attribute_scope  VARCHAR     NOT NULL,   -- attribute | resource | event | link
    attribute_name   VARCHAR     NOT NULL,
    value_type       VARCHAR     NOT NULL,   -- string | int | double | bool | bytes
                                             -- | array | kvlist | empty
    first_seen       TIMESTAMPTZ NOT NULL,
    collector_id     VARCHAR);
```

`array` and `kvlist` stay distinct although both land in `attributes_json`, because the key is the
OTLP `AnyValue` case, not the column.

### Configuration

Per stream, in the queue's own mapping — optional, and the only switch that turns recording on.
How many tables there end up being is whatever these name:

```hocon
ingestion_queue_table_mapping = [
    {
        ingestion_queue = "logs"
        catalog = "my_catalog", schema = "main", table = "logs"
        additional_parameters {
            attribute_types_table = "otel_attribute_types"
        }
    },
    {
        # Same schema, same name: this stream shares the table above.
        ingestion_queue = "traces"
        catalog = "my_catalog", schema = "main", table = "spans"
        additional_parameters {
            attribute_types_table = "otel_attribute_types"
        }
    },
    {
        # Same schema, a different name: its own table, next to the one above.
        ingestion_queue = "audit"
        catalog = "my_catalog", schema = "main", table = "audit"
        additional_parameters {
            attribute_types_table = "audit_attribute_types"
        }
    },
    {
        # No parameter: this stream records nothing and is not even scanned.
        ingestion_queue = "debug"
        catalog = "my_catalog", schema = "main", table = "debug"
    }
]
```

Process-wide, with nothing in it that names a table:

```hocon
otel_collector {
    attribute_types {
        enabled = true                   # honour those per-queue tables at all; false turns it off everywhere
        max_entries = 100000             # process-wide cap on mappings held in memory: the heap bound
        max_entries_per_stream = 10000   # per-queue cap: contains one stream's cardinality to itself
    }
}
```

A queue may raise or lower its own cap, for a stream legitimately wider than the default:

```hocon
ingestion_queue_table_mapping = [
    {
        ingestion_queue = "wide"
        attribute_types_table = "otel_attribute_types"
        attribute_types_max_entries = 50000   # this stream only; still bounded by max_entries
    }
]
```

Both levels are optional and default to harmless: `enabled` is on, but with no queue naming a table
nothing is recorded, nothing is scanned, and nothing is created at startup.

### Query patterns

```sql
-- the current type of each attribute
SELECT ingestion_queue, signal, attribute_scope, attribute_name,
       arg_max(value_type, first_seen) AS current_type
FROM otel_attribute_types GROUP BY ALL;

-- the ones that have had more than one
SELECT ingestion_queue, signal, attribute_scope, attribute_name,
       list(DISTINCT value_type) AS types
FROM otel_attribute_types GROUP BY ALL
HAVING count(DISTINCT value_type) > 1;

-- the deduplicated history, earliest sighting across collectors
SELECT ingestion_queue, signal, attribute_scope, attribute_name, value_type,
       min(first_seen) AS first_seen
FROM otel_attribute_types GROUP BY ALL;
```

Every read groups, because the table is append-only and duplicates are expected.

## Decisions

**The table is per-stream config, not a fixed layout:** an unqualified name in the queue's mapping,
resolved in that queue's own catalog and schema. So the
registry lands beside the tables it describes and a query can join them, and the operator decides
everything else — two streams may name one table and share it, their rows kept apart by
`ingestion_queue`, or each may name its own, and a stream that names none records nothing.

Nothing here imposes one table per schema, or any other arrangement; the shape of the deployment is
the config's to choose. What follows from it is only that resolution happens in
`DuckLakePostIngestionTask`, the one place that knows which catalog and schema a batch went to —
which is also why the name cannot be configured process-wide: with a dynamic queue registry the
collector does not know a queue's schema until a batch arrives.

**Written in the ingestion transaction, not by a background writer.** A background writer was
built first and replaced. The transaction gives the property that matters — a registered file
always has its mappings committed — and costs the property it had: a row the database rejects now
fails the batch, where a writer would have retried it off the ingest path. That is mitigated, not
eliminated: every statement is generated from a fixed column list with escaped literals, every
identifier is checked against `[A-Za-z_][A-Za-z0-9_]*`, and an attribute name that cannot be
written as a SQL literal at all (a NUL byte) is dropped rather than sent. The mechanism is
`SidecarRows` in commons: rows derived by
whoever built the batch, from information the write path never sees.

A failed registration leaves the mapping file unreferenced, which is the same outcome the batch's
own data file has and is cleaned up by the same `ducklake_cleanup_old_files` path.

**The collector creates and checks the table at startup, not while processing.** For each distinct
table its configured streams name, `CREATE TABLE IF NOT EXISTS` with the schema above, in that
stream's catalog and schema, before the gRPC server accepts a single export. The same statement
does both jobs: it creates a table that is missing, and it fails on a table that cannot be created
— a catalog that is not attached, a schema that does not exist, a name taken by something else,
insufficient permission. **A failure fails startup.**

Doing it at startup rather than in the ingestion transaction is what makes a misconfigured stream
a startup error with a message naming the table, instead of a batch that fails at commit once
traffic arrives and then fails forever. It is the same fail-fast rule the collector already applies
to a broken ingestion provider or startup script, and for the same reason: the alternative is a
process that looks healthy while dropping work on the floor.

It also keeps DDL out of the ingestion transaction entirely. That transaction now does exactly one
kind of thing — register files — so there is no DDL mixed with `ducklake_add_data_files` and no
per-batch catalog check for a table that was already there. Startup creates the table's data
directory too, since the file written per batch has to land in it.

The trade: a table dropped under a running collector is no longer silently recreated, so that
stream's batches fail until it is restored. That is the same class of operator action as dropping
the stream's target table, which breaks it just as thoroughly, and failing loudly beats recreating
a table someone deliberately removed.

**Written as a Parquet file and registered, not inserted.** The ingest path never inserts: it
`COPY`s to Parquet and calls `ducklake_add_data_files`, and these rows follow the same semantics.
The file is written before the transaction, exactly as the batch's own data file is, and registered
inside it.

Three things that follow, all verified against DuckLake rather than assumed:

- `ducklake_add_data_files` **references the file in place** — it copies nothing into the data
  path. So the file has to be written somewhere durable, not a scratch directory: it goes in the
  registry table's own data directory, which the catalog metadata gives from
  `ducklake_schema`/`ducklake_table`/`data_path` — the same query the ingestion handler already
  uses to find a queue's target path. Startup creates that directory along with the table.
- Two `ducklake_add_data_files` calls commit together in one transaction, so the batch's data file
  and its mapping file land or roll back as one.
- Duplicates accumulate as separate small files, and a reader's `GROUP BY` with `min(first_seen)`
  reads across them unchanged.

The cost is one small Parquet file per new-mapping event. That is per *first appearance* of a
mapping, not per batch — zero files in steady state — and the collector's own compaction
(`ducklake_merge_adjacent_files`, already configurable per catalog) merges what accumulates. An
`INSERT` would avoid those files, at the price of being the only write in the collector that does
not go through Parquet, and of depending on the catalog's write path rather than object storage.

**Append-only, duplicates tolerated.** Nothing is read, updated or deleted, so a write cannot
conflict with another process recording the same value. Two collectors seeing a mapping for the
first time both record it; a reader deduplicates with `GROUP BY` over the five key columns, taking
`min(first_seen)`. An earlier design used an anti-join to converge on one row; it was dropped
because tolerating duplicates is cheaper and conflict-free, which matters more than a tidy table.

**The cache is read at startup, in the same pass.** Having created or checked each table, the
collector reads it: one query per distinct table, restricted to the streams this process serves, so
a restart does not re-append what it already recorded and another process's streams are not cached
for nothing. A read that fails also fails startup — the table was just verified, so a failure here
is a real problem rather than the empty-table case, which is simply a new deployment with nothing
recorded yet.

**A batch carries every mapping that is not yet committed, and is settled by its own outcome.** A
mapping is marked known only once a batch that carried it has committed; a batch that fails changes
nothing. Marking a mapping known when it was merely sent would lose it on any rollback.

What a batch carries is decided against committed mappings only — *not* against what other batches
have in flight. An earlier draft skipped a mapping another in-flight batch was already carrying,
which is wrong, and in a way that defeats the whole point of the in-transaction write. With batches
A and B both containing a new `x:int`: A is built and carries it; B is built while A is in flight,
sees it as already handled and carries nothing; B commits; A fails. B's files are registered with
no `x:int` row to explain them. A later batch would repair it, but until then — and forever, if `x`
never arrives as an int again — the data holds a mapping the registry denies.

Carrying it on every batch gives the invariant instead: **a batch that does not carry a mapping its
data contains is one whose mapping was already committed**, since `known` only ever gains a mapping
from a commit that included it. The cost is a duplicate row per overlapping batch, on the first
appearance of a mapping only, bounded by how many batches are in flight at once — which the
append-only table and its grouping readers already absorb.

## Keeping it off the hot path

Per-attribute work dominates collector CPU, and the obvious implementation — hash each key, probe
a set, once per attribute per record — is the one already rejected in this exact loop: memoising
encoded keys by `HashMap.computeIfAbsent` cost **11.8% of collector Java CPU**, because protobuf
materialises a fresh `String` per message so `String`'s cached hash never carries over between
records (see the note in `MapColumnWriter`). Nothing in the detection path hashes a key.

Instead (`AttributeTypeScan`): each attribute set keeps the previous row's key list — protobuf's
own `String` references, never copied — and that row's `ValueCase` ordinals. A row whose keys and
kinds repeat the previous row's, in order, is skipped outright, and OTLP producers emit the same
layout record after record. A resource attribute list short-circuits on object identity, because
one `Resource` covers every record of an export. A row that differs is walked once into a
per-batch set. This is sound because every row is either collected itself or identical to the row
before it, which was.

Measured on the prototype, timing `LogRecordBatchWriter.write` over wire-parsed batches of 40
attributes per record and 17 resource attributes, 21 alternating rounds: the scan adds **66
ns/record at the minimum and 185 at the median**, against 2,600–2,900 ns/record for the batch
writer itself.

End-to-end is less conclusive and worth saying plainly. Four 90-second runs at the benchmark's
15.5k rec/s profile, measuring whole-process CPU (the load generator runs in the same process, so
this understates the collector's own share):

| | registry off | registry on |
|---|---|---|
| pair 1 (off first) | 88.0s | 91.5s |
| pair 2 (on first) | 88.7s | 88.3s |

That is +4.0% and −0.5%, averaging **+1.8%** — consistent with the per-call figure above, and
inside the 3% target on average, but not separated from run-to-run noise by these four runs. A
tighter measurement (more pairs, or JFR as in the typed-maps work) is deferred rather than treated
as settled; the per-call figure is the one to rely on until then.
`-Dbenchmark.attribute_types=true` reproduces the A/B.

## Memory

A process may run thousands of streams, and a set of mappings would cost an entry per
`(queue, signal, scope, name, type)` — hundreds of thousands of map nodes, each with a five-field
key object, and the queue id arriving as a fresh `String` per request. `AttributeTypeCache` holds
the same information as:

- one entry per **stream and signal** (at most three per stream);
- one entry per **attribute name** within it, valued by an `int` bitmask with one bit per
  `(scope, type)` pair — four scopes by eight types is exactly 32 bits;
- one **shared instance** of each name across every stream that records it.

So a name's second scope or type costs a bit rather than an entry, and a name common to a thousand
streams is stored once.

Two caps bound what this cache holds. Neither is a limit on the table, which is append-only and
unbounded. They exist because attribute *names* are client-controlled and can be: a producer that
puts an id in the name rather than the value — `user.8f21ac.action` — yields a new mapping per id
forever, which without a bound grows the cache until the collector runs out of heap and appends a
row per id on the way.

- **`max_entries`** (default 100,000, process-wide) is the **heap bound**. It is what stops the
  process dying, and it is sized against memory rather than against any stream's expected width.
- **`max_entries_per_stream`** (default 10,000, overridable per queue with
  `attribute_types_max_entries`) is the **containment and attribution bound**. It counts the
  mappings claimed for one ingestion queue, summed across that queue's signals.

Decided in review: the per-stream cap is needed, because the process-wide one alone has a failure
mode that is worse than the problem it solves. One stream sending id-bearing names consumes the
entire budget, and every *other* stream then silently stops recording new mappings — streams that
were behaving correctly lose their description because of a neighbour. The global counter climbing
says only that it happened somewhere. The per-stream cap keeps the damage inside the stream causing
it and lets the log and the `dropped` counter name that stream, which is the signal an operator can
act on.

Whichever cap is reached first stops new claims for that stream; the two are not redundant. With
thousands of streams in a process the per-stream default multiplied by the stream count far exceeds
the global cap, so `max_entries` stays the thing that actually protects the heap, and
`max_entries_per_stream` is what makes a single bad producer diagnosable instead of a process-wide
degradation. Raising a queue's own cap cannot escape `max_entries`.

At the cap new mappings are simply not claimed, so no row is written for them; they are counted
(`otel.attribute_types.dropped`, already tagged with the queue) and logged once per stream rather
than per drop. Nothing fails and nothing already recorded stops working — ingestion is untouched and
the attributes still land in their typed maps; only the description of them stops being extended. At
the defaults the cache is of the order of a few megabytes, less when streams share names, which they
normally do.

## Observability

Micrometer meters, tagged with `queue` like the collector's existing ones except where noted:

| Meter | Type | Meaning |
|---|---|---|
| `otel.attribute_types.new` | counter | mappings seen for the first time |
| `otel.attribute_types.persisted` | counter | mappings committed with a batch's files |
| `otel.attribute_types.rollbacks` | counter | mappings released because their batch did not commit; a later batch carries them again |
| `otel.attribute_types.dropped` | counter | mappings not recorded: past either cap, or a name that cannot be written as a SQL literal |
| `otel.attribute_types.in_flight` | gauge | mappings carried on a batch that has not committed yet (process-wide) |
| `otel.attribute_types.known` | gauge | mappings the cache holds (process-wide) |

A non-zero `rollbacks` rate means batches are failing, which the writer metrics already report. A
climbing `dropped` means a cap is too small for the attribute cardinality being sent — and because
it is tagged with `queue`, it says *which stream* is sending it, which is the point of having the
per-stream cap at all. `dropped` on one queue while others stay flat is a producer problem in that
stream; `dropped` appearing across every queue at once means the process-wide `max_entries` is the
one being hit.

## Open questions, and how they are answered here

1. **Attributes with no value set** — recorded as `value_type = 'empty'` rather than skipped. The
   key is the `AnyValue` case, and a name that only ever arrives unset is exactly what a reader
   cannot otherwise explain.
2. **Metric name as a dimension** — no. It would multiply rows by metric cardinality, and the
   question the registry answers ("which map holds `x`?") is per queue, signal and scope. It can
   be added as a column later without changing what is there.
3. **Event name as a dimension** — no, same reasoning.
4. **Detection fallback** — not needed; the previous-row filter met the target (above).
5. **Where the registry lives** — wherever each stream's config says. Not a fixed layout (above).
6. **Retention** — none. The table grows only with distinct mappings per stream, and the caps bound
   what any one process will add. Duplicates make it grow with collector restarts, which a
   reader's `GROUP BY` absorbs; if that ever matters, a periodic `DELETE`-and-reinsert of the
   deduplicated set is a compaction job, not a collector concern.

## Data inlining does not apply here

Worth stating, because an earlier draft of this spec claimed the opposite. DuckLake can keep a small
write in the catalog database instead of a Parquet file, which would defeat the decision above — but
it cannot happen to these rows. Inlining applies to `INSERT`, `UPDATE` and `DELETE` plans, not to
registering a file: `ducklake_add_data_files` with a one-row file at the default inlining limit of
10 registered it as a data file and put nothing in the catalog's inlined-data tables.

So writing Parquet and registering it is immune by construction, which is one more point in its
favour. The separate no-inlining rule (`doc/DUCKLAKE_NO_INLINING_SPEC.md`) covered the one ingestion
path that inserted, the watermark, which has since been removed. **This spec does not depend on it.**

## Deliberately out of scope

- **A generated view.** The registry makes "one correctly typed column per attribute" possible;
  generating it is a follow-up, not part of this.
- **Dynamic queues.** They cannot record, because `DynamicQueueRepository` stores no
  `additional_parameters`. Lifting it means changing the queue registry's schema, which is a change
  of its own.
- **`/stats`.** The cache size and in-flight count are Micrometer gauges; putting them on the
  dashboard can come later.

## What review settled, and what is left

One item is left, and it is a confirmation rather than an open design question:

1. The atomicity trade — a rejected row fails the batch. Accepted here, with the mitigations above;
   worth confirming that is the right call versus never touching ingestion.

Settled in review:

- **`max_entries` is per stream as well as per process.** `max_entries_per_stream` (default 10,000,
  overridable per queue) contains one producer's name cardinality to the stream sending it, and
  makes the `dropped` counter and the log name that stream; the process-wide cap stays as the heap
  bound. Sizing and the interaction between the two are under Memory above.
- **The collector creates and checks each table at startup**, failing startup if it cannot, so a
  misconfigured stream is a startup error rather than a batch that fails at commit forever — and the
  ingestion transaction carries no DDL.
- **A batch carries every uncommitted mapping**, rather than skipping one another batch has in
  flight.
- **The rows are written as a Parquet file and registered**, following the data path's semantics
  rather than inserting.

Deferred: a tighter end-to-end CPU measurement. Not a gate on the implementation.
