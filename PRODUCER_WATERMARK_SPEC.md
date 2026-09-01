# Durable Producer Watermarks — Design Specification

## Overview

Replace the in-memory producer de-duplication in `BulkIngestQueue` with a **durable,
hash-sharded watermark table stored in DuckLake**, so that the de-duplication claim and the
data it protects commit or roll back together.

The key property: because the watermark tables and the data tables live in the **same
attached DuckLake database**, a single transaction can write both. That yields exactly-once
ingestion with no external coordinator, no producer-to-collector ownership assignment, and
no post-crash reconciliation protocol.

**Status:** specification. No implementation yet.

---

## Motivation

`BulkIngestQueue` today tracks the last accepted batch id per producer in memory
(`inProgressBatchIds`, an access-ordered `LinkedHashMap`). It has two documented failure
modes:

1. **Bounded.** Capped at `MAX_PRODUCER_IDS` (10,000). Past that, the least-recently-used
   producer is evicted and, in the words of the existing comment, "duplicate and ordering
   protection no longer applies to it".
2. **Volatile.** Lost entirely on restart. After a collector restarts, every producer's
   next batch is accepted regardless of what was already ingested.

With more than one collector the situation is worse: each holds its own map, so a producer
that fails over from one collector to another is not de-duplicated at all.

---

## Measured behaviour this design depends on

All measured against DuckDB v1.5.5, `ducklake` extension `d8a1881e`, PostgreSQL 16 catalog,
concurrent writers as separate OS processes.

| Observation | Result |
|---|---|
| Claim plus data write in one DuckLake transaction | Atomic. `ROLLBACK` discards both. |
| Four writers racing the same producer and batch id | 1 commit, 3 `Transaction conflict`, exactly 1 data row |
| Four writers updating **different rows** of one table | 1 commit, **3 conflicts** |
| Four writers updating four **separate tables** | **4 commits, 0 conflicts** |
| Four writers updating four different **partitions** of one table | 1 commit, 3 conflicts |
| Appends (`INSERT` only), 16 concurrent writers | **16 commits, 0 conflicts**, snapshot ids unique and gapless |
| `DELETE` racing concurrent `INSERT`s | `DELETE` conflicts and loses; inserts commit |

Three consequences drive the whole design:

- **`UPDATE` conflicts are detected at table granularity.** Unrelated producers collide
  merely by sharing a table.
- **Partitioning does not scope conflicts; separate tables do.** Hence sharding into
  physical tables rather than partitions.
- **Appends never conflict.** Anything on the append path is free of contention.

---

## Design

### Watermark tables

`N` physical DuckLake tables, `N` a power of two:

```sql
CREATE TABLE producer_watermark_0 (
    producer_id    VARCHAR,
    last_batch_id  BIGINT,
    updated_at     TIMESTAMP
);
-- ... through producer_watermark_{N-1}
```

Each table holds one row per producer whose hash maps to that shard, plus one reserved
sentinel row (see "Registering a new producer").

Inlining should be **disabled** for these tables. DuckLake does not push predicates down to
inlined data — a point lookup reads the entire inlined table, and an index created on it by
hand is never consulted. Partitioned Parquet prunes on read; measured at 100,000 rows,
8.3 ms per lookup partitioned versus 30.0 ms inlined.

### Shard assignment

```
shard(producerId) = murmur3_32(producerId) & (N - 1)
```

Use an explicitly specified hash, not `String.hashCode`, so the mapping is stable across
JVM implementations and reproducible from SQL for operational queries. `N` is fixed at
catalog creation; changing it requires a rebuild (see "Open questions").

### Where the claim happens

`DuckLakePostIngestionTask` already registers written files and inserts watermark rows in a
single transaction. The producer claim joins that same transaction. It needs no new
plumbing: `IngestionResult.maxProducerIds` is already a `Map` of producer id to the highest
batch id in the bucket, populated by `Bucket.add`.

Within the existing transaction, for each producer in `maxProducerIds`:

```sql
UPDATE producer_watermark_{shard}
   SET last_batch_id = ?, updated_at = now()
 WHERE producer_id = ?
   AND last_batch_id < ?;
```

Then register files as today, and commit. If any statement reports zero rows affected, the
transaction is aborted (see "Rejection").

### Bucket-to-shard alignment (the central constraint)

A bucket may contain batches from many producers, which may hash to many shards. A
transaction touching `K` shard tables conflicts with any concurrent transaction touching any
of those `K`. If buckets routinely span all `N` shards, sharding buys nothing — every write
conflicts with every other write.

**Therefore batches must be routed to shard-aligned queues.** `BulkIngestQueue` instances
become keyed by `(queueId, shard)` rather than `queueId` alone, and a batch is routed by
`shard(producerId)`. Each write transaction then touches exactly one watermark table.

This is the most invasive part of the change and should be estimated first. The alternative
— leaving buckets unaligned — makes the rest of the design pointless, so it is not optional.

Cost: `N` times as many queues per logical queue id, each with its own writer thread and
its own bucket accumulating toward `min_bucket_size`. Small producers spread thinly across
shards will produce smaller files. `N` therefore trades write concurrency against file size
and thread count.

### Registering a new producer

The first batch from an unknown producer has no row to update, so the `UPDATE` matches
nothing and the claim would fall through to an `INSERT`. Appends do not conflict, so two
collectors seeing the same new producer simultaneously would both insert and both accept —
a duplicate.

To close this, every shard table carries a reserved sentinel row with
`producer_id = '__shard__'`. Producer creation runs in the same transaction as:

```sql
UPDATE producer_watermark_{shard}
   SET updated_at = now()
 WHERE producer_id = '__shard__';

INSERT INTO producer_watermark_{shard} VALUES (?, ?, now());
```

The sentinel `UPDATE` forces table-granularity conflict detection to serialize concurrent
creations within the shard, using the same mechanism that already serializes claims. Reads
must exclude the sentinel row.

### Rejection

A zero-row `UPDATE` means the batch id is not greater than what is already committed: a
replay, or an out-of-order batch. The existing `OutOfSequenceBatch` exception is the right
signal.

The complication is that by the time the post-ingestion transaction runs, the COPY has
already written a Parquet file containing every batch in the bucket, including any duplicate.
Rows cannot be selectively withdrawn from a written file.

**Resolution:** abort the whole bucket. Fail the affected futures with `OutOfSequenceBatch`,
fail the rest with the existing "batch could not be written" path so producers retry, and
leave the file unregistered for `ducklake_cleanup_old_files` to remove. The existing
`rollbackProducerSequences` logic already handles rolling the in-memory state back.

This is expensive, and it is why the in-memory cache below matters: it makes the expensive
path rare rather than routine.

### The in-memory map becomes a cache

`inProgressBatchIds` is retained, with its meaning changed from *authority* to *read cache*:

- On `add`, reject obvious duplicates from the cache exactly as today. This keeps the common
  case cheap and avoids buffering data that is going to be rejected.
- On a cache miss, read the shard table and populate the entry.
- On abort or restart, discard the entry and re-read.

Correctness no longer depends on the cache being complete or surviving restart, so the LRU
eviction that today silently drops protection becomes merely a performance concern.

### Conflict retry

A conflicting transaction fails with `Transaction conflict` and must be retried. Retries are
bounded exponential backoff with jitter, configurable, and surfaced as a metric. The
transaction must be replayable: it derives everything from `IngestionResult`, so retrying is
a matter of re-running the same statements against a fresh connection.

Note that `DELETE` loses to concurrent `INSERT`s, so any compaction of these tables must be
idempotent and expect to be starved under sustained write load.

---

## Configuration

Under the existing `dazzleduck_server` HOCON tree:

```hocon
ingestion.producer_watermark {
    enabled            = true
    shards             = 64        # power of two, fixed at catalog creation
    table_prefix       = "producer_watermark_"
    catalog            = "ducklake"
    retry.max_attempts = 5
    retry.initial_ms   = 20
    retry.max_ms       = 2000
}
```

When `enabled = false` the behaviour is exactly today's in-memory-only path, so the change
can be rolled out and rolled back per deployment.

---

## Metrics

Registered through the existing Micrometer plumbing:

- `ingestion.watermark.claim.conflicts` — transactions retried after `Transaction conflict`
- `ingestion.watermark.claim.retries` — total retry attempts
- `ingestion.watermark.rejected` — batches rejected as duplicate or out of order
- `ingestion.watermark.bucket.aborted` — buckets aborted by a failed claim
- `ingestion.watermark.cache.miss` — shard reads caused by a cache miss
- `ingestion.watermark.claim.duration` — timer around the claim statements

The existing `producerIdEvictions` counter keeps its meaning but stops indicating a
correctness risk.

---

## Testing

1. **Atomicity.** Claim plus data write in one transaction; force a failure between them and
   assert neither is visible.
2. **Concurrent duplicate.** `M` threads submit the same `(producerId, batchId)`; assert
   exactly one commit and exactly one data row.
3. **Restart.** Ingest, restart the collector with a cold cache, replay the same batches;
   assert all are rejected.
4. **Failover.** Two collectors against one catalog; producer switches between them mid
   sequence; assert no duplicate and no gap.
5. **New producer race.** `M` threads submit the first batch for the same unknown producer;
   assert exactly one row is created and one batch accepted.
6. **Shard isolation.** Writers on distinct shards run concurrently without conflicts;
   assert throughput scales with `N` and conflicts stay near zero.
7. **Abort path.** Inject a duplicate into a bucket containing several producers; assert the
   bucket aborts, futures fail with the right exceptions, and no file is registered.
8. **Cache-miss correctness.** Clear the cache mid-run; assert behaviour is unchanged.

Use `SharedTestServer` and testcontainers PostgreSQL, following
`DuckLakeWatermarkPostIngestionTest`. Concurrency tests must hold a JUnit `ResourceLock` on
the singleton connection, as the client module tests now do.

---

## Migration

The feature is additive and defaults off. Enabling it on an existing catalog:

1. Create the `N` shard tables and their sentinel rows.
2. Optionally backfill from existing data where producer id and batch id are recorded;
   otherwise start empty, which re-accepts one already-ingested batch per producer.
3. Enable per deployment, watch the conflict and reject metrics, and roll back by setting
   `enabled = false` if needed.

Backfilling is preferable where the columns exist, since starting empty means the first
batch after enablement is unprotected.

---

## Limitations and deferred work

Two consequences of the design above are understood but **not addressed in the first
implementation**. Both have a known workaround, and both are deliberately deferred.

### L1. Sharding thins out buckets

Routing batches to shard-aligned queues means each logical queue id becomes `N` queues, each
accumulating its own bucket toward `min_bucket_size`. A producer population spread thinly
across shards therefore produces more, smaller Parquet files than today, and `N` times as
many writer threads.

This is a direct trade: `N` buys write concurrency and costs file size. It is most visible
where a queue has many low-volume producers, which is precisely the workload the ingestion
queue exists to batch.

**Workaround — combine shard-aligned buckets before writing.** `BulkIngestQueue` already has
`combineBuckets`, used today to merge buckets that fit within `maxSize` and `maxBatchCount`.
The same mechanism applies here with one added constraint: **only combine buckets belonging
to the same shard**. That keeps one watermark table per write transaction — the property the
whole design rests on — while restoring file sizes toward what an unsharded queue produces.

Combining across shards must be rejected, not merely discouraged: a combined bucket spanning
`K` shards makes its transaction conflict with any concurrent transaction touching any of
those `K`, which is the failure mode sharding exists to avoid.

Open: whether same-shard combining recovers enough file size at realistic `N`, or whether
`N` must be kept small enough that combining is rarely needed. This needs measuring against
a real producer distribution.

### L2. A rejected batch aborts its whole bucket

By the time the post-ingestion transaction runs, the COPY has written a single Parquet file
containing every batch in the bucket. If the claim finds one producer's batch id is not
greater than what is already committed, that file contains rows which must not be ingested,
and rows cannot be selectively withdrawn from a written file.

The first implementation therefore aborts the entire bucket: every future fails, the file is
left unregistered for `ducklake_cleanup_old_files`, and producers retry. Batches from
unrelated producers that happened to share the bucket are punished for a duplicate they had
nothing to do with.

The in-memory cache makes this rare rather than routine, but "rare" is not "never" — it
happens whenever two collectors genuinely race, which is exactly the case this design exists
to handle correctly.

**Workaround — rewrite the file excluding rejected batches.** Rather than discarding the
bucket, rewrite the output within the same transaction:

1. The claim identifies which producers were rejected (their `UPDATE` matched zero rows).
2. Rewrite the Parquet file from the same source relation with those producers excluded,
   for example `WHERE producer_id NOT IN (...)`.
3. Register the rewritten file, commit the claims that did succeed, and fail only the
   rejected producers' futures with `OutOfSequenceBatch`.
4. Leave the original file unregistered for cleanup.

This keeps a single duplicate from costing an entire bucket's work, at the price of a second
write on the rejection path. It requires the source relation to still be reconstructible at
post-ingestion time — `ParquetIngestionQueue.constructSourceRelation` builds it from the
`WriteTask`, so the inputs are available, but the write path currently discards them after
the COPY. Confirming that lifetime is the first task if this is picked up.

Open: whether the rewrite belongs in the same transaction as the claim, or as a compensating
write afterwards. In-transaction is cleaner but lengthens a transaction that already holds a
table-granularity conflict window.

### Status

Both are to be revisited after the base implementation lands and the conflict, reject and
abort metrics show how often each path is actually taken. Implementing either before there
is data on the rejection rate risks optimizing a path that turns out to be cold.

---

## Open questions

- **Resharding.** `N` is fixed. Growing it remaps producers and invalidates existing rows.
  A rebuild procedure — read all shards, rewrite under the new `N` — is straightforward but
  needs a quiet window, and none is specified here.
- **Choosing `N`.** Conflicts fall as `N` rises, but so does average file size, and thread
  count rises. This needs measuring against a real producer population; the tests behind this
  spec only went to 16-way concurrency on small tables.
- **Watermark table growth.** One row per producer forever. Producers that disappear leave
  rows behind. Pruning them requires `DELETE`, which loses to concurrent inserts and needs a
  quiet window.
- **Interaction with the DuckLake watermark.** `WatermarkSpec` already inserts rows in this
  same transaction. Both are appends and should not conflict with each other, but the
  combined transaction has not been tested.
- **Upstream dependency.** If DuckLake gains row-granularity conflict detection for inlined
  tables, sharding and the queue-alignment change both become unnecessary. Enhancement
  requests have been drafted; this design deliberately does not depend on them landing.
