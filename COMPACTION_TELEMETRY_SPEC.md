# Compaction Run Telemetry — Design Specification

## Overview

Per-cycle telemetry for `dazzleduck-sql-ducklake-compactor`, so that an adaptive
compaction controller can later be designed against measured behaviour rather
than guesses.

**Scope: capture only.** This spec defines the per-run record, where each field
comes from, and what it costs to collect. It deliberately does *not* define the
control algorithm, change tier scheduling, or alter any existing behaviour. Ship
this, run it across varied load for a few days, then design the controller
against the resulting data.

**Guiding constraint: capture must not add catalog round-trips.** The compactor
already issues ~4 metadata queries per cycle; at a 1-second cadence that reached
roughly 400/minute against a catalog shared with the ingest path. Telemetry that
makes this worse is self-defeating.

### Motivation

Tier sizing is currently done blind. Over one day of manual tuning on the
benchmark the major tier went cadence 5 min → 3 min → 1 s → 2 min and batch
200 → 10 → 30 → 10, with each step chosen from a guess and validated only by
watching a band count. Two of those settings failed outright:

| setting | outcome |
|---|---|
| `max_compacted_files = 200` | spilled 37 GB, hit `max_temp_directory_size`, cycle failed |
| `max_compacted_files = 30`, cadence 1 s | ~6 min cycle, exceeded the catalog's `idle_in_transaction_session_timeout`, commit died with `Failed to execute query "ROLLBACK"` |
| `max_compacted_files = 10`, cadence 1 s | drained a 2,934-file backlog to 122 in 46 min, zero failures |

Identical cadence in the last two rows; batch size was the whole difference. The
signal that distinguished them was **cycle duration against a hard external
limit** — not memory, and not spill, which was 0 throughout.

---

## The Record

One record per compaction cycle, per `(database, tier)`.

### Identity and timing

| field | type | source | cost |
|---|---|---|---|
| `run_id` | `long` | monotonic counter per (database, tier) | free |
| `database` | `String` | `runTier` argument | free |
| `tier_name` | `String` | `CompactionTier.name()` | free |
| `scheduled_at` | `Instant` | when the scheduler fired | free |
| `started_at` | `Instant` | top of `runTier` | free |
| `ended_at` | `Instant` | `finally` block — already stamped today | free |
| `intended_delay_ms` | `long` | `tier.frequency()` | free |
| `actual_gap_ms` | `long` | previous `ended_at` → this `started_at` | free |

`actual_gap_ms ≈ 0` means the scheduler is saturated and cadence has stopped
being a control variable — `scheduleAtFixedRate` runs back-to-back once a cycle
outruns its interval.

### Work done

| field | type | source | cost |
|---|---|---|---|
| `band_files_before` | `Long` | `queryFileCount()` — already called | existing |
| `band_files_after` | `Long` | `queryFileCount()` — already called | existing |
| `files_retired` | `Long` | `before - after`, only when both present | free |
| `band_bytes_before` | `Long` | **add `SUM(file_size_bytes)` to the existing COUNT query** | same query |
| `band_bytes_after` | `Long` | same | same query |
| `groups_requested` | `long` | `tier.maxCompactedFiles()` | free |
| `groups_merged` | `Long` | result of `ducklake_merge_adjacent_files`, if available — see Q2 | free if available |

**Bytes, not file counts, are the real cost driver.** `max_compacted_files`
bounds merge *groups*, and a group may hold 2 files or 200. Ten groups can be
50 MB or 5 GB. Every sizing failure recorded above came from tuning a count
while the bytes varied underneath. Adding `SUM(file_size_bytes)` to the COUNT
query that already runs costs one extra aggregate in the same round trip.

### Duration — split, not total

| field | type | source | cost |
|---|---|---|---|
| `duration_total_ms` | `long` | `started_at` → `ended_at` | free |
| `duration_merge_ms` | `long` | around `statement.execute(sql)` in `DuckDbTierCompactor` | free |
| `duration_commit_ms` | `long` | around the commit, mirroring `ParquetIngestionQueue:120` | free |

A single total will not tell you which half to shrink. `ParquetIngestionQueue`
already logs exactly this split for the ingest path
(`commit phases: data(COPY)=Xms, postIngest(catalog)=Yms`) and it is what
identified COPY as ~83% of ingest commit time; the compactor should mirror it.

### Outcome

| field | type | source | cost |
|---|---|---|---|
| `outcome` | `enum` | `SUCCESS \| EMPTY \| FAILED` | free |
| `failure_class` | `enum` | classified from the exception — see taxonomy | free |
| `error_message` | `String` | truncated to ~200 chars | free |

### Resource headroom

| field | type | source | cost |
|---|---|---|---|
| `rss_peak_bytes` | `long` | `/proc/self/status` `VmHWM`, sampled at cycle end | local read |
| `spill_peak_bytes` | `long` | `temp_directory` size at cycle end | local stat |
| `memory_limit_bytes` | `long` | from the tier's `connection_settings` | free |

---

## Failure Taxonomy

Different classes demand opposite responses. Collapsing them will make any
controller oscillate — shrinking the batch in response to a transaction conflict
is exactly wrong.

| class | recognised by | meaning |
|---|---|---|
| `COMMIT_TIMEOUT` | `Failed to execute query "ROLLBACK"`; commit failure on a dead connection | cycle outran `idle_in_transaction_session_timeout`. Batch too large. |
| `TRANSACTION_CONFLICT` | `Transaction conflict` | another writer touched the same table. Batch size is *not* the problem — retry. |
| `OUT_OF_MEMORY` | `Out of Memory Error` | exceeded `memory_limit`. Batch too large. |
| `SPILL_EXCEEDED` | `failed to offload data block`; temp dir at cap | exceeded `max_temp_directory_size`. Batch too large. |
| `CATALOG_UNAVAILABLE` | connection refused / network | infrastructure. Back off; change nothing. |
| `OTHER` | anything else | record verbatim; revisit the taxonomy if this bucket grows. |

---

## Derived Quantities

Computed by the consumer from a window of recent records. Not stored.

```
throughput_files_s  = files_retired / duration_total_s
throughput_bytes_s  = (band_bytes_before - band_bytes_after) / duration_total_s

// arrivals fall out of consecutive records — no extra capture needed
arrivals            = band_files_before[n+1] - band_files_after[n]
arrival_rate        = arrivals / (started_at[n+1] - ended_at[n])
drain_rate          = throughput - arrival_rate      // < 0 means losing ground

duration_headroom   = p95(duration_total_ms) / timeout_limit_ms
saturated           = actual_gap_ms ~ 0              // cadence no longer a control variable
idle_ratio          = count(outcome == EMPTY) / N
```

**Arrival rate is the field people skip.** Without it you cannot distinguish
"backlog shrinking because compaction is fast" from "backlog shrinking because
ingest paused" — and a controller tuned on the second will be wrong the moment
load returns. It requires no new capture: it is the gap between one run's
`band_files_after` and the next run's `band_files_before`.

---

## Storage and Exposure

- **In-memory ring buffer**, last `N = 50` records per `(database, tier)`. This
  is control state, not analytics — it must be readable without touching the
  catalog or disk.
- **JSONL sink**, one line per run, rotated. Optional but recommended: it is the
  only way to analyse behaviour across restarts, and the compactor recycles every
  4 hours under `RuntimeMaxSec`.
- **`/health` extension** — expose the last record plus derived aggregates per
  tier, so behaviour is visible without log archaeology.
- **Micrometer** — the registry already exists in `CompactionState`. Emit the
  duration split, files/bytes retired, and a failure counter tagged by
  `failure_class`.

### One change that is not capture, but belongs in the same PR

`CompactionService.recordFileDelta` currently calls `updateAllTierFileCounts` on
*every* cycle — a full aggregate over all live files with one `COUNT(*) FILTER`
per tier, purely to feed gauges. It has no business on the control path and its
cost scales with cycle rate. Move it to its own 30–60 s schedule. This roughly
halves per-cycle catalog traffic and is what makes the telemetry above
affordable.

---

## Non-Goals

- The control algorithm. Deliberately deferred until there is data.
- Any change to tier scheduling, cadence, or batch sizing behaviour.
- New configuration keys for tiers. The existing `compaction_tiers` list stays as
  is. Note it is HOCON-only: the `config_provider` table override supports scalar
  keys only, so tier changes always require a redeploy.
- Persisting telemetry to the DuckLake catalog. It is shared with the ingest
  path; writing there to measure contention would be self-defeating.

---

## Open Questions

**Q1 — Keep the pre-run count, or probe by attempting?**
A no-op merge is itself just a metadata query: `GetFilesForCompaction` runs its
candidate SELECT, finds nothing, commits nothing. So `queryFileCount(before)`
could be dropped entirely, using the merge's own outcome as the probe. The
trade-off is losing `band_files_before`, and with it the arrival-rate
derivation. *Recommendation: keep the pre-run count while capturing baseline
data; revisit once the controller exists.*

**Q2 — Does `ducklake_merge_adjacent_files` return anything usable?**
If it reports groups or files merged, `groups_merged` is free. If not, it must be
inferred from before/after deltas, which conflates concurrent ingest into the
band. Needs a look at the function's result shape.

**Q3 — Raise `idle_in_transaction_session_timeout` first?**
The compactor's `ATTACH` carries no `options=`, so it inherits the 2-minute
server default. That is currently the binding constraint on batch size. Adding
`options=-c\ idle_in_transaction_session_timeout=600000` would give a future
controller real room to work in rather than tuning against a wall it cannot see.
Arguably a prerequisite.

**Q4 — Should ingest rate be an input?**
Compaction and ingest share the catalog and S3. Collector throughput rose from
~17,578 to ~19,766 rec/s as a backlog drained, so compaction at full throttle
during peak ingest may cost EPS. A future controller may need either an
ingest-rate signal or a simple aggressiveness cap. Out of scope for capture, but
worth deciding before the algorithm is designed.

---

## Classes Touched

| class | change |
|---|---|
| `CompactionService` | record assembly in `runTier`; ring buffer; move `updateAllTierFileCounts` off the control path |
| `DuckDbTierCompactor` | merge/commit duration split; surface merge result if available |
| `CompactionState` | new meters: duration split, bytes retired, failure counter by class |
| `HealthServer` | expose recent records and derived aggregates |
| *new* `CompactionRun` | the record type |
| *new* `CompactionRunLog` | ring buffer + optional JSONL sink |
