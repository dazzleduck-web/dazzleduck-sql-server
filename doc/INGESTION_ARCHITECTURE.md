# Ingestion Queue Architecture

A server runs a small number of ingestion queues — typically one per signal. Each queue is fed by
many clients, batches independently, and is drained by its own writer thread that applies the
queue's transformation and writes Parquet into DuckLake.

The same diagram is drawn as a figure alongside this file, in whichever form you need:

| File | Use |
|---|---|
| [ingestion-architecture.html](ingestion-architecture.html) | open in a browser; the figure plus explanatory notes |
| [ingestion-architecture.svg](ingestion-architecture.svg) | vector — imports into Keynote, PowerPoint, Figma, Excalidraw |
| [ingestion-architecture.png](ingestion-architecture.png) | 3060x1620 raster for Google Slides and anything that will not take SVG |

The SVG is self-contained: colours are literal, and it falls back to a system sans if IBM Plex
is not installed.

## Shape

```
 clients (many)              server                                     DuckLake
──────────────────   ─────────────────────────────────────   ────────────────────────────

 producer ─┐
 producer ─┼──▶  queue "logs"      ──▶  writer thread  ──┐
 producer ─┘     bucket fills           transform, COPY  │
                                                         │
 producer ─┐                                             ├──▶  Parquet data files
 producer ─┼──▶  queue "traces"    ──▶  writer thread  ──┤     (partitioned)
 producer ─┘     bucket fills           transform, COPY  │             │
                                                         │             │  ONE TRANSACTION
 producer ─┐                                             │             ▼
 producer ─┼──▶  queue "metrics"   ──▶  writer thread  ──┘     catalog: add_data_files
 producer ─┘     bucket fills           transform, COPY
```

Queues are independent all the way through: separate buckets, separate writer threads, one target
table each. A slow or failing queue does not stall the others.

## The bucket

Clients send small batches continuously. Writing each one straight through would produce a tiny
Parquet file per batch, which is expensive to store and worse to query. The bucket trades a bounded
delay for file size and flushes on whichever threshold is reached first:

| Setting | Meaning |
|---|---|
| `min_bucket_size` | flush once the accumulated batches reach this many bytes |
| `max_delay_ms` | flush after this long regardless of size |
| `max_batches` | flush once this many batches have accumulated |

Buckets that are eligible to be written together may be combined, so a burst does not produce one
file per threshold crossing.

## The writer

Each queue has **exactly one writer thread**, named `BulkIngestQueue-QUEUE_ID-writer`. Parallelism
comes from running several queues, not several writers on one queue — that keeps each queue's output
files and its ordering guarantees straightforward.

The writer builds a single DuckDB `COPY` over the batch files. The queue's transformation is folded
into that relation rather than applied as a separate pass, so the data is read once:

```sql
COPY
    (SELECT ... FROM read_arrow([...]))
    TO 'target/path/dd_UUID.parquet'
    (FORMAT parquet, RETURN_FILES, APPEND)
```

`RETURN_FILES` hands back the paths and row counts the post-ingestion step needs, so nothing has to
re-read what was just written.

## The commit

A batch's files are registered in **one transaction**, one `ducklake_add_data_files` call per file:
either all of them become visible, or none do and the Parquet files are left unregistered for
`ducklake_cleanup_old_files` to remove. A commit that conflicts with a concurrent DELETE on the table
registered nothing and is retried; see "Registration conflicts" in the README.

## What this diagram leaves out

These are real parts of the path that do not change the shape above:

- **Producer de-duplication.** Batches carry a producer id and a monotonic batch id; a replayed or
  out-of-order batch is rejected before it reaches a bucket.
- **Back pressure.** A queue whose pending writes exceed `max_pending_write` rejects new batches with
  a retry hint rather than growing without bound.
- **Compaction.** A separate job merges these files later; see the ducklake-compactor module.

## Related

- `doc/DYNAMIC_QUEUES.md` — registering queues at runtime instead of from static config
- `PRODUCER_WATERMARK_SPEC.md` — proposed durable producer watermarks
