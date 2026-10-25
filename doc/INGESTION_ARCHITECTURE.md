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
 producer ─┘     bucket fills           transform, COPY                 + watermark row
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

Registering the written files, appending the watermark row and appending the batch's sidecar rows
all happen in the **same transaction**:

```
ducklake_add_data_files(...)                      -- the batch's data file
  + INSERT INTO watermark_table VALUES (...)
  + ducklake_add_data_files(... sidecar table ...) -- the batch's sidecar file
```

Either the data becomes visible together with its accounting, or nothing lands and the Parquet files
are left unregistered for `ducklake_cleanup_old_files` to remove. There is never a registered file
without its accounting, and never a watermark for data that is not there.

The two appended things differ in where they come from, and in how they land. A watermark row is
**computed** from the batch inside the write path, and appended with an `INSERT`. A sidecar row is
**carried** by the batch, derived by whoever produced it from something the write path never sees —
the OTel collector's attribute type registry is the case it exists for, since which OTLP value type
an attribute arrived with is visible only while the OTLP message is being read — and it is written
the way data is, as a Parquet file `COPY`ed before the transaction and registered inside it. Both
are resolved against the target table's own catalog and schema, so each lands beside the data it
describes. See `SidecarRows`.

Watermark rows are computed **before** the `COPY`, from the same pre-COPY relation the output is
written from. That means the transformation is already applied, partition columns are real typed
columns rather than path fragments, and a misconfigured watermark fails fast instead of leaving an
unregistered file behind. See `WatermarkSpec` for the configuration keys.

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
