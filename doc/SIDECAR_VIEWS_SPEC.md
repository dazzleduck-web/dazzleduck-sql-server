# Sidecar Views — Design Specification

Status: **phase 1 implemented**, for queues of the static provider with sidecar queries given as
`transformation`: local staging, with the upload done by DuckDB itself rather than an uploader.
A batch of a queue with sidecars is written as base Parquet to a staging directory next to the
queue's input files; each sidecar is derived from those staged base files (strategy F); every staged
file is then uploaded unchanged with `COPY (SELECT content FROM read_blob(file)) TO 'dest'
(FORMAT blob)`, through DuckDB's own file systems and secrets (close to strategy I in
`tools/sidecar-bench`; see *Upload by DuckDB* under *Benchmark*). Queues without sidecars write
directly, as before. It is not the fastest: in the second run the AWS SDK uploader (H) beat it in
every case, by 5 percent on DuckDB 1.5.6 for small batches and 61 percent on 2.0.0. It was chosen for
what it leaves out: no `FileUploader`, no AWS SDK, no credentials outside DuckDB and no per-cloud
code, so *The uploader*, *Library*, *Authentication* and *Uploader configuration* below are not
pursued. Also built: a sidecar query read from a view (`transformation_view`, over the queue's table
or an `input_table`, rewritten to `__base`) with the reload check below; partitioned sidecar tables;
and sidecars on dynamic queues, stored in an `ingestion_queue_sidecars` table of the SQLite registry,
one row per sidecar (open question 1). Startup
also rejects sidecar columns DuckLake cannot register from Parquet (`HUGEINT`, `UHUGEINT`, `TIMETZ`).
Without an `input_schema`, the startup check runs over the target table's columns. User
documentation is in the README under *Sidecars*.

Replaces the producer-side `SidecarRows` mechanism specified in
`doc/ATTRIBUTE_TYPE_REGISTRY_SPEC.md` with sidecars defined as SQL views over the rows being
ingested. Measured with the harness in `tools/sidecar-bench`; the numbers below come from it.

## Summary

A queue may declare any number of **sidecars**. Each one is a table beside the queue's target table
and a SQL transformation over the batch's base rows, exposed as `__base`. For every batch:

1. The base rows are written as Parquet to a **local staging directory**, and their upload to the
   table's data path starts at once.
2. Each sidecar is derived **from that staged Parquet**, written to staging, and uploaded as soon as
   it is written.
3. When every upload has finished, one DuckLake transaction registers the base files and every
   sidecar file with `ducklake_add_data_files`, so they commit or roll back together.

No uploader is configured by default. Then the same outputs are written by parallel DuckDB `COPY`s
straight to the destination, as the base is written today; see *Without an uploader*.

## Why change the design

`SidecarRows` carries rows a producer built in Java, rendered as SQL literals, on each `Batch`. That
made every sidecar a code change in a producer, limited sidecars to producers running inside the
server, and needed a typed projection because `ducklake_add_data_files` does not cast.

The case it was built for no longer needs it. The attribute type registry recorded which OTLP value
types an attribute arrived with, which the write path could not see. Since #506 the typed attribute
maps (`attributes_int`, `attributes_double`, `attributes_bool`, `attributes_bytes`, …) are columns
of the base rows, so the registry is a query over them.

A sidecar defined as SQL over the base rows is configuration, not code. It works for any producer,
`/v1/ingest` included; its column types are whatever the query produces, so a file always matches
a table created from the same query; and several sidecars per queue cost nothing extra to support.

## Configuration

Sidecars sit on the queue's entry in `ingestion_queue_table_mapping`, beside its `transformation`:

```hocon
ingestion_queue_table_mapping = [{
    ingestion_queue = "log"
    catalog = "loglake", schema = "main", table = "log"
    transformation = "SELECT *, date_trunc('minute', ts) AS minute FROM __this"

    sidecars = [
        { table = "log_minute_metrics"
          transformation = "SELECT minute, service, severity, count(*) AS n FROM __base GROUP BY ALL" }
        { table = "log_attribute_types"
          transformation_view = "loglake.main.v_log_attribute_types" }
    ]
}]
```

- `__base` is the base table's rows for this batch as written: after the queue's transformation and
  its partition projections.
- `transformation_view` names a view whose SQL is read on the same refresh tick as a view-based
  queue transformation, so a sidecar can change without a restart, as long as its output still
  matches its table (see *Reloading a view*).
- The queue's ingestion variables apply, so a sidecar can read `getvariable('name')`.
- A sidecar table lives in the queue's catalog and schema.

**Tables are created and checked at startup.** For each sidecar, startup runs its transformation
over an empty `__base` built the way a batch builds it: the queue's transformation and its partition
projections over an empty relation of the batch's input schema. A missing table is created from that
result's schema; an existing table whose columns differ fails startup, naming the table and the
column. No DDL runs inside the ingestion transaction.

**Every sidecar file is written with its table's column types.** Each sidecar `COPY` selects its
columns cast to the table's types, by name. `ducklake_add_data_files` does not cast, so without this a
file could disagree with its table even when startup passed: the staged path's Parquet round trip
turns an `ENUM` into `VARCHAR` and can change some interval and timestamp types, hive-read partition
columns would be re-typed from directory names, and `mode = auto` can send the same sidecar down
either path. With the cast, both paths write the table's types, and a value that cannot be cast fails
the batch with a clear error instead of a registration failure.

**Reloading a view.** When a refresh tick changes a sidecar view's SQL, the new SQL is checked the
same way as at startup before it is used. If its output no longer matches the table, the reload is
rejected: the queue keeps the previous SQL and logs the table and the column. Changing a sidecar's
columns therefore takes a table change first (for example `ALTER TABLE ... ADD COLUMN`), then the
view change.

## Deriving from the staged Parquet, not the raw input

**Decided: from the staged base Parquet.** With the base write timed separately, deriving three
sidecars from 1M rows took 64 ms from the local base Parquet against 146 ms from the raw Arrow input
on DuckDB 1.5.6 (strategies F and E below). On DuckDB 2.0.0 the two were equal (69 and 68 ms), as they
were at 100k rows on both versions. Parquet lets each sidecar read only its columns, and the base
transformation runs once rather than once per sidecar; the main reason, though, is the property
below.

It also makes a sidecar a function of the registered base data. A sidecar can be re-derived later,
to backfill a new sidecar or repair one, by reading the base **through the DuckLake table** (for
example filtered to the files or snapshots in question), not the raw files in its data path. For a
partitioned table the files do not hold the partition columns, DuckLake keeps their values in its
catalog, and its paths need not follow a hive `key=value` layout; reading through the table returns
them with the table's types.

**Partitioned queues.** `PARTITION_BY` leaves the partition columns out of the staged files and in
the directory names. The staged base is read with `hive_partitioning = true` and `hive_types` set to
the base table's partition column types, so `__base` keeps every column the transformation produced,
with the types it produced.

**Reading the base back from S3 is ruled out.** It was the slowest strategy in every run, roughly
twice the alternatives once latency is realistic.

## Staging locally, then uploading

**Decided: stage locally and upload.** This section first chose the AWS SDK uploader (strategy H
below): it was the fastest strategy for 100k-row batches on both DuckDB versions, level with the
direct path for 1M-row batches on DuckDB 2.0.0 and about 15 percent faster on 1.5.6. **Implemented
instead: the upload by DuckDB itself** (close to strategy I, *Upload by DuckDB* under *Benchmark*),
which is slower than H on DuckDB 2.0.0 but needs no SDK or credentials of its own. The
sidecars are derived from the staged base Parquet, after it is written, not overlapped with its upload.

Two properties come with it besides speed: nothing reaches object storage unless every output was
derived, and a failed upload can be retried without recomputing anything.

**Failure handling.**
- A sidecar that fails to derive fails the batch. Files already uploaded for it are deleted on a
  best-effort basis; any left behind are unregistered, and DuckLake's orphan-file cleanup removes
  them, as it does for a batch's own data file today.
- Staged files are deleted once the transaction commits or the batch fails.
- **Each process stages in a subdirectory of its own**, named by a random instance id and holding a
  lock file the process keeps locked while it runs. `staging_dir` is often shared: the server and
  the otel-collector on one host, a shared mounted tmp, or an old and a new pod overlapping during a
  rolling restart. On startup a process removes only subdirectories whose lock it can acquire, which
  belong to processes that have exited; it never clears the directory as a whole.

**Disk.** The staging directory holds each in-flight batch's base and sidecar files, so it needs room
for the bucket size times the number of concurrent writes, plus the sidecars. It sits next to the
Arrow temp files the queue already stages.

## The uploader

Nothing in the server uploads a file today: every byte reaches object storage through a DuckDB
`COPY`, and the build carries no cloud SDK. DuckDB cannot upload an existing file as it is, only
re-encode it, so the staged path needs an uploader.

```java
public interface FileUploader {
    /** Uploads {@code local} to {@code destination}, a URI in this uploader's scheme. */
    CompletableFuture<Void> upload(Path local, URI destination);
}
```

One uploader per scheme, chosen by the destination: `s3://` first, a rename for `file://`, and Azure
and GCS later. A destination whose scheme has no uploader uses the direct path.

### Library

| Option | Jars | Size |
|---|---|---|
| AWS SDK v2 `s3` + `s3-transfer-manager` (async, Netty) | 47 | 13.7 MB |
| **AWS SDK v2 `s3` + `sts` + `url-connection-client`, no Netty** | 32 | **8.2 MB** |
| MinIO Java SDK 8.5 | 26 | 22.9 MB |
| Hand-written client: JDK `HttpClient` and SigV4 signing | 0 | 0 MB |

**Recommended: the AWS SDK's `s3` client on its JDK-based `url-connection-client`, without Netty**,
with the uploader running multipart parts and files concurrently on its own executor. It leaves out
the transfer manager and its Netty, which would sit beside the Netty that gRPC and Arrow Flight
already bring, with a version of its own. `sts` is what lets the default credential chain use web
identity (IRSA).

A hand-written client costs nothing in jars, and the S3 calls themselves are small. The credential
chain is not: web identity through STS, EKS Pod Identity and ECS container credentials, and IMDSv2
tokens are each a security-sensitive protocol to implement and keep current. Against an image whose
DuckDB jar alone is 85 MB, 8 MB is not worth that.

The MinIO SDK is not lighter, and talks to AWS S3 no better.

### Authentication

The uploader cannot reuse DuckDB's secrets. DuckDB refuses to show a secret unredacted
(`duckdb_secrets(redact := false)` fails with "Displaying unredacted secrets is disabled"), and the
setting that allows it would show secrets to any query a user is allowed to run.

So both read **one identity from the environment** instead of sharing a secret:

- DuckDB's secret uses the credential chain:

  ```sql
  CREATE SECRET s3_data (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');
  ```

- The uploader uses the AWS SDK's default chain: environment variables, system properties, web
  identity (IRSA), the shared profile, EKS Pod Identity or ECS container credentials, then IMDSv2.

On Kubernetes that is the pod's IAM role, with no keys anywhere. Static keys, as for MinIO, go in the
environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), where both chains read them.

There is no credentials setting in the uploader's configuration: a key configured only there would let
the uploader and DuckDB run as different identities, so that a write succeeds through one and fails
through the other.

### Uploader configuration

The shipped `application.conf` has `mode` and `staging_dir` but **no `s3` block**. An operator adds
one to enable the uploader:

```hocon
ingestion.upload {
    mode = auto                  # auto | staged | direct
    staging_dir = ${java.io.tmpdir}"/dazzleduck-staging"
    max_concurrent_uploads = 16

    # Absent by default; adding it enables the S3 uploader. Credentials come from the AWS chain.
    s3 {
        region = "us-east-1"
        endpoint = ""            # set for MinIO, R2 and other S3-compatible stores
        path_style = false
        multipart_threshold = 16MiB
        part_size = 8MiB
    }
}
```

`auto`, the default, stages when an uploader is configured for the destination's scheme and writes
directly otherwise. `staged` with no uploader for a destination's scheme fails startup, naming the
queue and the scheme. `direct` never stages.

### Without an uploader

The default. No `ingestion.upload.s3` block means no uploader, and every queue writing to object
storage takes the **direct path**:

- **The base** is written by the same `COPY` straight to its destination as today.
- **Each sidecar** is its own `COPY` straight to its table's data path, over the same source relation
  the base is written from: the batch's Arrow files with the queue's transformation applied, as
  `__base`. The sidecar `COPY`s run in parallel with the base's.
- **Registration** is unchanged: one DuckLake transaction registers every file.

The sidecars are derived from the source relation here rather than from the written base, because
the base is already in object storage and reading it back was the slowest strategy measured (D). They
return the same rows as long as the transformation is deterministic. What is lost is only that the
sidecar files are not computed from the registered files themselves.

This is strategy B below. Against staging with the recommended uploader (H) it was level for small
batches on DuckDB 1.5.6 and for large ones on 2.0.0, about 15 percent slower for large batches on
1.5.6, and about twice as slow for small batches on 2.0.0. It needs nothing beyond the DuckDB secret already configured, and
a queue that declares no sidecars behaves exactly as it does today.

**A local destination** (a filesystem path) never needs an uploader. The base is written in place and
each sidecar is derived from it there, since reading a local file back is cheap; this is strategy F
without the upload.

**Why the uploader is not enabled implicitly.** It is the one component that would talk to object
storage outside DuckDB, with credentials of its own. Turning it on should be a deliberate line of
configuration, not a side effect of credentials happening to be in the environment.

## Benchmark

One batch of OTel-like log rows as uncompressed Arrow IPC (4 files), written as a base table plus the
three sidecars in `tools/sidecar-bench/README.md`, to MinIO behind a proxy adding about 40 ms per
round trip. Median total in ms; DuckDB 1.5.6 and 2.0.0 measured back to back.

| Strategy | 100k rows, 1.5.6 | 100k rows, 2.0.0 | 1M rows, 1.5.6 | 1M rows, 2.0.0 |
|---|---|---|---|---|
| A: raw input, `COPY` to S3 per output, in turn | 514 | 903 | 1,959 | 1,479 |
| B: raw input, `COPY` to S3 per output, in parallel | 285 | 449 | 1,346 | 926 |
| C: transformed base in a temp table, `COPY`s to S3 | 550 | 960 | 1,685 | 1,630 |
| D: base to S3, sidecars read it back from S3 | 1,028 | 1,207 | 2,709 | 2,095 |
| E: raw input to local Parquet, then parallel upload | 395 | 311 | 1,124 | 977 |
| F: base to local Parquet, sidecars from it, then upload | 392 | 321 | **1,021** | 942 |
| G: as F, base upload overlapped with sidecars (async client, transfer manager) | 370 | 307 | 1,062 | 999 |
| H: as G, with the recommended uploader (`url-connection-client`, own executor) | **271** | **221** | 1,149 | **925** |

Every strategy wrote the same number of rows per output on every run. On the first measured run each
output was read back from S3, and its column types and a hash of its rows (floating-point columns
rounded to 6 decimals) were identical across all strategies.

**The uploader matters more than the pipeline.** G and H differ only in the S3 client. The
synchronous client on `url-connection-client`, with a single `PUT` below the multipart threshold and
parts and files uploaded concurrently on its own executor, cut the upload wait at 100k rows from
about 250 to about 150 ms. At 1M rows the two were within noise.

**DuckDB 2.0 cut the direct path's large-batch time** (B: 1,346 to 926 ms), likely from its
asynchronous Parquet writes, **and nearly doubled its small-batch time** (B: 285 to 449 ms). Its HTTP log
shows two `HEAD` requests per file before writing that 1.5.6 does not make, and with real latency
each one is a round trip. Strategies E to H do not go through DuckDB's S3 writer and did not change.
Queues without sidecars still write directly (B's path): measure it again on each DuckDB upgrade.

**Caveats.** One machine, synthetic rows, three sidecars, no bandwidth limit, and MinIO rather than
S3. Treat the numbers as directional until they are repeated against a real bucket.

### Upload by DuckDB (strategy I)

DuckDB can upload an existing file unchanged: `COPY (SELECT content FROM read_blob(file)) TO 'dest'
(FORMAT blob)` writes the file's bytes through its own S3 writer and secret. Strategy I is G with that
upload: it starts each file's upload as soon as the file is written, while the sidecars are still
being derived, one after another. The implemented design differs: it derives the sidecars in parallel
from the staged base, and uploads every file only once all are written, so I approximates it rather
than measures it. A second run (2026-11-12, all strategies again, median total in ms) and an upload benchmark
(`UploadBench`, one upload of a file of a fixed size) gave:

| | 100k rows, 1.5.6 | 100k rows, 2.0.0 | 1M rows, 1.5.6 | 1M rows, 2.0.0 |
|---|---|---|---|---|
| B: direct `COPY`s, in parallel | 236 | 436 | 1,059 | 775 |
| H: staged, AWS SDK uploader | 217 | 216 | 893 | 748 |
| I: staged, uploaded by DuckDB | 227 | 348 | 1,061 | 862 |

| One upload, median ms | 4 KB | 32 KB | 8 MB | 16 MB |
|---|---|---|---|---|
| DuckDB `FORMAT blob`, 1.5.6 | 52 | 56 | 210 | 283 |
| DuckDB `FORMAT blob`, 2.0.0 | 97 | 102 | 319 | 368 |
| AWS SDK, synchronous client | 110 | 104 | 173 | 310 |

- **On 1.5.6 DuckDB's upload is the fastest uploader**: one `PUT` at any size, half the SDK's time for
  small files.
- **On 2.0.0 it makes a `HEAD` before every upload, and a multipart upload (two `POST`s and a `PUT`,
  one after another) from 8 MB.** Small files are level with the SDK; 8 MB takes 319 against 173 ms.
  No DuckDB setting changes it (`s3_uploader_max_filesize`, the metadata cache, the curl client);
  forcing 2.0's 8 MB part size on 1.5.6 reproduces the slowdown.
- **Across the pipelines H beat I in every case** of this run: by 5 percent (100k rows, 1.5.6), 19
  percent (1M, 1.5.6), 61 percent (100k, 2.0.0) and 15 percent (1M, 2.0.0). Run-to-run noise is large
  (H's 1M-row time on 1.5.6 was 1,149 in the first run and 893 in this one), but on 2.0.0 the gap is
  consistent with the upload benchmark above.

I was chosen anyway, for what it leaves out: no `FileUploader`, no AWS SDK, no credentials outside
DuckDB, and one path for every store DuckDB writes to; the price is slower uploads on DuckDB 2.0.0,
most of all for small batches. If DuckDB's 2.0 writer drops the extra `HEAD` and the small multipart
upload, I should match 1.5.6 again; re-run this on each upgrade. Full tables, including every strategy and the settings tried, are in
`tools/sidecar-bench/README.md`.

## Effect on other work

- **`SidecarRows` is not needed.** It, `Batch.sidecarRows()` and their plumbing through
  `IngestionResult` go away. The attribute type registry becomes a sidecar view over the
  typed attribute maps, which `doc/ATTRIBUTE_TYPE_REGISTRY_SPEC.md` should be revised to say.
- **The watermark.** Removed (Task 7 of `doc/DUCKDB_2_UPGRADE_PLAN.md`). Its per-batch MIN, MAX
  and row counts were a `GROUP BY` over the base rows, so a sidecar view can produce them; the
  commit's snapshot id, known only at commit, has no sidecar equivalent.
- **Inlining.** Unaffected: every sidecar file is registered, and registration is never inlined.

## Open questions

1. **Dynamic queues.** Decided: a child table, `ingestion_queue_sidecars`, one row per sidecar keyed
   by queue and sidecar table, rather than a JSON column on the queue row.
2. **Azure and GCS.** Their SDKs are heavier than the S3 one. The alternative is a small DuckDB
   extension that copies a local file through DuckDB's own file system, which would reuse DuckDB's
   secrets for every cloud and need no Java SDK, at the cost of DuckDB's S3 writer and its 2.0
   `HEAD` requests.
3. **Upload tuning for large batches.** H is fastest for small batches but not for 1M rows, where F
   was faster on 1.5.6. Whether part size, `max_concurrent_uploads` or a separate pool for parts
   closes that gap.
4. **Real S3.** The benchmark against a real bucket, before staging is recommended as the production
   setting.
