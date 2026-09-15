# DazzleDuck / DuckDB Performance and Ingestion Benchmark — Specification

Status: **specification only**. No numbers in this document are measured. Every
figure in the final report must come from a run on the actual hardware and be
labelled `MEASURED`, `ESTIMATED`, `FAILED`, or `NOT TESTED`.

---

## 0. What this document is

A build-and-run spec for a reproducible benchmark of the DazzleDuck telemetry
pipeline and of the DuckDB engine underneath it, on a three-machine topology:

```text
SERVER 1  traffic generator (OTLP gRPC client)
SERVER 2  dazzleduck-sql-otel-collector   (OTLP -> Arrow -> Parquet -> DuckLake)
SERVER 3  dazzleduck-sql-ducklake-compactor (minor + major merge, housekeeping)
```

It maps the generic 20-test benchmark plan onto what this repository actually
does, names the exact metric source for every number, and lists the
instrumentation that must be built **before** the first measured run.

Read section 3 (gaps) before scheduling machine time. Several of the requested
numbers cannot be collected today without small code additions.

---

## 1. Two independent tracks — never mix results

| Track | Name | Question it answers | Runs on |
|-------|------|---------------------|---------|
| **A** | DuckDB engine benchmark | What can DuckDB 1.5.4 do on this hardware, with no DazzleDuck code in the path? | SERVER 3 (or any one machine), standalone |
| **B** | End-to-end pipeline benchmark | What can the OTLP-to-compacted-Parquet pipeline sustain? | SERVER 1 + 2 + 3 together |

Track A is the ceiling. Track B is the product. A Track B number is never
reported as a DuckDB number and vice versa. The final report states both and
identifies which stage is the binding constraint.

Track A uses the **same DuckDB version as the server**: `duckdb==1.5.4`
(the Java driver in `pom.xml` is `duckdb.version=1.5.4.0`). Pin it in
`requirements.txt`; a different minor version invalidates the comparison.

---

## 2. System under test — the real data path

Verified against the code, not assumed:

```text
OTLP ExportLogsServiceRequest (gRPC, port 4317)
  -> JwtServerInterceptor          verifies JWT, extracts x-dd-ingestion-queue claim
  -> OtelServiceBase.resolveQueue  claim -> ParquetIngestionQueue (lazily created)
  -> LogRecordConverter            OTLP protobuf -> flattened row objects
  -> writeArrowFile                Arrow IPC temp file, NO_COMPRESSION
  -> ParquetIngestionQueue.add     batch enters bucket; bucket flushes when
                                   min_bucket_size reached or max_delay_ms elapsed
  -> DuckDB COPY                   bucket -> one Parquet file        (data phase)
  -> DuckLake catalog commit       file registered + optional watermark row
                                                                  (post-ingest phase)
  -> compactor: CALL ducklake_merge_adjacent_files(db, max_file_size := N)
  -> compactor: ducklake_expire_snapshots + ducklake_cleanup_old_files
```

Four consequences that shape the whole benchmark:

1. **The export RPC does not ack until the batch is on disk.** `OtelServiceBase`
   completes the response from the `addBatch` future
   (`batchCompleteHandler`). So client-observed p99 latency is
   *queue wait + COPY + catalog commit*, not network time. Ack rate is the
   honest "accepted rows/sec".
2. **Backpressure is explicit and typed.** When pending write bytes exceed
   `max_pending_write` (default 500 MB) the queue throws
   `PendingWriteExceededException`, which surfaces as gRPC
   `RESOURCE_EXHAUSTED` carrying a `RetryInfo` retry delay. Counting these is
   how "rejected ingestion" is measured — it must never be silently retried
   away by the generator.
3. **Flush thresholds decide file size, and file size decides compaction load.**
   `min_bucket_size` (1 MB default in the collector `reference.conf`),
   `max_bucket_size` (100 MB), `max_delay_ms` (5000) are the primary knobs
   linking ingest rate to small-file production.
4. **Compaction is DuckLake merge, not generic Parquet rewriting.** Minor and
   major differ only by the `max_file_size` argument
   (`minor_compaction_max_size` 8 MB, `major_compaction_max_size` 64 MB by
   default) and by schedule. A "major" run also has housekeeping
   (`expire_snapshots`, `cleanup_old_files`) running on its own timer.
   Note the scheduler detail in `CompactionService.runCompaction`: when a major
   run is due it runs **instead of** the minor run for that tick, not in
   addition. TEST 16's "concurrent minor + major" therefore requires two
   catalogs or a code-level override — see 8.16.

---

## 3. Instrumentation gaps — close these first (P0)

These are blockers, discovered by reading the code. Each needs a decision:
build it, or mark the dependent tests `NOT TESTED`.

| ID | Gap | Impact | Proposed fix | Size |
|----|-----|--------|--------------|------|
| G1 | Collector uses `SimpleMeterRegistry` (`CollectorProperties` default) — metrics are held in memory with no exporter | No time series for `export.records`, `writer.pending_*`, `data_phase_ms`. TEST 16/17/18 lose their primary signal | Add an optional `PrometheusMeterRegistry` behind a config key and expose `/metrics` on the existing health `HttpServer` (port 8081) | S |
| G2 | Compactor uses `LoggingMeterRegistry` | Compaction timings only reach the log; no scrape | Same treatment, or parse the log lines into JSONL | S |
| G3 | No standalone traffic generator. `OtelCollectorBenchmark` is in-process, test-scope, and **closed-loop** (in-flight capped at client count) | Cannot drive a fixed offered rate from SERVER 1; cannot do the staircase test | New module `dazzleduck-sql-loadgen` — see section 6 | M |
| G4 | No backlog metric anywhere | "Backlog rows/GB", the core of TEST 16/17/19, is unmeasurable | Poller that runs the DuckLake metadata SQL in 7.3 every 10 s into JSONL | S |
| G5 | No per-record identity in generated telemetry | Cannot prove zero loss / zero duplication (section 9) | Generator stamps `bench.seq` and `bench.gen_id` attributes; validator reconciles | S |
| G6 | Collector `/health` exposes only `batchesProcessed` | Coarse; fine as a liveness/restart signal, not as throughput | Use G1 instead; keep `/health` for restart detection | — |

G1 and G3 are hard blockers for Track B. G2, G4, G5 are hard blockers for the
compaction, sustainability, drain, and correctness conclusions.

---

## 4. Topology and shared-state decisions

### 4.1 The catalog must be reachable from both SERVER 2 and SERVER 3

The collector writes files and commits them to the DuckLake catalog; the
compactor rewrites those files and commits new snapshots. Both attach the same
catalog. Two viable configurations:

| Option | Catalog | Data path | Notes |
|--------|---------|-----------|-------|
| **O1 (recommended)** | PostgreSQL, on SERVER 3 or a fourth small node | S3 / MinIO | Real concurrency story; matches production. Requires `ducklake` + `postgres` extensions in both startup scripts |
| **O2 (fallback)** | DuckDB/SQLite catalog file on shared storage | Same shared storage | Simpler, but concurrent writers over a network filesystem is a correctness risk — do not use for the soak test |

Record the chosen option in `environment.json`. If O1 is used, PostgreSQL
latency becomes part of the post-ingest phase and must be captured
(`dazzleduck.otel.writer.post_ingest_phase_ms` already isolates it from the
COPY phase — that split is the single most useful diagnostic in the whole
pipeline).

### 4.2 Storage

Object storage (MinIO on its own node, or real S3) is preferred over a shared
POSIX mount: it is what production uses, and it makes SERVER 2 and SERVER 3
symmetric. If MinIO is used, it is a fourth machine and its own throughput
must be characterised first (section 7.5) — otherwise MinIO, not DuckDB, is
what the benchmark measures.

### 4.3 Clock

All three servers run NTP/chrony against the same source. End-to-end lag
(section 7.4) is computed across machines; unsynchronised clocks make it
meaningless. Record max observed offset in `environment.json`.

### 4.4 Isolation

- Nothing else runs on the three machines during a measured run.
- If containerised, record CPU/memory **limits**, not host capacity, and set
  DuckDB `threads` and `memory_limit` explicitly rather than letting DuckDB
  infer from host values it cannot see.
- CPU governor set to `performance` on Linux; record it.

---

## 5. Environment capture

`bench/collect_env.py` writes `results/environment.json` on **each** server
before every measured run. Every result file references the environment hash.

```json
{
  "captured_at": "2026-09-08T10:00:00Z",
  "role": "collector",
  "host": {
    "os": "", "kernel": "", "cpu_model": "",
    "physical_cores": 0, "logical_cores": 0,
    "ram_gb": 0, "cpu_governor": "", "numa_nodes": 0
  },
  "disk": {
    "device": "", "type": "nvme|ssd|hdd|network",
    "capacity_gb": 0, "free_gb": 0,
    "seq_read_mbps": 0, "seq_write_mbps": 0, "fio_job": ""
  },
  "container": { "runtime": "", "cpu_limit": null, "memory_limit_gb": null },
  "kubernetes": { "cpu_request": null, "cpu_limit": null,
                  "memory_request": null, "memory_limit": null },
  "duckdb": { "version": "", "source_id": "",
              "threads": 0, "memory_limit": "", "temp_directory": "" },
  "java": { "vendor": "", "version": "", "max_heap_gb": 0, "gc": "" },
  "python": { "version": "", "duckdb_package": "" },
  "dazzleduck": { "git_commit": "", "version": "", "image": "" },
  "storage": { "kind": "local|minio|s3", "endpoint": "", "region": "" },
  "catalog": { "kind": "postgres|duckdb|sqlite", "endpoint": "" },
  "parquet": { "compression": "", "row_group_size": 0 },
  "clock": { "ntp_source": "", "max_offset_ms": 0 }
}
```

DuckDB side:

```sql
SELECT version();
SELECT source_id FROM pragma_version();
SELECT current_setting('threads');
SELECT current_setting('memory_limit');
SELECT current_setting('temp_directory');
```

Disk numbers come from `fio` with the exact job file recorded, not from
`dd`. On Windows dev boxes, note that Track A results are indicative only —
all reportable runs happen on the Linux servers.

---

## 6. Traffic generator (`dazzleduck-sql-loadgen`) — required build

New Maven module, JDK 21, depends on `opentelemetry-proto` and grpc (already
present in the collector module). Reuse the record-shaping logic from
`OtelCollectorBenchmark.buildRequest` but change the control model.

### 6.1 Requirements

| Req | Detail |
|-----|--------|
| R1 | **Open loop.** Target rate in records/sec from config; a token bucket paces submission. Never slow down because the server is slow — that is the measurement |
| R2 | **Bounded in-flight** with an explicit `generator_stalled_ms` counter when the bound is hit. A stall means the pipeline is behind; it must appear in the results, not be hidden |
| R3 | Configurable batch size (records per export RPC), channel count, and signal (logs / traces / metrics) |
| R4 | Every record carries `bench.gen_id` (per-generator UUID) and `bench.seq` (monotonic per generator), plus a deterministic payload from a seeded RNG |
| R5 | Per-second JSONL sample: `offered`, `acked`, `failed_resource_exhausted`, `failed_other`, `inflight`, `stalled_ms`, latency `p50/p95/p99/max` |
| R6 | Final `manifest.json`: total offered, total acked, per-`gen_id` seq range, wall-clock start/stop (UTC), config echo |
| R7 | Never auto-retry a `RESOURCE_EXHAUSTED`. Count it, honour `RetryInfo` only in an explicitly enabled "polite client" mode used for the soak test, and record which mode was used |
| R8 | Staircase mode: list of (rate, duration) steps executed back to back, each step's samples tagged with the step index |

### 6.2 Rate accounting

Because the RPC blocks until persistence, three rates must be reported
separately and never conflated:

```text
offered_rps   records the generator attempted to submit
accepted_rps  records acked OK by the collector
rejected_rps  records in RPCs that returned RESOURCE_EXHAUSTED
```

`offered_rps` is not system throughput. `accepted_rps` sustained with flat
backlog is.

---

## 7. Where every number comes from

### 7.1 Collector (SERVER 2) — after G1

Micrometer meters already registered in `OtelCollectorMetrics`, all tagged
`queue`:

```text
dazzleduck.otel.export.requests          RPCs received
dazzleduck.otel.export.records           records accepted
dazzleduck.otel.export.errors            failed exports
dazzleduck.otel.export.latency           p50/p95/p99 ack latency
dazzleduck.otel.writer.bytes_written     cumulative Parquet bytes
dazzleduck.otel.writer.batches_written   buckets flushed
dazzleduck.otel.writer.bytes_failed      bytes in failed writes
dazzleduck.otel.writer.batches_failed    batches in failed writes
dazzleduck.otel.writer.write_failures    failed bucket write attempts
dazzleduck.otel.writer.data_phase_ms     cumulative COPY-to-Parquet ms
dazzleduck.otel.writer.post_ingest_phase_ms  cumulative catalog-commit ms
dazzleduck.otel.writer.pending_batches   queue depth (batches)
dazzleduck.otel.writer.pending_buckets   queue depth (buckets)
```

`data_phase_ms` versus `post_ingest_phase_ms` answers "is the bottleneck DuckDB
or the catalog?" directly. Report both in every Track B result.

### 7.2 Compactor (SERVER 3)

`GET http://SERVER3:8080/health` returns JSON per database:
`totalMinorCompactions`, `totalMajorCompactions`, `totalFilesCompacted`,
`lastExecution`, `nextExecution`, `currentSmallFiles`, `currentMediumFiles`,
`currentTotalFiles`. Poll every 10 s into JSONL.

Timers (after G2): `type` in {minor, major, housekeeping}, `step` in
{merge, expire, cleanup}, tagged by database.

### 7.3 Backlog — the definition this benchmark uses

Three distinct quantities. Report all three; do not average them into one
"backlog".

**B1 — in-memory queue backlog** (collector): `writer.pending_batches`,
`writer.pending_buckets`. Bounded by `max_pending_write`; growth here means
the writer cannot keep up and rejection is imminent.

**B2 — uncompacted file backlog** (catalog). This is what compaction drains:

```sql
-- replace DB with the catalog name, THRESHOLD with minor_compaction_max_size
SELECT COUNT(*)              AS small_files,
       SUM(file_size_bytes)  AS small_bytes,
       SUM(record_count)     AS small_rows
FROM "__ducklake_metadata_DB".ducklake_data_file
WHERE end_snapshot IS NULL
  AND file_size_bytes < THRESHOLD;
```

The same table and predicate the compactor itself uses in
`CompactionService.updateFileCounts`. Verify the exact column names against the
attached catalog on first run and record them — DuckLake metadata schema is
version-dependent.

**B3 — end-to-end visibility lag**: with a watermark table configured
(`additional_parameters.watermark_*`, see the collector `reference.conf`),

```sql
SELECT now() - MAX(max_timestamp) AS lag FROM DB.main.ingest_watermark;
```

Enabling watermarks for the benchmark is recommended: it also gives per-batch
row counts committed transactionally with the files, which section 9 uses for
correctness.

### 7.4 Host metrics — all three servers

`bench/collect_system_metrics.py`, 1 s resolution, JSONL per host:
CPU total and per-core, load, RSS of the JVM / DuckDB process, page cache,
disk read/write MB/s and IOPS and util%, network RX/TX, JVM GC pause and heap
(via JFR or `jcmd GC.heap_info`), OOM-killer events (`dmesg`), process
restarts, container restarts, and — where containerised — cgroup
`memory.current`, `memory.max`, `cpu.stat` throttling counters.

Also capture DuckDB's own view during Track A:

```sql
SELECT * FROM duckdb_memory();
SELECT database_name, memory_usage_bytes, temporary_storage_bytes FROM duckdb_databases();
```

### 7.5 Storage baseline

Before any pipeline test, characterise the storage layer alone: `fio` for local
disk, `warp` or `s3-bench` for MinIO/S3. If the pipeline's Parquet write rate
approaches this ceiling, the benchmark is measuring storage. Record the
baseline in `environment.json` and reference it in the conclusions.

---

## 8. Test catalog

Notation for each test: **Track**, procedure, knobs, metrics, pass criteria,
output path. Repeat protocol from section 10 applies to every Track A test.

### 8.1 TEST 1 — Parquet read throughput (Track A)

Datasets 10M / 50M / 100M / 500M rows. Two queries: `COUNT(*)`, and the
five-aggregate query from the plan. Cold and warm cache (see 10.3).
Metrics: rows/s, MB/s, GB/s, wall time, CPU%, peak RSS, disk read.
Output: `results/parquet/read_throughput.jsonl`.

### 8.2 TEST 2 — File fragmentation (Track A)

Same logical bytes, five layouts: 1×10 GB, 10×1 GB, 100×100 MB, 1000×10 MB,
10000×1 MB. Identical query on each. Additionally record file-open overhead:
time the same query with `EXPLAIN ANALYZE` and extract scan-operator time.
This test calibrates what the compactor is worth — its output feeds conclusion
question 10 and the choice of `minor_compaction_max_size`.
Output: `results/parquet/fragmentation.jsonl`.

### 8.3 TEST 3 — Row group size (Track A)

32K / 64K / 128K / 256K / 512K / 1M via `COPY ... (FORMAT PARQUET, ROW_GROUP_SIZE n)`.
Measure write time, read time, merge time, output size.
Output: `results/parquet/row_groups.jsonl`.

### 8.4 TEST 4 — Compression (Track A)

`SNAPPY`, `ZSTD` (record the level), `UNCOMPRESSED`. Measure write/read time,
ratio, size, CPU. Cross-reference against the collector's actual setting —
report which one the pipeline uses today and whether the data supports changing it.
Output: `results/parquet/compression.jsonl`.

### 8.5 TEST 5 — Thread scaling (Track A)

`SET threads = 1|2|4|8|16` (cap at logical cores). Report time, rows/s,
speedup vs 1 thread, efficiency = speedup / N, CPU utilisation, peak memory.
Note explicitly if efficiency drops below 0.5 — that is the practical thread
ceiling and it feeds the collector's and compactor's DuckDB settings.
Output: `results/parquet/thread_scaling.jsonl`.

### 8.6 TEST 6 — Bulk ingestion (Track A)

`CREATE TABLE AS SELECT` from `read_parquet` and from `read_csv_auto`.
Timer wraps **only** the ingestion statement; the verification `COUNT(*)` is
timed separately and reported separately.
Output: `results/ingestion/bulk.jsonl`.

### 8.7 TEST 7 — Insert strategies (Track A)

Single-row INSERT (bounded sample, e.g. 100k rows — labelled worst case, not
extrapolated), batch INSERT (1k/10k), INSERT SELECT, CTAS, COPY, Parquet
scan+write. Metrics: rows/s, CPU, memory, transaction overhead.
Output: `results/ingestion/strategies.jsonl`.

### 8.8 TEST 8 — Query benchmark (Track A)

Q1 full scan, Q2 timestamp filter, Q3 group-by service, Q4 time-bucket
aggregation (minute/hour/day), Q5 multi-dimensional group-by
(tenant/service/status), Q6 join against a dimension table.
Run each against the OTLP-shaped dataset as well, so query numbers are
comparable to production tables.
Output: `results/queries/*.jsonl`.

### 8.9 TEST 9 — Profiling (Track A + selected Track B queries)

```sql
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'results/profiles/<name>.json';
```

Then `EXPLAIN ANALYZE` the query. Extract per-operator time, cardinality,
and rows processed. Deliverable: for each slow query, one named operator that
dominates. "Total time was N seconds" is not an acceptable finding.

### 8.10 TEST 10 — Compaction throughput (Track A, DuckLake)

Isolated: no traffic running. Build a catalog with a known fragmented state
(N small files, known rows and bytes), then time one merge:

```sql
CALL ducklake_merge_adjacent_files('DB', max_file_size := 8388608);
```

Snapshot `ducklake_data_file` before and after (count, bytes, rows) and time
only the `CALL`. Compute:

```text
compaction_rows_per_sec = input_rows / merge_seconds
compaction_gb_per_sec   = input_gb   / merge_seconds
```

Output: `results/compaction/isolated.jsonl`.

### 8.11 TEST 11 — Minor compaction (Track A)

Input file sizes 1 / 5 / 10 / 25 / 50 / 100 MB; file counts 100 / 1000 /
10000 / 100000. `max_file_size := minor_compaction_max_size` (8 MB default).
Report files and GB before/after, duration, rows/s, CPU, memory, and the point
at which duration stops scaling linearly with file count.
100000 files may be infeasible in the time budget — if skipped, label
`NOT TESTED` with the reason.
Output: `results/compaction/minor.jsonl`.

### 8.12 TEST 12 — Major compaction (Track A)

100 GB, 500 GB, 1 TB as hardware permits, `max_file_size :=
major_compaction_max_size`. Record input/output GB, duration, GB/s, rows/s,
peak memory, disk I/O, and temp/spill bytes (`temporary_storage_bytes`).
State the free-space requirement up front: a major merge needs headroom for
the new files before old ones are cleaned up. Sizes that do not fit are
`NOT TESTED`, never estimated.
Output: `results/compaction/major.jsonl`.

### 8.13 TEST 13 — Memory scaling (Track A)

`SET memory_limit = '1GB'|'2GB'|'4GB'|'8GB'` against a fixed workload
(the TEST 8 Q5 aggregation and a TEST 11 merge). Record time, peak memory,
spill bytes, GB/s, and outcome (`OK` / `OOM` / `error`). An OOM is a result.
Output: `results/memory/scaling.jsonl`.

### 8.14 TEST 14 — Out-of-core / spill (Track A)

Workload deliberately exceeding the limit — large group-by with ORDER BY over
the 500M-row dataset. For 1/2/4/8 GB limits record: completed yes/no, spill
bytes, duration, slowdown versus the unconstrained run, disk I/O.
Output: `results/memory/spill.jsonl`.

### 8.15 TEST 15 — Concurrency (Track A)

1 / 2 / 4 / 8 concurrent DuckDB jobs (mixed read and merge). Record total
throughput, per-job throughput, latency spread, CPU, memory, disk, failures.
Identify the concurrency level where total throughput peaks and where it
regresses.
Output: `results/concurrency/jobs.jsonl`.

### 8.16 TEST 16 — Ingestion plus compaction (Track B)

The first full three-server test. Offered rates 1k / 5k / 10k / 15k / 20k
records/s, each held for a fixed step duration (section 11.2). Compactor
running throughout.

Note the scheduler behaviour: in `CompactionService`, a due major run
**replaces** that tick's minor run. To get genuinely simultaneous minor and
major activity, use one of:

- **C1** two catalogs, one compactor instance per catalog, staggered
  schedules — closest to a real multi-tenant deployment;
- **C2** two compactor processes against the same catalog with different
  `minor_compaction_max_size` / `major_compaction_max_size` and frequencies —
  also exercises concurrent-writer conflict handling, which is worth knowing;
- **C3** single compactor, sequential minor/major — the current default; report
  it as such and do not claim concurrency.

Record which variant was used. Per level, capture: offered / accepted /
rejected rps, compacted rows/s, B1, B2, B3, CPU / memory / disk on all three
servers, and every error by class.
Output: `results/soak/ingest_plus_compaction.jsonl`.

### 8.17 TEST 17 — Maximum sustainable throughput (Track B)

Staircase: 1k, 2k, 5k, 10k, 12k, 15k, 20k, 25k, ... until failure. A level is
**sustainable** only if all hold for the whole step:

```text
accepted_rps  >= 0.99 * offered_rps
B2 slope over the second half of the step is not positive (linear fit)
B1 stable, no RESOURCE_EXHAUSTED
zero data loss and zero duplication (validated post-hoc, section 9)
no OOM, no restart, no failed writes
CPU, memory, disk below the limits declared in environment.json
```

Maximum sustainable rate = the highest level meeting all criteria, and it is
reported separately from the highest instantaneous accepted rate.
Output: `results/soak/staircase.jsonl` plus `results/soak/staircase.md`.

### 8.18 TEST 18 — Soak (Track B)

6 h first; 12 h and 24 h once 6 h is clean. Rate = 70–80% of the TEST 17
sustainable figure. Continuous 1 s host metrics, 10 s pipeline metrics, 60 s
backlog snapshots; rolled up at 1 / 5 / 15 min. Watch for slow drift —
backlog, RSS, GC time, and file count trending up over hours is the failure
mode this test exists to catch.
Output: `results/soak/soak_<duration>.jsonl`.

### 8.19 TEST 19 — Backlog drain (Track B)

At soak end: stop the generator, leave everything else running. Record initial
B2, sample every 10 s until it reaches steady state, compute drain time and
average drain rate. Also record B3 recovery.
Output: `results/drain/drain.jsonl`.

### 8.20 TEST 20 — Failure and stability (Track B)

Observed failures during the above are recorded as they occur. In addition,
run these deliberate faults on a non-soak instance:

| Fault | Injection | What to record |
|-------|-----------|----------------|
| Collector kill | `SIGKILL` mid-flush | records lost (from generator manifest versus catalog), duplicates, recovery time |
| Collector graceful stop | `SIGTERM` | drain behaviour, MAINTENANCE window on `/health`, records lost |
| Compactor kill | `SIGKILL` mid-merge | orphan files, catalog consistency, does compaction resume |
| Memory pressure | lower container memory limit | OOM-kill, restart, data integrity |
| Disk pressure | fill to 95% | error class, whether rejection is clean |
| Storage latency | `tc netem` delay to MinIO | latency propagation, backpressure onset |
| Catalog unavailable | stop PostgreSQL for 60 s | error class, recovery, duplicate commits |

Output: `results/failures/*.jsonl` plus a narrative per fault.

---

## 9. Correctness validation — a benchmark passes only if this passes

Enabled by G5 (`bench.gen_id` and `bench.seq` on every record).

After every Track B test:

```sql
-- 1. total accepted rows landed
SELECT COUNT(*) FROM DB.main.logs
WHERE timestamp BETWEEN :run_start AND :run_end;

-- 2. duplicates
SELECT gen_id, seq, COUNT(*) c FROM DB.main.logs
WHERE timestamp BETWEEN :run_start AND :run_end
GROUP BY 1,2 HAVING c > 1;

-- 3. gaps — expected contiguous ranges per generator
SELECT gen_id, MIN(seq), MAX(seq), COUNT(DISTINCT seq) FROM DB.main.logs
WHERE timestamp BETWEEN :run_start AND :run_end
GROUP BY 1;
```

Reconcile against the generator `manifest.json`: landed rows must equal acked
rows exactly. Acked-but-missing is data loss and is a hard failure regardless
of throughput. Offered-but-not-acked is expected under rejection and is not
loss — but it must be accounted for.

Post-compaction, re-run all three plus:

```text
row count unchanged across the merge
per-column aggregate checksums unchanged (SUM, MIN, MAX, COUNT of NULLs)
schema unchanged
every registered file readable (scan each path in ducklake_data_file)
```

A checksum harness over deterministic data makes this cheap:
`SELECT SUM(hash(col1)), SUM(hash(col2)), ... ` before and after.

Verdict per test: `PERF PASS/FAIL` and `CORRECTNESS PASS/FAIL`, both
recorded. A performance number from a run with failed correctness is reported
only with the failure attached.

---

## 10. Run protocol

### 10.1 Repeats

Track A: 1 warm-up (discarded) + 5 measured runs. Report min, max, mean,
median, stddev. Never report only the best run.

Track B: staircase steps run once each (they are long); the soak is the
repeat. Any Track B conclusion drawn from a single short run is labelled as
such.

### 10.2 Same input

Byte-identical datasets across compared configurations. Datasets are generated
once, hashed, and the hash recorded in `datasets.json` and in every result row.

### 10.3 Cold versus warm cache

- Warm: run the query twice, measure the second.
- Cold on Linux: `sync; echo 3 > /proc/sys/vm/drop_caches` (needs root) and
  verify by watching disk read bytes actually rise. If the drop cannot be
  verified, the run is labelled `warm-unknown`, not `cold`. Do not claim a cold
  cache that was not confirmed.
- Object storage: there is no local page cache to drop, but MinIO has its own —
  restart it or accept `warm-unknown`.

### 10.4 Failures are results

Every OOM, timeout, crash, disk-full, query error, rejected ingestion, memory
limit breach, and container restart is written to the result JSONL with
`status` and `error`. Nothing is rerun-until-green and silently replaced.
If a run is repeated, both the failed and the successful run are kept.

---

## 11. Track B execution parameters

### 11.1 Configuration matrix to hold constant

Fix these for the sustainability tests and vary them only in dedicated
sweeps; record all of them in every result row:

```text
collector:  min_bucket_size, max_bucket_size, max_delay_ms, max_pending_write
            DuckDB threads, memory_limit, JVM heap
compactor:  minor_compaction_frequency, major_compaction_frequency,
            minor_compaction_max_size, major_compaction_max_size,
            housekeeping_frequency, snapshot_retention
            DuckDB threads, memory_limit
generator:  batch size (records/RPC), channels, signal type, payload size
storage:    backend, Parquet compression, row group size
```

Suggested starting point, to be revised from Track A results:
`min_bucket_size = 16 MB`, `max_delay_ms = 5000`,
`minor_compaction_frequency = 1 minute`, `major_compaction_frequency = 10
minutes` (shortened from the 1 hour default so a staircase step can observe
several major cycles), `snapshot_retention = 15 minutes`.

### 11.2 Step duration

A staircase step must be long enough to contain the slowest periodic process,
otherwise backlog slope is noise. Minimum:

```text
step_duration >= max(10 * max_delay_ms, 5 * major_compaction_frequency, 10 min)
```

With the suggested settings that is 50 minutes per step. Budget accordingly:
an 8-level staircase is roughly a 7-hour run. If time forces shorter steps,
shorten `major_compaction_frequency` proportionally and say so — do not
shorten the step alone.

### 11.3 Warm-up and steady state

Discard the first `2 * max_delay_ms + one compaction cycle` of each step
before fitting the backlog slope. Record the discard window.

---

## 12. Results layout

```text
results/
├── environment.json            per server: environment.<role>.json
├── datasets.json
├── ingestion/
├── parquet/
├── queries/
├── compaction/
├── memory/
├── concurrency/
├── soak/
├── drain/
├── failures/
├── profiles/
├── raw/                        per-second host + pipeline JSONL, per server
└── summary.csv
```

Every result row (JSONL and `summary.csv`):

```text
test_name, track, variant, dataset, dataset_hash, rows, input_gb, output_gb,
file_count, threads, memory_limit, run_index, duration_seconds,
rows_per_second, gb_per_second, cpu_percent, peak_memory_gb,
disk_read_gbps, disk_write_gbps, offered_rps, accepted_rps, rejected_rps,
backlog_files, backlog_gb, backlog_rows, lag_seconds,
status, error, correctness, env_hash, git_commit, started_at, ended_at
```

Track A rows leave the Track B columns null and vice versa. One schema keeps
`summary.csv` usable.

---

## 13. Deliverables

Executable, config-driven, no source edits needed to change parameters.

```text
benchmark/
├── README.md
├── config.yaml
├── requirements.txt              duckdb==1.5.4 pinned
├── collect_env.py
├── generate_dataset.py
├── parquet_benchmark.py          TEST 1-4
├── thread_benchmark.py           TEST 5
├── ingestion_benchmark.py        TEST 6-7
├── query_benchmark.py            TEST 8-9
├── compaction_benchmark.py       TEST 10-12
├── memory_benchmark.py           TEST 13-14
├── concurrency_benchmark.py      TEST 15
├── pipeline_test.py              TEST 16-17 orchestration across 3 servers
├── soak_test.py                  TEST 18
├── drain_test.py                 TEST 19
├── failure_test.py               TEST 20
├── validate_correctness.py
├── collect_system_metrics.py
├── backlog_poller.py             G4
├── generate_report.py
└── compose/
    ├── collector.yml
    ├── compactor.yml
    └── minio.yml
```

Plus the Java module from section 6:

```text
dazzleduck-sql-loadgen/           OTLP open-loop rate-controlled generator
```

`config.yaml` skeleton:

```yaml
environment:
  servers:
    generator: { host: "", ssh: "" }
    collector: { host: "", ssh: "", health_port: 8081, grpc_port: 4317 }
    compactor: { host: "", ssh: "", health_port: 8080 }
  storage: { kind: minio, endpoint: "", bucket: "" }
  catalog: { kind: postgres, url: "", database: "bench_db" }

duckdb:
  version: "1.5.4"
  threads: [1, 2, 4, 8, 16]
  memory_limits: ["1GB", "2GB", "4GB", "8GB"]

datasets:
  rows: [10000000, 50000000, 100000000, 500000000]
  seed: 42
  layouts:
    - { name: "1x10GB",    files: 1,     target_file_mb: 10240 }
    - { name: "10x1GB",    files: 10,    target_file_mb: 1024 }
    - { name: "100x100MB", files: 100,   target_file_mb: 100 }
    - { name: "1000x10MB", files: 1000,  target_file_mb: 10 }
    - { name: "10000x1MB", files: 10000, target_file_mb: 1 }
  row_group_sizes: [32000, 64000, 128000, 256000, 512000, 1000000]
  compressions: ["snappy", "zstd", "uncompressed"]

benchmark:
  warmup_runs: 1
  measured_runs: 5
  cold_cache: true

collector:
  min_bucket_size: 16777216
  max_delay_ms: 5000
  max_pending_write: 524288000

compactor:
  minor_compaction_frequency: "1 minute"
  major_compaction_frequency: "10 minutes"
  minor_compaction_max_size: "8MB"
  major_compaction_max_size: "64MB"
  housekeeping_frequency: "5 minutes"
  snapshot_retention: "15 minutes"

traffic:
  batch_size: 1000
  channels: 4
  rates: [1000, 2000, 5000, 10000, 12000, 15000, 20000, 25000]
  step_duration: "50m"

soak:
  durations: ["6h", "12h", "24h"]
  rate_fraction: 0.75
```

---

## 14. Report

`benchmark_report.md`, generated by `generate_report.py` from
`summary.csv` — never hand-typed. Sections A–J exactly as in the source plan
(hardware, ingestion, Parquet, thread scaling, memory scaling, compaction,
concurrency, sustainable throughput, soak, drain), plus:

- a **stage table** for the end-to-end path, showing measured throughput at
  each stage (generator, collector accept, Parquet write, catalog commit,
  compaction) and naming the slowest one as system capacity;
- explicit answers to the 18 conclusion questions, each tagged `MEASURED`,
  `ESTIMATED`, `FAILED`, or `NOT TESTED` with the result file referenced;
- a failures appendix.

Rules: no number appears without a source file; no Track A number is presented
as system capacity; no generator rate is presented as throughput; anything not
run says `NOT TESTED`.

---

## 15. Phased plan

| Phase | Work | Depends on | Rough effort |
|-------|------|------------|--------------|
| P0 | Close G1, G2, G4, G5; build `dazzleduck-sql-loadgen` (G3) | — | the critical path; nothing measurable until done |
| P1 | Provision three servers, storage, catalog; capture environments; storage baseline (7.5) | P0 decisions on topology | short |
| P2 | Track A: TEST 1–15 | P1 | mostly unattended; 5 repeats each |
| P3 | Pick pipeline settings from P2 results (threads, memory, row group, compression, file size) | P2 | analysis |
| P4 | Track B: TEST 16, then TEST 17 staircase | P3 | roughly 7 h per full staircase |
| P5 | TEST 18 soak 6 h, then 12 h, then 24 h; TEST 19 drain after each | P4 | multi-day |
| P6 | TEST 20 fault injection on a separate instance | P3 | parallel with P5 |
| P7 | Correctness validation, report generation | all | short |

Track A and the P0 build can proceed in parallel.

---

## 16. Decisions needed before P0 starts

1. Catalog: PostgreSQL (O1) or file-based (O2)?
2. Storage: MinIO on a fourth node, real S3, or local disk per node?
3. Which signal drives Track B — logs only, or logs plus traces plus metrics?
   (Logs only is the cleaner first benchmark; the other two use different
   schemas and different converters.)
4. Are the three servers bare metal, VMs, or Kubernetes pods? Determines
   whether limits or host capacity govern.
5. Ceiling for TEST 12 — is 1 TB of free space available, or is 100 GB the cap?
6. Is `RESOURCE_EXHAUSTED` retry ("polite client", R7) in or out of scope for
   the soak definition of sustainable?
7. Time budget: a full staircase plus 24 h soak plus repeats is roughly a
   working week of machine time.
