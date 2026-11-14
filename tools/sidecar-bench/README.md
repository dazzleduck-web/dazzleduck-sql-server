# sidecar-bench

The benchmark behind `doc/SIDECAR_VIEWS_SPEC.md`. It writes one ingested batch as a base table plus
three sidecar tables, each sidecar a SQL view over the base rows, and times nine strategies for
deriving the sidecars and getting every file to S3.

It is a standalone Maven project, not a module of the build.

## What it measures

The input is what the server stores for a batch: local, uncompressed Arrow IPC files (the server
rewrites an ingest body with `ArrowStreamWriter` and no codec). The base transformation and the
sidecars are:

```sql
-- base, over the batch as __this
SELECT *, date_trunc('minute', ts) AS minute FROM __this
-- sidecars, over the base rows as __base
SELECT minute, service, severity, count(*) AS n, avg(duration_ms) AS avg_ms, max(duration_ms) AS max_ms FROM __base GROUP BY ALL
SELECT ts, service, trace_id, http_status, body FROM __base WHERE severity = 'ERROR'
SELECT DISTINCT service, unnest(attr_keys) AS attr_key FROM __base
```

| Strategy | Sidecars derived from | Files reach S3 by |
|---|---|---|
| A | the raw Arrow input | one DuckDB `COPY` per output, in turn |
| B | the raw Arrow input | one DuckDB `COPY` per output, in parallel |
| C | a temp table holding the transformed base | DuckDB `COPY`s |
| D | the base Parquet, read back from S3 | DuckDB `COPY`s |
| E | the raw Arrow input, written locally | parallel upload afterwards |
| F | the base Parquet, written locally | parallel upload afterwards |
| G | the base Parquet, written locally | upload of each file as soon as it is written, by the async S3 client and transfer manager (Netty) |
| H | the base Parquet, written locally | as G, by the client the spec recommends: the synchronous S3 client on `url-connection-client`, on its own executor |
| I | the base Parquet, written locally | as G, by DuckDB itself: `COPY (SELECT content FROM read_blob(file)) TO 's3://...' (FORMAT blob)` writes each staged file unchanged through DuckDB's S3 writer and secret, on its own executor; no AWS SDK |

Every run checks that all strategies wrote the same number of rows per output. The first measured
iteration also reads each output back from S3 and compares its column types and a hash of its rows,
taken as a multiset, across strategies; floating-point columns are rounded to 6 decimals first, since
a parallel `avg()` can differ in the last bits. Iterations rotate the strategy order.

For E to I the table also splits the time into writing the base, deriving the sidecars and waiting for
the uploads.

`HttpCount` counts the HTTP requests DuckDB makes for one `COPY` to S3, from its HTTP log.

## Running it

MinIO as the target, and optionally Toxiproxy in front of it to add latency:

```bash
docker network create sidecar-net
docker run -d --name sidecar-minio --network sidecar-net -p 19000:9000 \
  -e MINIO_ROOT_USER=bench -e MINIO_ROOT_PASSWORD=benchbench minio/minio:latest server /data
docker run -d --name sidecar-toxi --network sidecar-net -p 8474:8474 -p 29000:29000 \
  ghcr.io/shopify/toxiproxy:2.9.0
curl -X POST localhost:8474/proxies \
  -d '{"name":"s3","listen":"0.0.0.0:29000","upstream":"sidecar-minio:9000"}'
for d in upstream downstream; do
  curl -X POST localhost:8474/proxies/s3/toxics \
    -d "{\"name\":\"lat_$d\",\"type\":\"latency\",\"stream\":\"$d\",\"attributes\":{\"latency\":20,\"jitter\":5}}"
done
```

Build, then run with rows per batch, input files, measured runs and the S3 endpoint (`19000` for
MinIO directly, `29000` through the proxy):

```bash
../../mvnw -q package dependency:build-classpath -Dmdep.outputFile=cp.txt
java --enable-native-access=ALL-UNNAMED -cp "target/classes:$(cat cp.txt)" \
  bench.SidecarBench 1000000 4 5 localhost:29000
```

Pass `-Dduckdb.version=2.0.0.0` to both Maven goals to run it on another DuckDB version.

## Upload benchmark

`UploadBench` compares the uploaders alone, without any Parquet writing: it uploads local files of
fixed sizes (4 KB, 32 KB, 8 MB, 16 MB of random bytes) to MinIO with each uploader, and checks every
object's size on S3.

| Uploader | How |
|---|---|
| `duckdb-blob` | DuckDB's own S3 writer and secret: `COPY (SELECT content FROM read_blob(file)) TO 's3://...' (FORMAT blob)` |
| `sdk-sync` | strategy H's client: the synchronous AWS SDK client on `url-connection-client`, one `PUT` below 16 MiB, concurrent 8 MiB parts above |
| `sdk-tm` | strategy G's client: the async AWS SDK client and transfer manager |

For each size it reports one upload on its own (median, p95 and min over the runs), its throughput,
the time of four uploads at once (a base file and three sidecars), and the HTTP requests one DuckDB
upload makes, by method. Arguments: the endpoint, runs per cell, optional DuckDB settings applied with
`SET GLOBAL` (`name=value;name=value`, or `-` for none), and an optional comma-separated list of
uploaders:

```bash
java --enable-native-access=ALL-UNNAMED -cp "target/classes:$(cat cp.txt)" \
  bench.UploadBench localhost:29000 15
java --enable-native-access=ALL-UNNAMED -cp "target/classes:$(cat cp.txt)" \
  bench.UploadBench localhost:29000 15 "s3_uploader_max_filesize='800GB'" duckdb-blob
```

## Results, 2026-11-12

One machine (Apple Silicon, DuckDB threads = 11), MinIO behind Toxiproxy adding about 40 ms per round
trip, DuckDB 1.5.6 and 2.0.0.1 back to back. Every strategy wrote the same rows and column types.

### Strategies, median total in ms (5 runs)

| Strategy | 100k rows, 1.5.6 | 100k rows, 2.0.0 | 1M rows, 1.5.6 | 1M rows, 2.0.0 |
|---|---|---|---|---|
| A: raw input, `COPY` to S3 per output, in turn | 471 | 930 | 1,463 | 1,425 |
| B: raw input, `COPY` to S3 per output, in parallel | 236 | 436 | 1,059 | 775 |
| C: transformed base in a temp table, `COPY`s to S3 | 484 | 992 | 1,416 | 1,469 |
| D: base to S3, sidecars read it back from S3 | 986 | 1,192 | 2,051 | 1,907 |
| E: raw input to local Parquet, then parallel upload | 323 | 291 | 895 | 808 |
| F: base to local Parquet, sidecars from it, then upload | 341 | 322 | 809 | 796 |
| G: as F, upload overlapped, async client and transfer manager | 297 | 294 | 778 | 752 |
| H: as G, synchronous client on `url-connection-client` | 217 | 216 | 893 | 748 |
| I: as G, uploaded by DuckDB with `FORMAT blob` | 227 | 348 | 1,061 | 862 |

### Uploaders, median ms for one upload (15 runs)

| Size | `duckdb-blob` 1.5.6 | `duckdb-blob` 2.0.0 | `sdk-sync` | `sdk-tm` |
|---|---|---|---|---|
| 4 KB | 52 | 97 | 110 | 109 |
| 32 KB | 56 | 102 | 104 | 112 |
| 8 MB | 210 | 319 | 173 | 183 |
| 16 MB | 283 | 368 | 310 | 304 |

The AWS SDK does not depend on DuckDB's version; its times are those of the 1.5.6 run. Four uploads
at once took about as long as one for the small files with every uploader.

HTTP requests for one `duckdb-blob` upload:

| Size | DuckDB 1.5.6 | DuckDB 2.0.0 |
|---|---|---|
| 4 KB, 32 KB | 1 `PUT` | 1 `HEAD`, 1 `PUT` |
| 8 MB, 16 MB | 1 `PUT` | 1 `HEAD`, 2 `POST`, 1 `PUT` (a one-part multipart upload) |

### DuckDB settings, `duckdb-blob` on 2.0.0, median ms

| Settings | 4 KB | 32 KB | 8 MB | 16 MB |
|---|---|---|---|---|
| defaults | 102 | 105 | 328 | 370 |
| `s3_uploader_max_filesize='800GB'` | 103 | 102 | 323 | 370 |
| `s3_uploader_max_filesize='40GB'` | 102 | 101 | 321 | 372 |
| `s3_uploader_max_filesize='20GB'` | 102 | 103 | 317 | 371 |
| `enable_http_metadata_cache=true` | 98 | 103 | 316 | 371 |
| `httpfs_client_implementation='curl'` | 101 | 102 | 322 | 375 |

None of them changed 2.0's requests: the `HEAD` and the multipart upload from 8 MB stay. On 1.5.6,
`s3_uploader_max_filesize='80GB'` (8 MB parts, 2.0's default part size) does switch large files to
multipart, and reproduces the slowdown: 8 MB from 222 to 305 ms, 16 MB from 285 to 336 ms.
