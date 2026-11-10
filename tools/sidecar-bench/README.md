# sidecar-bench

The benchmark behind `doc/SIDECAR_VIEWS_SPEC.md`. It writes one ingested batch as a base table plus
three sidecar tables, each sidecar a SQL view over the base rows, and times seven strategies for
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

Every run checks that all strategies wrote the same number of rows per output. The first measured
iteration also reads each output back from S3 and compares its column types and a hash of its rows,
taken as a multiset, across strategies; floating-point columns are rounded to 6 decimals first, since
a parallel `avg()` can differ in the last bits. Iterations rotate the strategy order.

For E to H the table also splits the time into writing the base, deriving the sidecars and waiting for
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
