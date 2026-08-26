# Docker Build Guide

## Images

There are two images involved:

| Image | Purpose | When to rebuild |
|-------|---------|----------------|
| `dazzleduck/base-jre:${JAVA_VERSION}-noble-duckdb-${DUCKDB_VERSION}` | Ubuntu noble JRE + stripped DuckDB jar | Java or DuckDB version bump |
| `dazzleduck/dazzleduck:latest` | Application image built by Jib | Every release |

### Why a custom base image?

Two reasons:

1. **glibc compatibility** — DuckDB's JDBC native library bundles jemalloc, which makes
   glibc-specific syscalls. Alpine (musl libc) is incompatible and causes a SIGSEGV at startup.
   The base image uses `eclipse-temurin:${JAVA_VERSION}-jre-noble` (Ubuntu 24.04, glibc).

2. **DuckDB pre-installed** — The DuckDB JDBC jar is downloaded, stripped to the target platform's
   native lib only, and placed in `/app/libs/` inside the base image. Jib then declares DuckDB as
   `provided` in the runtime module and skips bundling it, keeping the app layer small and ensuring
   DuckDB is not duplicated across releases.

**Size comparison:**

| Configuration | App image size |
|--------------|---------------|
| Ubuntu + DuckDB in Jib layers + Hadoop included (0.0.35) | ~950 MB |
| Ubuntu noble base + DuckDB pre-installed + Hadoop excluded (current) | ~755 MB |

Hadoop (`hadoop-client-runtime` + `hadoop-client-api`, ~48 MB) is declared `provided` in the
runtime module and excluded from the image by default. To enable Delta Lake, add the Hadoop jars
to `/app/extra` at runtime.

---

## Step 1 — Build the base image

Only needed when upgrading Java, DuckDB version, or example data/startup scripts.

> All commands run from the **repo root** (`dazzleduck-sql-server/`) — the build context
> must include the `example/` folder so Docker can `COPY` it into the image.

```bash
# Read version directly from pom.xml
DUCKDB_VERSION=$(./mvnw help:evaluate -Dexpression=duckdb.version -q -DforceStdout)
JAVA_VERSION=25   # must match the base-jre tag referenced by the module poms (currently 25)

# Multi-platform — push to Docker Hub (CI/CD)
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --build-arg JAVA_VERSION=$JAVA_VERSION \
  --build-arg DUCKDB_VERSION=$DUCKDB_VERSION \
  -t dazzleduck/base-jre:${JAVA_VERSION}-noble-duckdb-${DUCKDB_VERSION} \
  -f dazzleduck-sql-runtime/docker/Dockerfile.base \
  . \
  --push

# Local only — amd64 (development)
docker build \
  --platform linux/amd64 \
  --build-arg JAVA_VERSION=$JAVA_VERSION \
  --build-arg DUCKDB_VERSION=$DUCKDB_VERSION \
  -t dazzleduck/base-jre:${JAVA_VERSION}-noble-duckdb-${DUCKDB_VERSION} \
  -f dazzleduck-sql-runtime/docker/Dockerfile.base \
  .
```

> Run from the repo root (`dazzleduck-sql-server/`). The version is read directly
> from `duckdb.version` in `pom.xml` — no manual sync needed.

---

## Step 2 — Build the application image

Run from the repo root (`dazzleduck-sql-server/`).

### Build and push to Docker Hub (CI/CD)

```bash
./mvnw clean package jib:build -pl dazzleduck-sql-runtime -am -DskipTests
```

### Build to local Docker daemon only (development)

```bash
./mvnw clean package jib:dockerBuild -pl dazzleduck-sql-runtime -am -DskipTests
```

> Defaults to `amd64`, which works on Intel/AMD Linux, Windows, and Apple Silicon (via Rosetta 2).
> Override with `-Djib.architecture=arm64` only when pushing for ARM64 production servers.

### Build only the runtime module (if other modules are already built)

```bash
./mvnw jib:dockerBuild -pl dazzleduck-sql-runtime -DskipTests
```

---

## When to rebuild the base image

| Change | Rebuild base? | Rebuild app? |
|--------|-------------|-------------|
| DuckDB version bump | Yes — update `duckdb.version` in parent `pom.xml` | Yes |
| Java version bump | Yes — update `JAVA_VERSION` arg | Yes |
| `example/data` or `example/startup` changed | Yes | Yes |
| Application code changed | No | Yes |

---

## Entrypoint

The container runs `/app/entrypoint.sh` via `sh`. It supports two modes:

```bash
# Default: start the DazzleDuck runtime server
docker run dazzleduck/dazzleduck:latest

# Run a specific main class (e.g. for logger or metrics demos)
docker run dazzleduck/dazzleduck:latest io.dazzleduck.sql.SomeMainClass [args...]
```

---

## Switching target platform (amd64 vs arm64)

`Dockerfile.base` uses Docker's `TARGETARCH` build argument to automatically strip
the correct native library. No manual changes needed when using `--platform`.

To rebuild the base for ARM64 only:

```bash
DUCKDB_VERSION=$(./mvnw help:evaluate -Dexpression=duckdb.version -q -DforceStdout)
JAVA_VERSION=25
docker build \
  --platform linux/arm64 \
  --build-arg JAVA_VERSION=$JAVA_VERSION \
  --build-arg DUCKDB_VERSION=$DUCKDB_VERSION \
  -t dazzleduck/base-jre:${JAVA_VERSION}-noble-duckdb-${DUCKDB_VERSION}-arm64 \
  -f dazzleduck-sql-runtime/docker/Dockerfile.base \
  .
```
