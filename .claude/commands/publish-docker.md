# Publish Docker Image

Builds and publishes multi-arch Docker images for `dazzleduck/dazzleduck`, `dazzleduck/dazzleduck-otel-collector`, and `dazzleduck/ducklake-compactor` to Docker Hub.

## Steps

1. Verify working tree is clean (warn if uncommitted changes)
2. Read current version from `pom.xml`
3. Run `mvn clean install -DskipTests`
4. Build and push `arm64` + `amd64` images for all three targets:
   - `dazzleduck-sql-runtime` and `dazzleduck-sql-otel-collector` — 4 builds, run in parallel
   - `dazzleduck-sql-ducklake-compactor` — 2 builds, run **sequentially** (see note)
5. Create and push multi-arch manifests for `<version>` and `latest` for all three images
6. Verify the published manifests with `docker manifest inspect`
7. Report the final digests and all tags

## Instructions

Run the following, stopping immediately on any failure and reporting the error:

### Step 1 — Pre-flight checks

Run these in parallel:
- `git status --short` — if any tracked files are modified or staged, print a warning ("Uncommitted changes detected — image may not match HEAD") but continue
- Extract the version: `grep -m1 '<version>' pom.xml | sed 's/.*<version>\(.*\)<\/version>.*/\1/'`
- Confirm Jib can read Docker Hub credentials (see the credentials note below) — the `dazzleduck/base-jre` base image is private, so an unauthenticated build fails on the base-image pull.

### Step 2 — Maven install

```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
./mvnw clean install -DskipTests
```

Fail fast if this returns non-zero.

### Step 3a — Build and push runtime + otel-collector images (run all 4 in parallel)

**dazzleduck/dazzleduck arm64:**
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
./mvnw package -DskipTests jib:build -pl dazzleduck-sql-runtime \
  -Djib.architecture=arm64 \
  -Djib.to.image=docker.io/dazzleduck/dazzleduck:<version>-arm64
```

**dazzleduck/dazzleduck amd64:**
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
./mvnw package -DskipTests jib:build -pl dazzleduck-sql-runtime \
  -Djib.architecture=amd64 \
  -Djib.to.image=docker.io/dazzleduck/dazzleduck:<version>-amd64
```

**dazzleduck/dazzleduck-otel-collector arm64:**
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
./mvnw package -DskipTests jib:build -pl dazzleduck-sql-otel-collector \
  -Djib.architecture=arm64 \
  -Djib.to.image=docker.io/dazzleduck/dazzleduck-otel-collector:<version>-arm64
```

**dazzleduck/dazzleduck-otel-collector amd64:**
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
./mvnw package -DskipTests jib:build -pl dazzleduck-sql-otel-collector \
  -Djib.architecture=amd64 \
  -Djib.to.image=docker.io/dazzleduck/dazzleduck-otel-collector:<version>-amd64
```

### Step 3b — Build and push the compactor image (run these two SEQUENTIALLY)

The compactor module uses the maven-shade-plugin (fat jar). Two `mvn package` runs on the
same module in parallel corrupt each other's `target/` shade output, so run arm64 then amd64
one after the other — **not** in parallel, and not alongside another build of the same module.

The compactor pom already sets the target image to
`dazzleduck/ducklake-compactor:${project.version}-${jib.architecture}` (plus a `latest-${arch}`
tag), so no `-Djib.to.image` override is needed — only `-Djib.architecture`.

```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
./mvnw package -DskipTests jib:build -pl dazzleduck-sql-ducklake-compactor -Djib.architecture=arm64
./mvnw package -DskipTests jib:build -pl dazzleduck-sql-ducklake-compactor -Djib.architecture=amd64
```

### Step 4 — Create and push multi-arch manifests

```bash
for repo in dazzleduck/dazzleduck dazzleduck/dazzleduck-otel-collector dazzleduck/ducklake-compactor; do
  for tag in <version> latest; do
    docker manifest rm docker.io/$repo:$tag 2>/dev/null || true
    docker manifest create docker.io/$repo:$tag \
      docker.io/$repo:<version>-amd64 \
      docker.io/$repo:<version>-arm64
    docker manifest push docker.io/$repo:$tag
  done
done
```

### Step 5 — Verify

```bash
for repo in dazzleduck/dazzleduck dazzleduck/dazzleduck-otel-collector dazzleduck/ducklake-compactor; do
  echo "=== $repo:<version> ==="
  docker manifest inspect docker.io/$repo:<version> | grep -A2 '"platform"' | grep -E 'architecture|os'
done
```

Confirm both `amd64` and `arm64` platforms appear in each manifest.

### Step 6 — Summary

Print a table:

| Image | Tag | Platforms | Digest |
|-------|-----|-----------|--------|
| `dazzleduck/dazzleduck` | `<version>` | linux/amd64, linux/arm64 | `sha256:...` |
| `dazzleduck/dazzleduck` | `latest` | linux/amd64, linux/arm64 | `sha256:...` |
| `dazzleduck/dazzleduck-otel-collector` | `<version>` | linux/amd64, linux/arm64 | `sha256:...` |
| `dazzleduck/dazzleduck-otel-collector` | `latest` | linux/amd64, linux/arm64 | `sha256:...` |
| `dazzleduck/ducklake-compactor` | `<version>` | linux/amd64, linux/arm64 | `sha256:...` |
| `dazzleduck/ducklake-compactor` | `latest` | linux/amd64, linux/arm64 | `sha256:...` |

## Notes

- Jib pushes directly to the registry — Docker daemon is not required for the build step (the `docker manifest` steps in Step 4/5 do need the daemon).
- The `<version>-arm64` / `<version>-amd64` intermediate tags are pushed by Jib; the canonical multi-arch entry points are `<version>` and `latest`.
- **Docker Hub credentials for Jib:** the base image `dazzleduck/base-jre` is private and the targets push to the `dazzleduck` org, so Jib must be authenticated. Jib reads `~/.docker/config.json`; if it uses `"credsStore": "desktop"`, a plain `docker login docker.io` (Docker Desktop session) may leave that keychain empty and Jib falls back to anonymous → `UNAUTHORIZED` on the base-image pull/target push. Verify with `echo "https://index.docker.io/v1/" | docker-credential-desktop get` — it must return a JSON credential (not "credentials not found"). If empty, run `docker login docker.io -u <dockerhub-user>` and paste a Docker Hub access token, or pass creds explicitly: `JIB_TO_AUTH_USERNAME`/`JIB_TO_AUTH_PASSWORD` and `JIB_FROM_AUTH_USERNAME`/`JIB_FROM_AUTH_PASSWORD`.
- **Do not run two arch builds of the same module in parallel** — modules that shade a fat jar (the compactor) corrupt `target/`. Different modules can build in parallel. runtime/otel do not shade, so their two arches are safe to parallelize.
- Build from the release commit/tag, not a `-SNAPSHOT` branch — `${project.version}` determines the image tag.
- sqlite-jdbc uses the multi-platform fat JAR (13.6 MB); platform-specific JARs are not published to Maven Central.
- `dazzleduck-sql-runtime` bundles Flight SQL + HTTP + OTel Collector (all-in-one).
- `dazzleduck-sql-otel-collector` is a lightweight standalone OTel collector image.
- `dazzleduck-sql-ducklake-compactor` is the DuckLake compaction service; image `dazzleduck/ducklake-compactor`, base `dazzleduck/base-jre:25-noble-duckdb-<duckdb.version>`, main class `io.dazzleduck.sql.compaction.Main`.
