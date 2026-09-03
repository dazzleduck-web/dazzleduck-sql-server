# Release Process

How to cut a release of DazzleDuck SQL Server: version bump, tests, tag, Docker images,
GitHub release, and the follow-up development bump.

Releases go through pull requests against `dazzleduck-web/dazzleduck-sql-server`. Nothing is
pushed directly to `main`.

## Prerequisites

- **JDK 21.** JDK 25 causes test failures. Everything below assumes:

  ```bash
  export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home
  export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
  ```

- **Docker Hub credentials that Jib can read.** The base image `dazzleduck/base-jre` is private
  and the targets push to the `dazzleduck` org, so an unauthenticated build fails when pulling the
  base image. See [Docker Hub authentication](#docker-hub-authentication).
- **Docker daemon running.** Jib pushes without it, but `docker manifest` needs it.
- **`gh` CLI**, authenticated against the upstream repository.

## Steps

### 1. Sync

```bash
git fetch upstream --tags
git checkout main
git merge --ff-only upstream/main
git push origin main
```

Confirm every pull request intended for this release is merged before continuing.

### 2. Open the release pull request

Bump every module pom from `X.Y.Z-SNAPSHOT` to `X.Y.Z`:

```bash
git checkout -b release/X.Y.Z main
./mvnw -q versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false
git commit -am "release: set version X.Y.Z-SNAPSHOT → X.Y.Z"
git push -u origin release/X.Y.Z
gh pr create --repo dazzleduck-web/dazzleduck-sql-server --base main --title "release: set version X.Y.Z-SNAPSHOT → X.Y.Z"
```

The bump should touch all 16 poms and nothing else. Wait for the pull request to be merged, then
sync `main` again as in step 1.

### 3. Run the full test suite

```bash
./mvnw clean install
```

Use `install`, not `test`. The javadoc jar is built during `install` and the javadoc plugin fails
the build on a broken `@link` reference even when compilation and tests pass — a green test run is
not sufficient evidence that the release will build.

### 4. Tag the release commit

```bash
git tag -a vX.Y.Z -m "vX.Y.Z" <merged-release-commit>
git push upstream vX.Y.Z
git push origin vX.Y.Z
```

Tags are annotated, and pushed to both the upstream repository and the fork.

### 5. Build and publish the Docker images

Run from the release commit, never from a `-SNAPSHOT` branch: Jib derives the image tag from
`project.version`, so a snapshot build would publish snapshot-tagged images and move `latest` onto
them.

```bash
./scripts/docker-publish.sh
```

The script installs all modules, builds each image for both architectures, then creates and pushes
the multi-arch manifests for `X.Y.Z` and `latest`. Useful flags:

```bash
./scripts/docker-publish.sh --local                # build to the local daemon only
./scripts/docker-publish.sh --arch arm64           # one architecture, skip manifests
./scripts/docker-publish.sh --module compactor     # one module only
./scripts/docker-publish.sh --skip-build           # manifests only, no Jib
VERSION=X.Y.Z ./scripts/docker-publish.sh          # override the detected version
```

Images it publishes:

| Module alias | Maven module | Image | Architectures |
|---|---|---|---|
| `runtime` | `dazzleduck-sql-runtime` | `dazzleduck/dazzleduck` | amd64, arm64 |
| `otel-collector` | `dazzleduck-sql-otel-collector` | `dazzleduck/dazzleduck-otel-collector` | amd64, arm64 |
| `compactor` | `dazzleduck-sql-ducklake-compactor` | `dazzleduck/ducklake-compactor` | amd64, arm64 |
| `scrapper` | `dazzleduck-sql-scrapper` | `dazzleduck/dazzleduck-sql-scrapper` | single arch, no manifest |

Then verify each published manifest reports both platforms:

```bash
docker manifest inspect docker.io/dazzleduck/dazzleduck:X.Y.Z
```

### 6. Create the GitHub release

```bash
gh release create vX.Y.Z --repo dazzleduck-web/dazzleduck-sql-server \
  --title "vX.Y.Z" --notes-file notes.md --latest
```

Include in the notes:

- any breaking changes first, with the migration a user has to perform
- notable features and fixes, linked by pull request number
- the image table with the published digests
- the test count the release was verified with
- a full changelog link comparing the previous tag to this one

### 7. Open the next development version pull request

```bash
git checkout -b chore/X.Y.(Z+1)-SNAPSHOT main
./mvnw -q versions:set -DnewVersion=X.Y.(Z+1)-SNAPSHOT -DgenerateBackupPoms=false
git commit -am "Prepare next development version X.Y.(Z+1)-SNAPSHOT"
```

Push and open the pull request as in step 2.

## Things that bite

**Docker Hub authentication.** Jib reads `~/.docker/config.json`. If that file sets
`"credsStore": "desktop"`, a Docker Desktop login can leave the keychain empty and Jib falls back
to anonymous, failing on the private base image. Check with:

```bash
echo "https://index.docker.io/v1/" | docker-credential-desktop get
```

It must return a JSON credential. If it does not, run `docker login docker.io -u USERNAME` with an
access token, or pass credentials explicitly through `JIB_TO_AUTH_USERNAME` / `JIB_TO_AUTH_PASSWORD`
and `JIB_FROM_AUTH_USERNAME` / `JIB_FROM_AUTH_PASSWORD`.

When publishing manually rather than through the script, build one image first to confirm
authentication before starting parallel builds — otherwise every build fails the same way at once.

**The compactor must build one architecture at a time.** It uses the maven-shade-plugin, and two
`package` runs against the same module in parallel corrupt each other's `target/` output. Different
modules may build in parallel; two architectures of the compactor may not.

**`latest-amd64` and `latest-arm64` move on every build.** All three multi-arch poms declare
`latest-${jib.architecture}` as an extra tag, so any Jib build shifts those tags even when only a
version was intended. The multi-arch `latest` manifest is separate and only moves when the manifest
step runs.

**`mvn install` runs the javadoc plugin.** Compilation and tests passing does not mean the release
builds. Always run `install` before tagging.

**Snapshot builds are not releases.** Confirm `project.version` has no `-SNAPSHOT` suffix before
publishing images.

## Open items

- The base image is referenced by tag (`dazzleduck/base-jre:25-noble-duckdb-${duckdb.version}`)
  rather than by digest, so a release is not reproducible if that tag is republished. Pinning it
  would mean adding a digest property to the parent pom and referencing it from the four poms that
  name the base image.
- `dazzleduck/dazzleduck-sql-scrapper` has a `latest` tag but no version tags. It is registered as
  a single-architecture module and so never gets a manifest; whether it should be part of a release
  at all is unresolved.
