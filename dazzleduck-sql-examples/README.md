# DazzleDuck SQL Examples — docker-compose integration tests

This module runs the example demos under `example/docker/*` as end-to-end
integration tests using Testcontainers' `ComposeContainer`:

| Test | Compose stack |
|------|---------------|
| `LogbackDemoTest` | `example/docker/logback-demo/docker-compose.yml` |
| `MicrometerDemoTest` | `example/docker/micrometer-demo/docker-compose.yml` |
| `NamedQueryDemoTest` | `example/docker/named-query-demo/docker-compose.yml` |

The tests are tagged `@Tag("docker-compose")` and **excluded by default**
(`excluded.test.groups=docker-compose`); the `docker-compose` profile clears
that exclusion.

## Prerequisites

- Docker must be running.
- The compose files reference the image `dazzleduck/dazzleduck:latest`
  (unsuffixed). You must build it for your architecture and tag it `latest`
  first — the jib build only produces arch-suffixed tags (`latest-arm64` /
  `latest-amd64`):

  ```bash
  export JAVA_HOME=$(/usr/libexec/java_home -v 25)   # build on JDK 25
  # Install the reactor so the runtime image picks up local changes.
  # (jib:dockerBuild is a direct goal and will NOT rebuild upstream modules,
  #  so -am is not enough — install first.)
  ./mvnw install -DskipTests
  # Build the runtime image for your arch (use amd64 on Intel/CI):
  # (generate-resources and the flag download the patched DuckLake extension the image bakes in.)
  ./mvnw -pl dazzleduck-sql-runtime generate-resources jib:dockerBuild -Djib.architecture=arm64 -Dducklake.extension.download.skip=false
  # Tag the unsuffixed name the compose files expect:
  docker tag dazzleduck/dazzleduck:latest-arm64 dazzleduck/dazzleduck:latest
  ```

## Running the suite

This module has `packaging=pom`, so the default Maven lifecycle binds neither
`testCompile` nor `surefire:test` — `./mvnw test -pl dazzleduck-sql-examples`
(even with `-Pdocker-compose`) compiles and runs **nothing**. Invoke the goals
directly:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 25)
./mvnw -pl dazzleduck-sql-examples -Pdocker-compose \
  resources:testResources compiler:testCompile surefire:test
```

Expected: 7 tests (`LogbackDemoTest` 2, `MicrometerDemoTest` 2,
`NamedQueryDemoTest` 3), all passing.
