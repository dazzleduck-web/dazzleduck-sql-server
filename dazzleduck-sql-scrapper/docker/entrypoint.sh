#!/bin/bash
set -e

# JVM options for Arrow memory management
JVM_OPTS=(
  "-Xms256m"
  "-XX:+UseContainerSupport"
  "-XX:+UseG1GC"
  "-XX:+ExitOnOutOfMemoryError"
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

# Jib-layered classpath layout
CLASSPATH="/app/resources:/app/classes:/app/libs/*"

# --------------------------------------------------------------------------
# Convert dotted environment variables to Java system properties.
#
# Docker Compose allows env var names with dots, e.g.:
#   collector.enabled=true
#   collector.target-prefix=http://envoy:9901
#
# Bash cannot reference these via $VAR syntax, so we read /proc/self/environ
# (null-delimited entries) and pass matching vars as -D flags.
# --------------------------------------------------------------------------
PROP_ARGS=()
while IFS= read -r -d '' entry; do
  key="${entry%%=*}"
  value="${entry#*=}"
  if [[ "$key" == collector.* ]] || [[ "$key" == logging.* ]]; then
    PROP_ARGS+=("-D${key}=${value}")
  fi
done < /proc/self/environ

# --------------------------------------------------------------------------
# Handle COLLECTOR_TARGETS — the Docker-friendly way to configure the targets
# list, since HOCON does not support list-index notation in system properties.
#
# Set as a comma-separated list:
#   COLLECTOR_TARGETS=/stats/prometheus?usedonly
#   COLLECTOR_TARGETS=http://svc-a:8080/metrics,http://svc-b:8080/metrics
#
# This generates a small HOCON override file passed via --config.
# --------------------------------------------------------------------------
EXTRA_ARGS=()
if [ -n "${COLLECTOR_TARGETS}" ]; then
  OVERRIDE_FILE="/tmp/collector-override.conf"
  IFS=',' read -ra raw_targets <<< "${COLLECTOR_TARGETS}"
  targets_hocon="["
  for t in "${raw_targets[@]}"; do
    # trim leading/trailing whitespace
    t="${t#"${t%%[![:space:]]*}"}"
    t="${t%"${t##*[![:space:]]}"}"
    targets_hocon="${targets_hocon}\"${t}\","
  done
  targets_hocon="${targets_hocon%,}]"
  printf 'collector.targets = %s\n' "${targets_hocon}" > "${OVERRIDE_FILE}"
  EXTRA_ARGS+=(--config "${OVERRIDE_FILE}")
fi

echo "Starting DazzleDuck Metrics Scrapper..."
exec java "${JVM_OPTS[@]}" "${PROP_ARGS[@]}" \
  -cp "${CLASSPATH}" \
  io.dazzleduck.sql.scrapper.Main \
  "${EXTRA_ARGS[@]}" "$@"
