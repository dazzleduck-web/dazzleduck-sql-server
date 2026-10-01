#!/usr/bin/env bash
# Build and publish all Docker images with multi-arch manifests.
#
# Usage:
#   ./scripts/docker-publish.sh                        # build + push all arches + manifests
#   ./scripts/docker-publish.sh --local                # build to local Docker daemon only (amd64)
#   ./scripts/docker-publish.sh --arch arm64           # push one arch only (skip manifests)
#   ./scripts/docker-publish.sh --module compactor     # build + push one module only
#   ./scripts/docker-publish.sh --skip-build           # skip jib, only create/push manifests
#
# Module aliases: runtime, compactor, otel-collector, scrapper
#
# Native images (GraalVM) are also built for the modules that have a Dockerfile.native
# (otel-collector, compactor). Unlike jib, native-image does NOT cross-compile, so the host
# architecture is always built natively as <image>:<version>-<hostarch>. The other architecture is
# then cross-built via `docker buildx build --platform` under QEMU emulation and pushed directly
# (buildx --push; it can't load a foreign-arch image into the local daemon). Emulation is slow
# (compactor ~6 min, otel-collector ~10-15 min on Apple Silicon under load) and memory-hungry, but
# workable. Skip native entirely with --no-native, or skip just the emulated cross-build (host arch
# only, as before) with --no-emulate.
#
# Native image manifests are built with `docker buildx imagetools create`, not `docker manifest` —
# buildx --push attaches a provenance attestation, turning each arch tag into an OCI image index
# rather than a plain manifest, which `docker manifest create` does not flatten correctly.
#
# Override version:
#   VERSION=0.2.9 ./scripts/docker-publish.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."

# ── Options ───────────────────────────────────────────────────────────────────
LOCAL=false
SKIP_BUILD=false
SINGLE_ARCH=""
MODULE_FILTER=""
NO_NATIVE=false
NO_EMULATE=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --local)       LOCAL=true; shift ;;
    --skip-build)  SKIP_BUILD=true; shift ;;
    --arch)        SINGLE_ARCH="$2"; shift 2 ;;
    --module)      MODULE_FILTER="$2"; shift 2 ;;
    --no-native)   NO_NATIVE=true; shift ;;
    --no-emulate)  NO_EMULATE=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# native-image builds for the host arch only; map uname -m to the Docker arch string.
case "$(uname -m)" in
  aarch64|arm64) HOST_ARCH=arm64 ;;
  x86_64|amd64)  HOST_ARCH=amd64 ;;
  *) HOST_ARCH="$(uname -m)" ;;
esac

# ── Version ───────────────────────────────────────────────────────────────────
VERSION="${VERSION:-$(cd "$ROOT" && ./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null)}"
echo "Version: $VERSION"

# ── Java env ──────────────────────────────────────────────────────────────────
export JAVA_HOME="${JAVA_HOME:-/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home}"
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

MVN="$ROOT/mvnw"

# ── Module registry ───────────────────────────────────────────────────────────
# Format: "alias|maven-module|docker-image|arch-support"  (arch-support: multi|single)
MODULES=(
  "runtime|dazzleduck-sql-runtime|dazzleduck/dazzleduck|multi"
  "compactor|dazzleduck-sql-ducklake-compactor|dazzleduck/ducklake-compactor|multi"
  "otel-collector|dazzleduck-sql-otel-collector|dazzleduck/dazzleduck-otel-collector|multi"
  "scrapper|dazzleduck-sql-scrapper|dazzleduck/dazzleduck-sql-scrapper|multi"
)

# Filter to a single module if requested
if [[ -n "$MODULE_FILTER" ]]; then
  filtered=()
  for entry in "${MODULES[@]}"; do
    alias="${entry%%|*}"
    if [[ "$alias" == "$MODULE_FILTER" ]]; then
      filtered+=("$entry")
    fi
  done
  if [[ ${#filtered[@]} -eq 0 ]]; then
    echo "Unknown module: $MODULE_FILTER. Valid: runtime, compactor, otel-collector, scrapper"
    exit 1
  fi
  MODULES=("${filtered[@]}")
fi

# ── Native image registry ─────────────────────────────────────────────────────
# Format: "alias|dockerfile|docker-image". Built via Dockerfile.native for the host arch only.
NATIVE_IMAGES=(
  "otel-collector|dazzleduck-sql-otel-collector/Dockerfile.native|dazzleduck/dazzleduck-otel-collector-native"
  "compactor|dazzleduck-sql-ducklake-compactor/Dockerfile.native|dazzleduck/ducklake-compactor-native"
)
if [[ -n "$MODULE_FILTER" ]]; then
  nfiltered=()
  for entry in "${NATIVE_IMAGES[@]}"; do
    [[ "${entry%%|*}" == "$MODULE_FILTER" ]] && nfiltered+=("$entry")
  done
  NATIVE_IMAGES=("${nfiltered[@]:-}")
fi

# ── Install all JARs to local Maven repo ──────────────────────────────────────
if [[ "$SKIP_BUILD" == false ]]; then
  echo ""
  echo "▶ Installing all modules..."
  "$MVN" install -DskipTests -f "$ROOT/pom.xml"
fi

# ── Build helpers ─────────────────────────────────────────────────────────────
jib_goal() {
  local goal="$1"
  [[ "$LOCAL" == true ]] && goal="${goal/jib:build/jib:dockerBuild}" || true
  echo "$goal"
}

build_multi_arch() {
  local maven_module="$1"
  local arch="$2"
  local goal
  # Only the compactor bakes in the patched DuckLake extension (see DUCKLAKE_PATCH.md); the
  # download step is skipped by default so plain test/install/verify don't need network access
  # to a GitHub release. Arrays are expanded as ${a[@]+"${a[@]}"} because macOS's /bin/bash
  # (3.2) treats an empty "${a[@]}" as an unbound variable under set -u.
  # The download is bound to generate-resources, and a jib goal invoked on its own runs no
  # lifecycle phase, so that phase must run first (as release.yml's `package jib:build` does);
  # otherwise jib fails on the missing target/ducklake-extension/linux_<arch> directory.
  # (Only generate-resources, not package: re-packaging the shaded jar isn't needed here.)
  local phases=()
  local extra_args=()
  if [[ "$maven_module" == "dazzleduck-sql-ducklake-compactor" ]]; then
    phases+=("generate-resources")
    extra_args+=("-Dducklake.extension.download.skip=false")
  fi

  # runtime uses named executions with arch hardcoded; others use -Djib.architecture
  if [[ "$maven_module" == "dazzleduck-sql-runtime" ]]; then
    goal=$(jib_goal "jib:build@docker-${arch}")
    echo ""
    echo "▶ $maven_module ($arch)"
    "$MVN" ${phases[@]+"${phases[@]}"} "$goal" -pl "$maven_module" -DskipTests -f "$ROOT/pom.xml" ${extra_args[@]+"${extra_args[@]}"}
  else
    goal=$(jib_goal "jib:build")
    echo ""
    echo "▶ $maven_module ($arch)"
    "$MVN" ${phases[@]+"${phases[@]}"} "$goal" -pl "$maven_module" -Djib.architecture="$arch" -DskipTests -f "$ROOT/pom.xml" ${extra_args[@]+"${extra_args[@]}"}
  fi
}

build_single_arch() {
  local maven_module="$1"
  local goal
  goal=$(jib_goal "jib:build")
  echo ""
  echo "▶ $maven_module"
  "$MVN" ${phases[@]+"${phases[@]}"} "$goal" -pl "$maven_module" -DskipTests -f "$ROOT/pom.xml"
}

push_manifests() {
  local image="$1"
  echo ""
  echo "▶ manifest: $image"
  for tag in "$VERSION" "latest"; do
    # Remove stale local manifest cache so --amend picks up freshly pushed arch digests
    docker manifest rm "${image}:${tag}" 2>/dev/null || true
    docker manifest create "${image}:${tag}" \
      "${image}:${tag}-amd64" \
      "${image}:${tag}-arm64"
    docker manifest push "${image}:${tag}"
    echo "  pushed ${image}:${tag}"
  done
}

build_native() {
  local dockerfile="$1" image="$2"
  local other_arch="amd64"
  [[ "$HOST_ARCH" == "amd64" ]] && other_arch="arm64"

  echo ""
  echo "▶ native $image ($HOST_ARCH, native build)"
  DOCKER_BUILDKIT=1 docker build -f "$ROOT/$dockerfile" -t "${image}:${VERSION}-${HOST_ARCH}" "$ROOT"
  if [[ "$LOCAL" == true ]]; then
    return
  fi
  docker push "${image}:${VERSION}-${HOST_ARCH}"

  local have_other_arch=false
  if [[ "$NO_EMULATE" == false ]]; then
    echo ""
    echo "▶ native $image ($other_arch, emulated via buildx/QEMU — slow, can take 15-20+ minutes)"
    if docker buildx build --platform "linux/${other_arch}" --provenance=false \
        -f "$ROOT/$dockerfile" -t "${image}:${VERSION}-${other_arch}" --push "$ROOT"; then
      have_other_arch=true
    else
      echo "  ⚠ emulated $other_arch build failed; publishing $HOST_ARCH only for $image"
    fi
  fi

  # latest-<arch> tags, by digest — buildx --push images aren't loaded locally so `docker tag`
  # doesn't work; imagetools re-tags by referencing the registry digest directly.
  docker buildx imagetools create -t "${image}:latest-${HOST_ARCH}" "${image}:${VERSION}-${HOST_ARCH}"
  if [[ "$have_other_arch" == true ]]; then
    docker buildx imagetools create -t "${image}:latest-${other_arch}" "${image}:${VERSION}-${other_arch}"
    docker buildx imagetools create -t "${image}:${VERSION}" "${image}:${VERSION}-amd64" "${image}:${VERSION}-arm64"
    docker buildx imagetools create -t "${image}:latest" "${image}:latest-amd64" "${image}:latest-arm64"
    echo "  pushed ${image}:${VERSION} and :latest (multi-arch: amd64+arm64)"
  else
    docker buildx imagetools create -t "${image}:${VERSION}" "${image}:${VERSION}-${HOST_ARCH}"
    docker buildx imagetools create -t "${image}:latest" "${image}:latest-${HOST_ARCH}"
    echo "  pushed ${image}:${VERSION} and :latest ($HOST_ARCH only)"
  fi
}

# ── Build and publish ─────────────────────────────────────────────────────────
ARCHES=("amd64" "arm64")
[[ -n "$SINGLE_ARCH" ]] && ARCHES=("$SINGLE_ARCH")

for entry in "${MODULES[@]}"; do
  IFS='|' read -r alias maven_module image arch_support <<< "$entry"

  if [[ "$SKIP_BUILD" == false ]]; then
    if [[ "$arch_support" == "multi" ]]; then
      for arch in "${ARCHES[@]}"; do
        build_multi_arch "$maven_module" "$arch"
      done
    else
      build_single_arch "$maven_module"
    fi
  fi

  if [[ "$LOCAL" == false && "$arch_support" == "multi" && -z "$SINGLE_ARCH" ]]; then
    push_manifests "$image"
  fi
done

# ── Native images (host arch native, other arch emulated) ─────────────────────
if [[ "$NO_NATIVE" == false && ${#NATIVE_IMAGES[@]} -gt 0 && -n "${NATIVE_IMAGES[0]}" ]]; then
  for entry in "${NATIVE_IMAGES[@]}"; do
    IFS='|' read -r alias dockerfile image <<< "$entry"
    build_native "$dockerfile" "$image"
  done
  if [[ "$NO_EMULATE" == true ]]; then
    echo ""
    echo "⚠ --no-emulate: native images pushed for $HOST_ARCH only. Re-run without --no-emulate,"
    echo "  or build the other arch on a host of that arch, to get a true multi-arch manifest."
  fi
fi

echo ""
echo "✓ Done."
