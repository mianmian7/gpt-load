#!/usr/bin/env bash
# Build a self-contained gpt-load image from the committed HEAD of this checkout.
#
# Uses the upstream Dockerfile's default `source-build` target (web UI + Go
# binary are compiled inside Docker), so the host needs only Docker/buildx.
#
# Environment overrides:
#   FLAVOR       suffix naming this customization   (default: grok-media)
#   VERSION      version string baked into the binary (default: <base-tag>-<FLAVOR>)
#   IMAGE        image reference to tag              (default: gpt-load:v<base>-<FLAVOR>-<sha>)
#   ALLOW_DIRTY  set to 1 to build with uncommitted tracked changes
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if [[ -n "$(git status --porcelain --untracked-files=no)" && "${ALLOW_DIRTY:-0}" != 1 ]]; then
  echo "error: tracked files have uncommitted changes; commit them or set ALLOW_DIRTY=1" >&2
  exit 1
fi

base_tag="$(git describe --tags --abbrev=0 --match 'v*')"   # e.g. v2.0.0-rc.61
flavor="${FLAVOR:-grok-media}"
revision="$(git rev-parse HEAD)"
short_sha="$(git rev-parse --short=8 HEAD)"
version="${VERSION:-${base_tag#v}-${flavor}}"
image="${IMAGE:-gpt-load:${base_tag}-${flavor}-${short_sha}}"

echo "base tag : ${base_tag}"
echo "revision : ${revision}"
echo "version  : ${version}"
echo "image    : ${image}"

docker build \
  --target source-build \
  --build-arg VERSION="${version}" \
  --label org.opencontainers.image.version="${version}" \
  --label org.opencontainers.image.revision="${revision}" \
  --label org.opencontainers.image.base.name="tbphp/gpt-load:${base_tag}" \
  -t "${image}" \
  .

echo "built ${image}"
