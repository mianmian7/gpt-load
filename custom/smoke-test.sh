#!/usr/bin/env bash
# Start a throwaway container from IMAGE (using the image's own empty
# /app/data, no volume) on a spare loopback port, check /health and that the
# custom routes are registered, then remove the container. Never touches a
# running deployment.
#
# Usage: custom/smoke-test.sh IMAGE [HOST_PORT]
set -euo pipefail

image="${1:?usage: $0 IMAGE [HOST_PORT]}"
port="${2:-13001}"
name="gpt-load-smoke-$$"

cleanup() {
  docker rm -f "${name}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --name "${name}" -p "127.0.0.1:${port}:3001" \
  -e HOST=0.0.0.0 -e PORT=3001 -e DATA_DIR=/app/data \
  "${image}" >/dev/null

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then break; fi
  sleep 1
done
if ! health="$(curl -fsS "http://127.0.0.1:${port}/health")"; then
  echo "smoke test FAILED: /health not reachable" >&2
  docker logs --tail 50 "${name}" >&2 || true
  exit 1
fi
echo "health: ${health}"

fail=0
# Control: an unknown path must 404, otherwise the route checks below prove nothing.
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST -d '{}' "http://127.0.0.1:${port}/v1/smoke-no-such-route")"
echo "POST /v1/smoke-no-such-route -> ${code} (control, expect 404)"
[[ "${code}" != 404 ]] && fail=1
# Unauthenticated calls must be rejected by auth (401/403), not 404 "no such route".
for path in /v1/videos /v1/videos/generations /v1/images/generations; do
  code="$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d '{}' "http://127.0.0.1:${port}${path}")"
  echo "POST ${path} -> ${code}"
  [[ "${code}" == 404 ]] && fail=1
done
code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${port}/v1/videos/smoke-request-id")"
echo "GET /v1/videos/:request_id -> ${code}"
[[ "${code}" == 404 ]] && fail=1

if [[ "${fail}" != 0 ]]; then
  echo "smoke test FAILED: a custom route returned 404" >&2
  docker logs --tail 50 "${name}" >&2 || true
  exit 1
fi
echo "smoke test passed"
